package peering

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"faf-pioneer/applog"
	"faf-pioneer/icebreaker"
	"faf-pioneer/moho"

	"github.com/pion/ice/v4"
	"github.com/pion/stun/v3"
	"go.uber.org/zap"
)

const (
	// connectTimeout is the time we give a single ICE attempt to find a working candidate pair.
	connectTimeout = 30 * time.Second
	// gameReadBufferSize must fit any datagram the game sends.
	gameReadBufferSize = 65535
)

type PeerMeta interface {
	IsOfferer() bool
	PeerId() uint
	GetUdpPort() uint
}

// Peer is the plain ICE connection to one remote player. There is no WebRTC peer connection: each
// attempt is a single ice.Agent and the game datagrams are sent as-is over the selected candidate pair.
type Peer struct {
	offerer bool
	peerId  uint
	ctx     context.Context
	udpPort uint

	gameToIceChannel chan []byte
	iceToGameChannel chan []byte
	gameDataProxy    *moho.GameUDPProxy

	onCandidatesGathered func(session *icebreaker.IceSession, candidates []string, stillCurrent func() bool)
	onStateChanged       func(peer *Peer, state ice.ConnectionState)
	onRemoteRestarted    func(peer *Peer)

	// mu guards the per-attempt state below. Never call out to the PeerManager while holding it.
	mu              sync.Mutex
	agent           *ice.Agent
	agentGen        uint64
	state           ice.ConnectionState
	dialStarted     bool
	usedRemoteUfrag string
	pendingRemote   *icebreaker.CandidatesMessage
	closed          bool
	connCancel      context.CancelFunc

	forceTurnRelay        bool
	reconnectionScheduled bool
	reconnectMu           sync.Mutex
	disabled              bool
}

func (p *Peer) IsOfferer() bool  { return p.offerer }
func (p *Peer) PeerId() uint     { return p.peerId }
func (p *Peer) GetUdpPort() uint { return p.udpPort }

func (p *Peer) Disable() {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()
	p.disabled = true
	applog.FromContext(p.ctx).Info(
		"Peer disabled – no more reconnection attempts",
		zap.Uint("peerId", p.peerId),
	)
}

func (p *Peer) IsDisabled() bool {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()
	return p.disabled
}

func CreatePeer(
	parentContext context.Context,
	offerer bool,
	peerId uint,
	peerManager *PeerManager,
	gameToIcePort uint,
	iceToGamePort uint,
) (*Peer, error) {
	ctx := applog.AddContextFields(parentContext,
		zap.Uint("remotePlayerId", peerId),
		zap.Bool("localOfferer", offerer),
		zap.Uint("gameToIcePort", gameToIcePort),
		zap.Uint("iceToGamePort", iceToGamePort),
	)

	applog.FromContext(ctx).Debug("Creating a peer")

	gameToIceChannel := make(chan []byte)
	iceToGameChannel := make(chan []byte)

	// `iceToGamePort` is the udp port the game listens on for all peers.
	// `gameToIcePort` is from where we're proxying all the data to local game port.
	gameUdpProxy, err := moho.NewGameUDPProxy(
		ctx,
		iceToGamePort,
		gameToIcePort,
		gameToIceChannel,
		iceToGameChannel,
	)
	if err != nil {
		return nil, err
	}

	return &Peer{
		ctx:                  ctx,
		offerer:              offerer,
		peerId:               peerId,
		udpPort:              gameToIcePort,
		gameToIceChannel:     gameToIceChannel,
		iceToGameChannel:     iceToGameChannel,
		gameDataProxy:        gameUdpProxy,
		onCandidatesGathered: peerManager.onPeerCandidatesGathered(peerId),
		onStateChanged:       peerManager.onPeerStateChanged,
		onRemoteRestarted:    peerManager.scheduleReconnectionForPeer,
		forceTurnRelay:       peerManager.forceTurnRelay,
	}, nil
}

// ConnectOnce starts a fresh ICE attempt: a new agent gathers candidates, which are published through
// onCandidatesGathered. As soon as the remote candidates are known the agent connects.
func (p *Peer) ConnectOnce(iceServers []*stun.URI) error {
	if p.IsDisabled() {
		return errors.New("peer is disabled")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Close can have finished between the check above and taking the lock.
	if p.closed {
		return errors.New("peer is closed")
	}

	p.closeAgentLocked()

	agent, err := p.newAgent(iceServers)
	if err != nil && p.forceTurnRelay {
		applog.FromContext(p.ctx).Warn(
			"Failed to create ICE agent with relay-only candidates, falling back",
			zap.Error(err),
		)
		p.forceTurnRelay = false
		agent, err = p.newAgent(iceServers)
	}
	if err != nil {
		return fmt.Errorf("cannot create ICE agent: %w", err)
	}

	p.agentGen++
	gen := p.agentGen
	p.agent = agent
	p.state = ice.ConnectionStateNew
	p.dialStarted = false
	p.usedRemoteUfrag = ""

	if err := agent.OnConnectionStateChange(func(state ice.ConnectionState) {
		p.handleStateChange(gen, state)
	}); err != nil {
		p.closeAgentLocked()
		return fmt.Errorf("cannot register state handler: %w", err)
	}

	var gathered []string
	if err := agent.OnCandidate(func(c ice.Candidate) {
		if c != nil {
			gathered = append(gathered, c.Marshal())
			return
		}

		// Gathering finished.
		ufrag, pwd, err := agent.GetLocalUserCredentials()
		if err != nil {
			applog.FromContext(p.ctx).Error("Cannot read local ICE credentials", zap.Error(err))
			return
		}
		if !p.isCurrentAgent(gen) {
			return // replaced or closed while gathering, do not publish outdated credentials
		}
		p.onCandidatesGathered(&icebreaker.IceSession{Ufrag: ufrag, Pwd: pwd}, gathered,
			func() bool { return p.isCurrentAgent(gen) })
	}); err != nil {
		p.closeAgentLocked()
		return fmt.Errorf("cannot register candidate handler: %w", err)
	}

	applog.FromContext(p.ctx).Info("Gathering ICE candidates", zap.Bool("relayOnly", p.forceTurnRelay))
	if err := agent.GatherCandidates(); err != nil {
		p.closeAgentLocked()
		return fmt.Errorf("cannot gather candidates: %w", err)
	}

	// If the candidates never reach us (signalling down on either side) no connect attempt would start and
	// nothing would ever replace this agent. End the attempt then, so that a reconnection gets scheduled.
	time.AfterFunc(connectTimeout, func() {
		p.mu.Lock()
		stalled := p.agentGen == gen && p.agent != nil && !p.dialStarted
		p.mu.Unlock()
		if stalled {
			applog.FromContext(p.ctx).Warn("No remote candidates received in time, restarting ICE attempt")
			p.handleStateChange(gen, ice.ConnectionStateClosed)
		}
	})

	// The remote already sent us its candidates while we had no agent.
	if pending := p.pendingRemote; pending != nil {
		p.pendingRemote = nil
		return p.startConnectLocked(pending)
	}

	return nil
}

func (p *Peer) newAgent(iceServers []*stun.URI) (*ice.Agent, error) {
	candidateTypes := []ice.CandidateType{
		ice.CandidateTypeHost,
		ice.CandidateTypeServerReflexive,
		ice.CandidateTypeRelay,
	}
	if p.forceTurnRelay {
		candidateTypes = []ice.CandidateType{ice.CandidateTypeRelay}
	}

	disconnectedTimeout := peerDisconnectedTimeout
	failedTimeout := peerFailedTimeout
	keepAliveInterval := peerKeepAliveInterval

	return ice.NewAgent(&ice.AgentConfig{
		Urls:                iceServers,
		NetworkTypes:        []ice.NetworkType{ice.NetworkTypeUDP4, ice.NetworkTypeUDP6},
		CandidateTypes:      candidateTypes,
		DisconnectedTimeout: &disconnectedTimeout,
		FailedTimeout:       &failedTimeout,
		KeepaliveInterval:   &keepAliveInterval,
	})
}

// AddCandidates handles the offer/answer (credentials + candidates) of the remote peer.
func (p *Peer) AddCandidates(msg *icebreaker.CandidatesMessage) error {
	if msg.Session == nil || msg.Session.Ufrag == "" || msg.Session.Pwd == "" {
		return errors.New("candidates message without ICE credentials")
	}

	p.mu.Lock()
	if p.agent == nil {
		// Not ready yet, ConnectOnce will pick it up.
		p.pendingRemote = msg
		p.mu.Unlock()
		return nil
	}

	if p.dialStarted {
		if p.usedRemoteUfrag == msg.Session.Ufrag {
			p.mu.Unlock()
			return nil // duplicate delivery
		}

		// The remote started a new attempt (new credentials) while ours is still running:
		// drop our agent and reconnect with the new offer.
		applog.FromContext(p.ctx).Info("Remote restarted ICE, restarting our attempt")
		p.pendingRemote = msg
		p.closeAgentLocked()
		p.mu.Unlock()
		p.onRemoteRestarted(p)
		return nil
	}

	defer p.mu.Unlock()
	return p.startConnectLocked(msg)
}

func (p *Peer) startConnectLocked(msg *icebreaker.CandidatesMessage) error {
	agent := p.agent
	gen := p.agentGen

	for _, raw := range msg.Candidates {
		candidate, err := ice.UnmarshalCandidate(raw)
		if err != nil {
			applog.FromContext(p.ctx).Warn("Ignoring invalid remote candidate",
				zap.String("candidate", raw), zap.Error(err))
			continue
		}
		if err := agent.AddRemoteCandidate(candidate); err != nil {
			applog.FromContext(p.ctx).Warn("Cannot add remote candidate",
				zap.String("candidate", raw), zap.Error(err))
		}
	}

	p.dialStarted = true
	p.usedRemoteUfrag = msg.Session.Ufrag

	go p.connect(agent, gen, msg.Session.Ufrag, msg.Session.Pwd)
	return nil
}

// connect blocks until the agent has a working candidate pair, then starts forwarding game data.
func (p *Peer) connect(agent *ice.Agent, gen uint64, remoteUfrag, remotePwd string) {
	ctx, cancel := context.WithTimeout(p.ctx, connectTimeout)
	defer cancel()

	var (
		conn *ice.Conn
		err  error
	)
	// The offerer is the controlling agent.
	if p.offerer {
		conn, err = agent.Dial(ctx, remoteUfrag, remotePwd)
	} else {
		conn, err = agent.Accept(ctx, remoteUfrag, remotePwd)
	}

	p.mu.Lock()
	if p.agentGen != gen || p.agent != agent {
		p.mu.Unlock() // superseded or closed while connecting
		return
	}
	if err != nil {
		p.mu.Unlock()
		applog.FromContext(p.ctx).Warn("ICE connect failed", zap.Error(err))
		p.handleStateChange(gen, ice.ConnectionStateFailed)
		return
	}

	connCtx, connCancel := context.WithCancel(p.ctx)
	p.connCancel = connCancel
	p.mu.Unlock()

	applog.FromContext(p.ctx).Info("ICE connection established, forwarding game data")

	go p.forwardIceToGame(connCtx, conn)
	go p.forwardGameToIce(connCtx, conn)
}

func (p *Peer) forwardIceToGame(ctx context.Context, conn *ice.Conn) {
	buffer := make([]byte, gameReadBufferSize)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if ctx.Err() == nil {
				applog.FromContext(p.ctx).Debug("ICE connection read ended", zap.Error(err))
			}
			return
		}

		b := append([]byte(nil), buffer[:n]...)
		select {
		case p.iceToGameChannel <- b:
		case <-ctx.Done():
			return
		default:
			applog.FromContext(p.ctx).Warn("Dropping received game packet, data to game channel busy")
		}
	}
}

func (p *Peer) forwardGameToIce(ctx context.Context, conn *ice.Conn) {
	for {
		select {
		case msg, ok := <-p.gameToIceChannel:
			if !ok {
				return
			}
			if _, err := conn.Write(msg); err != nil {
				applog.FromContext(p.ctx).Debug("Could not send data to ICE connection", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *Peer) isCurrentAgent(gen uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.agentGen == gen && p.agent != nil && !p.closed
}

func (p *Peer) handleStateChange(gen uint64, state ice.ConnectionState) {
	p.mu.Lock()
	if p.agentGen != gen || p.agent == nil {
		p.mu.Unlock() // event of an agent that has been replaced already
		return
	}
	p.state = state
	p.mu.Unlock()

	if p.onStateChanged != nil {
		p.onStateChanged(p, state)
	}
}

// SelectedPair returns the local and remote candidate of the connected pair.
func (p *Peer) SelectedPair() (local, remote ice.Candidate, ok bool) {
	p.mu.Lock()
	agent := p.agent
	p.mu.Unlock()
	if agent == nil {
		return nil, nil, false
	}

	pair, err := agent.GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return nil, nil, false
	}
	return pair.Local, pair.Remote, true
}

func (p *Peer) IsActive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.agent != nil && (p.state == ice.ConnectionStateConnected || p.state == ice.ConnectionStateCompleted)
}

// IsConnecting is true while an attempt is running that has not failed or been closed yet.
func (p *Peer) IsConnecting() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.agent != nil && (p.state == ice.ConnectionStateNew || p.state == ice.ConnectionStateChecking)
}

func (p *Peer) closeAgentLocked() {
	if p.connCancel != nil {
		p.connCancel()
		p.connCancel = nil
	}
	if p.agent != nil {
		agent := p.agent
		p.agent = nil
		p.agentGen++ // silences the handlers of the old agent
		p.state = ice.ConnectionStateClosed
		// Closing can block on network teardown, do not hold up the caller.
		go func() {
			if err := agent.Close(); err != nil {
				applog.FromContext(p.ctx).Debug("Error closing ICE agent", zap.Error(err))
			}
		}()
	}
}

func (p *Peer) Close() error {
	p.Disable()

	p.gameDataProxy.Close()

	p.mu.Lock()
	p.closed = true
	p.closeAgentLocked()
	p.mu.Unlock()
	return nil
}
