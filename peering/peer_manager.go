package peering

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"github.com/pion/ice/v4"
	"github.com/pion/stun/v3"
	"go.uber.org/zap"
	"net"
	"strconv"
	"sync"
	"time"
)

// stillCurrent reports whether the ICE attempt that produced the session is still the active one.
type onPeerCandidatesGatheredCallback = func(session *icebreaker.IceSession, candidates []string, stillCurrent func() bool)

const (
	maxLobbyPeers = 30
)
const (
	// peerDisconnectedTimeout is a duration without network activity before an Agent is considered disconnected.
	// Default is 5 Seconds.
	peerDisconnectedTimeout = time.Second * 10
	// peerFailedTimeout is a duration without network activity before an Agent is considered
	// failed after disconnected.
	// Default is 25 Seconds.
	peerFailedTimeout = time.Second * 30
	// peerKeepAliveInterval is an interval how often the ICE Agent sends extra traffic if there is no activity,
	// if media is flowing no traffic will be sent.
	peerKeepAliveInterval = time.Second * 5
	// peerReconnectionInterval is an interval how often we will be trying to reconnect after failed reconnection
	// attempt when Peer goes to Failed/Closed state.
	peerReconnectionInterval = time.Second * 10
)

type PeerHandler interface {
	AddPeerIfMissing(playerId uint) PeerMeta
	RemovePeer(playerId uint)
	GetPeerById(playerId uint) (*Peer, bool)
}

type PeerManager struct {
	ctx                  context.Context
	localUserId          uint
	gameId               uint64
	peersMu              sync.Mutex
	peers                map[uint]*Peer
	icebreakerClient     *icebreaker.Client
	icebreakerEvents     <-chan icebreaker.EventMessage
	turnServer           []*stun.URI
	gameUdpPort          uint
	forceTurnRelay       bool
	reconnectionRequests chan uint
	gpgNetToGameChannel  chan<- gpgnet.Message
}

func NewPeerManager(
	ctx context.Context,
	icebreakerClient *icebreaker.Client,
	launcherInfo *launcher.Info,
	sessionInfo *icebreaker.SessionGameResponse,
	gameUdpPort uint,
	turnServer []*stun.URI,
	icebreakerEvents <-chan icebreaker.EventMessage,
	gpgNetToGameChannel chan<- gpgnet.Message,
) *PeerManager {
	peerManager := PeerManager{
		ctx:                  ctx,
		localUserId:          launcherInfo.UserId,
		gameId:               launcherInfo.GameId,
		peers:                make(map[uint]*Peer),
		icebreakerClient:     icebreakerClient,
		icebreakerEvents:     icebreakerEvents,
		turnServer:           turnServer,
		gameUdpPort:          gameUdpPort,
		forceTurnRelay:       launcherInfo.ForceTurnRelay || sessionInfo.ForceRelay,
		reconnectionRequests: make(chan uint, maxLobbyPeers),
		gpgNetToGameChannel:  gpgNetToGameChannel,
	}

	// Note:
	// Setting maxLobbyPeers for `reconnectionRequests` initial capacity does not bound its size:
	// maps grow to accommodate the number of items stored in them.

	return &peerManager
}

func (p *PeerManager) IsTurnRelayForced() bool {
	return p.forceTurnRelay
}

func (p *PeerManager) GetGameUdpPort() uint {
	return p.gameUdpPort
}

func (p *PeerManager) Start() {
	go p.runReconnectionManagement()

	for {
		select {
		case msg, ok := <-p.icebreakerEvents:
			if !ok {
				return
			}

			p.handleIceBreakerEvent(msg)
		case <-p.ctx.Done():
			applog.Debug("Peer manager exited, context canceled")
			return
		}
	}
}

func (p *PeerManager) runReconnectionManagement() {
	for {
		select {
		case peerId := <-p.reconnectionRequests:
			p.handleReconnection(peerId)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PeerManager) handleReconnection(playerId uint) {
	applog.Debug("Handling reconnection for peer", zap.Uint("playerId", playerId))

	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	if !ok || peer.IsDisabled() {
		p.peersMu.Unlock()
		return
	}
	p.peersMu.Unlock()

	// The request is consumed. Clear the flag so a failure of this attempt can schedule the next one.
	peer.reconnectMu.Lock()
	peer.reconnectionScheduled = false
	peer.reconnectMu.Unlock()

	if peer.IsActive() || peer.IsConnecting() {
		applog.Info("Peer already active/connecting, skipping reconnection", zap.Uint("peer", playerId))
		return
	}

	applog.Info("Connecting to peer", zap.Uint("playerId", playerId))

	if err := peer.ConnectOnce(p.turnServer); err != nil {
		applog.Error("Peer connection failed", zap.Uint("peer", playerId), zap.Error(err))

		// Retry after `peerReconnectionInterval`.
		select {
		case <-p.ctx.Done():
		case <-time.After(peerReconnectionInterval):
			peer.reconnectMu.Lock()
			peer.reconnectionScheduled = false
			peer.reconnectMu.Unlock()
			p.scheduleReconnection(playerId)
		}
		return
	}

	applog.Info("Peer connected successfully", zap.Uint("peer", playerId))
}

func (p *PeerManager) scheduleReconnectionForPeer(peer *Peer) {
	p.scheduleReconnection(peer.PeerId())
}

func (p *PeerManager) scheduleReconnection(playerId uint) {
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	p.peersMu.Unlock()
	if !ok || peer.IsDisabled() {
		return
	}

	peer.reconnectMu.Lock()
	if peer.reconnectionScheduled || peer.IsConnecting() {
		peer.reconnectMu.Unlock()
		return
	}
	peer.reconnectionScheduled = true
	peer.reconnectMu.Unlock()

	select {
	case p.reconnectionRequests <- playerId:
		applog.Debug("Reconnect scheduled", zap.Uint("peer", playerId))
	default:
		peer.reconnectMu.Lock()
		peer.reconnectionScheduled = false
		peer.reconnectMu.Unlock()
		applog.Warn("Reconnect queue overflow", zap.Uint("peer", playerId))
	}
}

func (p *PeerManager) handleIceBreakerEvent(msg icebreaker.EventMessage) {
	switch event := msg.(type) {
	case *icebreaker.ConnectedMessage:
		applog.Info("Connecting to peer", zap.Any("event", event))
		p.addPeerIfMissing(event.SenderID)
	case *icebreaker.CandidatesMessage:
		applog.Info("Received CandidatesMessage", zap.Any("event", event))

		peer, ok := p.GetPeerById(event.SenderID)
		if !ok {
			peer = p.addPeerIfMissing(event.SenderID)
			if peer == nil {
				applog.Error("Peer still nil after adding it as missing one")
				return
			}
		}

		if err := peer.AddCandidates(event); err != nil {
			applog.FromContext(peer.ctx).Error(
				"Could not use candidates of remote peer",
				zap.Error(err),
			)
		}
	case *icebreaker.PeerClosingMessage:
		applog.Info("Peer connection closed", zap.Any("event", event))

		p.RemovePeer(event.SenderID)

		applog.Info("Sending peer disconnected message to game from icebreaker peer closing message")
		p.gpgNetToGameChannel <- gpgnet.NewDisconnectFromPeerMessage(int32(event.SenderID))
	default:
		applog.Info("Received unknown event type", zap.Any("event", event))
	}
}

func (p *PeerManager) AddPeerIfMissing(playerId uint) PeerMeta {
	return p.addPeerIfMissing(playerId)
}

func (p *PeerManager) RemovePeer(playerId uint) {
	p.removePeer(playerId)
}

func (p *PeerManager) GetPeerById(playerId uint) (*Peer, bool) {
	p.peersMu.Lock()
	defer p.peersMu.Unlock()
	peer, ok := p.peers[playerId]
	return peer, ok
}

func (p *PeerManager) GetAllPeerIds() []uint {
	ids := make([]uint, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}
	return ids
}

func (p *PeerManager) removePeer(playerId uint) {
	peer := p.peers[playerId]
	if peer != nil {
		_ = peer.Close()
		delete(p.peers, playerId)
	}
}

func (p *PeerManager) addPeerIfMissing(playerId uint) *Peer {
	p.peersMu.Lock()
	// If peer exists, schedule reconnection if needed and return peer pointer instantly.
	if peer, ok := p.peers[playerId]; ok {
		p.peersMu.Unlock()
		if !peer.IsActive() && !peer.IsDisabled() {
			applog.Info("Peer exists but is inactive, scheduling reconnection",
				zap.Uint("playerId", playerId),
			)
			p.scheduleReconnection(playerId)
		}

		applog.Info("Peer already exists and is active", zap.Uint("playerId", playerId))
		return peer
	}

	// The smaller user id is always the offerer.
	isOfferer := p.localUserId < playerId
	peerUdpPort, err := util.GetFreeUdpPort()
	if err != nil {
		p.peersMu.Unlock()
		applog.Error("Failed to get UDP port for new peer", zap.Uint("playerId", playerId), zap.Error(err))
		return nil
	}

	applog.Info("Creating new peer", zap.Uint("playerId", playerId))
	newPeer, err := CreatePeer(
		p.ctx,
		isOfferer,
		playerId,
		p,
		peerUdpPort,
		p.gameUdpPort,
	)
	if err != nil {
		p.peersMu.Unlock()
		applog.FromContext(p.ctx).Error("Failed to create peer", zap.Error(err))
		return nil
	}

	newPeer.onStateChanged = p.onPeerStateChanged
	p.peers[playerId] = newPeer
	p.peersMu.Unlock()

	// Initiate connection in async manner instantly.
	go func() {
		p.reconnectPeer(playerId)
	}()

	applog.Debug("Peer successfully created", zap.Uint("playerId", playerId))
	return newPeer
}

func (p *PeerManager) reconnectPeer(playerId uint) {
	peer, ok := p.GetPeerById(playerId)
	if !ok || peer.IsDisabled() {
		return
	}

	applog.Info("Initial connecting to peer", zap.Uint("peer", playerId))
	if err := peer.ConnectOnce(p.turnServer); err != nil {
		applog.Error("Initial peer connection failed", zap.Uint("peer", playerId), zap.Error(err))
		// Reschedule connection attempt via queue.
		p.scheduleReconnection(playerId)
		return
	}
	applog.Info("Peer connected on initial attempt", zap.Uint("peer", playerId))
}

func (p *PeerManager) onPeerStateChanged(peer *Peer, state ice.ConnectionState) {
	applog.FromContext(peer.ctx).Info(
		"Peer connection state has changed",
		zap.String("state", state.String()),
	)

	switch state {
	case ice.ConnectionStateFailed, ice.ConnectionStateClosed:
		applog.FromContext(peer.ctx).Info(
			"Peer connection failed or closed, scheduling immediate reconnection")

		if state == ice.ConnectionStateFailed {
			peer.mu.Lock()
			if peer.forceTurnRelay {
				applog.FromContext(peer.ctx).Info("Switching to fallback relay All policy")
				peer.forceTurnRelay = false
			}
			peer.mu.Unlock()
		}

		p.scheduleReconnection(peer.PeerId())
	case ice.ConnectionStateDisconnected:
		// The ICE agent noticed that no traffic arrives anymore. This can be transient on a flaky network,
		// so we wait for either recovery or the failed state (after peerFailedTimeout) to reconnect.
		applog.FromContext(peer.ctx).Info("Peer disconnected, waiting to see if it recovers")
	case ice.ConnectionStateConnected, ice.ConnectionStateCompleted:
		peer.reconnectMu.Lock()
		peer.reconnectionScheduled = false
		peer.reconnectMu.Unlock()

		local, remote, ok := peer.SelectedPair()
		if !ok {
			applog.FromContext(peer.ctx).Error("Could not find selected candidate pair of peer")
			return
		}

		applog.FromContext(peer.ctx).Info(
			"Selected candidate pair",
			zap.String("localType", local.Type().String()),
			zap.String("local", net.JoinHostPort(local.Address(), strconv.Itoa(local.Port()))),
			zap.String("remoteType", remote.Type().String()),
			zap.String("remote", net.JoinHostPort(remote.Address(), strconv.Itoa(remote.Port()))),
		)
	default:
	}
}

func (p *PeerManager) onPeerCandidatesGathered(remotePeer uint) onPeerCandidatesGatheredCallback {
	return func(session *icebreaker.IceSession, candidates []string, stillCurrent func() bool) {
		err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   icebreaker.EventKindCandidates,
					GameID:      p.gameId,
					SenderID:    p.localUserId,
					RecipientID: &remotePeer,
				},
				Session:    session,
				Candidates: candidates,
			})

		if err != nil {
			applog.Error("Failed to send candidates",
				zap.Uint("playerId", p.localUserId),
				zap.Error(err),
			)
			go p.retrySendCandidates(remotePeer, session, candidates, 5, stillCurrent)
		}
	}
}

func (p *PeerManager) retrySendCandidates(
	remote uint,
	session *icebreaker.IceSession,
	candidates []string,
	attempts int,
	stillCurrent func() bool,
) {
	for i := 0; i < attempts && p.ctx.Err() == nil; i++ {
		d := time.Second * time.Duration(1<<i) // 1s, 2s, 4s, 8s, 16s
		select {
		case <-time.After(d):
		case <-p.ctx.Done():
			return
		}
		if !stillCurrent() {
			return // a newer attempt has its own credentials, never send outdated ones
		}
		if err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   icebreaker.EventKindCandidates,
					GameID:      p.gameId,
					SenderID:    p.localUserId,
					RecipientID: &remote,
				},
				Session:    session,
				Candidates: candidates,
			}); err == nil {
			return
		}
	}
	p.scheduleReconnection(remote)
}

func (p *PeerManager) HandleGameDisconnected() {
	for _, peer := range p.peers {
		_ = peer.Close()
	}

	err := p.icebreakerClient.SendEvent(
		icebreaker.PeerClosingMessage{
			BaseEvent: icebreaker.BaseEvent{
				EventType: icebreaker.EventKindPeerClosing,
				GameID:    p.gameId,
				SenderID:  p.localUserId,
			},
		},
	)
	if err != nil {
		applog.Error("Failed to send closing event to icebreaker", zap.Error(err))
	}
}

func (p *PeerManager) HandleGameEnded() {
	for _, peer := range p.peers {
		peer.Disable()
	}
}
