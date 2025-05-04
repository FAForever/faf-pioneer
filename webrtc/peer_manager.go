package webrtc

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
)

type onPeerCandidatesGatheredCallback = func(*webrtc.SessionDescription, []webrtc.ICECandidate)

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
	turnServer           []webrtc.ICEServer
	gameUdpPort          uint
	forceTurnRelay       bool
	reconnectionRequests chan uint
	gpgNetToGameChannel  chan<- gpgnet.Message
}

func NewPeerManager(
	ctx context.Context,
	icebreakerClient *icebreaker.Client,
	launcherInfo *launcher.Info,
	gameUdpPort uint,
	turnServer []webrtc.ICEServer,
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
		forceTurnRelay:       launcherInfo.ForceTurnRelay,
		reconnectionRequests: make(chan uint, maxLobbyPeers),
		gpgNetToGameChannel:  gpgNetToGameChannel,
	}

	// Note:
	// Setting maxLobbyPeers for `reconnectionRequests` initial capacity does not bound its size:
	// maps grow to accommodate the number of items stored in them.

	return &peerManager
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

	peer, ok := p.peers[playerId]
	if !ok || peer.IsDisabled() {
		return
	}

	applog.Info("Connecting to peer", zap.Uint("playerId", playerId))

	if err := peer.ConnectOnce(p.turnServer); err != nil {
		applog.Error("Peer connection failed", zap.Uint("peer", playerId), zap.Error(err))

		// Retry after `peerReconnectionInterval`.
		select {
		case <-p.ctx.Done():
		case <-time.After(peerReconnectionInterval):
			p.scheduleReconnection(playerId)
		}
		return
	}

	applog.Info("Peer connected successfully", zap.Uint("peer", playerId))
}

func (p *PeerManager) scheduleReconnection(playerId uint) {
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	p.peersMu.Unlock()
	if !ok || peer.IsDisabled() {
		return
	}

	select {
	case p.reconnectionRequests <- playerId:
		applog.Debug("Reconnect scheduled", zap.Uint("peer", playerId))
	default:
		peer.reconnectionScheduled = false
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

		peer := p.peers[event.SenderID]
		if peer == nil {
			peer = p.addPeerIfMissing(event.SenderID)
			if peer == nil {
				applog.Error("Peer still nil after adding it as missing one")
				return
			}
		}

		if peer.connection == nil {
			applog.Warn("Peer connection is not initialized yet")
			go func() {
				time.Sleep(time.Millisecond * 500)
				p.handleIceBreakerEvent(msg)
			}()
			return
		}

		if peer.connection.ICEConnectionState() != webrtc.ICEConnectionStateConnected {
			err := peer.AddCandidates(event.Session, event.Candidates)
			if err != nil {
				applog.FromContext(peer.ctx).Error(
					"Could not add candidate to active peer connection",
					zap.Error(err),
				)
			}
		}
	case *icebreaker.PeerClosingMessage:
		applog.Info("Peer connection closed", zap.Any("event", event))

		peer := p.peers[event.SenderID]
		if peer != nil {
			_ = peer.Close()
			delete(p.peers, event.SenderID)
		}

		applog.Info("Sending peer disconnected message to game from icebreaker peer closing message")
		p.gpgNetToGameChannel <- gpgnet.NewDisconnectFromPeerMessage(int32(event.SenderID))
	default:
		applog.Info("Received unknown event type", zap.Any("event", event))
	}
}

func (p *PeerManager) AddPeerIfMissing(playerId uint) PeerMeta {
	return p.addPeerIfMissing(playerId)
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

func (p *PeerManager) onPeerStateChanged(peer *Peer, state webrtc.PeerConnectionState) {
	applog.FromContext(peer.ctx).Info(
		"Peer connection state has changed",
		zap.String("state", state.String()),
	)

	switch state {
	case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
		select {
		case <-peer.localAddrReady:
		default:
			peer.localAddrReadyOnce.Do(func() {
				close(peer.localAddrReady)
			})
		}

		applog.FromContext(peer.ctx).Info(
			"Peer connection failed or closed, scheduling immediate reconnection")

		if state == webrtc.PeerConnectionStateFailed && peer.forceTurnRelay {
			applog.FromContext(peer.ctx).Info("Switching to fallback relay All policy")
			peer.forceTurnRelay = false
		}

		peer.reconnectMu.Lock()
		peer.reconnectionScheduled = false
		peer.reconnectMu.Unlock()

		p.scheduleReconnection(peer.PeerId())
		break
	case webrtc.PeerConnectionStateDisconnected:
		// WebRTC documentation saying:
		// The ICE Agent has determined that connectivity is currently lost for this RTCIceTransport.
		// This is a transient state that may trigger intermittently (and resolve itself without action)
		// on a flaky network.
		// However:
		// The way this state is determined is implementation dependent.
		// Suggesting to handle reconnection only on Failed or Closed state instead.
		applog.FromContext(peer.ctx).Info("Peer disconnected, waiting to see if it recovers")
		break

	case webrtc.PeerConnectionStateConnected:
		peer.reconnectionScheduled = false

		// Theoretically there could be a situation when `webrtc.PeerConnection` does not gather
		// statistics yet when we entered `Connected` state as a webrtc.ICECandidatePairStats might be in a
		// webrtc.StatsICECandidatePairStateInProgress state here.

		var pair webrtc.ICECandidatePairStats
		candidates := make(map[string]webrtc.ICECandidateStats)

		for _, s := range peer.connection.GetStats() {
			switch stat := s.(type) {
			case webrtc.ICECandidateStats:
				candidates[stat.ID] = stat
			case webrtc.ICECandidatePairStats:
				if stat.State == webrtc.StatsICECandidatePairStateSucceeded {
					pair = stat
				}
			default:
			}
		}

		applog.Debug("Candidate pairs received, updating map")

		localCandidate, okLocal := candidates[pair.LocalCandidateID]
		remoteCandidate, okRemote := candidates[pair.RemoteCandidateID]
		if !okLocal || !okRemote {
			applog.FromContext(peer.ctx).Error("Could not find candidate pair in peer stats")
			return
		}

		// Ignoring resolve error as it shouldn't really happen as WebRTC will be putting
		// a valid IP address here.
		localAddress, _ := net.ResolveIPAddr("ip", localCandidate.IP)
		remoteAddress, _ := net.ResolveIPAddr("ip", remoteCandidate.IP)

		peer.localAddress = localAddress
		peer.remoteAddress = remoteAddress
		peer.localAddrReadyOnce.Do(func() {
			close(peer.localAddrReady)
		})

		applog.FromContext(peer.ctx).Info(
			"Local candidate",
			zap.Any("candidate", localCandidate),
		)
		applog.FromContext(peer.ctx).Info(
			"Remote candidate",
			zap.Any("candidate", remoteCandidate),
		)
		break
	default:
		break
	}
}

func (p *PeerManager) onPeerCandidatesGathered(remotePeer uint) onPeerCandidatesGatheredCallback {
	return func(description *webrtc.SessionDescription, candidates []webrtc.ICECandidate) {
		err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   icebreaker.EventKindCandidates,
					GameID:      p.gameId,
					SenderID:    p.localUserId,
					RecipientID: &remotePeer,
				},
				Session:    description,
				Candidates: candidates,
			})

		if err != nil {
			applog.Error("Failed to send candidates",
				zap.Uint("playerId", p.localUserId),
				zap.Error(err),
			)
		}
	}
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
