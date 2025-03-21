package webrtc

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
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
	GetPeerById(playerId uint) *Peer
}

type PeerManager struct {
	ctx                  context.Context
	localUserId          uint
	gameId               uint64
	peerMu               sync.Mutex
	peers                map[uint]*Peer
	icebreakerClient     *icebreaker.Client
	icebreakerEvents     <-chan icebreaker.EventMessage
	turnServer           []webrtc.ICEServer
	gameUdpPort          uint
	nextPeerUdpPort      uint
	forceTurnRelay       bool
	reconnectionRequests chan uint
}

func NewPeerManager(
	ctx context.Context,
	icebreakerClient *icebreaker.Client,
	launcherInfo *launcher.Info,
	basePeerUdpPort uint,
	turnServer []webrtc.ICEServer,
	icebreakerEvents <-chan icebreaker.EventMessage,
) *PeerManager {
	peerManager := PeerManager{
		ctx:                  ctx,
		localUserId:          launcherInfo.UserId,
		gameId:               launcherInfo.GameId,
		peers:                make(map[uint]*Peer),
		icebreakerClient:     icebreakerClient,
		icebreakerEvents:     icebreakerEvents,
		turnServer:           turnServer,
		gameUdpPort:          launcherInfo.GameUdpPort,
		nextPeerUdpPort:      basePeerUdpPort,
		forceTurnRelay:       launcherInfo.ForceTurnRelay,
		reconnectionRequests: make(chan uint, maxLobbyPeers),
	}

	return &peerManager
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
	peer, exists := p.peers[playerId]
	if !exists || peer.IsActive() || peer.IsDisabled() {
		return
	}

	if err := peer.ConnectWithRetry(p.turnServer, peerReconnectionInterval); err != nil {
		applog.Error("Reconnection failed for peer", zap.Uint("playerId", playerId), zap.Error(err))
		p.scheduleReconnection(playerId)
		return
	}

	peer.reconnectionScheduled = false
	applog.Info("Reconnection succeeded for peer", zap.Uint("playerId", playerId))
}

func (p *PeerManager) scheduleReconnection(playerId uint) {
	peer := p.GetPeerById(playerId)
	if peer == nil {
		return
	}

	peer.reconnectMu.Lock()
	defer peer.reconnectMu.Unlock()
	if peer.reconnectionScheduled {
		applog.Info("Reconnection already scheduled for peer", zap.Uint("playerId", playerId))
		return
	}
	peer.reconnectionScheduled = true

	select {
	case p.reconnectionRequests <- playerId:
		applog.Info("Scheduled reconnection for peer", zap.Uint("playerId", playerId))
	default:
		applog.Info("Reconnection already scheduled (channel full) for peer", zap.Uint("playerId", playerId))
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

		if peer.connection.ICEConnectionState() != webrtc.ICEConnectionStateConnected {
			err := peer.AddCandidates(event.Session, event.Candidates)
			if err != nil {
				panic(err)
			}
		}
	default:
		applog.Info("Received unknown event type", zap.Any("event", event))
	}
}

func (p *PeerManager) AddPeerIfMissing(playerId uint) PeerMeta {
	return p.addPeerIfMissing(playerId)
}

func (p *PeerManager) GetPeerById(playerId uint) *Peer {
	existingPeer, exists := p.peers[playerId]
	if exists {
		return existingPeer
	}

	return nil
}

func (p *PeerManager) GetAllPeerIds() []uint {
	ids := make([]uint, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}
	return ids
}

func (p *PeerManager) addPeerIfMissing(playerId uint) *Peer {
	if peer, exists := p.peers[playerId]; exists {
		if peer.IsActive() {
			applog.Info("Peer already exists and is active", zap.Uint("playerId", playerId))
			return peer
		}

		applog.Info("Peer exists but is inactive, scheduling reconnection", zap.Uint("playerId", playerId))
		p.scheduleReconnection(playerId)
		return peer
	}

	applog.Info("Creating new peer", zap.Uint("playerId", playerId))

	// The smaller user id is always the offerer
	isOfferer := p.localUserId < playerId

	newPeer, err := CreatePeer(
		isOfferer,
		playerId,
		p.turnServer,
		p.nextPeerUdpPort,
		p.gameUdpPort,
		p.onPeerCandidatesGathered(playerId),
		p.onPeerStateChanged,
		p.forceTurnRelay,
	)
	if err != nil {
		applog.Error("Failed to create peer", zap.Uint("playerId", playerId), zap.Error(err))
		return nil
	}

	newPeer.onStateChanged = p.onPeerStateChanged

	p.peers[playerId] = newPeer
	p.nextPeerUdpPort++
	return newPeer
}

func (p *PeerManager) onPeerStateChanged(peer *Peer, state webrtc.PeerConnectionState) {
	applog.FromContext(peer.context).Info(
		"Peer connection state has changed",
		zap.String("state", state.String()),
	)

	switch state {
	case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
		applog.FromContext(peer.context).Info("Peer connection failed or closed, scheduling immediate reconnection")
		if state == webrtc.PeerConnectionStateFailed && peer.forceTurnRelay {
			applog.FromContext(peer.context).Info("Switching to fallback relay All policy")
			peer.forceTurnRelay = false
		}
		p.scheduleReconnection(peer.PeerId())

	case webrtc.PeerConnectionStateDisconnected:
		// WebRTC documentation saying:
		// The ICE Agent has determined that connectivity is currently lost for this RTCIceTransport.
		// This is a transient state that may trigger intermittently (and resolve itself without action)
		// on a flaky network.
		// However:
		// The way this state is determined is implementation dependent.
		// Suggesting to handle reconnection only on Failed or Closed state instead.
		applog.FromContext(peer.context).Info("Peer disconnected, waiting to see if it recovers")

	case webrtc.PeerConnectionStateConnected:
		peer.reconnectionScheduled = false

		var selectedCandidatePair webrtc.ICECandidatePairStats
		candidates := make(map[string]webrtc.ICECandidateStats)

		for _, s := range peer.connection.GetStats() {
			switch stat := s.(type) {
			case webrtc.ICECandidateStats:
				candidates[stat.ID] = stat
			case webrtc.ICECandidatePairStats:
				if stat.State == webrtc.StatsICECandidatePairStateSucceeded {
					selectedCandidatePair = stat
				}
			default:
			}
		}

		applog.FromContext(peer.context).Info(
			"Local candidate",
			zap.Any("candidate", candidates[selectedCandidatePair.LocalCandidateID]),
		)
		applog.FromContext(peer.context).Info(
			"Remote candidate",
			zap.Any("candidate", candidates[selectedCandidatePair.RemoteCandidateID]),
		)
	default:
	}
}

func (p *PeerManager) onPeerCandidatesGathered(remotePeer uint) onPeerCandidatesGatheredCallback {
	return func(description *webrtc.SessionDescription, candidates []webrtc.ICECandidate) {
		err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   "candidates",
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
