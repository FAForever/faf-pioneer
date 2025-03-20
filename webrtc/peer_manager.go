package webrtc

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/icebreaker"
	pionwebrtc "github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"time"
)

type PeerHandler interface {
	AddPeerIfMissing(playerId uint) PeerMeta
	GetPeerById(playerId uint) *Peer
}

type PeerManager struct {
	ctx              context.Context
	userId           uint
	gameId           uint64
	peers            map[uint]*Peer
	icebreakerClient *icebreaker.Client
	icebreakerEvents <-chan icebreaker.EventMessage
	turnServer       []pionwebrtc.ICEServer
	gameUdpPort      uint
	nextPeerUdpPort  uint
}

func NewPeerManager(
	ctx context.Context,
	icebreakerClient *icebreaker.Client,
	userId uint,
	gameId uint64,
	gameUdpPort uint,
	basePeerUdpPort uint,
	turnServer []pionwebrtc.ICEServer,
	icebreakerEvents <-chan icebreaker.EventMessage,
) PeerManager {
	peerManager := PeerManager{
		ctx:              ctx,
		userId:           userId,
		gameId:           gameId,
		peers:            make(map[uint]*Peer),
		icebreakerClient: icebreakerClient,
		icebreakerEvents: icebreakerEvents,
		turnServer:       turnServer,
		gameUdpPort:      gameUdpPort,
		nextPeerUdpPort:  basePeerUdpPort,
	}

	return peerManager
}

func (p *PeerManager) Start() {
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

		if peer.connection.ICEConnectionState() != pionwebrtc.ICEConnectionStateConnected {
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
	ids := make([]uint, len(p.peers))
	for id, _ := range p.peers {
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

		applog.Info("Peer exists but is inactive, recreating", zap.Uint("playerId", playerId))
		err := peer.Reconnect(p.turnServer)
		if err != nil {
			applog.Warn("Failed to reconnect to peer, reconnecting in 5 seconds",
				zap.Uint("playerId", playerId),
				zap.Error(err),
			)

			go func() {
				time.Sleep(5 * time.Second)
				applog.Info("Reconnecting to peer", zap.Uint("playerId", playerId))
				if p != nil {
					_, stillExists := p.peers[playerId]
					if !stillExists {
						applog.Info("Reconnecting to peer canceled, peer was removed",
							zap.Uint("playerId", playerId))
						return
					}

					p.addPeerIfMissing(playerId)
				}
			}()
		}
	}

	applog.Info("Creating new peer", zap.Uint("playerId", playerId))

	// The smaller user id is always the offerer
	newPeer, err := CreatePeer(
		p.userId < playerId,
		playerId,
		p.turnServer,
		p.nextPeerUdpPort,
		p.gameUdpPort,
		p.onCandidatesGathered(playerId),
	)
	if err != nil {
		applog.Error("Failed to create peer", zap.Uint("playerId", playerId), zap.Error(err))
		return nil
	}

	p.peers[playerId] = newPeer
	p.nextPeerUdpPort++
	return newPeer
}

func (p *PeerManager) onCandidatesGathered(remotePeer uint) func(*pionwebrtc.SessionDescription, []pionwebrtc.ICECandidate) {
	return func(description *pionwebrtc.SessionDescription, candidates []pionwebrtc.ICECandidate) {
		err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   "candidates",
					GameID:      p.gameId,
					SenderID:    p.userId,
					RecipientID: &remotePeer,
				},
				Session:    description,
				Candidates: candidates,
			})

		if err != nil {
			applog.Error("Failed to send candidates",
				zap.Uint("playerId", p.userId),
				zap.Error(err),
			)
		}
	}
}
