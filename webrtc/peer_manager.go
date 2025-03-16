package webrtc

import (
	"faf-pioneer/icebreaker"
	"faf-pioneer/util"
	pionwebrtc "github.com/pion/webrtc/v4"
	"github.com/sirupsen/logrus"
	"log/slog"
)

type PeerHandler interface {
	AddPeerIfMissing(playerId uint) PeerMeta
}

type PeerManager struct {
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
	icebreakerClient *icebreaker.Client,
	userId uint,
	gameId uint64,
	gameUdpPort uint,
	basePeerUdpPort uint,
	turnServer []pionwebrtc.ICEServer,
	icebreakerEvents <-chan icebreaker.EventMessage,
) PeerManager {
	peerManager := PeerManager{
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
	for msg := range p.icebreakerEvents {
		switch event := msg.(type) {
		case *icebreaker.ConnectedMessage:
			logrus.WithField("event", event).Info("Connecting to peer")
			p.addPeerIfMissing(event.SenderID)
		case *icebreaker.CandidatesMessage:
			logrus.WithField("event", event).Info("Received CandidatesMessage")
			peer := p.peers[event.SenderID]

			if peer == nil {
				peer = p.addPeerIfMissing(event.SenderID)
			}

			if peer.connection.ICEConnectionState() != pionwebrtc.ICEConnectionStateConnected {
				err := peer.AddCandidates(event.Session, event.Candidates)
				if err != nil {
					panic(err)
				}
			}

		default:
			logrus.WithField("event", event).Info("Received unknown event type")
		}
	}
}

func (p *PeerManager) AddPeerIfMissing(playerId uint) PeerMeta {
	return p.addPeerIfMissing(playerId)
}

func (p *PeerManager) addPeerIfMissing(playerId uint) *Peer {
	if peer, ok := p.peers[playerId]; ok {
		slog.Info("Peer already exists", slog.Any("playerId", playerId))
		// TODO: What if peer exists but was disconnected already?
		err := peer.InitiateConnection()
		if err != nil {
			panic(err)
		}
		return peer
	}

	slog.Info("Creating peer", slog.Any("playerId", playerId))

	// The smaller user id is always the offerer
	newPeer, err := CreatePeer(p.userId < playerId, playerId, p.turnServer, p.nextPeerUdpPort, p.gameUdpPort, p.onCandidatesGathered(playerId))
	if err != nil {
		panic(err)
	}

	p.peers[playerId] = newPeer
	p.nextPeerUdpPort++

	err = newPeer.InitiateConnection()
	if err != nil {
		panic(err)
	}

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
			slog.Error("Failed to send candidates", util.ErrorAttr(err))
		}
	}
}
