package webrtc

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"sync"
	"time"
)

type PeerMeta interface {
	IsOfferer() bool
	PeerId() uint
}

type Peer struct {
	offerer              bool
	peerId               uint
	context              context.Context
	connection           *webrtc.PeerConnection
	gameDataChannel      *webrtc.DataChannel
	offer                *webrtc.SessionDescription
	answer               *webrtc.SessionDescription
	pendingCandidates    []webrtc.ICECandidate
	candidatesMux        sync.Mutex
	onCandidatesGathered func(*webrtc.SessionDescription, []webrtc.ICECandidate)
	gameToWebrtcChannel  chan []byte
	webrtcToGameChannel  chan []byte
	gameDataProxy        *util.GameUDPProxy
}

func (p *Peer) IsOfferer() bool {
	return p.offerer
}

func (p *Peer) PeerId() uint {
	return p.peerId
}

func (p *Peer) wrapError(format string, a ...any) error {
	return fmt.Errorf("[Peer %d] %s", p.peerId, fmt.Sprintf(format, a...))
}

func (p *Peer) GetPort() uint16 {
	return 14080
}

func CreatePeer(
	offerer bool,
	peerId uint,
	iceServers []webrtc.ICEServer,
	gameToWebrtcPort uint,
	webrtcToGamePort uint,
	onCandidatesGathered func(*webrtc.SessionDescription, []webrtc.ICECandidate),
) (*Peer, error) {
	var err error

	ctx := applog.AddFields(context.Background(),
		zap.Uint("remotePlayerId", peerId),
		zap.Bool("localOfferer", offerer),
	)

	gameToWebrtcChannel := make(chan []byte)
	webrtcToGameChannel := make(chan []byte)

	// `webrtcToGamePort` is our local `--game-udp-port`.
	// `gameToWebrtcPort` is from where we're proxying all the data to local game port.

	gameUdpProxy, err := util.NewGameUDPProxy(
		webrtcToGamePort,
		gameToWebrtcPort,
		gameToWebrtcChannel,
		webrtcToGameChannel,
	)
	if err != nil {
		return nil, err
	}

	peer := Peer{
		context:              ctx,
		offerer:              offerer,
		peerId:               peerId,
		gameToWebrtcChannel:  gameToWebrtcChannel,
		webrtcToGameChannel:  webrtcToGameChannel,
		onCandidatesGathered: onCandidatesGathered,
		gameDataProxy:        gameUdpProxy,
	}

	if err = peer.Reconnect(iceServers); err != nil {
		return nil, peer.wrapError("cannot create peer connection", err)
	}

	return &peer, nil
}

func (p *Peer) Reconnect(iceServers []webrtc.ICEServer) error {
	if p.connection != nil {
		_ = p.connection.Close()
	}

	newConn, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
	if err != nil {
		return p.wrapError("cannot recreate peer connection", err)
	}

	p.connection = newConn
	p.registerConnectionHandlers(iceServers)

	if p.offerer {
		if err := p.InitiateConnection(); err != nil {
			return p.wrapError("failed to initiate connection on reconnect", err)
		}
	}

	return nil
}

func (p *Peer) registerConnectionHandlers(iceServers []webrtc.ICEServer) {
	ctx := p.context

	p.connection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		p.candidatesMux.Lock()
		defer p.candidatesMux.Unlock()

		if candidate == nil {
			var sessionDescription *webrtc.SessionDescription

			if p.offerer {
				sessionDescription = p.offer
			} else {
				sessionDescription = p.answer
			}

			p.onCandidatesGathered(sessionDescription, p.pendingCandidates)
			return
		}

		p.pendingCandidates = append(p.pendingCandidates, *candidate)
	})

	p.connection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		applog.FromContext(ctx).Info(
			"Peer connection state has changed",
			zap.String("state", state.String()),
		)

		switch state {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			applog.FromContext(ctx).Warn("Connection failed or closed, initiating reconnection")
			go func() {
				time.Sleep(5 * time.Second)
				if err := p.Reconnect(iceServers); err != nil {
					applog.FromContext(ctx).Error("Reconnection failed", zap.Error(err))
				} else {
					applog.FromContext(ctx).Info("Reconnection succeeded")
				}
			}()
		case webrtc.PeerConnectionStateConnected:
			var selectedCandidatePair webrtc.ICECandidatePairStats
			candidates := make(map[string]webrtc.ICECandidateStats)

			for _, s := range p.connection.GetStats() {
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

			applog.FromContext(ctx).Info(
				"Local candidate",
				zap.Any("candidate", candidates[selectedCandidatePair.LocalCandidateID]),
			)
			applog.FromContext(ctx).Info(
				"Remote candidate",
				zap.Any("candidate", candidates[selectedCandidatePair.RemoteCandidateID]),
			)
		default:
		}
	})

	// Register data channel creation handling
	p.connection.OnDataChannel(func(dataChannel *webrtc.DataChannel) {
		p.gameDataChannel = dataChannel
		p.RegisterDataChannel()
		dataChannel.Transport()
	})
}

func (p *Peer) AddCandidates(session *webrtc.SessionDescription, candidates []webrtc.ICECandidate) error {
	p.answer = session

	if err := p.connection.SetRemoteDescription(*session); err != nil {
		return p.wrapError("cannot set remote description: %w", err)
	}

	for _, candidate := range candidates {
		if err := p.connection.AddICECandidate(candidate.ToJSON()); err != nil {
			return p.wrapError("cannot add candidate to peer", err)
		}
	}

	if !p.offerer {
		answer, err := p.connection.CreateAnswer(nil)
		if err != nil {
			return p.wrapError("cannot create answer: %w", err)
		}

		p.answer = &answer
		// Sets the LocalDescription, and starts our UDP listeners
		err = p.connection.SetLocalDescription(answer)
		if err != nil {
			return p.wrapError("cannot set local description (answer): %w", err)
		}
	}

	return nil
}

func (p *Peer) InitiateConnection() error {
	if p.offerer && p.connection.ICEConnectionState() == webrtc.ICEConnectionStateNew {
		applog.FromContext(p.context).Info("Initiating connection")

		// default is ordered and announced, we don't need to pass options
		dataChannel, err := p.connection.CreateDataChannel("gameData", nil)
		if err != nil {
			return p.wrapError("cannot create data channel", err)
		}

		p.gameDataChannel = dataChannel
		p.RegisterDataChannel()

		// Sets the LocalDescription, and starts our UDP listeners
		// Note: this will start the gathering of ICE candidates
		offer, err := p.connection.CreateOffer(nil)
		if err != nil {
			return p.wrapError("cannot create offer", err)
		}

		p.offer = &offer

		if err = p.connection.SetLocalDescription(offer); err != nil {
			return p.wrapError("cannot set local description", err)
		}

		return nil
	}

	applog.FromContext(p.context).Debug("Not initiating connection")
	return nil
}

func (p *Peer) IsActive() bool {
	state := p.connection.ConnectionState()
	return state != webrtc.PeerConnectionStateClosed &&
		state != webrtc.PeerConnectionStateFailed
}

func (p *Peer) Close() error {
	p.gameDataProxy.Close()
	if err := p.connection.Close(); err != nil {
		return p.wrapError("cannot close peerConnection: %v\n", err)
	}

	return nil
}

func (p *Peer) RegisterDataChannel() {
	applog.FromContext(p.context).Info(
		"Registering data channel handlers",
		zap.String("label", p.gameDataChannel.Label()),
		zap.Any("id", util.PtrValueOrDef(p.gameDataChannel.ID(), 0)),
	)

	// Register channel opening handling
	p.gameDataChannel.OnOpen(func() {
		applog.FromContext(p.context).Info(
			"Data channel opened",
			zap.String("label", p.gameDataChannel.Label()),
			zap.Any("id", util.PtrValueOrDef(p.gameDataChannel.ID(), 0)),
		)

		go func() {
			for msg := range p.gameToWebrtcChannel {
				err := p.gameDataChannel.Send(msg)
				if err != nil {
					applog.FromContext(p.context).Error(
						"Could not send data to WebRTC data channel",
						zap.Error(err),
					)
				}
			}
		}()
	})

	// Register text message handling
	p.gameDataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		p.webrtcToGameChannel <- msg.Data
	})
}
