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
	offerer               bool
	peerId                uint
	context               context.Context
	connection            *webrtc.PeerConnection
	gameDataChannel       *webrtc.DataChannel
	offer                 *webrtc.SessionDescription
	answer                *webrtc.SessionDescription
	pendingCandidates     []webrtc.ICECandidate
	candidatesMux         sync.Mutex
	onCandidatesGathered  onPeerCandidatesGatheredCallback
	onStateChanged        func(peer *Peer, state webrtc.PeerConnectionState)
	gameToWebrtcChannel   chan []byte
	webrtcToGameChannel   chan []byte
	gameDataProxy         *util.GameUDPProxy
	webrtcApi             *webrtc.API
	forceTurnRelay        bool
	lastConnectionPolicy  webrtc.ICETransportPolicy
	reconnectionScheduled bool
	reconnectMu           sync.Mutex
	disabled              bool
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

func (p *Peer) Disable() {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()
	p.disabled = true
	applog.FromContext(p.context).Info(
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
	offerer bool,
	peerId uint,
	iceServers []webrtc.ICEServer,
	gameToWebrtcPort uint,
	webrtcToGamePort uint,
	onCandidatesGathered func(*webrtc.SessionDescription, []webrtc.ICECandidate),
	onStateChanged func(p *Peer, newState webrtc.PeerConnectionState),
	forceTurnRelay bool,
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

	se := webrtc.SettingEngine{}
	se.SetICETimeouts(
		peerDisconnectedTimeout,
		peerFailedTimeout,
		peerKeepAliveInterval,
	)

	webrtcApi := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	peer := Peer{
		context:              ctx,
		offerer:              offerer,
		peerId:               peerId,
		gameToWebrtcChannel:  gameToWebrtcChannel,
		webrtcToGameChannel:  webrtcToGameChannel,
		onCandidatesGathered: onCandidatesGathered,
		onStateChanged:       onStateChanged,
		gameDataProxy:        gameUdpProxy,
		webrtcApi:            webrtcApi,
		forceTurnRelay:       forceTurnRelay,
	}

	if err = peer.ConnectWithRetry(iceServers, peerReconnectionInterval); err != nil {
		return nil, peer.wrapError("cannot create peer connection", err)
	}

	return &peer, nil
}

func (p *Peer) ConnectWithRetry(iceServers []webrtc.ICEServer, retryDelay time.Duration) error {
	if p.IsDisabled() {
		return p.wrapError("peer is disabled")
	}

	var err error
	for {
		// If peed are disconnected/died/disabled while reconnecting, just gave up.
		if p.IsDisabled() {
			return p.wrapError("peer is disabled during reconnection")
		}

		err = p.Reconnect(iceServers)
		if err == nil {
			return nil
		}

		applog.FromContext(p.context).Error("Reconnection attempt failed", zap.Error(err))
		time.Sleep(retryDelay)
	}
}

func (p *Peer) Reconnect(iceServers []webrtc.ICEServer) error {
	if p.forceTurnRelay {
		return p.reconnectWithPolicy(iceServers, webrtc.ICETransportPolicyRelay)
	}

	return p.reconnectWithPolicy(iceServers, webrtc.ICETransportPolicyAll)
}

func (p *Peer) reconnectWithPolicy(iceServers []webrtc.ICEServer, policy webrtc.ICETransportPolicy) error {
	if p.connection != nil {
		_ = p.connection.Close()
	}

	p.pendingCandidates = nil
	p.offer = nil
	p.answer = nil

	webrtcConfig := webrtc.Configuration{
		ICEServers:         iceServers,
		ICETransportPolicy: policy,
	}

	newConn, err := p.reconnectWebRtcPeer(webrtcConfig)
	if err != nil {
		return err
	}

	p.connection = newConn
	p.registerConnectionHandlers()

	if p.offerer {
		if err := p.InitiateConnection(); err != nil {
			return p.wrapError("failed to initiate connection on reconnect", err)
		}
	}

	return nil
}

func (p *Peer) reconnectWebRtcPeer(config webrtc.Configuration) (*webrtc.PeerConnection, error) {
	p.lastConnectionPolicy = config.ICETransportPolicy

	applog.Info("Creating new WebRTC connection",
		zap.String("ICETransportPolicy", config.ICETransportPolicy.String()),
	)

	newConn, err := p.webrtcApi.NewPeerConnection(config)
	if err != nil {
		if config.ICETransportPolicy == webrtc.ICETransportPolicyRelay && p.forceTurnRelay {
			applog.FromContext(p.context).Warn(
				"Failed to create peer connection with ICE-Relay policy, falling back",
				zap.Error(err),
			)

			p.forceTurnRelay = false
			config.ICETransportPolicy = webrtc.ICETransportPolicyAll
			return p.reconnectWebRtcPeer(config)
		}

		return nil, p.wrapError("cannot recreate peer connection", err)
	}

	return newConn, err
}

func (p *Peer) registerConnectionHandlers() {
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
			p.pendingCandidates = nil
			return
		}

		p.pendingCandidates = append(p.pendingCandidates, *candidate)
	})

	p.connection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if p.onStateChanged != nil {
			p.onStateChanged(p, state)
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
	if p.connection == nil {
		return false
	}

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
