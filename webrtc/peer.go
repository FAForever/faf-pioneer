package webrtc

import (
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
)

type PeerMeta interface {
	IsOfferer() bool
	PeerId() uint
	GetUdpPort() uint
}

type Peer struct {
	offerer               bool
	peerId                uint
	ctx                   context.Context
	udpPort               uint
	connection            *webrtc.PeerConnection
	gameDataChannel       *webrtc.DataChannel
	offer                 *webrtc.SessionDescription
	answer                *webrtc.SessionDescription
	pendingCandidates     []webrtc.ICECandidate
	candidatesMux         sync.Mutex
	onCandidatesGathered  onPeerCandidatesGatheredCallback
	onStateChanged        func(peer *Peer, state webrtc.PeerConnectionState)
	disabledChannel       chan struct{}
	gameToWebrtcChannel   chan []byte
	webrtcToGameChannel   chan []byte
	gameDataProxy         *util.GameUDPProxy
	webrtcApi             *webrtc.API
	forceTurnRelay        bool
	lastConnectionPolicy  webrtc.ICETransportPolicy
	reconnectionScheduled bool
	reconnectMu           sync.Mutex
	disabled              bool
	localAddress          *net.IPAddr
	localAddrReady        chan struct{}
	localAddrReadyOnce    sync.Once
	remoteAddress         *net.IPAddr
}

func (p *Peer) IsOfferer() bool {
	return p.offerer
}

func (p *Peer) PeerId() uint {
	return p.peerId
}

func (p *Peer) GetUdpPort() uint {
	return p.udpPort
}

func (p *Peer) Disable() {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()
	p.disabled = true
	applog.FromContext(p.ctx).Info(
		"Peer disabled – no more reconnection attempts",
		zap.Uint("peerId", p.peerId),
	)

	// Close channel to notify `waitForCandidatePair` that we should exit.
	select {
	case <-p.disabledChannel:
		// Already closed, ignore.
	default:
		close(p.disabledChannel)
	}
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
	gameToWebrtcPort uint,
	webrtcToGamePort uint,
) (*Peer, error) {
	var err error

	ctx := applog.AddContextFields(parentContext,
		zap.Uint("remotePlayerId", peerId),
		zap.Bool("localOfferer", offerer),
		zap.Uint("gameToWebrtcPort", gameToWebrtcPort),
		zap.Uint("webrtcToGamePort", webrtcToGamePort),
	)

	applog.FromContext(ctx).Debug("Creating a peer")

	gameToWebrtcChannel := make(chan []byte)
	webrtcToGameChannel := make(chan []byte)

	// `webrtcToGamePort` is the udp port the game listens on for all peers.
	// `gameToWebrtcPort` is from where we're proxying all the data to local game port.

	gameUdpProxy, err := util.NewGameUDPProxy(
		ctx,
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
		ctx:                  ctx,
		offerer:              offerer,
		peerId:               peerId,
		udpPort:              gameToWebrtcPort,
		gameToWebrtcChannel:  gameToWebrtcChannel,
		webrtcToGameChannel:  webrtcToGameChannel,
		onCandidatesGathered: peerManager.onPeerCandidatesGathered(peerId),
		onStateChanged:       peerManager.onPeerStateChanged,
		gameDataProxy:        gameUdpProxy,
		webrtcApi:            webrtcApi,
		forceTurnRelay:       peerManager.forceTurnRelay,
	}

	return &peer, nil
}

func (p *Peer) ConnectOnce(iceServers []webrtc.ICEServer) error {
	if p.IsDisabled() {
		return errors.New("peer is disabled")
	}

	// If peed are disconnected/died/disabled while reconnecting, just gave up.
	if p.IsDisabled() {
		return errors.New("peer is disabled during reconnection")
	}

	return p.reconnect(iceServers)
}

func (p *Peer) reconnect(iceServers []webrtc.ICEServer) error {
	if p.forceTurnRelay {
		return p.reconnectWithPolicy(iceServers, webrtc.ICETransportPolicyRelay)
	}

	return p.reconnectWithPolicy(iceServers, webrtc.ICETransportPolicyAll)
}

func (p *Peer) reconnectWithPolicy(iceServers []webrtc.ICEServer, policy webrtc.ICETransportPolicy) error {
	if p.connection != nil {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = p.connection.Close()
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			applog.FromContext(p.ctx).Warn(
				"Unable to gracefully close connection to peer within 3 seconds, ignoring")
		}
	}

	p.localAddress = nil
	p.localAddrReady = make(chan struct{})
	p.disabledChannel = make(chan struct{})
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
		if err := p.initiateConnection(); err != nil {
			return fmt.Errorf("failed to initiate connection on reconnect: %w", err)
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
			applog.FromContext(p.ctx).Warn(
				"Failed to create peer connection with ICE-Relay policy, falling back",
				zap.Error(err),
			)

			p.forceTurnRelay = false
			config.ICETransportPolicy = webrtc.ICETransportPolicyAll
			return p.reconnectWebRtcPeer(config)
		}

		return nil, fmt.Errorf("cannot recreate peer connection: %w", err)
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
		applog.FromContext(p.ctx).Debug(
			"Data channel opened for peer connection; waiting for local address form candidate pairs.")

		// If local address are not set yet in `onPeerStateChanged` we will wait for it,
		// otherwise it will be read instantly and no lock will occur,
		// so DataChannel will be registered straight away.
		<-p.localAddrReady

		applog.FromContext(p.ctx).Debug(
			"Data channel set for peer connection, registering it; local address are set.")

		p.gameDataChannel = dataChannel
		p.RegisterDataChannel()
		dataChannel.Transport()
	})
}

func (p *Peer) AddCandidates(session *webrtc.SessionDescription, candidates []webrtc.ICECandidate) error {
	p.answer = session

	if err := p.connection.SetRemoteDescription(*session); err != nil {
		return fmt.Errorf("cannot set remote description: %w", err)
	}

	for _, candidate := range candidates {
		if err := p.connection.AddICECandidate(candidate.ToJSON()); err != nil {
			return fmt.Errorf("cannot add candidate to peer: %w", err)
		}
	}

	if !p.offerer {
		answer, err := p.connection.CreateAnswer(nil)
		if err != nil {
			return fmt.Errorf("cannot create answer: %w", err)
		}

		p.answer = &answer
		// Sets the LocalDescription, and starts our UDP listeners
		err = p.connection.SetLocalDescription(answer)
		if err != nil {
			return fmt.Errorf("cannot set local description (answer): %w", err)
		}
	}

	return nil
}

func (p *Peer) initiateConnection() error {
	if p.offerer && p.connection.ICEConnectionState() == webrtc.ICEConnectionStateNew {
		applog.FromContext(p.ctx).Info("Initiating connection")

		// default is ordered and announced, we don't need to pass options
		dataChannel, err := p.connection.CreateDataChannel("gameData", nil)
		if err != nil {
			return fmt.Errorf("cannot create data channel: %w", err)
		}

		p.gameDataChannel = dataChannel
		p.RegisterDataChannel()

		// Sets the LocalDescription, and starts our UDP listeners
		// Note: this will start the gathering of ICE candidates
		offer, err := p.connection.CreateOffer(nil)
		if err != nil {
			return fmt.Errorf("cannot create offer: %w", err)
		}

		p.offer = &offer

		if err = p.connection.SetLocalDescription(offer); err != nil {
			return fmt.Errorf("cannot set local description: %w", err)
		}

		return nil
	}

	applog.FromContext(p.ctx).Debug("Not initiating connection")
	return nil
}

func (p *Peer) RegisterDataChannel() {
	applog.FromContext(p.ctx).Info(
		"Registering data channel handlers",
		zap.String("label", p.gameDataChannel.Label()),
		zap.Any("id", util.PtrValueOrDef(p.gameDataChannel.ID(), 0)),
	)

	// Register channel opening handling
	p.gameDataChannel.OnOpen(func() {
		applog.FromContext(p.ctx).Info(
			"Data channel opened, waiting for local address to begin sending data",
			zap.String("label", p.gameDataChannel.Label()),
			zap.Any("id", util.PtrValueOrDef(p.gameDataChannel.ID(), 0)),
		)

		// If local address are not set yet in `onPeerStateChanged` we will wait for it,
		// otherwise it will be read instantly and no lock will occur,
		// so DataChannel will be registered straight away.
		<-p.localAddrReady

		applog.FromContext(p.ctx).Info(
			"Received local address, starting data channel send exchange",
			zap.String("label", p.gameDataChannel.Label()),
			zap.Any("id", util.PtrValueOrDef(p.gameDataChannel.ID(), 0)),
		)

		go func() {
			for {
				select {
				case msg, ok := <-p.gameToWebrtcChannel:
					if !ok {
						return
					}

					if p.IsDisabled() {
						return
					}

					err := p.gameDataChannel.Send(msg)
					if err != nil {
						applog.FromContext(p.ctx).Error(
							"Could not send data to WebRTC data channel",
							zap.Error(err),
						)
					}
				case <-p.ctx.Done():
					return
				}
			}
		}()
	})

	// Register text message handling
	p.gameDataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		p.webrtcToGameChannel <- msg.Data
	})
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
	if !p.disabled {
		p.Disable()
	}

	p.gameDataProxy.Close()
	if err := p.gameDataChannel.Close(); err != nil {
		return fmt.Errorf("cannot close peer data channel: %w", err)
	}
	if err := p.connection.Close(); err != nil {
		return fmt.Errorf("cannot close peer connection: %w", err)
	}

	return nil
}
