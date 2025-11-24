package webrtc

import (
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/moho"
	"faf-pioneer/util"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
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
	ctxCancel             context.CancelFunc
	udpPort               uint
	connectionMu          sync.RWMutex
	connection            *webrtc.PeerConnection
	connectionGeneration  uint64
	gameDataChannel       *webrtc.DataChannel
	offer                 *webrtc.SessionDescription
	answer                *webrtc.SessionDescription
	pendingCandidates     []webrtc.ICECandidate
	candidatesMux         sync.Mutex
	onCandidatesGathered  onPeerCandidatesGatheredCallback
	onStateChanged        func(peer *Peer, state webrtc.PeerConnectionState)
	gameToWebrtcChannel   chan []byte
	webrtcToGameChannel   chan []byte
	gameDataProxy         *moho.GameUDPProxy
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
	wg                    sync.WaitGroup
	shutdownOnce          sync.Once
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
}

func (p *Peer) IsDisabled() bool {
	p.reconnectMu.Lock()
	defer p.reconnectMu.Unlock()
	return p.disabled
}

// getConnection returns the current connection and its generation number for safe concurrent access
func (p *Peer) getConnection() (*webrtc.PeerConnection, uint64) {
	p.connectionMu.RLock()
	defer p.connectionMu.RUnlock()
	return p.connection, p.connectionGeneration
}

// setConnection safely updates the connection and increments the generation
func (p *Peer) setConnection(conn *webrtc.PeerConnection) {
	p.connectionMu.Lock()
	defer p.connectionMu.Unlock()
	p.connection = conn
	p.connectionGeneration++
}

// getConnectionState safely gets the current connection state
func (p *Peer) getConnectionState() webrtc.PeerConnectionState {
	p.connectionMu.RLock()
	defer p.connectionMu.RUnlock()
	if p.connection == nil {
		return webrtc.PeerConnectionStateNew
	}
	return p.connection.ConnectionState()
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

	ctx, cancel := context.WithCancel(parentContext)
	ctx = applog.AddContextFields(ctx,
		zap.Uint("remotePlayerId", peerId),
		zap.Bool("localOfferer", offerer),
		zap.Uint("gameToWebrtcPort", gameToWebrtcPort),
		zap.Uint("webrtcToGamePort", webrtcToGamePort),
	)

	applog.FromContext(ctx).Debug("Creating a peer")

	gameToWebrtcChannel := make(chan []byte, 100)
	webrtcToGameChannel := make(chan []byte, 100)

	// `webrtcToGamePort` is the udp port the game listens on for all peers.
	// `gameToWebrtcPort` is from where we're proxying all the data to local game port.

	gameUdpProxy, err := moho.NewGameUDPProxy(
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
		ctxCancel:            cancel,
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
	// Safely get current connection
	oldConn, _ := p.getConnection()

	if oldConn != nil {
		// Close old connection safely with timeout
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = oldConn.Close()
		}()

		// Increased timeout for connection cleanup from 3s to 10s
		// This gives more time for proper cleanup on slow networks
		select {
		case <-done:
			applog.FromContext(p.ctx).Debug("Connection closed gracefully")
		case <-time.After(10 * time.Second):
			applog.FromContext(p.ctx).Warn(
				"Unable to gracefully close connection to peer within 10 seconds, continuing with new connection")
		}
	}

	p.localAddress = nil
	p.localAddrReady = make(chan struct{})
	p.localAddrReadyOnce = sync.Once{}
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

	// Safely set new connection
	p.setConnection(newConn)
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
	conn, generation := p.getConnection()
	if conn == nil {
		return
	}

	conn.OnICECandidate(func(candidate *webrtc.ICECandidate) {
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

	conn.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		// Check if this callback is for the current connection
		currentConn, currentGeneration := p.getConnection()
		if currentConn != conn || currentGeneration != generation {
			return
		}
		if p.onStateChanged != nil {
			p.onStateChanged(p, state)
		}
	})

	// Register data channel creation handling
	conn.OnDataChannel(func(dataChannel *webrtc.DataChannel) {
		// Check if this callback is for the current connection
		currentConn, currentGeneration := p.getConnection()
		if currentConn != conn || currentGeneration != generation {
			return
		}

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
	})
}

func (p *Peer) AddCandidates(session *webrtc.SessionDescription, candidates []webrtc.ICECandidate) error {
	p.answer = session

	conn, _ := p.getConnection()
	if conn == nil {
		return fmt.Errorf("no active connection")
	}

	if err := conn.SetRemoteDescription(*session); err != nil {
		return fmt.Errorf("cannot set remote description: %w", err)
	}

	for _, candidate := range candidates {
		if err := conn.AddICECandidate(candidate.ToJSON()); err != nil {
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
	conn, _ := p.getConnection()
	if conn == nil {
		return fmt.Errorf("no active connection")
	}

	if p.offerer && conn.ICEConnectionState() == webrtc.ICEConnectionStateNew {
		applog.FromContext(p.ctx).Info("Initiating connection")

		// default is ordered and announced, we don't need to pass options
		dataChannel, err := conn.CreateDataChannel("gameData", nil)
		if err != nil {
			return fmt.Errorf("cannot create data channel: %w", err)
		}

		p.gameDataChannel = dataChannel
		p.RegisterDataChannel()

		// Sets the LocalDescription, and starts our UDP listeners
		// Note: this will start the gathering of ICE candidates
		offer, err := conn.CreateOffer(nil)
		if err != nil {
			return fmt.Errorf("cannot create offer: %w", err)
		}

		p.offer = &offer

		if err = conn.SetLocalDescription(offer); err != nil {
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

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
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
		b := append([]byte(nil), msg.Data...)
		select {
		case p.webrtcToGameChannel <- b:
		case <-p.ctx.Done():
			return
		default:
			applog.FromContext(p.ctx).Warn(
				"Dropping received game packet, data to game channel busy or closed",
			)
		}
	})
}

func (p *Peer) IsActive() bool {
	return p.getConnectionState() == webrtc.PeerConnectionStateConnected
}

func (p *Peer) Close() error {
	var closeError error

	p.shutdownOnce.Do(func() {
		// Mark as disabled first to prevent new operations
		if !p.disabled {
			p.Disable()
		}

		// Signal shutdown to all goroutines
		p.ctxCancel()

		// Wait for goroutines to finish
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			applog.FromContext(p.ctx).Debug("All goroutines stopped gracefully")
		case <-time.After(5 * time.Second):
			applog.FromContext(p.ctx).Warn("Force closing peer, some goroutines may leak")
		}

		// Close resources in order
		if p.gameDataProxy != nil {
			p.gameDataProxy.Close()
		}

		if p.gameDataChannel != nil {
			if err := p.gameDataChannel.Close(); err != nil {
				closeError = fmt.Errorf("cannot close peer data channel: %w", err)
			}
		}

		conn, _ := p.getConnection()
		if conn != nil {
			if err := conn.Close(); err != nil {
				if closeError == nil {
					closeError = fmt.Errorf("cannot close peer connection: %w", err)
				}
			}
		}
	})

	return closeError
}
