package faf

import (
	"bufio"
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"faf-pioneer/util"
	"faf-pioneer/webrtc"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net"
	"sync"
	"time"
)

type Peer interface {
	IsOfferer() bool
}

// PendingMessage represents a message waiting for prerequisites
type PendingMessage struct {
	Message  gpgnet.Message
	PeerId   uint
	QueuedAt time.Time
}

// GpgNetServer is using to establish communication as:
// FAF.exe <--> FAF-Pioneer (ICE-Adapter) <--> FAF-Client.
type GpgNetServer struct {
	ctx                     context.Context
	cancel                  context.CancelFunc
	peerManager             *webrtc.PeerManager
	port                    uint
	tcpListener             net.Listener
	state                   gpgnet.GameState
	fromGameChannel         chan<- gpgnet.Message
	toGameChannel           chan gpgnet.Message
	currentConnection       net.Conn
	currentConnectionMu     sync.Mutex
	currentConnectionCancel context.CancelFunc
	
	// Message queue for JoinGame/ConnectToPeer that need to wait for prerequisites
	pendingMessagesMu       sync.RWMutex
	pendingMessages         []PendingMessage
	stateCheckTicker        *time.Ticker
}

func NewGpgNetServer(
	ctx context.Context,
	cancel context.CancelFunc,
	peerManager *webrtc.PeerManager,
	port uint,
) *GpgNetServer {
	return &GpgNetServer{
		ctx:              ctx,
		cancel:           cancel,
		peerManager:      peerManager,
		port:             port,
		state:            gpgnet.GameStateNone,
		pendingMessages:  make([]PendingMessage, 0),
		stateCheckTicker: time.NewTicker(500 * time.Millisecond), // Check every 500ms
	}
}

func (s *GpgNetServer) Listen(
	fromGameChannel chan<- gpgnet.Message,
	toGameChannel chan gpgnet.Message,
) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	s.ctx = applog.AddContextFields(s.ctx,
		zap.Uint("listenPort", s.port),
	)

	applog.FromContext(s.ctx).Info("Listening GPG-Net control server")

	s.tcpListener = listener
	s.fromGameChannel = fromGameChannel
	s.toGameChannel = toGameChannel

	// Start background goroutine to process pending messages
	go s.processPendingMessagesLoop()

	for {
		conn, acceptErr := util.NetAcceptWithContext(s.ctx, listener)
		if acceptErr != nil {
			if s.ctx.Err() != nil {
				applog.FromContext(s.ctx).Debug("Context canceled, stopping accepting game connections")
				return nil
			}

			applog.FromContext(s.ctx).Error("Failed to accept new GPG-Net game connection", zap.Error(acceptErr))
			continue
		}

		_ = s.closeCurrentConnection()
		s.currentConnectionMu.Lock()
		s.currentConnection = conn
		s.currentConnectionMu.Unlock()

		s.acceptConnection(conn)
	}
}

func (s *GpgNetServer) acceptConnection(conn net.Conn) {
	clientCtx, cancel := context.WithCancel(s.ctx)
	s.currentConnectionCancel = cancel

	clientCtx = applog.AddContextFields(clientCtx,
		zap.String("remoteAddr", conn.RemoteAddr().String()),
	)

	applog.FromContext(clientCtx).Info("New GPG-Net client (game) connected")

	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(conn)
	faStreamReader := NewFaStreamReader(bufferReader)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(conn)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleFromGame(clientCtx, faStreamReader)
	go s.handleToGame(clientCtx, faStreamWriter)
}

func (s *GpgNetServer) handleFromGame(ctx context.Context, stream *StreamReader) {
	applog.FromContext(ctx).Info("Waiting for incoming GPG-Net messages from game")

	// Read one message from the connection, process it and continue reading.
	for {
		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, io.EOF) {
			applog.FromContext(ctx).Info("Closing GPG-Net connection from game (EOF reached)")
			_ = s.closeCurrentConnection()
			return
		}

		if err != nil {
			applog.FromContext(ctx).Error(
				"Error parsing GPG-Net command from game, closing connection",
				zap.Error(err),
			)
			_ = s.closeCurrentConnection()
			return
		}

		select {
		case <-ctx.Done():
			applog.FromContext(ctx).Debug("Context canceled in handleFromGame, stopping read loop")
			_ = s.closeCurrentConnection()
			return
		default:
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			applog.FromContext(ctx).Info("Closing GPG-Net connection from game (EOF reached)")
			_ = s.closeCurrentConnection()
			return
		}
		if err != nil {
			applog.FromContext(ctx).Error(
				"Error parsing GPG-Net command chunks from game, closing connection",
				zap.Error(err),
			)
			_ = s.closeCurrentConnection()
			return
		}

		select {
		case <-ctx.Done():
			applog.FromContext(ctx).Debug("Context canceled in handleFromGame, stopping read loop")
			_ = s.closeCurrentConnection()
			return
		default:
		}

		unparsedMsg := gpgnet.BaseMessage{
			Command: command,
			Args:    chunks,
		}

		// Try to parse GPG-Net message based on the command type/name.
		parsedMsg, err := unparsedMsg.TryParse()
		if err != nil {
			applog.FromContext(ctx).Error(
				"Failed to parse GPG-Net message from game",
				zap.Error(err),
			)
		}

		// Process parsed GPG-Net command.
		parsedMsg = s.ProcessMessage(parsedMsg)
		if parsedMsg != nil {
			s.fromGameChannel <- parsedMsg
		}
	}
}

func (s *GpgNetServer) handleToGame(ctx context.Context, stream *StreamWriter) {
	applog.FromContext(ctx).Info("Waiting for GPG-Net messages to be forwarded to the game")

	for {
		select {
		case msg, ok := <-s.toGameChannel:
			if !ok {
				applog.FromContext(ctx).Debug("Channel (toGameChannel) closed, GpgNetServer::handleToGame aborted")
				_ = s.closeCurrentConnection()
				return
			}

			applog.FromContext(ctx).Debug(
				"Forwarding GPG-Net message in server from (toGameChannel) to the game",
				zap.String("command", msg.GetCommand()),
			)

			err := stream.WriteMessage(msg)
			if errors.Is(err, net.ErrClosed) {
				applog.FromContext(ctx).Error(
					"Failed to write GPG-Net message to the game, connection was closed",
					zap.Error(err),
				)
				_ = s.closeCurrentConnection()
				return
			}

			if err != nil {
				applog.FromContext(ctx).Error("Failed to write GPG-Net message to game", zap.Error(err))
				_ = s.closeCurrentConnection()
				return
			}
			if err = stream.w.Flush(); err != nil {
				applog.FromContext(ctx).Error("Failed to flush GPG-Net message to game", zap.Error(err))
				_ = s.closeCurrentConnection()
				return
			}

			if baseMsg, okBase := msg.(*gpgnet.BaseMessage); okBase {
				baseMsg.CallSentHandler()
			}
		case <-ctx.Done():
			return
		}
	}
}

// processPendingMessagesLoop periodically checks if pending messages can be processed
func (s *GpgNetServer) processPendingMessagesLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.stateCheckTicker.C:
			s.checkAndProcessPendingMessages()
		}
	}
}

// checkAndProcessPendingMessages checks if prerequisites are met and processes queued messages
func (s *GpgNetServer) checkAndProcessPendingMessages() {
	s.pendingMessagesMu.Lock()
	defer s.pendingMessagesMu.Unlock()

	if len(s.pendingMessages) == 0 {
		return
	}

	// Filter out messages that can be processed
	remainingMessages := make([]PendingMessage, 0)
	processedCount := 0

	for _, pending := range s.pendingMessages {
		if s.canProcessPeerMessage(pending.PeerId) {
			// Process the message directly (it's already been queued, so skip queueing logic)
			processed := s.processMessageDirect(pending.Message, pending.PeerId)
			if processed != nil {
				// Send to game (not to client channel)
				s.toGameChannel <- processed
			}
			processedCount++
			
			applog.FromContext(s.ctx).Info(
				"Processed pending message after prerequisites met",
				zap.String("command", pending.Message.GetCommand()),
				zap.Uint("peerId", pending.PeerId),
				zap.Duration("waitTime", time.Since(pending.QueuedAt)),
			)
		} else {
			// Check if message has been waiting too long (30 seconds timeout)
			if time.Since(pending.QueuedAt) > 30*time.Second {
				applog.FromContext(s.ctx).Warn(
					"Dropping pending message due to timeout",
					zap.String("command", pending.Message.GetCommand()),
					zap.Uint("peerId", pending.PeerId),
					zap.Duration("waitTime", time.Since(pending.QueuedAt)),
				)
				processedCount++
			} else {
				// Keep in queue
				remainingMessages = append(remainingMessages, pending)
			}
		}
	}

	s.pendingMessages = remainingMessages

	if processedCount > 0 {
		applog.FromContext(s.ctx).Info(
			"Processed pending messages",
			zap.Int("processed", processedCount),
			zap.Int("remaining", len(remainingMessages)),
		)
	}
}

// canProcessPeerMessage checks if prerequisites are met for peer connection messages
func (s *GpgNetServer) canProcessPeerMessage(peerId uint) bool {
	// For host messages (peerId=0), game must be Idle OR Lobby
	// (HostGame/CreateLobby can be sent in either state)
	if peerId == 0 {
		return s.state == gpgnet.GameStateIde || s.state == gpgnet.GameStateLobby
	}

	// For join/connect messages: Game must be in Idle OR Lobby state (not loading, not launching)
	if s.state != gpgnet.GameStateIde && s.state != gpgnet.GameStateLobby {
		applog.FromContext(s.ctx).Debug(
			"canProcessPeerMessage: game not in Idle or Lobby state",
			zap.Uint("peerId", peerId),
			zap.String("currentState", string(s.state)),
		)
		return false
	}

	// Check: Peer WebRTC connection must be established (for join/connect messages)
	peer, exists := s.peerManager.GetPeerById(peerId)
	if !exists || peer == nil {
		applog.FromContext(s.ctx).Debug(
			"canProcessPeerMessage: peer does not exist",
			zap.Uint("peerId", peerId),
		)
		// Peer doesn't exist yet, but we need to wait for it to be created and connected
		return false
	}

	// Peer must have active WebRTC connection
	isActive := peer.IsActive()
	applog.FromContext(s.ctx).Debug(
		"canProcessPeerMessage: checking peer active state",
		zap.Uint("peerId", peerId),
		zap.Bool("isActive", isActive),
	)
	return isActive
}

// queueMessage adds a message to the pending queue
func (s *GpgNetServer) queueMessage(msg gpgnet.Message, peerId uint) {
	s.pendingMessagesMu.Lock()
	defer s.pendingMessagesMu.Unlock()

	pending := PendingMessage{
		Message:  msg,
		PeerId:   peerId,
		QueuedAt: time.Now(),
	}

	s.pendingMessages = append(s.pendingMessages, pending)

	applog.FromContext(s.ctx).Info(
		"Queued message waiting for prerequisites",
		zap.String("command", msg.GetCommand()),
		zap.Uint("peerId", peerId),
		zap.String("currentGameState", string(s.state)),
	)
}

func (s *GpgNetServer) ProcessMessage(rawMessage gpgnet.Message) gpgnet.Message {
	applog.FromContext(s.ctx).Debug(
		"Processing message",
		zap.String("command", rawMessage.GetCommand()),
		zap.String("rawMessageType", fmt.Sprintf("%T", rawMessage)),
	)

	switch msg := rawMessage.(type) {
	case *gpgnet.CreateLobbyMessage:
		// CreateLobby must wait for game to be Idle (finished loading)
		// No peer required since we're hosting
		if s.state != gpgnet.GameStateIde && s.state != gpgnet.GameStateLobby {
			// Queue for later processing
			s.queueMessage(rawMessage, 0) // peerId=0 for host messages
			
			applog.FromContext(s.ctx).Info(
				"Queued CreateLobby message - waiting for game to be Idle or Lobby",
				zap.String("currentGameState", string(s.state)),
			)
			
			return nil
		}
		
		// Game is Idle/Lobby, process immediately
		targetPort := s.peerManager.GetGameUdpPort()

		applog.FromContext(s.ctx).Info(
			"Received create lobby message, swapping lobby port",
			zap.Uint("targetPort", targetPort),
		)

		return gpgnet.NewCreateLobbyMessage(
			msg.LobbyInitMode,
			int32(targetPort),
			msg.LocalPlayerName,
			msg.LocalPlayerId,
		)
	case *gpgnet.HostGameMessage:
		// HostGame must also wait for game to be Idle or Lobby
		if s.state != gpgnet.GameStateIde && s.state != gpgnet.GameStateLobby {
			// Queue for later processing
			s.queueMessage(rawMessage, 0) // peerId=0 for host messages
			
			applog.FromContext(s.ctx).Info(
				"Queued HostGame message - waiting for game to be Idle or Lobby",
				zap.String("currentGameState", string(s.state)),
			)
			
			return nil
		}
		
		// Game is Idle/Lobby, let it pass through
		applog.FromContext(s.ctx).Info("Processing HostGame message")
	case *gpgnet.GameStateMessage:
		applog.FromContext(s.ctx).Info(
			"Local game gameState changed",
			zap.String("gameState", msg.State),
		)

		s.state = msg.State
	case *gpgnet.JoinGameMessage:
		peerId := uint(msg.RemotePlayerId)
		
		applog.FromContext(s.ctx).Debug(
			"ProcessMessage: Received JoinGame",
			zap.Uint("peerId", peerId),
			zap.String("currentGameState", string(s.state)),
		)
		
		// Check if prerequisites are met (game Idle + WebRTC connected)
		if !s.canProcessPeerMessage(peerId) {
			// Queue the message for later processing
			s.queueMessage(rawMessage, peerId)
			
			applog.FromContext(s.ctx).Info(
				"Queued JoinGame message - waiting for game to be ready (Idle/Lobby) and WebRTC to connect",
				zap.Uint("peerId", peerId),
				zap.String("currentGameState", string(s.state)),
			)
			
			// Return nil to prevent forwarding to game immediately
			return nil
		}
		
		// Prerequisites met, process immediately
		return s.processMessageDirect(rawMessage, peerId)
		
	case *gpgnet.ConnectToPeerMessage:
		peerId := uint(msg.RemotePlayerId)
		
		applog.FromContext(s.ctx).Debug(
			"ProcessMessage: Received ConnectToPeer",
			zap.Uint("peerId", peerId),
			zap.String("currentGameState", string(s.state)),
		)
		
		// Check if prerequisites are met (game Idle + WebRTC connected)
		if !s.canProcessPeerMessage(peerId) {
			// Queue the message for later processing
			s.queueMessage(rawMessage, peerId)
			
			applog.FromContext(s.ctx).Info(
				"Queued ConnectToPeer message - waiting for game to be ready (Idle/Lobby) and WebRTC to connect",
				zap.Uint("peerId", peerId),
				zap.String("currentGameState", string(s.state)),
			)
			
			// Return nil to prevent forwarding to game immediately
			return nil
		}
		
		// Prerequisites met, process immediately
		return s.processMessageDirect(rawMessage, peerId)
		
	case *gpgnet.DisconnectFromPeerMessage:
		applog.FromContext(s.ctx).Info("Disconnecting from peer and disabling it from reconnects",
			zap.Int32("peerId", msg.RemotePlayerId),
		)

		s.peerManager.RemovePeer(uint(msg.RemotePlayerId))
	case *gpgnet.GameEndedMessage:
		// We have to keep connections still open, otherwise all players get instant disconnect timeout screens for
		// the other players and can't open their stats
		applog.FromContext(s.ctx).Info("Game has ended")
		// s.peerManager.HandleGameEnded()
	default:
		applog.FromContext(s.ctx).Debug(
			"Message command ignored",
			zap.String("command", msg.GetCommand()),
		)
	}

	return rawMessage
}

// processMessageDirect handles JoinGame and ConnectToPeer messages directly (called after prerequisites check)
func (s *GpgNetServer) processMessageDirect(rawMessage gpgnet.Message, peerId uint) gpgnet.Message {
	switch msg := rawMessage.(type) {
	case *gpgnet.CreateLobbyMessage:
		targetPort := s.peerManager.GetGameUdpPort()

		applog.FromContext(s.ctx).Info(
			"Processing CreateLobby (swapping lobby port)",
			zap.Uint("targetPort", targetPort),
		)

		return gpgnet.NewCreateLobbyMessage(
			msg.LobbyInitMode,
			int32(targetPort),
			msg.LocalPlayerName,
			msg.LocalPlayerId,
		)
	case *gpgnet.HostGameMessage:
		// HostGame passes through unchanged
		applog.FromContext(s.ctx).Info("Processing HostGame message")
		return rawMessage
	case *gpgnet.JoinGameMessage:
		peer := s.peerManager.AddPeerIfMissing(peerId)

		applog.FromContext(s.ctx).Info(
			"Joining game (swapping the address/port)",
			zap.Uint("targetPort", peer.GetUdpPort()),
		)

		return gpgnet.NewJoinGameMessage(
			msg.RemotePlayerLogin,
			msg.RemotePlayerId,
			fmt.Sprintf("127.0.0.1:%d", peer.GetUdpPort()),
		)
	case *gpgnet.ConnectToPeerMessage:
		peer := s.peerManager.AddPeerIfMissing(peerId)

		applog.FromContext(s.ctx).Info(
			"Connecting to peer (swapping the address/port)",
			zap.Uint("targetPort", peer.GetUdpPort()),
		)

		return gpgnet.NewConnectToPeerMessage(
			msg.RemotePlayerLogin,
			msg.RemotePlayerId,
			fmt.Sprintf("127.0.0.1:%d", peer.GetUdpPort()),
		)
	}
	
	return rawMessage
}

func (s *GpgNetServer) closeCurrentConnection() error {
	s.currentConnectionMu.Lock()
	defer s.currentConnectionMu.Unlock()
	if s.currentConnection != nil {
		s.handleGameConnectionLost()
	}
	s.currentConnection = nil
	return nil
}

func (s *GpgNetServer) handleGameConnectionLost() {
	applog.FromContext(s.ctx).Info("Game connection has been lost, canceling context")
	s.peerManager.HandleGameDisconnected()
	s.cancel()
}

func (s *GpgNetServer) Close() error {
	// Stop the state check ticker
	if s.stateCheckTicker != nil {
		s.stateCheckTicker.Stop()
	}
	
	if s.currentConnection != nil {
		var disconnectWg sync.WaitGroup
		peerIds := s.peerManager.GetAllPeerIds()
		disconnectWg.Add(len(peerIds))

		for _, peerId := range peerIds {
			msg := gpgnet.NewDisconnectFromPeerMessage(int32(peerId))
			if baseMsg, baseOk := msg.(*gpgnet.BaseMessage); baseOk {
				baseMsg.SetSentHandler(func() {
					disconnectWg.Done()
				})
				s.toGameChannel <- msg
			} else {
				applog.Debug("Failed to send disconnect from peer message on close, could not use BaseMessage")
				disconnectWg.Done()
			}
		}

		done := make(chan struct{})
		go func() {
			disconnectWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			applog.FromContext(s.ctx).Debug(
				"Sent DisconnectFromPeerMessage to all peers, exiting")
		case <-time.After(5 * time.Second):
			applog.FromContext(s.ctx).Debug(
				"Could not sent DisconnectFromPeerMessage to all peers, timed out, exiting")
		}
	}

	_ = s.closeCurrentConnection()
	return s.tcpListener.Close()
}
