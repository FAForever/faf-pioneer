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
	"go.uber.org/zap/zapcore"
	"io"
	"net"
	"sync"
	"time"
)

type Peer interface {
	IsOfferer() bool
}

// GpgNetServer is using to establish communication as:
// FAF.exe <--> FAF-Pioneer (ICE-Adapter) <--> FAF-Client.
type GpgNetServer struct {
	ctx                     context.Context
	peerManager             *webrtc.PeerManager
	port                    uint
	tcpListener             net.Listener
	loggerFields            []zap.Field
	state                   gpgnet.GameState
	fromGameChannel         chan<- gpgnet.Message
	toGameChannel           chan gpgnet.Message
	currentConnection       net.Conn
	currentConnectionMu     sync.Mutex
	currentConnectionCancel context.CancelFunc
	udpProxyPort            uint
}

func NewGpgNetServer(context context.Context, peerManager *webrtc.PeerManager, port uint) *GpgNetServer {
	return &GpgNetServer{
		ctx:         context,
		peerManager: peerManager,
		port:        port,
		state:       gpgnet.GameStateNone,
	}
}

func (s *GpgNetServer) Listen(
	fromGameChannel chan<- gpgnet.Message,
	toGameChannel chan gpgnet.Message,
	udpProxyPort uint,
) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	applog.Info("Listening GPG-Net control server", zap.Uint("listenPort", s.port))

	s.tcpListener = listener
	s.fromGameChannel = fromGameChannel
	s.toGameChannel = toGameChannel
	s.udpProxyPort = udpProxyPort

	for {
		conn, acceptErr := util.NetAcceptWithContext(s.ctx, listener)
		if acceptErr != nil {
			if s.ctx.Err() != nil {
				applog.Debug("Context canceled, stopping accepting game connections")
				return nil
			}

			applog.Error("Failed to accept new GPG-Net game connection", zap.Error(err))
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

	s.loggerFields = []zapcore.Field{
		zap.Uint("listenPort", s.port),
		zap.String("remoteAddr", conn.RemoteAddr().String()),
	}

	applog.Info("New GPG-Net client (game) connected", s.loggerFields...)

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
	applog.Info("Waiting for incoming GPG-Net messages from game", s.loggerFields...)

	// Read one message from the connection, process it and continue reading.
	for {
		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, io.EOF) {
			applog.Info(
				"Closing GPG-Net connection from game (EOF reached)",
				s.loggerFields...,
			)
			_ = s.closeCurrentConnection()
			return
		}

		if err != nil {
			applog.Error(
				"Error parsing GPG-Net command from game, closing connection",
				append(s.loggerFields, zap.Error(err))...,
			)
			_ = s.closeCurrentConnection()
			return
		}

		select {
		case <-ctx.Done():
			applog.Debug("Context canceled in handleFromGame, stopping read loop", s.loggerFields...)
			_ = s.closeCurrentConnection()
			return
		default:
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			applog.Info(
				"Closing GPG-Net connection from game (EOF reached)",
				s.loggerFields...,
			)
			_ = s.closeCurrentConnection()
			return
		}
		if err != nil {
			applog.Error(
				"Error parsing GPG-Net command chunks from game, closing connection",
				append(s.loggerFields, zap.Error(err))...,
			)
			_ = s.closeCurrentConnection()
			return
		}

		select {
		case <-ctx.Done():
			applog.Debug("Context canceled in handleFromGame, stopping read loop", s.loggerFields...)
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
			applog.Error(
				"Failed to parse GPG-Net message from game",
				append(s.loggerFields, zap.Error(err))...,
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
	applog.Info(
		"Waiting for GPG-Net messages to be forwarded to the game",
		s.loggerFields...,
	)

	for {
		select {
		case msg, ok := <-s.toGameChannel:
			if !ok {
				applog.Debug(
					"Channel (toGameChannel) closed, GpgNetServer::handleToGame aborted",
					s.loggerFields...,
				)
				_ = s.closeCurrentConnection()
				return
			}

			applog.Debug(
				fmt.Sprintf(
					"Forwarding GPG-Net message '%s' in server from (toGameChannel) to the game",
					msg.GetCommand()),
				s.loggerFields...,
			)

			err := stream.WriteMessage(msg)
			if errors.Is(err, net.ErrClosed) {
				applog.Error(
					"Failed to write GPG-Net message to the game, connection was closed",
					append(s.loggerFields, zap.Error(err))...,
				)
				_ = s.closeCurrentConnection()
				return
			}

			if err != nil {
				applog.Error(
					"Failed to write GPG-Net message to game",
					append(s.loggerFields, zap.Error(err))...,
				)
				_ = s.closeCurrentConnection()
				return
			}
			if err = stream.w.Flush(); err != nil {
				applog.Error(
					"Failed to flush GPG-Net message to game",
					append(s.loggerFields, zap.Error(err))...,
				)
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

func (s *GpgNetServer) ProcessMessage(rawMessage gpgnet.Message) gpgnet.Message {
	applog.Debug("Processing message",
		zap.String("command", rawMessage.GetCommand()),
		zap.String("rawMessageType", fmt.Sprintf("%T", rawMessage)))

	switch msg := rawMessage.(type) {
	case *gpgnet.GameStateMessage:
		applog.Info(
			"Local game gameState changed",
			append(s.loggerFields, zap.String("gameState", msg.State))...,
		)

		s.state = msg.State
		break
	case *gpgnet.JoinGameMessage:
		applog.Info(
			"Joining game (swapping the address/port)",
			append(s.loggerFields, zap.Uint("targetPort", s.udpProxyPort))...,
		)

		s.peerManager.AddPeerIfMissing(uint(msg.RemotePlayerId))

		return gpgnet.NewJoinGameMessage(
			msg.RemotePlayerLogin,
			msg.RemotePlayerId,
			fmt.Sprintf("127.0.0.1:%d", s.udpProxyPort),
		)
	case *gpgnet.ConnectToPeerMessage:
		applog.Info(
			"Connecting to peer (swapping the address/port)",
			append(s.loggerFields, zap.Uint("targetPort", s.udpProxyPort))...,
		)

		s.peerManager.AddPeerIfMissing(uint(msg.RemotePlayerId))

		return gpgnet.NewConnectToPeerMessage(
			msg.RemotePlayerLogin,
			msg.RemotePlayerId,
			fmt.Sprintf("127.0.0.1:%d", s.udpProxyPort),
		)
	case *gpgnet.DisconnectFromPeerMessage:
		applog.Info("Disconnecting from peer and disabling it from reconnects",
			append(s.loggerFields, zap.Int32("peerId", msg.RemotePlayerId))...,
		)

		if peer := s.peerManager.GetPeerById(uint(msg.RemotePlayerId)); peer != nil {
			peer.Disable()
		}
		break
	case *gpgnet.GameEndedMessage:
		applog.Info("Game is ended, disabling/disconnecting all peers")
		for _, peerId := range s.peerManager.GetAllPeerIds() {
			if peer := s.peerManager.GetPeerById(peerId); peer != nil {
				peer.Disable()
			}
		}
		break
	default:
		applog.Debug(
			"Message command ignored",
			append(s.loggerFields, zap.String("command", msg.GetCommand()))...,
		)
	}

	return rawMessage
}

func (s *GpgNetServer) closeCurrentConnection() error {
	s.currentConnectionMu.Lock()
	defer s.currentConnectionMu.Unlock()
	return nil
}

func (s *GpgNetServer) Close() error {
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
			applog.Debug("Sent DisconnectFromPeerMessage to all peers, exiting",
				s.loggerFields...)
		case <-time.After(5 * time.Second):
			applog.Debug("Could not sent DisconnectFromPeerMessage to all peers, timed out, exiting",
				s.loggerFields...)
		}
	}

	_ = s.closeCurrentConnection()
	return s.tcpListener.Close()
}
