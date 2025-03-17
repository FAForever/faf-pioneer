package faf

import (
	"bufio"
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/webrtc"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"net"
	"sync"
)

type Peer interface {
	IsOfferer() bool
}

// GpgNetServer is using to establish communication as:
// FAF.exe <--> FAF-Pioneer (ICE-Adapter) <--> FAF-Client.
type GpgNetServer struct {
	ctx                     context.Context
	peerHandler             webrtc.PeerHandler
	port                    uint
	tcpListener             net.Listener
	loggerFields            []zap.Field
	state                   GameState
	fromGameChannel         chan<- *GpgMessage
	toGameChannel           chan *GpgMessage
	currentConnection       net.Conn
	currentConnectionMu     sync.Mutex
	currentConnectionCancel context.CancelFunc
}

func NewGpgNetServer(context context.Context, peerManager webrtc.PeerHandler, port uint) *GpgNetServer {
	return &GpgNetServer{
		ctx:         context,
		peerHandler: peerManager,
		port:        port,
		state:       GameStateDisconnected,
	}
}

func (s *GpgNetServer) Listen(fromGameChannel chan<- *GpgMessage, toGameChannel chan *GpgMessage) error {
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

	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			applog.Error("failed to accept GPG-Net connection", zap.Error(acceptErr))
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

		unparsedMsg := GenericGpgMessage{
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
		parsedMsg = s.processMessage(parsedMsg)
		if parsedMsg != nil {
			s.fromGameChannel <- &parsedMsg
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
					(*msg).GetCommand()),
				s.loggerFields...,
			)

			err := stream.WriteMessage(*msg)
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
		case <-ctx.Done():
			return
		}
	}
}

func (s *GpgNetServer) processMessage(rawMessage GpgMessage) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		applog.Info(
			"Local game state changed",
			append(s.loggerFields, zap.String("gameState", msg.State))...,
		)

		s.state = msg.State
		break
	case *JoinGameMessage:
		applog.Info(
			"Joining game (swapping the address/port)",
			append(s.loggerFields, zap.Uint("targetPort", s.port))...,
		)

		s.peerHandler.AddPeerIfMissing(msg.RemotePlayerId)

		mappedAddress := JoinGameMessage{
			Command:           msg.Command,
			RemotePlayerLogin: msg.RemotePlayerLogin,
			RemotePlayerId:    msg.RemotePlayerId,
			Destination:       fmt.Sprintf("127.0.0.1:%d", s.port),
		}

		var mappedMsg GpgMessage = &mappedAddress
		return mappedMsg
	case *ConnectToPeerMessage:
		applog.Info(
			"Connecting to peer (swapping the address/port)",
			append(s.loggerFields, zap.Uint("targetPort", s.port))...,
		)

		s.peerHandler.AddPeerIfMissing(msg.RemotePlayerId)

		mappedAddress := ConnectToPeerMessage{
			Command:           msg.Command,
			RemotePlayerLogin: msg.RemotePlayerLogin,
			RemotePlayerId:    msg.RemotePlayerId,
			Destination:       fmt.Sprintf("127.0.0.1:%d", s.port),
		}

		var mappedMsg GpgMessage = &mappedAddress
		return mappedMsg
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
	if s.currentConnection != nil {
		s.currentConnectionCancel()
		return s.currentConnection.Close()
	}
	return nil
}

func (s *GpgNetServer) Close() error {
	return s.tcpListener.Close()
}
