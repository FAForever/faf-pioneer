package faf

import (
	"bufio"
	"context"
	"errors"
	"faf-pioneer/webrtc"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"log/slog"
	"net"
	"sync"
)

type Peer interface {
	IsOfferer() bool
}

// GpgNetServer is using to establish communication as:
// FAF.exe <--> FAF-Pioneer (ICE-Adapter) <--> FAF-Client.
type GpgNetServer struct {
	ctx                 context.Context
	peerHandler         webrtc.PeerHandler
	port                uint
	tcpListener         *net.Listener
	state               string
	gameToAdapter       chan<- *GpgMessage
	adapterToGame       chan *GpgMessage
	currentConnectionMu sync.Mutex
	currentConnection   *net.Conn
}

func NewGpgNetServer(context context.Context, peerManager webrtc.PeerHandler, port uint) *GpgNetServer {
	return &GpgNetServer{
		ctx:         context,
		peerHandler: peerManager,
		port:        port,
		state:       "disconnected",
	}
}

func (s *GpgNetServer) Listen(gameToAdapter chan<- *GpgMessage, adapterToGame chan *GpgMessage) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	logrus.Infof("Listening GPG-Net control server at port :%d", s.port)

	s.tcpListener = &listener
	s.gameToAdapter = gameToAdapter
	s.adapterToGame = adapterToGame

	for {
		conn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("failed to accept GPG-Net connection: %v", err)
			continue
		}

		s.currentConnectionMu.Lock()
		if s.currentConnection != nil {
			_ = (*s.currentConnection).Close()
		}

		s.currentConnection = &conn
		s.currentConnectionMu.Unlock()

		s.acceptConnection(conn)
	}
}

func (s *GpgNetServer) acceptConnection(conn net.Conn) {
	logger := logrus.WithFields(map[string]interface{}{
		"remoteAddress": conn.RemoteAddr().String(),
	})

	logger.Info("New GPG-Net client connected")

	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(conn)
	faStreamReader := NewFaStreamReader(bufferReader)

	go s.handleFromGame(faStreamReader, logger)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(conn)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleToGame(faStreamWriter, logger)
}

func (s *GpgNetServer) handleFromGame(stream *StreamReader, logger *logrus.Entry) {
	logger.Info("Waiting for incoming GPG-Net messages from game")

	// Read one message from the connection, process it and continue reading.
	for {
		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, io.EOF) {
			logger.Info("Closing GPG-Net connection from game (EOF reached)")
			return
		}

		if err != nil {
			logger.
				WithError(err).
				Error("Error parsing GPG-Net command from game, closing connection")
			return
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			logger.Info("Closing GPG-Net connection from game (EOF reached)")
			return
		}
		if err != nil {
			logger.
				WithError(err).
				Error("Error parsing GPG-Net command chunks from game, closing connection")
			return
		}

		unparsedMsg := GenericGpgMessage{
			Command: command,
			Args:    chunks,
		}

		// Try to parse GPG-Net message based on the command type/name.
		parsedMsg, err := unparsedMsg.TryParse()
		if err != nil {
			logger.WithError(err).Error("Failed to parse GPG-Net message")
		}

		// Process parsed GPG-Net command.
		parsedMsg = s.processMessage(parsedMsg, logger)
		if parsedMsg != nil {
			s.gameToAdapter <- &parsedMsg
		}
	}
}

func (s *GpgNetServer) handleToGame(stream *StreamWriter, logger *logrus.Entry) {
	logger.Info("Waiting for GPG-Net messages to be forwarded to the game")

	for {
		select {
		case msg, ok := <-s.adapterToGame:
			if !ok {
				return
			}

			err := stream.WriteMessage(*msg)
			if err != nil {
				logger.WithError(err).Error("Failed to write GPG-Net message to game")
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *GpgNetServer) processMessage(rawMessage GpgMessage, logger *logrus.Entry) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		logger.
			WithField("state", msg.State).
			Info("Local GameState changed")

		s.state = msg.State

		if msg.State == "idle" {

		}
		break
	case *JoinGameMessage:
		slog.Info("Joining game (swapping the address/port)")
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
		slog.Info("Connecting to peer (swapping the address/port)")
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
		logger.
			WithField("command", msg.GetCommand()).
			Debug("Message command ignored")
	}

	return rawMessage
}

func (s *GpgNetServer) Close() error {
	return (*s.tcpListener).Close()
}
