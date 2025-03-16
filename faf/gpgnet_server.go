package faf

import (
	"context"
	"faf-pioneer/webrtc"
	"fmt"
	"github.com/sirupsen/logrus"
	"log/slog"
	"net"
	"sync"
)

type Peer interface {
	IsOfferer() bool
}

type GpgNetServer struct {
	ctx             context.Context
	peerHandler     webrtc.PeerHandler
	port            uint
	tcpListener     *net.Listener
	state           string
	gameToAdapter   chan<- *GpgMessage
	adapterToGame   chan *GpgMessage
	currentClientMu sync.Mutex
	currentClient   *GpgNetClient
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

	s.tcpListener = &listener
	s.gameToAdapter = gameToAdapter
	s.adapterToGame = adapterToGame

	for {
		conn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("failed to accept GPG-Net connection: %v", err)
			continue
		}

		s.currentClientMu.Lock()
		if s.currentClient != nil {
			s.onGpgNetConnectionLost()
		}

		s.currentClient = s.accept(conn)
		s.currentClientMu.Unlock()
	}
}

func (s *GpgNetServer) accept(conn net.Conn) *GpgNetClient {
	client := &GpgNetClient{
		ctx:        s.ctx,
		connection: &conn,
		server:     s,
		logger: logrus.WithFields(map[string]interface{}{
			"remoteAddress": conn.RemoteAddr().String(),
		}),
		gameToAdapter: s.gameToAdapter,
		adapterToGame: s.adapterToGame,
	}

	err := client.Listen()
	if err != nil {
		logrus.WithError(err).Error("Failed to process accepted GPG-Net connection")
		return nil
	}

	return client
}

func (s *GpgNetServer) addPeerIfMissing(playerId uint) {
	s.peerHandler.AddPeerIfMissing(playerId)
}

func (s *GpgNetServer) Close() error {
	return (*s.tcpListener).Close()
}

func (s *GpgNetServer) onGpgNetConnectionLost() {
	s.currentClient.Close()
	s.currentClient = nil
}

func (s *GpgNetServer) ProcessMessage(rawMessage GpgMessage, logger *logrus.Entry) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		logger.
			WithField("state", msg.State).
			Info("Local GameState changed")

		s.state = msg.State
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
