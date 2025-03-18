package faf

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net"
)

type GameCommand interface {
	GetInitiatePackets() []gpgnet.Message
}

type GameCommandHostGame struct {
	PlayerId   uint32
	PlayerName string
}

func (gc *GameCommandHostGame) GetInitiatePackets() []gpgnet.Message {
	freePort, _ := util.GetFreeUdpPort()

	return []gpgnet.Message{
		gpgnet.NewCreateLobbyMessage(
			gpgnet.LobbyInitModeNormal,
			uint16(freePort),
			gc.PlayerName,
			gc.PlayerId,
		),
		gpgnet.NewHostGameMessage(""),
	}
}

type GameCommandJoinGame struct {
	LocalPlayerId     uint32
	LocalPlayerName   string
	RemotePlayerLogin string
	RemotePlayerId    uint
	Destination       string
}

func (gc *GameCommandJoinGame) GetInitiatePackets() []gpgnet.Message {
	freePort, _ := util.GetFreeUdpPort()

	return []gpgnet.Message{
		gpgnet.NewCreateLobbyMessage(
			gpgnet.LobbyInitModeNormal,
			uint16(freePort),
			gc.LocalPlayerName,
			gc.LocalPlayerId,
		),
		gpgnet.NewJoinGameMessage(
			gc.RemotePlayerLogin,
			gc.RemotePlayerId,
			gc.Destination,
		),
	}
}

type GpgNetLauncherServer struct {
	ctx                  context.Context
	port                 uint
	tcpListener          net.Listener
	state                gpgnet.GameState
	loggerFields         []zap.Field
	fafClientToAdapter   chan<- gpgnet.Message
	fafClientFromAdapter chan gpgnet.Message
	currentClient        *GpgNetLauncherClient
	initialCommand       GameCommand
}

func NewGpgNetLauncherServer(context context.Context, port uint) *GpgNetLauncherServer {
	return &GpgNetLauncherServer{
		ctx:   context,
		port:  port,
		state: gpgnet.GameStateNone,
	}
}

func (s *GpgNetLauncherServer) Listen(
	fafClientToAdapter chan<- gpgnet.Message,
	fafClientFromAdapter chan gpgnet.Message,
) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	applog.Info("Listening GPG-Net launcher server", zap.Uint("port", s.port))

	s.tcpListener = listener
	s.fafClientToAdapter = fafClientToAdapter
	s.fafClientFromAdapter = fafClientFromAdapter

	for {
		conn, acceptErr := util.NetAcceptWithContext(s.ctx, listener)
		if acceptErr != nil {
			if s.ctx.Err() != nil {
				applog.Debug("Context canceled, stopping accepting launcher server connections")
				return nil
			}

			applog.Error("Failed to accept new GPG-Net adapter connection", zap.Error(err))
			continue
		}

		if s.currentClient != nil {
			_ = s.currentClient.Close()
		}

		s.currentClient = s.acceptConnection(conn)
	}
}

func (s *GpgNetLauncherServer) acceptConnection(conn net.Conn) *GpgNetLauncherClient {
	s.loggerFields = []zapcore.Field{
		zap.Uint("listenPort", s.port),
		zap.String("remoteAddr", conn.RemoteAddr().String()),
	}

	client := &GpgNetLauncherClient{
		ctx:                  s.ctx,
		connection:           conn,
		server:               s,
		loggerFields:         s.loggerFields,
		fafClientToAdapter:   s.fafClientToAdapter,
		fafClientFromAdapter: s.fafClientFromAdapter,
	}

	applog.Info("Adapter connected to the launcher server", s.loggerFields...)

	client.listen(conn)
	return client
}

func (s *GpgNetLauncherServer) Close() error {
	if s.currentClient != nil {
		return s.currentClient.Close()
	}

	err := s.tcpListener.Close()
	if err != nil {
		applog.Error(
			"Failed to close launcher server listener",
			append(s.loggerFields, zap.Error(err))...,
		)
	}

	return err
}

func (s *GpgNetLauncherServer) SetGameCommand(command GameCommand) {
	s.initialCommand = command
}
