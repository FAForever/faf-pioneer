package faf

import (
	"bufio"
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net"
)

type GpgNetLauncherClient struct {
	ctx                  context.Context
	connection           net.Conn
	connCancel           context.CancelFunc
	server               *GpgNetLauncherServer
	loggerFields         []zap.Field
	fafClientToAdapter   chan<- *GpgMessage
	fafClientFromAdapter chan *GpgMessage
}

func (s *GpgNetLauncherClient) listen(conn net.Conn) {
	clientCtx, cancel := context.WithCancel(s.ctx)
	s.connCancel = cancel

	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(conn)
	faStreamReader := NewFaStreamReader(bufferReader)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(conn)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleFromAdapter(clientCtx, faStreamReader)
	go s.handleToAdapter(clientCtx, faStreamWriter)
}

func (s *GpgNetLauncherClient) handleFromAdapter(ctx context.Context, stream *StreamReader) {
	applog.Info("Waiting for incoming GPG-Net messages from adapter", s.loggerFields...)

	// Read one message from the connection, process it and continue reading.
	for {
		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, net.ErrClosed) {
			applog.Info(
				"Closing GPG-Net connection from adapter (remotely closed)",
				s.loggerFields...,
			)
			_ = s.Close()
			return
		}

		if errors.Is(err, io.EOF) {
			applog.Info(
				"Closing GPG-Net connection from adapter (EOF reached)",
				s.loggerFields...,
			)
			_ = s.Close()
			return
		}

		if err != nil {
			applog.Error(
				"Error parsing GPG-Net command from adapter, closing connection",
				append(s.loggerFields, zap.Error(err))...,
			)
			_ = s.Close()
			return
		}

		select {
		case <-ctx.Done():
			applog.Debug("Context canceled in handleFromAdapter, stopping read loop", s.loggerFields...)
			_ = s.Close()
			return
		default:
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			applog.Info(
				"Closing GPG-Net connection from adapter (EOF reached)",
				s.loggerFields...,
			)
			_ = s.Close()
			return
		}
		if err != nil {
			applog.Error(
				"Error parsing GPG-Net command chunks from adapter, closing connection",
				append(s.loggerFields, zap.Error(err))...,
			)
			_ = s.Close()
			return
		}

		select {
		case <-ctx.Done():
			applog.Debug("Context canceled in handleFromAdapter, stopping read loop", s.loggerFields...)
			_ = s.Close()
			return
		default:
		}

		unparsedMsg := GenericGpgMessage{
			Command: command,
			Args:    chunks,
		}

		// Try to parse GPG-Net message based on the command type/name.
		parsedMsg, err := unparsedMsg.TryParse()
		if err != nil {
			applog.Error(
				"Failed to parse GPG-Net message from adapter",
				append(s.loggerFields, zap.Error(err))...,
			)
			// TODO: Forward unparsed?
		}

		// Process parsed GPG-Net command.
		parsedMsg = s.processMessage(parsedMsg)
		if parsedMsg != nil {
			s.fafClientToAdapter <- &parsedMsg
		}
	}
}

func (s *GpgNetLauncherClient) handleToAdapter(ctx context.Context, stream *StreamWriter) {
	applog.Info(
		"Waiting for GPG-Net messages from game to be forwarded to the adapter",
		s.loggerFields...,
	)

	for {
		select {
		case msg, ok := <-s.fafClientFromAdapter:
			if !ok {
				applog.Debug(
					"Channel (fafClientFromAdapter) closed, GpgNetLauncherClient::handleToAdapter aborted",
					s.loggerFields...,
				)
				_ = s.Close()
				return
			}

			applog.Debug(
				fmt.Sprintf(
					"Forwarding GPG-Net message '%s' in server from (fafClientFromAdapter) to the adapter",
					(*msg).GetCommand()),
				s.loggerFields...,
			)

			err := stream.WriteMessage(*msg)
			if errors.Is(err, net.ErrClosed) {
				applog.Error(
					"Failed to write GPG-Net message to the adapter, connection was closed",
					append(s.loggerFields, zap.Error(err))...,
				)
				_ = s.Close()
				return
			}

			if err != nil {
				applog.Error(
					"Failed to write GPG-Net message to the adapter",
					append(s.loggerFields, zap.Error(err))...,
				)
			}
			if err = stream.w.Flush(); err != nil {
				applog.Error(
					"Failed to flush GPG-Net message to game",
					append(s.loggerFields, zap.Error(err))...,
				)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *GpgNetLauncherClient) processMessage(rawMessage GpgMessage) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		applog.Info(
			"Received game state changed",
			append(s.loggerFields, zap.String("gameState", msg.State))...,
		)

		switch msg.State {
		case GameStateIde:
			// TODO: Player service emulation?
			freePort, _ := util.GetFreeUdpPort()

			createGameLobbyMessage := NewCreateLobbyMessage(
				LobbyInitModeNormal,
				uint16(freePort),
				"Draiget",
				1,
			)

			s.sendMessage(createGameLobbyMessage)

			var hostGameMessage GpgMessage = &HostGameMessage{
				Command: GpgMessageCommandHostGame,
				MapName: "",
			}
			s.sendMessage(hostGameMessage)
		case GameStateLobby:
		}

		break
	case *GameFullMessage:
		applog.Info(
			"Received GameFullMessage",
			s.loggerFields...,
		)
		break
	default:
		applog.Debug(
			"Message command ignored",
			append(s.loggerFields, zap.String("command", msg.GetCommand()))...,
		)
	}

	return rawMessage
}

func (s *GpgNetLauncherClient) sendMessage(message GpgMessage) {
	s.fafClientFromAdapter <- &message
}

func (s *GpgNetLauncherClient) Close() error {
	s.connCancel()
	return s.connection.Close()
}
