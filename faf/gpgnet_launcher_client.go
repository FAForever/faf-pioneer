package faf

import (
	"bufio"
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"time"
)

type GpgNetLauncherClient struct {
	ctx          context.Context
	connection   *net.Conn
	server       *GpgNetLauncherServer
	logger       *logrus.Entry
	readChannel  chan<- *GpgMessage
	writeChannel chan *GpgMessage
}

func (s *GpgNetLauncherClient) listen(conn *net.Conn) {
	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(*conn)
	faStreamReader := NewFaStreamReader(bufferReader)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(*conn)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleFromAdapter(faStreamReader)
	go s.handleToAdapter(faStreamWriter)
}

func (s *GpgNetLauncherClient) handleFromAdapter(stream *StreamReader) {
	s.logger.Info("Waiting for incoming GPG-Net messages from adapter")

	// Read one message from the connection, process it and continue reading.
	for {
		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, io.EOF) {
			s.logger.Info("Closing GPG-Net connection from game (EOF reached)")
			return
		}

		if err != nil {
			s.logger.
				WithError(err).
				Error("Error parsing GPG-Net command from game, closing connection")
			return
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			s.logger.Info("Closing GPG-Net connection from game (EOF reached)")
			return
		}
		if err != nil {
			s.logger.
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
			s.logger.WithError(err).Error("Failed to parse GPG-Net message")
			// TODO: Forward unparsed?
		}

		// Process parsed GPG-Net command.
		parsedMsg = s.processMessage(parsedMsg)
		if parsedMsg != nil {
			s.readChannel <- &parsedMsg
		}
	}
}

func (s *GpgNetLauncherClient) handleToAdapter(stream *StreamWriter) {
	s.logger.Info("Waiting for GPG-Net messages from game to be forwarded to the adapter")

	for {
		select {
		case msg, ok := <-s.writeChannel:
			if !ok {
				return
			}

			s.logger.Debugf("Sending GPG-Net message '%s' to the adapter", (*msg).GetCommand())
			err := stream.WriteMessage(*msg)
			if errors.Is(err, net.ErrClosed) {
				s.logger.WithError(err).Error(
					"Failed to write GPG-Net message to the adapter, connection was closed")
				return
			}

			if err != nil {
				s.logger.WithError(err).Error("Failed to write GPG-Net message to the adapter")
			}
			if err = stream.w.Flush(); err != nil {
				s.logger.WithError(err).Error("Failed to flush GPG-Net message to game")
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *GpgNetLauncherClient) processMessage(rawMessage GpgMessage) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		s.logger.
			WithField("gameState", msg.State).
			Info("Received GameStateMessage")

		switch msg.State {
		case "Idle":
			// TODO: Player service emulation?
			createGameLobbyMessage := NewCreateLobbyMessage(
				0,
				60001,
				"Draiget",
				1,
			)

			s.sendMessage(createGameLobbyMessage)
			time.Sleep(time.Second)

			var hostGameMessage GpgMessage = &HostGameMessage{
				Command: "HostGame",
				MapName: "",
			}
			s.sendMessage(hostGameMessage)
			break
		case "Lobby":
			break
		}

		break
	case *GameFullMessage:
		s.logger.Info("Received GameFullMessage")
		break
	default:
		s.logger.
			WithField("command", msg.GetCommand()).
			Debug("Message command ignored")
	}

	return rawMessage
}

func (s *GpgNetLauncherClient) sendMessage(message GpgMessage) {
	s.writeChannel <- &message
}

func (s *GpgNetLauncherClient) Close() error {
	return (*s.connection).Close()
}
