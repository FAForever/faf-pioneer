package faf

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

type GpgNetLauncherClient struct {
	ctx          context.Context
	connection   *net.Conn
	server       *GpgNetLauncherServer
	logger       *logrus.Entry
	readChannel  chan<- *GpgMessage
	writeChannel chan *GpgMessage
}

func (s *GpgNetLauncherClient) listen(reader *StreamReader, writer *StreamWriter) {
	s.readChannel = make(chan *GpgMessage)
	s.writeChannel = make(chan *GpgMessage)

	go s.handleFromAdapter(reader)
	go s.handleToAdapter(writer)
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
		}

		// Process parsed GPG-Net command.
		parsedMsg = s.processMessage(parsedMsg)
		if parsedMsg != nil {
			s.readChannel <- &parsedMsg
		}
	}
}

func (s *GpgNetLauncherClient) handleToAdapter(stream *StreamWriter) {
	s.logger.Info("Waiting for GPG-Net messages to be forwarded to the game")

	for {
		select {
		case msg, ok := <-s.writeChannel:
			if !ok {
				return
			}

			err := stream.WriteMessage(*msg)
			if err != nil {
				s.logger.WithError(err).Error("Failed to write GPG-Net message to game")
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
		case "idle":
			// TODO: Player service emulation?

			var createGameLobbyMessage GpgMessage = &CreateLobbyMessage{
				Command:          "CreateLobby",
				LobbyInitMode:    0,
				LobbyPort:        60001,
				LocalPlayerName:  "p4block",
				LocalPlayerId:    1, //18746,
				UnknownParameter: 1,
			}

			s.sendMessage(createGameLobbyMessage)
			break
		case "lobby":
			break
		}

		return msg
	case *GameFullMessage:
		s.logger.Info("Received GameFullMessage")

		return msg
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
