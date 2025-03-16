package faf

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"log/slog"
	"net"
)

// GpgNetClient is using to establish communication as:
// FAF-Pioneer (ICE-Adapter) <--> FAF-Launcher.
// Only used for emulation purposes.
type GpgNetClient struct {
	ctx          context.Context
	connection   *net.Conn
	server       *GpgNetServer
	logger       *logrus.Entry
	port         uint
	state        string
	readChannel  chan *GpgMessage
	writeChannel chan *GpgMessage
}

func NewGpgNetClient(context context.Context, port uint) *GpgNetClient {
	return &GpgNetClient{
		ctx:   context,
		port:  port,
		state: "disconnected",
	}
}

func (s *GpgNetClient) Listen(readChannel chan *GpgMessage, writeChannel chan *GpgMessage) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return err
	}

	s.logger = logrus.WithFields(map[string]interface{}{
		"remoteAddress": conn.RemoteAddr().String(),
	})

	s.logger.Infof("GPG-Net client connected to parent GpgNetServer at port %d", s.port)

	s.connection = &conn
	s.readChannel = readChannel
	s.writeChannel = writeChannel

	go func() {
		for {
			select {
			case msg, ok := <-s.readChannel:
				if !ok {
					return
				}

				s.writeChannel <- msg
			case <-s.ctx.Done():
				return
			}
		}
	}()

	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(*s.connection)
	faStreamReader := NewFaStreamReader(bufferReader)

	go s.handleFromGame(faStreamReader)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(*s.connection)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleToGame(faStreamWriter)

	return nil
}

func (s *GpgNetClient) handleFromGame(stream *StreamReader) {
	s.logger.Info("Waiting for incoming GPG-Net messages from game")

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

func (s *GpgNetClient) handleToGame(stream *StreamWriter) {
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

func (s *GpgNetClient) processMessage(rawMessage GpgMessage) GpgMessage {
	switch msg := rawMessage.(type) {
	case *GameStateMessage:
		s.logger.
			WithField("state", msg.State).
			Info("Local GameState changed")

		s.state = msg.State

		if msg.State == "idle" {

		}
		break
	case *JoinGameMessage:
		slog.Info("Received joining game (swapping the address/port)")

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

		mappedAddress := ConnectToPeerMessage{
			Command:           msg.Command,
			RemotePlayerLogin: msg.RemotePlayerLogin,
			RemotePlayerId:    msg.RemotePlayerId,
			Destination:       fmt.Sprintf("127.0.0.1:%d", s.port),
		}

		var mappedMsg GpgMessage = &mappedAddress
		return mappedMsg
	default:
		s.logger.
			WithField("command", msg.GetCommand()).
			Debug("Message command ignored")
	}

	return rawMessage
}

func (s *GpgNetClient) Close() {
	err := (*s.connection).Close()
	if err != nil {
		logrus.WithError(err).Warn("Error on closing connection to parent GPG-Net server")
		return
	}
}
