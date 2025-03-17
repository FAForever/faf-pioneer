package faf

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
)

// GpgNetClient is using to establish communication as:
// FAF-Pioneer (ICE-Adapter) <--> FAF-Launcher.
// Only used for emulation purposes.
type GpgNetClient struct {
	ctx                  context.Context
	connection           *net.Conn
	server               *GpgNetServer
	logger               *logrus.Entry
	port                 uint
	state                string
	toFafClientChannel   chan *GpgMessage
	fromFafClientChannel chan *GpgMessage
}

func NewGpgNetClient(context context.Context, port uint) *GpgNetClient {
	return &GpgNetClient{
		ctx:   context,
		port:  port,
		state: "disconnected",
	}
}

func (s *GpgNetClient) Listen(toFafClientChannel chan *GpgMessage, fromFafClientChannel chan *GpgMessage) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return err
	}

	s.logger = logrus.WithFields(map[string]interface{}{
		"remoteAddress": conn.RemoteAddr().String(),
	})

	s.logger.Infof("GPG-Net client connected to parent GpgNetServer at port %d", s.port)

	// Channel `fromFafClientChannel` is being redirected to `gpgNetToGame`
	// All the messages written to `fromFafClientChannel` are redirected to the FAF.exe.

	// Channel `gpgNetFromGame` is being redirected to `gpgNetToFafClient`
	// All the messages coming from FAF.exe are passing to FAF-Client (toFafClientChannel).

	// Socket connection below handles connectivity between FAF-Pioneer and FAF-Client.
	s.connection = &conn

	s.toFafClientChannel = toFafClientChannel
	s.fromFafClientChannel = fromFafClientChannel

	// Wrap connection to FAF-Client into buffered reader.
	bufferReader := bufio.NewReader(*s.connection)
	faStreamReader := NewFaStreamReader(bufferReader)

	go s.handleFromClient(faStreamReader)

	// Wrap connection to FAF-Client into buffered writer.
	bufferedWriter := bufio.NewWriter(*s.connection)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleToClient(faStreamWriter)

	return nil
}

func (s *GpgNetClient) handleFromClient(stream *StreamReader) {
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

		var baseMessage GpgMessage = &unparsedMsg

		// Write all the messages from FAF-client to `fromFafClientChannel` which is redirected to
		// game channel `gpgNetToGame`.
		// CreateLobby, HostGame, JoinGame, ConnectToPeer, DisconnectFromPeer, and other messages
		// will be directly forwarded from FAF-Client to FAF.exe.
		s.fromFafClientChannel <- &baseMessage
	}
}

func (s *GpgNetClient) handleToClient(stream *StreamWriter) {
	s.logger.Info("Waiting for GPG-Net messages to be forwarded to the client")

	for {
		select {
		case msg, ok := <-s.toFafClientChannel:
			if !ok {
				return
			}

			s.logger.Debugf(
				"Forwarding GPG-Net message from game (toFafClientChannel) '%s' to client",
				(*msg).GetCommand())

			err := stream.WriteMessage(*msg)
			if err != nil {
				s.logger.WithError(err).Error("Failed to write GPG-Net message to game")
			}
			if err = stream.w.Flush(); err != nil {
				s.logger.WithError(err).Error("Failed to flush GPG-Net message to game")
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *GpgNetClient) Close() {
	err := (*s.connection).Close()
	if err != nil {
		logrus.WithError(err).Warn("Error on closing connection to parent GPG-Net server")
		return
	}
}
