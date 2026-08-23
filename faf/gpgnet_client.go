package faf

import (
	"bufio"
	"context"
	"errors"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net"
)

// GpgNetClient is using to establish communication as:
// FAF-Pioneer (ICE-Adapter) <--> FAF-Launcher.
// Only used for emulation purposes.
type GpgNetClient struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	connection           net.Conn
	server               *GpgNetServer
	port                 uint
	state                gpgnet.GameState
	toFafClientChannel   chan gpgnet.Message
	fromFafClientChannel chan gpgnet.Message
}

func NewGpgNetClient(parentContext context.Context, port uint) *GpgNetClient {
	ctx, cancel := context.WithCancel(parentContext)
	return &GpgNetClient{
		ctx:    ctx,
		cancel: cancel,
		port:   port,
		state:  "disconnected",
	}
}

func (s *GpgNetClient) Connect(toFafClientChannel chan gpgnet.Message, fromFafClientChannel chan gpgnet.Message) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return err
	}

	s.ctx = applog.AddContextFields(
		s.ctx,
		zap.String("launcherPort", fmt.Sprintf("%d", s.port)),
	)

	applog.FromContext(s.ctx).Info(
		fmt.Sprintf("GPG-Net client connected to parent GpgNetServer (FAF-Client)"),
	)

	// Channel `fromFafClientChannel` is being redirected to `gpgNetToGame`
	// All the messages written to `fromFafClientChannel` are redirected to the FAF.exe.

	// Channel `gpgNetFromGame` is being redirected to `gpgNetToFafClient`
	// All the messages coming from FAF.exe are passing to FAF-Client (toFafClientChannel).

	// Socket connection below handles connectivity between FAF-Pioneer and FAF-Client.
	s.connection = conn

	s.toFafClientChannel = toFafClientChannel
	s.fromFafClientChannel = fromFafClientChannel

	// Wrap connection to FAF-Client into buffered reader.
	bufferReader := bufio.NewReader(s.connection)
	faStreamReader := NewFaStreamReader(bufferReader)

	// Wrap connection to FAF-Client into buffered writer.
	bufferedWriter := bufio.NewWriter(s.connection)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	go s.handleFromClient(faStreamReader)
	go s.handleToClient(faStreamWriter)

	<-s.ctx.Done()
	return nil
}

func (s *GpgNetClient) handleFromClient(stream *StreamReader) {
	applog.FromContext(s.ctx).Info(
		"Waiting for incoming GPG-Net messages from FAF-Client",
	)

	// Read one message from the connection, process it and continue reading.
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// First, read length-prefixed string from the stream to determine chunks size.
		command, err := stream.ReadString()
		if errors.Is(err, io.EOF) {
			applog.FromContext(s.ctx).Info(
				"Closing GPG-Net connection from FAF-Client (EOF reached)",
				zap.Error(err),
			)
			s.closeCurrentConnection()
			return
		}

		if err != nil {
			applog.FromContext(s.ctx).Error(
				"Error reading GPG-Net command from FAF-Client, closing connection",
				zap.Error(err),
			)
			s.closeCurrentConnection()
			return
		}

		// Then, read the "chunks" (actual message data).
		chunks, err := stream.ReadChunks()
		if errors.Is(err, io.EOF) {
			applog.FromContext(s.ctx).Info(
				"Closing GPG-Net connection from FAF-Client (EOF reached)",
				zap.Error(err),
			)
			s.closeCurrentConnection()
			return
		}
		if err != nil {
			applog.FromContext(s.ctx).Error(
				"Error reading GPG-Net command chunks from FAF-Client, closing connection",
				zap.Error(err),
			)
			s.closeCurrentConnection()
			return
		}

		unparsedMsg := &gpgnet.BaseMessage{
			Command: command,
			Args:    chunks,
		}

		// Write all the messages from FAF-client to `fromFafClientChannel` which is redirected to
		// game channel `gpgNetToGame`.
		// CreateLobby, HostGame, JoinGame, ConnectToPeer, DisconnectFromPeer, and other messages
		// will be directly forwarded from FAF-Client to FAF.exe.
		s.fromFafClientChannel <- unparsedMsg
	}
}

func (s *GpgNetClient) handleToClient(stream *StreamWriter) {
	applog.FromContext(s.ctx).Info(
		"Waiting for GPG-Net messages to be forwarded to the FAF-Client",
	)

	for {
		select {
		case msg, ok := <-s.toFafClientChannel:
			if !ok {
				applog.FromContext(s.ctx).Debug(
					"Channel (toFafClientChannel) closed, GpgNetClient::handleToClient aborted",
				)
				s.closeCurrentConnection()
				return
			}

			applog.FromContext(s.ctx).Debug(
				fmt.Sprintf("Forwarding GPG-Net message '%s' from game (toFafClientChannel) to FAF-Client",
					msg.GetCommand()),
			)

			err := stream.WriteMessage(msg)
			if err != nil {
				applog.FromContext(s.ctx).Error(
					"Failed to write GPG-Net message to the FAF-Client",
					zap.Error(err),
				)
			}
			if err = stream.w.Flush(); err != nil {
				applog.FromContext(s.ctx).Error(
					"Failed to flush GPG-Net message to the FAF-Client",
					zap.Error(err),
				)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *GpgNetClient) closeCurrentConnection() {
	s.cancel()
}

func (s *GpgNetClient) Close() {
	err := s.connection.Close()
	if err != nil {
		applog.FromContext(s.ctx).Error(
			"Error on closing client connection to parent GPG-Net server",
			zap.Error(err),
		)
		return
	}
}
