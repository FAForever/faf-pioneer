package faf

import (
	"bufio"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
)

type GpgNetLauncherServer struct {
	ctx                  context.Context
	port                 uint
	tcpListener          *net.Listener
	state                string
	fafClientToAdapter   chan<- *GpgMessage
	fafClientFromAdapter chan *GpgMessage
	currentClientMu      sync.Mutex
	currentClient        *GpgNetLauncherClient
}

func NewGpgNetLauncherServer(context context.Context, port uint) *GpgNetLauncherServer {
	return &GpgNetLauncherServer{
		ctx:   context,
		port:  port,
		state: "disconnected",
	}
}

func (s *GpgNetLauncherServer) Listen(fafClientToAdapter chan<- *GpgMessage, fafClientFromAdapter chan *GpgMessage) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(s.ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", s.port, err)
	}

	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	logrus.Infof("Listening GPG-Net launcher server at port :%d", s.port)

	s.tcpListener = &listener
	s.fafClientToAdapter = fafClientToAdapter
	s.fafClientFromAdapter = fafClientFromAdapter

	for {
		conn, err := listener.Accept()
		if err != nil {
			logrus.Errorf("failed to accept GPG-Net adapter connection: %v", err)
			continue
		}

		s.currentClientMu.Lock()
		if s.currentClient != nil {
			_ = s.currentClient.Close()
		}

		s.currentClient = s.acceptConnection(conn)
		s.currentClientMu.Unlock()
	}
}

func (s *GpgNetLauncherServer) acceptConnection(conn net.Conn) *GpgNetLauncherClient {
	logger := logrus.WithFields(map[string]interface{}{
		"remoteAddress": conn.RemoteAddr().String(),
	})

	client := &GpgNetLauncherClient{
		ctx:          s.ctx,
		connection:   &conn,
		server:       s,
		logger:       logger,
		readChannel:  s.fafClientToAdapter,
		writeChannel: s.fafClientFromAdapter,
	}

	logger.Infof("Adapter connected to the launcher server from %s", conn.RemoteAddr().String())

	// Wrap the connection in a buffered reader.
	bufferReader := bufio.NewReader(conn)
	faStreamReader := NewFaStreamReader(bufferReader)

	// Wrap second goroutine with GPG-Net messages forwarder to game.
	bufferedWriter := bufio.NewWriter(conn)
	faStreamWriter := NewFaStreamWriter(bufferedWriter)

	client.listen(faStreamReader, faStreamWriter)
	return client
}

func (s *GpgNetLauncherServer) Close() error {
	err := (*s.tcpListener).Close()
	if err != nil {
		logrus.Error("Failed to close launcher server listener")
	}

	s.currentClientMu.Lock()
	defer s.currentClientMu.Unlock()
	if s.currentClient != nil {
		return s.currentClient.Close()
	}

	return nil
}
