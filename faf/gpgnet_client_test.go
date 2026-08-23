package faf

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"net"
	"testing"
	"time"
)

func TestGpgNetClientConnectWaitsForDisconnect(t *testing.T) {
	if err := applog.Initialize(1, 1, 0, t.TempDir()); err != nil {
		t.Fatalf("failed to initialize logging: %v", err)
	}
	t.Cleanup(applog.Shutdown)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	port := uint(listener.Addr().(*net.TCPAddr).Port)
	client := NewGpgNetClient(ctx, port)
	connectDone := make(chan error, 1)
	go func() {
		connectDone <- client.Connect(make(chan gpgnet.Message), make(chan gpgnet.Message))
	}()

	var conn net.Conn
	select {
	case conn = <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("client did not connect")
	}

	select {
	case connectErr := <-connectDone:
		t.Fatalf("Connect returned before the launcher disconnected: %v", connectErr)
	case <-time.After(50 * time.Millisecond):
	}

	if err = conn.Close(); err != nil {
		t.Fatalf("failed to close launcher connection: %v", err)
	}

	select {
	case connectErr := <-connectDone:
		if connectErr != nil {
			t.Fatalf("Connect returned an error after disconnect: %v", connectErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Connect did not return after the launcher disconnected")
	}
}
