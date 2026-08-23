package adapter

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/launcher"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdapterStopsWhenLauncherDisconnects(t *testing.T) {
	if err := applog.Initialize(1, 1, 0, t.TempDir()); err != nil {
		t.Fatalf("failed to initialize logging: %v", err)
	}
	t.Cleanup(applog.Shutdown)

	icebreaker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"jwt":"session-token"}`)
		case "/session/game/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"id":"1","forceRelay":false,"servers":[]}`)
		case "/session/game/1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			flusher.Flush()

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					_, _ = fmt.Fprint(w, ": keepalive\n\n")
					flusher.Flush()
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer icebreaker.Close()

	launcherListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for adapter: %v", err)
	}
	defer launcherListener.Close()

	gamePort := freeTCPPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := New(ctx, cancel, &launcher.Info{
		UserId:           1,
		UserName:         "test-user",
		GameId:           1,
		AccessToken:      "access-token",
		ApiRoot:          icebreaker.URL,
		GpgNetPort:       gamePort,
		GpgNetClientPort: uint(launcherListener.Addr().(*net.TCPAddr).Port),
	})

	startDone := make(chan error, 1)
	go func() {
		startDone <- instance.Start()
	}()

	launcherConnection, err := launcherListener.Accept()
	if err != nil {
		t.Fatalf("adapter did not connect to launcher: %v", err)
	}

	select {
	case startErr := <-startDone:
		_ = launcherConnection.Close()
		t.Fatalf("adapter stopped before launcher disconnected: %v", startErr)
	case <-time.After(50 * time.Millisecond):
	}

	if err = launcherConnection.Close(); err != nil {
		t.Fatalf("failed to close launcher connection: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("adapter context was not canceled after launcher disconnected")
	}

	select {
	case startErr := <-startDone:
		if startErr != nil {
			t.Fatalf("adapter returned an error after launcher disconnected: %v", startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not stop after launcher disconnected")
	}
}

func freeTCPPort(t *testing.T) uint {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve game port: %v", err)
	}
	port := uint(listener.Addr().(*net.TCPAddr).Port)
	if err = listener.Close(); err != nil {
		t.Fatalf("failed to release game port: %v", err)
	}
	return port
}
