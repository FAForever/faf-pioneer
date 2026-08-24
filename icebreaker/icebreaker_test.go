package icebreaker

import (
	"context"
	"errors"
	"faf-pioneer/applog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestListenReportsUnauthorizedResponseAsNotConnected(t *testing.T) {
	if err := applog.Initialize(1, 1, 0, t.TempDir()); err != nil {
		t.Fatalf("failed to initialize logging: %v", err)
	}
	t.Cleanup(applog.Shutdown)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	client := NewClient(context.Background(), server.URL, 1, "access-token")
	client.sessionToken = "session-token"

	connected, err := client.Listen(make(chan EventMessage))
	if connected {
		t.Fatal("rejected event connection was reported as connected")
	}
	if err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Fatalf("expected 401 connection error, got %v", err)
	}
}

func TestListenDoesNotLeakContextWatchersAfterReconnects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, server.URL, 1, "access-token")
	client.sessionToken = "session-token"

	watchersBefore := countListenContextWatchers()
	for range 10 {
		_, err := client.Listen(make(chan EventMessage))
		if err == nil {
			t.Fatal("expected the closed event stream to return an error")
		}
	}

	deadline := time.Now().Add(time.Second)
	for countListenContextWatchers() > watchersBefore && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if watchersAfter := countListenContextWatchers(); watchersAfter > watchersBefore {
		t.Fatalf("event reconnects leaked %d context watcher goroutines", watchersAfter-watchersBefore)
	}
}

func TestListenPreservesContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"eventType\":\"connected\",\"gameId\":1,\"senderId\":2}\n\n"))
		flusher.Flush()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				_, _ = w.Write([]byte(": keepalive\n\n"))
				flusher.Flush()
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, server.URL, 1, "access-token")
	client.sessionToken = "session-token"
	events := make(chan EventMessage, 1)

	listenDone := make(chan error, 1)
	go func() {
		_, err := client.Listen(events)
		listenDone <- err
	}()

	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the event stream to deliver a message")
	}
	cancel()

	select {
	case err := <-listenDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("event listener did not stop after context cancellation")
	}
}

func countListenContextWatchers() int {
	stack := make([]byte, 1<<20)
	length := runtime.Stack(stack, true)
	return strings.Count(string(stack[:length]), "faf-pioneer/icebreaker.(*Client).Listen.func")
}
