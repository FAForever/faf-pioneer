package icebreaker

import (
	"context"
	"errors"
	"faf-pioneer/applog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetGameSessionRegistersAvailableAddressFamily(t *testing.T) {
	var registrations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/session/game/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"1","forceRelay":false,"servers":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session/game/1/addresses":
			if got := r.Header.Get("Authorization"); got != "Bearer session-token" {
				t.Errorf("unexpected authorization header %q", got)
			}
			registrations.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(context.Background(), server.URL, 1, "access-token")
	client.sessionToken = "session-token"

	session, err := client.GetGameSession()
	if err != nil {
		t.Fatalf("getting game session failed: %v", err)
	}
	if session.Id != "1" {
		t.Fatalf("unexpected game session id %q", session.Id)
	}
	if got := registrations.Load(); got != 1 {
		t.Fatalf("expected the available IPv4 family to be registered once, got %d registrations", got)
	}
}

func TestRegisterAddressesUsesBothFamiliesConcurrently(t *testing.T) {
	const accessToken = "e30.eyJleHQiOnsiaG1hYyI6InNlY3JldCJ9fQ.signature"
	var registrations atomic.Int32
	bothArrived := make(chan struct{})
	requestErrors := make(chan string, 8)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/session/game/1/addresses" {
			requestErrors <- "unexpected request " + r.Method + " " + r.URL.Path
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer session-token" {
			requestErrors <- "unexpected authorization header " + got
		}
		if got := r.Header.Get("X-HMAC"); got != "secret" {
			requestErrors <- "unexpected X-HMAC header " + got
		}

		if registrations.Add(1) == 2 {
			close(bothArrived)
		}
		select {
		case <-bothArrived:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, server.URL, 1, accessToken)
	client.sessionToken = "session-token"
	client.addressRegistrationTimeout = time.Second
	client.addressRegistrationClients = []addressRegistrationClient{
		{addressFamily: "IPv4", httpClient: newHTTPClient(accessToken, "")},
		{addressFamily: "IPv6", httpClient: newHTTPClient(accessToken, "")},
	}

	done := make(chan struct{})
	go func() {
		client.registerAddresses()
		close(done)
	}()

	select {
	case <-bothArrived:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("address-family registrations were not in flight concurrently")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("address-family registrations did not complete")
	}

	close(requestErrors)
	for requestError := range requestErrors {
		t.Error(requestError)
	}
	if got := registrations.Load(); got != 2 {
		t.Fatalf("expected both address families to be registered, got %d registrations", got)
	}
}

func TestGetGameSessionIgnoresAddressRegistrationRejection(t *testing.T) {
	var registrations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/session/game/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"1","forceRelay":false,"servers":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session/game/1/addresses":
			registrations.Add(1)
			http.Error(w, "not deployed", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(context.Background(), server.URL, 1, "access-token")
	client.sessionToken = "session-token"

	if _, err := client.GetGameSession(); err != nil {
		t.Fatalf("address registration rejection prevented game session setup: %v", err)
	}
	if got := registrations.Load(); got != 1 {
		t.Fatalf("expected the available IPv4 family to attempt registration once, got %d", got)
	}
}

func TestRegisterAddressesHasBoundedWait(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewClient(context.Background(), server.URL, 1, "access-token")
	client.sessionToken = "session-token"
	client.addressRegistrationTimeout = 50 * time.Millisecond
	client.addressRegistrationClients = []addressRegistrationClient{
		{addressFamily: "IPv4", httpClient: newHTTPClient("access-token", "")},
		{addressFamily: "IPv6", httpClient: newHTTPClient("access-token", "")},
	}

	startedAt := time.Now()
	client.registerAddresses()
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("address registration exceeded its bounded wait: %v", elapsed)
	}
}

func TestGetGameSessionRegistersAvailableIPv6Family(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}

	var registrations atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/session/game/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"1","forceRelay":false,"servers":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session/game/1/addresses":
			registrations.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	if err := server.Listener.Close(); err != nil {
		t.Fatalf("closing default test listener failed: %v", err)
	}
	server.Listener = listener
	server.Start()
	defer server.Close()

	client := NewClient(context.Background(), server.URL, 1, "access-token")
	client.sessionToken = "session-token"

	session, err := client.GetGameSession()
	if err != nil {
		t.Fatalf("getting game session over IPv6 failed: %v", err)
	}
	if session.Id != "1" {
		t.Fatalf("unexpected game session id %q", session.Id)
	}
	if got := registrations.Load(); got != 1 {
		t.Fatalf("expected the available IPv6 family to be registered once, got %d registrations", got)
	}
}

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
	for size := 1 << 20; ; size *= 2 {
		stack := make([]byte, size)
		if length := runtime.Stack(stack, true); length < size {
			return strings.Count(string(stack[:length]), "faf-pioneer/icebreaker.(*Client).Listen.func")
		}
	}
}
