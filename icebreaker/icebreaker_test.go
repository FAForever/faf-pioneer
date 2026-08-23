package icebreaker

import (
	"context"
	"faf-pioneer/applog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
