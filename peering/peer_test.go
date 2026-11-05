package peering

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"faf-pioneer/applog"
	"faf-pioneer/icebreaker"
	"faf-pioneer/moho"
	"faf-pioneer/util"

	"github.com/pion/ice/v4"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "peering-test")
	if err != nil {
		panic(err)
	}
	if err := applog.Initialize(1, 1, 0, dir); err != nil {
		panic(err)
	}
	code := m.Run()
	applog.Shutdown()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func newTestPeer(t *testing.T, ctx context.Context, offerer bool, gameConn *net.UDPConn) *Peer {
	t.Helper()

	proxyPort, err := util.GetFreeUdpPort()
	require.NoError(t, err)

	gameToIce := make(chan []byte)
	iceToGame := make(chan []byte)
	gamePort := uint(gameConn.LocalAddr().(*net.UDPAddr).Port)
	proxy, err := moho.NewGameUDPProxy(ctx, gamePort, proxyPort, gameToIce, iceToGame)
	require.NoError(t, err)

	p := &Peer{
		ctx:              ctx,
		offerer:          offerer,
		peerId:           1,
		udpPort:          proxyPort,
		gameToIceChannel: gameToIce,
		iceToGameChannel: iceToGame,
		gameDataProxy:    proxy,
		onStateChanged:   func(*Peer, ice.ConnectionState) {},
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func listenGame(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Two peers connect using plain ICE (host candidates) and the game datagrams of one side arrive at the
// game of the other side. No WebRTC involved.
func TestPeersExchangeGameDataOverPlainIce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	gameA, gameB := listenGame(t), listenGame(t)
	a := newTestPeer(t, ctx, true, gameA)
	b := newTestPeer(t, ctx, false, gameB)

	a.onCandidatesGathered = func(s *icebreaker.IceSession, c []string, _ func() bool) {
		go func() { require.NoError(t, b.AddCandidates(&icebreaker.CandidatesMessage{Session: s, Candidates: c})) }()
	}
	b.onCandidatesGathered = func(s *icebreaker.IceSession, c []string, _ func() bool) {
		go func() { require.NoError(t, a.AddCandidates(&icebreaker.CandidatesMessage{Session: s, Candidates: c})) }()
	}

	require.NoError(t, a.ConnectOnce(nil))
	require.NoError(t, b.ConnectOnce(nil))

	// The game of A sends to the proxy of A, the game of B must receive it.
	toProxyA := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(a.GetUdpPort())}
	buffer := make([]byte, 1500)
	require.Eventually(t, func() bool {
		_, _ = gameA.WriteToUDP([]byte("hello"), toProxyA) // dropped until connected
		_ = gameB.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		n, _, err := gameB.ReadFromUDP(buffer)
		return err == nil && string(buffer[:n]) == "hello"
	}, 30*time.Second, 50*time.Millisecond)

	require.True(t, a.IsActive())
	_, _, ok := a.SelectedPair()
	require.True(t, ok)
}

func TestAddCandidatesBeforeConnectOnceIsKept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newTestPeer(t, ctx, false, listenGame(t))
	msg := &icebreaker.CandidatesMessage{Session: &icebreaker.IceSession{Ufrag: "u", Pwd: "p"}}
	require.NoError(t, p.AddCandidates(msg))
	require.Same(t, msg, p.pendingRemote)

	require.Error(t, p.AddCandidates(&icebreaker.CandidatesMessage{}), "missing credentials must be rejected")
	require.Error(t, p.AddCandidates(&icebreaker.CandidatesMessage{
		Session: &icebreaker.IceSession{Ufrag: "new"},
	}), "empty password must be rejected")
}

func TestParseIceServers(t *testing.T) {
	uris, err := ParseIceServers([]icebreaker.SessionGameResponseServer{{
		Username:   "user",
		Credential: "secret",
		Urls: []string{
			"stun://coturn.example:3478",
			"turn://coturn.example:3478?transport=udp",
			"turns://coturn.example:5349?transport=tcp",
		},
	}})
	require.NoError(t, err)
	require.Len(t, uris, 3)
	require.Empty(t, uris[0].Username)
	require.Equal(t, "user", uris[1].Username)
	require.Equal(t, "secret", uris[1].Password)
	require.Equal(t, "coturn.example", uris[1].Host)
	require.Equal(t, 3478, uris[1].Port)
	require.Equal(t, "user", uris[2].Username)

	_, err = ParseIceServers([]icebreaker.SessionGameResponseServer{{Urls: []string{"http://nope"}}})
	require.Error(t, err)
}

func TestConnectOnceAfterCloseDoesNotCreateAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newTestPeer(t, ctx, true, listenGame(t))
	require.NoError(t, p.Close())
	require.NoError(t, p.Close(), "closing twice must not panic")
	require.Error(t, p.ConnectOnce(nil))
	require.False(t, p.IsConnecting())
}

func TestStalledAttemptWithoutRemoteCandidatesEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	states := make(chan ice.ConnectionState, 8)
	p := newTestPeer(t, ctx, true, listenGame(t))
	p.onCandidatesGathered = func(*icebreaker.IceSession, []string, func() bool) {}
	p.onStateChanged = func(_ *Peer, s ice.ConnectionState) { states <- s }

	require.NoError(t, p.ConnectOnce(nil))
	require.True(t, p.IsConnecting())

	// What the timer does when no candidates ever arrived.
	p.mu.Lock()
	gen := p.agentGen
	p.mu.Unlock()
	p.handleStateChange(gen, ice.ConnectionStateClosed)
	require.Equal(t, ice.ConnectionStateClosed, <-states)
	require.False(t, p.IsConnecting(), "a stalled attempt must not block the next reconnection")
}

func TestOutdatedAgentIsNotCurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newTestPeer(t, ctx, true, listenGame(t))
	p.onCandidatesGathered = func(*icebreaker.IceSession, []string, func() bool) {}
	require.NoError(t, p.ConnectOnce(nil))
	p.mu.Lock()
	first := p.agentGen
	p.mu.Unlock()
	require.True(t, p.isCurrentAgent(first))

	require.NoError(t, p.ConnectOnce(nil))
	require.False(t, p.isCurrentAgent(first))
}
