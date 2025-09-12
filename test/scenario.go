package test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Scenario struct {
	Name  string
	Steps []ScenarioStep
}

type ScenarioStep interface {
	Run(s *ScenarioContext, st *execState) error
	String() string
}

type HostStep struct {
	UID     int
	Restart bool
}

type JoinStep struct {
	UID            int
	HostUID        int
	DelayAfterJoin time.Duration
	AfterJoin      []ScenarioStep
}

type JoinManyStep struct {
	UIDs          []int
	HostUID       int
	DelayBetween  time.Duration
	AfterEachJoin []ScenarioStep
}

type DropKind int

const (
	DropSoft          DropKind = iota // "disconnect <peerUID>" — clean disconnect without killing processes
	DropQuit                          // "quit --with-exe" — gracefully close the game/GPGNet (legacy DropSoft behavior)
	DropCrashLauncher                 // kill launcher
	DropCrashAdapter                  // kill adapter
	DropHardKillBoth                  // kill both processes
)

type KillStep struct {
	UID       int
	Kind      DropKind
	TargetUID int
}

type WaitStep struct {
	Delay time.Duration
}

type ParallelStep struct {
	Steps []ScenarioStep
	Until []Condition

	MeshNonHost bool
	MeshWait    time.Duration
}

type EndStep struct{}

type Condition interface {
	Wait(s *ScenarioContext, st *execState, to time.Duration) error
	String() string
}

type CondLobbyCreated struct {
	HostUID int
	Timeout time.Duration
}

func (c CondLobbyCreated) String() string {
	return fmt.Sprintf("CondLobbyCreated{host=%d}", c.HostUID)
}

func (c CondLobbyCreated) Wait(s *ScenarioContext, st *execState, to time.Duration) error {
	host := st.Host
	if c.HostUID != 0 {
		h, _, err := s.PlayerByUID(c.HostUID)
		if err != nil {
			return err
		}
		host = h
	}
	if host == nil {
		return errors.New("CondLobbyCreated: no host")
	}
	d := c.Timeout
	if d <= 0 {
		d = to
		if d <= 0 {
			d = 8 * time.Second
		}
	}
	return s.WaitForLobbyCreated(host, d)
}

type CondHostSwappedWith struct {
	HostUID int
	UIDs    []int
	Timeout time.Duration
}

func (c CondHostSwappedWith) String() string {
	return fmt.Sprintf("CondHostSwappedWith{host=%d uids=%v}", c.HostUID, c.UIDs)
}

func (c CondHostSwappedWith) Wait(s *ScenarioContext, st *execState, to time.Duration) error {
	host, _, err := s.PlayerByUID(c.HostUID)
	if err != nil {
		return err
	}

	d := c.Timeout
	if d <= 0 {
		d = to
		if d <= 0 {
			d = 12 * time.Second
		}
	}
	deadline := time.Now().Add(d)

	need := map[int]struct{}{}
	for _, uid := range c.UIDs {
		need[uid] = struct{}{}
	}

	check := func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for uid := range need {
			if m := s.linkSwap[host.UID]; m == nil || m[uid].IsZero() {
				return false
			}
		}
		return true
	}

	if check() {
		return nil
	}
	for time.Now().Before(deadline) {
		if check() {
			return nil
		}
		time.Sleep(120 * time.Millisecond)
	}
	return fmt.Errorf("host %d not swapped with %v within %s", host.UID, c.UIDs, d)
}

type execState struct {
	Host        *Player
	LastJoiner  *Player
	startedUIDs map[int]struct{}
}

func (st *execState) markStarted(uid int) {
	if st.startedUIDs == nil {
		st.startedUIDs = map[int]struct{}{}
	}
	st.startedUIDs[uid] = struct{}{}
}

func smallGap(s *ScenarioContext) time.Duration {
	if s.cfg.StepDelay > 0 {
		return s.cfg.StepDelay
	}
	return 120 * time.Millisecond
}

func RunScenario(s *ScenarioContext, sc Scenario) error {
	st := &execState{startedUIDs: map[int]struct{}{}}
	fmt.Printf("=== scenario: %s ===\n", sc.Name)
	for _, step := range sc.Steps {
		fmt.Printf("-- Running step (%s) ...\n", step.String())
		if err := step.Run(s, st); err != nil {
			return fmt.Errorf("scenario (%s) step %s failed: %w", sc.Name, step.String(), err)
		}
	}
	return nil
}

func RunScenarios(s *ScenarioContext, list []Scenario) error {
	for _, sc := range list {
		if err := RunScenario(s, sc); err != nil {
			return fmt.Errorf("scenario (%s) failed: %w", sc.Name, err)
		}
	}
	return nil
}

func (s *ScenarioContext) PlayerByUID(uid int) (*Player, int, error) {
	for i := range s.cfg.Players {
		if s.cfg.Players[i].UID == uid {
			return &s.cfg.Players[i], i, nil
		}
	}
	return nil, -1, fmt.Errorf("player uid %d not in config", uid)
}

func (st HostStep) String() string { return fmt.Sprintf("HostStep{uid=%d}", st.UID) }

// Run executes the scenario step for a given struct.
// It coordinates process startup, event epochs, and cross-player connection choreography for that step.
// The logic here is intentionally idempotent because steps can be retried across epochs.
func (st HostStep) Run(s *ScenarioContext, es *execState) error {
	host, idx, err := s.PlayerByUID(st.UID)
	if err != nil {
		return err
	}
	if st.Restart {
		s.HardKillBoth(host)
	}
	if err = s.EnsurePlayerRunning(idx); err != nil {
		return err
	}
	if err = s.WaitForIdle(host, 60*time.Second); err != nil {
		return fmt.Errorf("host not Idle: %w", err)
	}
	if err = s.WriteLauncher(host, "host\n"); err != nil {
		return err
	}
	s.startNewLobbyEpoch()
	if err = s.WaitForLobbyCreated(host, 8*time.Second); err != nil {
		return err
	}
	time.Sleep(smallGap(s))
	es.Host = host
	es.markStarted(host.UID)
	return nil
}

func (st JoinStep) String() string {
	return fmt.Sprintf("JoinStep{uid=%d host=%d}", st.UID, st.HostUID)
}

func (st JoinStep) Run(s *ScenarioContext, es *execState) error {
	if es.Host == nil && st.HostUID == 0 {
		return errors.New("JoinStep: no current host; add HostStep first or set HostUID")
	}
	host := es.Host
	if st.HostUID != 0 {
		h, _, err := s.PlayerByUID(st.HostUID)
		if err != nil {
			return err
		}
		host = h
	}
	joiner, jIdx, err := s.PlayerByUID(st.UID)
	if err != nil {
		return err
	}

	if err = s.EnsurePlayerRunning(jIdx); err != nil {
		return err
	}
	if err = s.WaitForIdle(joiner, 60*time.Second); err != nil {
		return fmt.Errorf("joiner not Idle: %w", err)
	}

	// Wait until host reached Lobby (idempotent)
	// Quick check: by lobbyCreated mark or by launcher state
	lobbyUp := func() bool {
		s.mu.RLock()
		_, ok := s.lobbyCreated[host.UID]
		s.mu.RUnlock()
		return ok || host.getGameState() == "Lobby"
	}
	if !lobbyUp() {
		if err := s.WaitForLobbyCreated(host, 8*time.Second); err != nil {
			return fmt.Errorf("JoinStep: host %d not in Lobby: %w", host.UID, err)
		}
	}

	// joiner -> join_to host
	if err = s.WriteLauncher(joiner, fmt.Sprintf("join_to %s %d 0\n", host.Name, host.UID)); err != nil {
		return err
	}
	if err = s.WaitForJoinAck(joiner, 8*time.Second); err != nil {
		return fmt.Errorf("no join ACK: %w", err)
	}

	time.Sleep(smallGap(s))
	if err = s.SendConnectTo(host, joiner); err != nil {
		return err
	}
	if err = s.WaitForSwapOneWay(host, joiner, 8*time.Second); err != nil {
		return err
	}
	if err = s.WaitPairReady(host, joiner, 12*time.Second); err != nil {
		return err
	}

	es.LastJoiner = joiner
	es.markStarted(joiner.UID)

	if st.DelayAfterJoin > 0 {
		time.Sleep(st.DelayAfterJoin)
	}
	for _, sub := range st.AfterJoin {
		if err = sub.Run(s, es); err != nil {
			return fmt.Errorf("after-join %s: %w", sub.String(), err)
		}
	}
	return nil
}

func (st JoinManyStep) String() string {
	return fmt.Sprintf("JoinManyStep{n=%d host=%d}", len(st.UIDs), st.HostUID)
}

func (st JoinManyStep) Run(s *ScenarioContext, es *execState) error {
	for i, uid := range st.UIDs {
		if err := (JoinStep{UID: uid, HostUID: st.HostUID, AfterJoin: st.AfterEachJoin}).Run(s, es); err != nil {
			return fmt.Errorf("JoinMany idx=%d uid=%d: %w", i, uid, err)
		}
		if st.DelayBetween > 0 && i < len(st.UIDs)-1 {
			time.Sleep(st.DelayBetween)
		}
	}
	return nil
}

func (st KillStep) String() string {
	return fmt.Sprintf("KillStep{uid=%d kind=%d target=%d}", st.UID, st.Kind, st.TargetUID)
}

func (st KillStep) Run(s *ScenarioContext, es *execState) error {
	var p *Player
	var err error
	if st.UID == 0 {
		if es.LastJoiner == nil {
			return errors.New("KillStep: no last joiner")
		}
		p = es.LastJoiner
	} else {
		p, _, err = s.PlayerByUID(st.UID)
		if err != nil {
			return err
		}
	}

	switch st.Kind {
	case DropSoft:
		target := st.TargetUID
		if target == 0 {
			if es.Host == nil {
				return errors.New("KillStep DropSoft: Host unknown; set TargetUID")
			}
			target = es.Host.UID
		}
		if err = s.SendDisconnect(p, target); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 350)
		if err = s.WriteLauncher(p, "quit --with-exe\n"); err != nil && !isBenignClosedPipe(err) {
			return err
		}
		if err = s.WaitProcessesDown(p, 10*time.Second); err != nil {
			return err
		}
		s.resetAfterKill(p)
	case DropQuit:
		if err = s.WriteLauncher(p, "quit --with-exe\n"); err != nil && !isBenignClosedPipe(err) {
			return err
		}
		if err = s.WaitProcessesDown(p, 10*time.Second); err != nil {
			return err
		}
		s.resetAfterKill(p)
	case DropCrashLauncher:
		if err = s.CrashLauncher(p); err != nil {
			return err
		}
	case DropCrashAdapter:
		if err = s.CrashAdapter(p); err != nil {
			return err
		}
	case DropHardKillBoth:
		s.HardKillBoth(p)
	default:
		return fmt.Errorf("KillStep: unknown kind %d", st.Kind)
	}
	return nil
}

func (st WaitStep) String() string { return fmt.Sprintf("WaitStep{%s}", st.Delay) }
func (st WaitStep) Run(_ *ScenarioContext, _ *execState) error {
	if st.Delay > 0 {
		time.Sleep(st.Delay)
	}
	return nil
}

func (st ParallelStep) String() string { return "ParallelStep{...}" }
func (st ParallelStep) Run(s *ScenarioContext, es *execState) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(st.Steps)+len(st.Until))
	for _, step := range st.Steps {
		currentStep := step
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := currentStep.Run(s, es); err != nil {
				errCh <- fmt.Errorf("parallel sub-step %s: %w", currentStep.String(), err)
			}
		}()
	}

	for _, cond := range st.Until {
		currentCond := cond
		wg.Add(1)
		go func() {
			defer wg.Done()
			to := 30 * time.Second
			if err := currentCond.Wait(s, es, to); err != nil {
				errCh <- fmt.Errorf("parallel condition %s: %w", currentCond.String(), err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		return e
	}

	if st.MeshNonHost && es.Host != nil {
		var ps []*Player
		for uid := range es.startedUIDs {
			if p, _, err := s.PlayerByUID(uid); err == nil && p != nil {
				ps = append(ps, p)
			}
		}
		deadline := time.Now().Add(func() time.Duration {
			if st.MeshWait > 0 {
				return st.MeshWait
			}
			return 20 * time.Second
		}())
		for i := 0; i < len(ps); i++ {
			for j := i + 1; j < len(ps); j++ {
				a, b := ps[i], ps[j]
				if a.UID == es.Host.UID || b.UID == es.Host.UID {
					continue // pairs with host already established
				}
				// IMPORTANT: don't rely on IsPairReady — require an actual link (post-epoch "connected" state)
				if s.IsConnectedPair(a, b) {
					continue
				}

				// Send idempotent connect_to commands with a pending-swap marker
				_ = s.SendConnectTo(a, b) // marks pendingSwap
				_ = s.SendConnectTo(b, a)
				time.Sleep(smallGap(s))
				_ = s.SendConnectTo(a, b)
				_ = s.SendConnectTo(b, a)

				// Wait for swap in both directions (now correctly detected)
				_ = s.WaitForSwapOneWay(a, b, 8*time.Second)
				_ = s.WaitForSwapOneWay(b, a, 8*time.Second)

				// And finally — ensure the pair is "ready" (now IsPairReady is acceptable)
				to := time.Until(deadline)
				if to < 3*time.Second {
					to = 3 * time.Second
				}
				if err := s.WaitPairReady(a, b, to); err != nil {
					return fmt.Errorf("mesh pair %d<->%d: %w", a.UID, b.UID, err)
				}
			}
		}
	}
	return nil
}

func (st EndStep) String() string { return "EndStep{}" }
func (st EndStep) Run(s *ScenarioContext, es *execState) error {
	var toStop []int
	for uid := range es.startedUIDs {
		toStop = append(toStop, uid)
	}
	if len(toStop) == 0 {
		for i := range s.cfg.Players {
			toStop = append(toStop, s.cfg.Players[i].UID)
		}
	}
	s.StopRosterGraceful(toStop...)
	return nil
}

type pendingSwapItem struct {
	remote int
	at     time.Time
}

type ScenarioContext struct {
	ctx        context.Context
	cfg        *Config
	mu         sync.RWMutex
	epochStart time.Time

	lobbyCreated map[int]time.Time         // hostUID -> t (Lobby created)
	joinAck      map[int]time.Time         // joinerUID -> t (Join ACK)
	linkConn     map[int]map[int]time.Time // A->B: state:"connected" after epoch
	linkActive   map[int]map[int]time.Time // A->B: "Peer already exists and is active" after epoch
	linkSwap     map[int]map[int]time.Time // A->B: "Connecting to peer (swapping...)" after epoch
	port2peer    map[int]map[int]int
	linkDataOpen map[int]map[int]time.Time // A->B: "Data channel opened ..."
	pendingSwap  map[int][]pendingSwapItem
	idleAt       map[int]time.Time
	idleBarrier  map[int]time.Time
}

func NewScenarioContext(ctx context.Context, cfg *Config) *ScenarioContext {
	return &ScenarioContext{
		ctx:          ctx,
		cfg:          cfg,
		epochStart:   time.Now(),
		lobbyCreated: map[int]time.Time{},
		joinAck:      map[int]time.Time{},
		linkConn:     map[int]map[int]time.Time{},
		linkActive:   map[int]map[int]time.Time{},
		linkSwap:     map[int]map[int]time.Time{},
		port2peer:    map[int]map[int]int{},
		linkDataOpen: map[int]map[int]time.Time{},
		pendingSwap:  make(map[int][]pendingSwapItem),
		idleAt:       map[int]time.Time{},
		idleBarrier:  map[int]time.Time{},
	}
}

func (s *ScenarioContext) StartAll() error {
	wg := &sync.WaitGroup{}
	for i := range s.cfg.Players {
		if err := s.startPlayer(i, wg); err != nil {
			return err
		}
	}
	go func() { <-s.ctx.Done() }()
	return nil
}

func (s *ScenarioContext) startNewLobbyEpoch() {
	s.mu.Lock()
	s.epochStart = time.Now()
	s.linkConn = map[int]map[int]time.Time{}
	s.linkActive = map[int]map[int]time.Time{}
	s.linkSwap = map[int]map[int]time.Time{}
	s.mu.Unlock()
}

func (s *ScenarioContext) clearPeerHistory(uid int) {
	s.mu.Lock()
	for a := range s.linkConn {
		delete(s.linkConn[a], uid)
	}
	delete(s.linkConn, uid)
	for a := range s.linkActive {
		delete(s.linkActive[a], uid)
	}
	delete(s.linkActive, uid)
	for a := range s.linkSwap {
		delete(s.linkSwap[a], uid)
	}
	delete(s.linkSwap, uid)
	s.mu.Unlock()
}

func (s *ScenarioContext) markDataOpen(local, remote int, t time.Time) {
	s.mu.Lock()
	if s.linkDataOpen[local] == nil {
		s.linkDataOpen[local] = map[int]time.Time{}
	}
	// record only the first occurrence and do not overwrite — it is a "fact", not tied to an epoch
	if s.linkDataOpen[local][remote].IsZero() {
		s.linkDataOpen[local][remote] = t
		fmt.Printf("[U%d] webrtc: data-channel OPEN (A->B) %d->%d\n", local, local, remote)
	}
	s.mu.Unlock()
}

// startPlayer launches launcher and adapter processes for a player
// (with appropriate CLI flags), starts their stdout/stderr readers, and
// registers cancellation/cleanup hooks.

func (s *ScenarioContext) startPlayer(idx int, wg *sync.WaitGroup) error {
	p := &s.cfg.Players[idx]
	p.logPrefix = fmt.Sprintf("U%d", p.UID)

	if s.cfg.UseDelve {
		p.dlvPortL = s.cfg.DelveBasePort + idx*2
		p.dlvPortA = s.cfg.DelveBasePort + idx*2 + 1
	}

	launcherArgs := []string{
		"--user-id", strconv.Itoa(p.UID),
		"--user-name", p.Name,
		"--game-id", strconv.Itoa(s.cfg.GameID),
		"--gpgnet-client-port", strconv.Itoa(p.GpgClient),
		"--gpgnet-port", strconv.Itoa(p.GpgServer),
		"--api-root", s.cfg.IcebreakerAPI,
		"--access-token", p.Token,
	}
	adapterArgs := []string{
		"--user-id", strconv.Itoa(p.UID),
		"--user-name", p.Name,
		"--game-id", strconv.Itoa(s.cfg.GameID),
		"--gpgnet-client-port", strconv.Itoa(p.GpgClient),
		"--gpgnet-port", strconv.Itoa(p.GpgServer),
		"--api-root", s.cfg.IcebreakerAPI,
		"--access-token", p.Token,
	}
	if s.cfg.ForceTurn {
		adapterArgs = append(adapterArgs, "--force-turn-relay")
	}

	p.Launcher = makeCmd(s.cfg.LauncherPath, launcherArgs, s.cfg.UseDelve, s.cfg.DelvePath, p.dlvPortL)
	p.Adapter = makeCmd(s.cfg.AdapterPath, adapterArgs, s.cfg.UseDelve, s.cfg.DelvePath, p.dlvPortA)

	in, err := p.Launcher.StdinPipe()
	if err != nil {
		return err
	}
	p.inW = in

	lOut, lErr := makeLogWriters(s.cfg.LogDir, fmt.Sprintf("%s-launcher", p.logPrefix))
	aOut, aErr := makeLogWriters(s.cfg.LogDir, fmt.Sprintf("%s-adapter", p.logPrefix))

	lr, lw := io.Pipe()
	ar, aw := io.Pipe()

	p.Launcher.Stdout = io.MultiWriter(lOut, lw)
	p.Launcher.Stderr = lErr
	p.Adapter.Stdout = io.MultiWriter(aOut, aw)
	p.Adapter.Stderr = aErr

	if err = p.Launcher.Start(); err != nil {
		return fmt.Errorf("launcher U%d: %w", p.UID, err)
	}
	fmt.Printf("Launcher [user=%d, name=%s] started [pid=%d]\n", p.UID, p.Name, p.Launcher.Process.Pid)
	p.MarkLauncherUp(true)

	go s.watchLauncherLogs(p, lr)
	go s.watchAdapterLogs(p, ar)

	time.Sleep(850 * time.Millisecond)
	if err = p.Adapter.Start(); err != nil {
		return fmt.Errorf("adapter U%d: %w", p.UID, err)
	}
	fmt.Printf("Adapter [user=%d, name=%s] started [pid=%d]\n", p.UID, p.Name, p.Adapter.Process.Pid)
	p.MarkAdapterUp(true)

	s.mu.Lock()
	s.idleBarrier[p.UID] = time.Now()
	s.mu.Unlock()

	if wg != nil {
		wg.Add(2)
		go func(pp *Player) { defer wg.Done(); _ = pp.Launcher.Wait(); pp.MarkLauncherUp(false) }(p)
		go func(pp *Player) { defer wg.Done(); _ = pp.Adapter.Wait(); pp.MarkAdapterUp(false) }(p)
	}
	return nil
}

// EnsurePlayerRunning guarantees both launcher and adapter are running for a player.
// It spawns processes if absent, attaches log readers, and wires the stdout/err scanners
// into the state machine that tracks lobby joins and WebRTC link phases.

func (s *ScenarioContext) EnsurePlayerRunning(idx int) error {
	p := &s.cfg.Players[idx]
	p.aliveMu.RLock()
	alive := p.launcherUp && p.adapterUp
	p.aliveMu.RUnlock()
	if alive {
		return nil
	}
	var wg sync.WaitGroup
	return s.startPlayer(idx, &wg)
}

func (s *ScenarioContext) findIndexByUID(uid int) int {
	for i := range s.cfg.Players {
		if s.cfg.Players[i].UID == uid {
			return i
		}
	}
	return -1
}

func (s *ScenarioContext) StartRoster(uids ...int) error {
	var wg sync.WaitGroup
	for _, uid := range uids {
		idx := s.findIndexByUID(uid)
		if idx < 0 {
			return fmt.Errorf("unknown UID in roster: %d", uid)
		}
		if err := s.EnsurePlayerRunning(idx); err != nil {
			return err
		}
	}
	_ = wg
	return nil
}

func (s *ScenarioContext) StopRoster(uids ...int) {
	for _, uid := range uids {
		idx := s.findIndexByUID(uid)
		if idx < 0 {
			continue
		}
		p := &s.cfg.Players[idx]
		s.HardKillBoth(p)
	}
}

func (s *ScenarioContext) StopRosterGraceful(uids ...int) {
	for _, uid := range uids {
		idx := s.findIndexByUID(uid)
		if idx < 0 {
			continue
		}
		p := &s.cfg.Players[idx]
		_ = s.WriteLauncher(p, "quit --with-exe\n")
		if err := s.WaitProcessesDown(p, 8*time.Second); err != nil {
			s.HardKillBoth(p)
		}
		s.resetAfterKill(p)
	}
}

func (s *ScenarioContext) SendDisconnect(p *Player, remoteUID int) error {
	if p == nil || p.inW == nil {
		return errors.New("soft quit: launcher stdin is nil")
	}
	cmd := fmt.Sprintf("disconnect %d\n", remoteUID)
	remote, _, err := s.PlayerByUID(remoteUID)
	if err != nil {
		return fmt.Errorf("failed to find player with UID %d: %w", remoteUID, err)
	}

	fmt.Printf("Disconnecting from player [user=%d, name=%s] of [user=%d, name=%s]: %s\n",
		remote.UID,
		remote.Name,
		p.UID,
		p.Name,
		strings.TrimSpace(cmd),
	)

	_, err = io.WriteString(p.inW, cmd)
	return err
}

func (s *ScenarioContext) SoftQuit(p *Player, withExe bool) error {
	if p == nil || p.inW == nil {
		return errors.New("soft quit: launcher stdin is nil")
	}
	cmd := "quit\n"
	if withExe {
		cmd = "quit --with-exe\n"
	}
	fmt.Printf("SoftQuit [user=%d, name=%s]: %s", p.UID, p.Name, strings.TrimSpace(cmd))
	_, err := io.WriteString(p.inW, cmd)
	return err
}

func (s *ScenarioContext) resetAfterKill(p *Player) {
	uid := p.UID
	s.mu.Lock()
	s.idleBarrier[uid] = time.Now()
	delete(s.lobbyCreated, uid)
	delete(s.joinAck, uid)
	for a := range s.linkDataOpen {
		delete(s.linkDataOpen[a], uid)
		if len(s.linkDataOpen[a]) == 0 {
			delete(s.linkDataOpen, a)
		}
	}
	delete(s.linkDataOpen, uid)
	delete(s.port2peer, uid)
	delete(s.pendingSwap, uid)
	s.mu.Unlock()

	s.clearPeerHistory(uid)
	p.setGameState("")
}

func isBenignClosedPipe(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	// windows: "file already closed"; *nix: EPIPE / "broken pipe"
	msg := err.Error()
	return strings.Contains(msg, "file already closed") ||
		strings.Contains(msg, "use of closed file") ||
		strings.Contains(msg, "broken pipe") || errors.Is(err, syscall.EPIPE)
}

func (s *ScenarioContext) WaitProcessesDown(p *Player, to time.Duration) error {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		p.aliveMu.RLock()
		alive := p.launcherUp || p.adapterUp
		p.aliveMu.RUnlock()
		if !alive {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("player %d: processes not down within %s", p.UID, to)
}

func (s *ScenarioContext) CrashLauncher(p *Player) error {
	if p != nil && p.Launcher != nil && p.Launcher.Process != nil {
		return p.Launcher.Process.Kill()
	}
	return nil
}

func (s *ScenarioContext) CrashAdapter(p *Player) error {
	if p != nil && p.Adapter != nil && p.Adapter.Process != nil {
		return p.Adapter.Process.Kill()
	}
	return nil
}

func (s *ScenarioContext) HardKillBoth(p *Player) {
	if p == nil {
		return
	}
	fmt.Printf("HardKillBoth [user=%d, name=%s]\n", p.UID, p.Name)
	if p.Adapter != nil && p.Adapter.Process != nil {
		_ = p.Adapter.Process.Kill()
	}
	if p.Launcher != nil && p.Launcher.Process != nil {
		_ = p.Launcher.Process.Kill()
	}
	p.MarkLauncherUp(false)
	p.MarkAdapterUp(false)
}

func (s *ScenarioContext) KillPlayer(p *Player) {
	fmt.Printf("Killing player [user=%d, name=%s, launcherPID=%d]",
		p.UID, p.Name, p.Launcher.Process.Pid)

	_ = s.WriteLauncher(p, "quit --with-exe\n")
	time.Sleep(300 * time.Millisecond)
	if p.Adapter != nil && p.Adapter.Process != nil {
		err := p.Adapter.Process.Kill()
		if err != nil {
			fmt.Printf("Error killing adapter [user=%d, name=%s, launcherPID=%d]: %s",
				p.UID, p.Name, p.Launcher.Process.Pid, err)
		}
	}
	if p.Launcher != nil && p.Launcher.Process != nil {
		err := p.Launcher.Process.Kill()
		if err != nil {
			fmt.Printf("Error killing launcher [user=%d, name=%s, launcherPID=%d]: %s",
				p.UID, p.Name, p.Launcher.Process.Pid, err)
		}
	}
	p.MarkLauncherUp(false)
	p.MarkAdapterUp(false)
}

func (s *ScenarioContext) WriteLauncher(p *Player, cmd string) error {
	if p.inW == nil {
		return errors.New("launcher stdin is nil")
	}
	fmt.Printf("Sending launcher command [user=%d, name=%s, launcherPID=%d]: %s",
		p.UID, p.Name, p.Launcher.Process.Pid, cmd)
	_, err := io.WriteString(p.inW, cmd)
	return err
}

func (s *ScenarioContext) WaitForIdle(p *Player, timeout time.Duration) error {
	dead := time.After(timeout)
	tk := time.NewTicker(100 * time.Millisecond)
	defer tk.Stop()
	for {
		if p.getGameState() == "Idle" {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-dead:
			return fmt.Errorf("timeout waiting for Idle (state=%s)", p.getGameState())
		case <-tk.C:
		}
	}
}

func (s *ScenarioContext) ConnectBoth(a, b *Player) {
	_ = s.WriteLauncher(a, fmt.Sprintf("connect_to %s %d 0\n", b.Name, b.UID))
	_ = s.WriteLauncher(b, fmt.Sprintf("connect_to %s %d 0\n", a.Name, a.UID))
	time.Sleep(s.cfg.StepDelay)
	_ = s.WriteLauncher(a, fmt.Sprintf("connect_to %s %d 0\n", b.Name, b.UID))
	_ = s.WriteLauncher(b, fmt.Sprintf("connect_to %s %d 0\n", a.Name, a.UID))
}

// indexPort records a mapping from observed UDP port to peer UID within a player's context.
// We index both game->webrtc and webrtc->game ports when they appear in logs, so later we can
// resolve a swap destination when only the port is known.

func (s *ScenarioContext) indexPort(localUID, port, remoteUID int) {
	if port == 0 || remoteUID == 0 {
		return
	}
	s.mu.Lock()
	if s.port2peer[localUID] == nil {
		s.port2peer[localUID] = map[int]int{}
	}
	s.port2peer[localUID][port] = remoteUID
	s.mu.Unlock()
}

// peerByPort resolves a peer UID by looking up a previously indexed UDP port in the player's map.

func (s *ScenarioContext) peerByPort(localUID, port int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.port2peer[localUID][port]
}

func (s *ScenarioContext) markConn(local, remote int, t time.Time, tag string) {
	s.mu.Lock()
	if s.linkConn[local] == nil {
		s.linkConn[local] = map[int]time.Time{}
	}
	s.linkConn[local][remote] = t
	s.mu.Unlock()
	fmt.Printf("[U%d] webrtc: %s (A->B) %d->%d\n", local, tag, local, remote)
}

func (s *ScenarioContext) markActive(local, remote int, t time.Time) {
	s.mu.Lock()
	if s.linkActive[local] == nil {
		s.linkActive[local] = map[int]time.Time{}
	}
	s.linkActive[local][remote] = t
	s.mu.Unlock()
	fmt.Printf("[U%d] webrtc: already-active (A->B) %d->%d\n", local, local, remote)
}

func (s *ScenarioContext) markSwap(local, remote int, t time.Time) {
	s.mu.Lock()
	if s.linkSwap[local] == nil {
		s.linkSwap[local] = map[int]time.Time{}
	}
	s.linkSwap[local][remote] = t
	s.mu.Unlock()
	fmt.Printf("[U%d] gpgnet: swap (A->B) %d->%d\n", local, local, remote)
}

func (s *ScenarioContext) watchLauncherLogs(p *Player, r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		line := sc.Bytes()

		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}

		if msg, _ := m["msg"].(string); msg == "Game state has been changed" {
			if gs, _ := m["gameState"].(string); gs != "" {
				p.setGameState(gs)
				fmt.Printf("[%s] launcher observed gameState=%s\n", p.logPrefix, gs)
				if gs == "Idle" {
					s.mu.Lock()
					s.idleAt[p.UID] = time.Now()
					s.mu.Unlock()
				}
			}
		}
	}
}

func (s *ScenarioContext) watchAdapterLogs(p *Player, r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		p.bumpAdapterActivity()

		msg, _ := m["msg"].(string)
		now := time.Now()

		if strings.Contains(msg, "Received create lobby message") ||
			(strings.Contains(msg, "Local game gameState changed") && m["gameState"] == "Lobby") {
			s.mu.Lock()
			s.lobbyCreated[p.UID] = now
			s.mu.Unlock()
			fmt.Printf("[%s] gpgnet: create-lobby ACK / Lobby\n", p.logPrefix)
		}
		if strings.Contains(msg, "Joining game (swapping the address/port)") {
			s.mu.Lock()
			s.joinAck[p.UID] = now
			s.mu.Unlock()
			s.clearPeerHistory(p.UID)
			fmt.Printf("[%s] gpgnet: join ACK\n", p.logPrefix)
		}

		if strings.Contains(msg, "Initiating connection") ||
			strings.Contains(msg, "Peer connection state has changed") {
			remote := toInt(m["remotePlayerId"])
			if remote != 0 {
				g2w := toInt(m["gameToWebrtcPort"])
				w2g := toInt(m["webrtcToGamePort"])
				s.indexPort(p.UID, g2w, remote)
				s.indexPort(p.UID, w2g, remote)
			}
		}

		if strings.Contains(msg, "Data channel opened") {
			b := toInt(m["remotePlayerId"])
			if b != 0 {
				s.markDataOpen(p.UID, b, now)
			}
		}

		// filter "after epoch"
		s.mu.RLock()
		epoch := s.epochStart
		s.mu.RUnlock()
		if !now.After(epoch) {
			continue
		}

		// --- A->B: state:"connected" ---
		if strings.Contains(msg, "Peer connection state has changed") && m["state"] == "connected" {
			b := toInt(m["remotePlayerId"])
			if b != 0 {
				s.markConn(p.UID, b, now, "connected")
			}
			continue
		}

		// --- A->B: "Peer already exists and is active" ---
		if strings.Contains(msg, "Peer already exists and is active") {
			b := toInt(m["playerId"])
			if b != 0 {
				s.markActive(p.UID, b, now)
			}
			continue
		}

		// --- A->B: "Connecting to peer (swapping the address/port)" ---
		if strings.Contains(msg, "Connecting to peer (swapping the address/port)") {
			tp := toInt(m["targetPort"])
			var b int
			if tp != 0 {
				b = s.peerByPort(p.UID, tp) // primary path: by port->peer index
			}
			if b == 0 {
				// FALLBACK: bind to the peer we most recently sent connect_to for
				if cand, ok := s.popPendingSwap(p.UID); ok {
					b = cand
				}
			}
			if b != 0 {
				s.markSwap(p.UID, b, now)
			} else {
				fmt.Printf("[%s] gpgnet: swap targetPort=%d but peer unknown (skip)\n", p.logPrefix, tp)
			}
			continue
		}
	}
}

func (s *ScenarioContext) WaitForLobbyCreated(host *Player, to time.Duration) error {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		s.mu.RLock()
		t := s.lobbyCreated[host.UID]
		s.mu.RUnlock()
		if !t.IsZero() {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(80 * time.Millisecond):
		}
	}
	return fmt.Errorf("host %d: lobby not created within %s", host.UID, to)
}

func (s *ScenarioContext) IsLobbyUp(host *Player) bool {
	s.mu.RLock()
	_, ok := s.lobbyCreated[host.UID]
	s.mu.RUnlock()
	if ok {
		return true
	}
	return host.getGameState() == "Lobby"
}

func (s *ScenarioContext) WaitPairReady(a, b *Player, to time.Duration) error {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		if s.IsPairReady(a, b) {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(120 * time.Millisecond):
		}
	}
	return fmt.Errorf("pair %d<->%d not ready within %s", a.UID, b.UID, to)
}

func (s *ScenarioContext) WaitForJoinAck(joiner *Player, to time.Duration) error {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		s.mu.RLock()
		t := s.joinAck[joiner.UID]
		s.mu.RUnlock()
		if !t.IsZero() {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(80 * time.Millisecond):
		}
	}
	return fmt.Errorf("joiner %d: no join ACK within %s", joiner.UID, to)
}

func (s *ScenarioContext) hasDataOpen(a, b *Player) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.linkDataOpen[a.UID][b.UID]
	return !t.IsZero()
}

func (s *ScenarioContext) hasBindingOneWay(a, b *Player) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	epoch := s.epochStart
	if t := s.linkConn[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	if t := s.linkActive[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	if t := s.linkSwap[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	return false
}

func (s *ScenarioContext) IsPairReady(a, b *Player) bool {
	if !(s.hasDataOpen(a, b) && s.hasDataOpen(b, a)) {
		return false
	}
	if s.hasBindingOneWay(a, b) || s.hasBindingOneWay(b, a) {
		return true
	}
	return s.IsLobbyUp(a) && s.IsLobbyUp(b)
}

// WaitForSwapOneWay waits until we observe an address/port swap from A to B after the current epoch.
// A successful swap means the launcher/adapter rewired the game port for A so that traffic to B goes
// through the local proxy; it's a stronger signal than a mere 'pair ready' heuristic.

func (s *ScenarioContext) WaitForSwapOneWay(a, b *Player, to time.Duration) error {
	dl := time.Now().Add(to)
	for time.Now().Before(dl) {
		s.mu.RLock()
		epoch := s.epochStart
		t := time.Time{}
		if m := s.linkSwap[a.UID]; m != nil {
			t = m[b.UID]
		}
		s.mu.RUnlock()
		if !t.IsZero() && t.After(epoch) {
			return nil
		}
		time.Sleep(80 * time.Millisecond)
	}
	return fmt.Errorf("no swap A->B (%d->%d) within %s", a.UID, b.UID, to)
}

func (s *ScenarioContext) hasOneWay(a, b *Player) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	epoch := s.epochStart
	if t := s.linkConn[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	if t := s.linkActive[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	if t := s.linkSwap[a.UID][b.UID]; !t.IsZero() && t.After(epoch) {
		return true
	}
	return false
}

func (s *ScenarioContext) IsConnectedPair(a, b *Player) bool {
	return s.hasOneWay(a, b) && s.hasOneWay(b, a)
}

func (s *ScenarioContext) Cleanup() {
	for i := range s.cfg.Players {
		p := &s.cfg.Players[i]
		_ = s.WriteLauncher(p, "quit\n")
	}
	time.Sleep(300 * time.Millisecond)
	for i := range s.cfg.Players {
		s.KillPlayer(&s.cfg.Players[i])
	}
}

// SendConnectTo sends an idempotent ConnectToPeer request to the game via the launcher's stdin.
// Before sending, it notes a 'pending swap' for the (A->B) direction.
// Later, when the adapter reports 'swapping the address/port', that pending mark helps
// us reliably attribute the swap event to the intended peer even if the UDP port
// has not been indexed yet.

func (s *ScenarioContext) SendConnectTo(initiator, responder *Player) error {
	s.notePendingSwap(initiator.UID, responder.UID)
	return s.WriteLauncher(initiator, fmt.Sprintf("connect_to %s %d 0\n", responder.Name, responder.UID))
}

// notePendingSwap records that player A intends to swap routing towards peer B.
// This is used as a fallback attribution when a 'swap' log arrives with a port we don't know yet.

func (s *ScenarioContext) notePendingSwap(local, remote int) {
	if local == 0 || remote == 0 {
		return
	}
	s.mu.Lock()
	s.pendingSwap[local] = append(s.pendingSwap[local], pendingSwapItem{remote: remote, at: time.Now()})
	s.mu.Unlock()
}

// popPendingSwap returns the most recent 'pending swap' peer UID for player A, if any,
// and removes it from the stack. This provides last-write-wins attribution for ambiguous swap logs.

func (s *ScenarioContext) popPendingSwap(local int) (remote int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.pendingSwap[local]
	cutoff := time.Now().Add(-8 * time.Second)
	i := 0
	for i < len(q) && q[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		q = q[i:]
	}
	if len(q) == 0 {
		s.pendingSwap[local] = q
		return 0, false
	}
	remote = q[0].remote
	s.pendingSwap[local] = q[1:]
	return remote, true
}

func makeCmd(bin string, args []string, withDlv bool, dlvPath string, dlvPort int) *exec.Cmd {
	if withDlv {
		all := []string{
			"exec", bin,
			"--headless",
			"--listen", fmt.Sprintf("127.0.0.1:%d", dlvPort),
			"--api-version", "2",
			"--",
		}
		all = append(all, args...)
		cmd := exec.Command(dlvPath, all...)
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
		return cmd
	}
	cmd := exec.Command(bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
	return cmd
}

func makeLogWriters(dir, base string) (io.Writer, io.Writer) {
	if dir == "" {
		pr, pw := io.Pipe()
		go prefixCopy(os.Stdout, pr, base)
		er, ew := io.Pipe()
		go prefixCopy(os.Stderr, er, base)
		return pw, ew
	}
	_ = os.MkdirAll(dir, 0o755)
	outF, _ := os.Create(filepath.Join(dir, base+".out.log"))
	errF, _ := os.Create(filepath.Join(dir, base+".err.log"))
	return outF, errF
}

func prefixCopy(dst io.Writer, r io.Reader, prefix string) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		_, _ = fmt.Fprintf(dst, "[%s] %s\n", prefix, sc.Text())
	}
}

func toInt(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case string:
		if x, err := strconv.Atoi(t); err == nil {
			return x
		}
	}
	return 0
}
