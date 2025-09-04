package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Player struct {
	UID       int
	Name      string
	Token     string
	GpgClient int // --gpgnet-client-port (launcher listens, adapter connects)
	GpgServer int // --gpgnet-port (adapter listens)
	Launcher  *exec.Cmd
	Adapter   *exec.Cmd
	inW       io.WriteCloser // stdin of launcher
	logPrefix string
	dlvPortL  int // delve for launcher
	dlvPortA  int // delve for adapter
}

type Config struct {
	IcebreakerAPI string
	GameID        int
	Players       []Player
	LauncherPath  string
	AdapterPath   string
	ForceTurn     bool
	UseDelve      bool
	DelvePath     string
	DelveBasePort int
	LogDir        string
	StepDelay     time.Duration
	StartDelay    time.Duration
}

func main() {
	cfg := parseFlags()

	if runtime.GOOS != "windows" {
		fmt.Println("Developed and tested for Windows tests, might not work on unix")
	}

	if cfg.LogDir != "" {
		_ = os.MkdirAll(cfg.LogDir, 0o755)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("Starting everything ...")

	if err := startAll(ctx, &cfg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "start error: %v\n", err)
		cleanup(&cfg)
		os.Exit(1)
	}

	// Waiting a bit for launcher listeners to start.
	time.Sleep(cfg.StartDelay)

	if err := runScenario(&cfg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "scenario error: %v\n", err)
	}

	<-ctx.Done()
	fmt.Println("\nStopping...")
	cleanup(&cfg)
}

func parseFlags() Config {
	var (
		api      = flag.String("api-root", "http://127.0.0.1:8080", "Icebreaker API root")
		gameID   = flag.Int("game-id", 100, "Game ID")
		tokFile  = flag.String("tokens", "TEST_TOKEN.md", "Path to TEST_TOKEN.md")
		launcher = flag.String("launcher", ".\\launcher-emulator.exe", "Path to launcher-emulator")
		adapter  = flag.String("adapter", ".\\adapter.exe", "Path to adapter")
		logDir   = flag.String("logs", ".\\logs", "Directory for per-process logs")
		forceTR  = flag.Bool("force-turn-relay", false, "Pass --force-turn-relay to adapter")
		useDlv   = flag.Bool("dlv", false, "Run processes under Delve (headless)")
		dlvPath  = flag.String("dlv-path", "dlv.exe", "Path to Delve")
		dlvBase  = flag.Int("dlv-base-port", 40001, "Base port for Delve headless servers")
		startD   = flag.Duration("start-delay", 9500*time.Millisecond, "Delay before sending host/join")
		stepD    = flag.Duration("step-delay", 500*time.Millisecond, "Delay between join/connect commands")
		users    = flag.String("users", "1,2,3", "Comma-separated user IDs to run")
		names    = flag.String("names", "", "Optional comma-separated user names (defaults: User<UID>)")
	)
	flag.Parse()

	tokens, err := parseTokens(*tokFile)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to parse tokens from %s: %v\n", *tokFile, err)
		os.Exit(2)
	}

	var ids []int
	for _, s := range strings.Split(*users, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.Atoi(s)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Bad user id %q: %v\n", s, err)
			os.Exit(2)
		}
		ids = append(ids, id)
	}
	var nameList []string
	if *names != "" {
		nameList = strings.Split(*names, ",")
	}

	// assemble players
	var ps []Player
	for i, id := range ids {
		tok := tokens[id]
		if tok == "" {
			_, _ = fmt.Fprintf(os.Stderr, "No token for User ID=%d in %s\n", id, *tokFile)
			os.Exit(2)
		}
		n := fmt.Sprintf("User%d", id)
		if i < len(nameList) && strings.TrimSpace(nameList[i]) != "" {
			n = strings.TrimSpace(nameList[i])
		}
		ps = append(ps, Player{UID: id, Name: n, Token: tok})
	}

	// randomize base for port picking a bit
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// pick ports
	for i := range ps {
		ps[i].GpgClient = mustFreePort()
		ps[i].GpgServer = mustFreePort()
	}

	return Config{
		IcebreakerAPI: *api,
		GameID:        *gameID,
		Players:       ps,
		LauncherPath:  *launcher,
		AdapterPath:   *adapter,
		ForceTurn:     *forceTR,
		UseDelve:      *useDlv,
		DelvePath:     *dlvPath,
		DelveBasePort: *dlvBase,
		LogDir:        *logDir,
		StepDelay:     *stepD,
		StartDelay:    *startD,
	}
}

func parseTokens(path string) (map[int]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(b), "\n")
	reUID := regexp.MustCompile(`User ID\s*=\s*(\d+)`)
	reTok := regexp.MustCompile(`Token\s*=\s*(.+)$`)
	out := map[int]string{}
	var curUID *int
	for _, ln := range lines {
		if m := reUID.FindStringSubmatch(ln); m != nil {
			id, _ := strconv.Atoi(m[1])
			curUID = new(int)
			*curUID = id
			continue
		}
		if m := reTok.FindStringSubmatch(ln); m != nil && curUID != nil {
			tok := strings.TrimSpace(m[1])
			out[*curUID] = tok
			curUID = nil
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no tokens found")
	}
	return out, nil
}

func startAll(ctx context.Context, cfg *Config) error {
	wg := &sync.WaitGroup{}
	for i := range cfg.Players {
		p := &cfg.Players[i]
		p.logPrefix = fmt.Sprintf("U%d", p.UID)

		// compute delve ports if needed
		if cfg.UseDelve {
			p.dlvPortL = cfg.DelveBasePort + i*2
			p.dlvPortA = cfg.DelveBasePort + i*2 + 1
		}

		// launcher
		launcherArgs := []string{
			"--user-id", strconv.Itoa(p.UID),
			"--user-name", p.Name,
			"--game-id", strconv.Itoa(cfg.GameID),
			"--gpgnet-client-port", strconv.Itoa(p.GpgClient),
			"--gpgnet-port", strconv.Itoa(p.GpgServer),
			"--api-root", cfg.IcebreakerAPI,
			"--access-token", p.Token,
		}
		// adapter
		adapterArgs := []string{
			"--user-id", strconv.Itoa(p.UID),
			"--user-name", p.Name,
			"--game-id", strconv.Itoa(cfg.GameID),
			"--gpgnet-client-port", strconv.Itoa(p.GpgClient),
			"--gpgnet-port", strconv.Itoa(p.GpgServer),
			"--api-root", cfg.IcebreakerAPI,
			"--access-token", p.Token,
		}
		if cfg.ForceTurn {
			adapterArgs = append(adapterArgs, "--force-turn-relay")
		}

		p.Launcher = makeCmd(cfg.LauncherPath, launcherArgs, cfg.UseDelve, cfg.DelvePath, p.dlvPortL)
		p.Adapter = makeCmd(cfg.AdapterPath, adapterArgs, cfg.UseDelve, cfg.DelvePath, p.dlvPortA)

		// pipes & logs
		in, err := p.Launcher.StdinPipe()
		if err != nil {
			return err
		}
		p.inW = in

		lOut, lErr := makeLogWriters(cfg.LogDir, fmt.Sprintf("%s-launcher", p.logPrefix))
		aOut, aErr := makeLogWriters(cfg.LogDir, fmt.Sprintf("%s-adapter", p.logPrefix))

		p.Launcher.Stdout, p.Launcher.Stderr = lOut, lErr
		p.Adapter.Stdout, p.Adapter.Stderr = aOut, aErr

		// start launcher then adapter
		if err = p.Launcher.Start(); err != nil {
			return fmt.Errorf("launcher U%d: %w", p.UID, err)
		}

		fmt.Println(fmt.Sprintf(
			"Launcher [user=%d, name=%s] started [pid=%d]", p.UID, p.Name, p.Launcher.Process.Pid))

		// wait for port to be obtained
		time.Sleep(850 * time.Millisecond)
		if err = p.Adapter.Start(); err != nil {
			return fmt.Errorf("adapter U%d: %w", p.UID, err)
		}

		fmt.Println(fmt.Sprintf(
			"Adapter [user=%d, name=%s] started [pid=%d]", p.UID, p.Name, p.Launcher.Process.Pid))

		// waiters
		wg.Add(2)
		go func(pp *Player) { defer wg.Done(); _ = pp.Launcher.Wait() }(p)
		go func(pp *Player) { defer wg.Done(); _ = pp.Adapter.Wait() }(p)
	}

	// stop all when context finishes
	go func() {
		<-ctx.Done()
	}()

	return nil
}

func makeCmd(bin string, args []string, withDlv bool, dlvPath string, dlvPort int) *exec.Cmd {
	if withDlv {
		// dlv exec <bin> --headless --listen=127.0.0.1:<port> --api-version=2 -- <args...>
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
		// console prefixing
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

func cleanup(cfg *Config) {
	// send "quit" to launchers then kill process
	for i := range cfg.Players {
		p := &cfg.Players[i]
		_ = writeLauncher(p, "quit\n")
	}
	time.Sleep(400 * time.Millisecond)
	for i := range cfg.Players {
		p := &cfg.Players[i]
		if p.Adapter != nil && p.Adapter.Process != nil {
			_ = p.Adapter.Process.Kill()
		}
		if p.Launcher != nil && p.Launcher.Process != nil {
			_ = p.Launcher.Process.Kill()
		}
	}
}

func runScenario(cfg *Config) error {
	if len(cfg.Players) < 2 {
		return errors.New("need >=2 players")
	}

	host := &cfg.Players[0]

	// 1 - Host the game.
	_ = writeLauncher(host, "host\n")
	// Ideally here we should wait for Idle state, but just wait a bit.
	time.Sleep(cfg.StepDelay)

	joined := map[int]bool{host.UID: true}

	// Connect both lambda for connect and little delay.
	connectBoth := func(a, b *Player) {
		_ = writeLauncher(a, fmt.Sprintf("connect_to %s %d 0\n", b.Name, b.UID))
		_ = writeLauncher(b, fmt.Sprintf("connect_to %s %d 0\n", a.Name, a.UID))
		time.Sleep(cfg.StepDelay)
		// Burst-repeat, just in case.
		_ = writeLauncher(a, fmt.Sprintf("connect_to %s %d 0\n", b.Name, b.UID))
		_ = writeLauncher(b, fmt.Sprintf("connect_to %s %d 0\n", a.Name, a.UID))
	}

	// 2 - Connect players one by one.
	for i := 1; i < len(cfg.Players); i++ {
		p := &cfg.Players[i]

		// Ideally here we should wait for Idle state, but just wait a bit.
		time.Sleep(cfg.StepDelay)

		// Join to host name using `join_to`.
		_ = writeLauncher(p, fmt.Sprintf("join_to %s %d 0\n", host.Name, host.UID))
		time.Sleep(cfg.StepDelay)

		// Connect player <-> host.
		connectBoth(host, p)

		// Then for every player to each other, like p <-> (each other except host).
		for j := 1; j < i; j++ {
			x := &cfg.Players[j]
			if joined[x.UID] {
				connectBoth(x, p)
			}
		}

		joined[p.UID] = true
	}

	fmt.Println("Lobby commands sent")
	return nil
}

func writeLauncher(p *Player, s string) error {
	if p.inW == nil {
		return errors.New("launcher stdin is nil")
	}

	fmt.Printf(fmt.Sprintf(
		"Sending launcher command [user=%d, name=%s, launcherPID=%d]: %s",
		p.UID,
		p.Name,
		p.Launcher.Process.Pid,
		s))

	_, err := io.WriteString(p.inW, s)
	return err
}

func mustFreePort() int {
	port, err := freeTCPPort()
	if err != nil {
		return 30000 + rand.Intn(20000)
	}
	return port
}

func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	return strconv.Atoi(portStr)
}
