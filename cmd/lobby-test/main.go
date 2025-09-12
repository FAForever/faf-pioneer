package main

import (
	"context"
	"errors"
	"faf-pioneer/test"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

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

	scenarioCtx := test.NewScenarioContext(ctx, &cfg)
	time.Sleep(cfg.StartDelay)

	scenarios := []test.Scenario{
		//{
		//	Name: "normal-lobby",
		//	Steps: []test.ScenarioStep{
		//		test.ParallelStep{
		//			Steps: []test.ScenarioStep{
		//				test.HostStep{UID: 1},
		//				test.JoinStep{
		//					UID:     2,
		//					HostUID: 1,
		//				},
		//				test.JoinStep{
		//					UID:     3,
		//					HostUID: 1,
		//				},
		//			},
		//			Until: []test.Condition{
		//				test.CondLobbyCreated{HostUID: 1, Timeout: 20 * time.Second},
		//				test.CondHostSwappedWith{HostUID: 1, UIDs: []int{2, 3}, Timeout: 20 * time.Second},
		//			},
		//			MeshNonHost: true,
		//			MeshWait:    15 * time.Second,
		//		},
		//		test.WaitStep{Delay: 20 * time.Second},
		//		test.EndStep{},
		//	},
		//},
		{
			Name: "join-halt",
			Steps: []test.ScenarioStep{
				test.ParallelStep{
					Steps: []test.ScenarioStep{
						test.HostStep{UID: 1},
						test.JoinStep{
							UID:     2,
							HostUID: 1,
						},
					},
					Until: []test.Condition{
						test.CondLobbyCreated{HostUID: 1, Timeout: 20 * time.Second},
						test.CondHostSwappedWith{HostUID: 1, UIDs: []int{2}, Timeout: 20 * time.Second},
					},
				},
				test.WaitStep{Delay: 6 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     3,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 3},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.KillStep{Kind: test.DropSoft, UID: 2},
				test.WaitStep{Delay: 1 * time.Second},
				test.JoinStep{
					UID:     2,
					HostUID: 1,
				},
				test.WaitStep{Delay: 10 * time.Second},
				test.EndStep{},
			},
		},
	}
	for _, sc := range scenarios {
		if err := test.RunScenario(scenarioCtx, sc); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Scenario '%s' failed: %v\n", sc.Name, err)
		}
	}

	<-ctx.Done()
	fmt.Println("\nStopping...")
}

// -------- flags & token parsing --------

func parseFlags() test.Config {
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
		startD   = flag.Duration("start-delay", 750*time.Millisecond, "Initial small delay (optional)")
		stepD    = flag.Duration("step-delay", 300*time.Millisecond, "Delay between join/connect commands")
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

	var ps []test.Player
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
		ps = append(ps, test.Player{UID: id, Name: n, Token: tok})
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range ps {
		ps[i].GpgClient = mustFreePort()
		ps[i].GpgServer = mustFreePort()
		ps[i].MarkLauncherUp(false)
		ps[i].MarkAdapterUp(false)
	}

	return test.Config{
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
	defer func(l net.Listener) {
		_ = l.Close()
	}(l)
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	return strconv.Atoi(portStr)
}
