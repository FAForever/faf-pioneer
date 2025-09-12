package test

import (
	"io"
	"os/exec"
	"sync"
	"time"
)

type Player struct {
	UID       int
	Name      string
	Token     string
	GpgClient int // --gpgnet-client-port (launcher listens, adapter connects)
	GpgServer int // --gpgnet-port (adapter listens)

	Launcher *exec.Cmd
	Adapter  *exec.Cmd
	inW      io.WriteCloser // stdin of launcher

	logPrefix string
	dlvPortL  int // delve for launcher
	dlvPortA  int // delve for adapter

	// observed state
	stateMu     sync.RWMutex
	gameState   string
	aliveMu     sync.RWMutex
	launcherUp  bool
	adapterUp   bool
	lastAdapter time.Time
}

func (p *Player) setGameState(s string) {
	p.stateMu.Lock()
	p.gameState = s
	p.stateMu.Unlock()
}

func (p *Player) getGameState() string {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.gameState
}

func (p *Player) MarkLauncherUp(v bool) {
	p.aliveMu.Lock()
	p.launcherUp = v
	p.aliveMu.Unlock()
}

func (p *Player) MarkAdapterUp(v bool) {
	p.aliveMu.Lock()
	p.adapterUp = v
	if v {
		p.lastAdapter = time.Now()
	}
	p.aliveMu.Unlock()
}

func (p *Player) bumpAdapterActivity() {
	p.aliveMu.Lock()
	p.lastAdapter = time.Now()
	p.aliveMu.Unlock()
}

func (p *Player) lastAdapterActivity() time.Time {
	p.aliveMu.RLock()
	defer p.aliveMu.RUnlock()
	return p.lastAdapter
}
