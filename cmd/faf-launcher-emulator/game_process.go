package main

import (
	"context"
	"faf-pioneer/launcher"
	"fmt"
	"os"
	"os/exec"
	"path"
	"time"
)

type GameProcess struct {
	ctx         context.Context
	rootPath    string
	exePath     string
	logFilePath string
	cmd         *exec.Cmd
}

func NewGameProcess(ctx context.Context, info *launcher.Info) *GameProcess {
	programDataPath := os.Getenv("ProgramData")
	gp := &GameProcess{
		ctx:         ctx,
		rootPath:    path.Join(programDataPath, "FAForever"),
		logFilePath: path.Join(programDataPath, "ForgedAlliance.log"),
	}

	gp.exePath = path.Join(gp.rootPath, "bin", "ForgedAlliance.exe")
	gp.logFilePath = path.Join(gp.rootPath, "log", gp.generateLogFileName(info))
	gp.cmd = exec.CommandContext(
		ctx,
		gp.exePath,
		"/init", "init.lua",
		"/country", "NL",
		"/nobugreport",
		"/gpgnet", fmt.Sprintf("127.0.0.1:%d", info.GpgNetPort),
		"/numgames", "1",
		"/log", fmt.Sprintf("\"%s\"", gp.logFilePath),
	)

	return gp
}

func (p *GameProcess) Start() error {
	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("could not start game process: %v", err)
	}

	return nil
}

func (p *GameProcess) GetPid() int {
	if p.cmd.Process == nil {
		return -1
	}
	return p.cmd.Process.Pid
}

func (p *GameProcess) generateLogFileName(info *launcher.Info) string {
	return fmt.Sprintf("game_uId_%d_%s.log",
		info.UserId,
		time.Now().Format("2006-01-02_15-04-05"))
}
