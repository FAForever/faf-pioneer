package test

import "time"

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
