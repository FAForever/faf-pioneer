package main

import (
	"context"
	"faf-pioneer/adapter"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"github.com/sirupsen/logrus"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer cancel()

	info := launcher.NewInfoFromFlags()
	util.SetupLogging(info.UserId, info.GameId)

	if err := info.Validate(); err != nil {
		logrus.Fatalf("%v", err)
	}

	adapterInstance := adapter.New(ctx, info)
	if err := adapterInstance.Start(); err != nil {
		logrus.Fatalf("Failed to start adtaper: %v", err)
	}
}
