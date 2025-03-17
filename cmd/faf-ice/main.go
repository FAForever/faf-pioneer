package main

import (
	"context"
	"faf-pioneer/adapter"
	"faf-pioneer/applog"
	"faf-pioneer/launcher"
	"go.uber.org/zap"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer cancel()

	info := launcher.NewInfoFromFlags()
	applog.Initialize(info.UserId, info.GameId)
	defer applog.Shutdown()

	if err := info.Validate(); err != nil {
		applog.Fatal("Failed to validate command line arguments", zap.Error(err))
		return
	}

	adapterInstance := adapter.New(ctx, info)
	if err := adapterInstance.Start(); err != nil {
		applog.Fatal("Failed to start adapter", zap.Error(err))
	}
}
