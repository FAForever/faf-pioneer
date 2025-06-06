package main

import (
	"context"
	"faf-pioneer/adapter"
	"faf-pioneer/applog"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer cancel()

	info := launcher.NewInfoFromFlags()
	err := applog.Initialize(info.UserId, info.GameId, info.LogLevel, info.LogPath)
	if err != nil {
		fmt.Printf("Failed to initialize app logger: %v\n", err)
	}

	defer applog.Shutdown()
	defer util.WrapAppContextCancelExitMessage(ctx, "Adapter")

	if err = info.Validate(); err != nil {
		applog.Error("Failed to validate command line arguments", zap.Error(err))
		return
	}

	adapterInstance := adapter.New(ctx, cancel, info)

	// If we agreed to share adapter logs, set the remote log sender for logging.
	if info.ConsentLogSharing {
		applog.NoRemote().Info("Log sharing are enabled")
		applog.SetRemoteLogSender(adapterInstance)
	} else {
		applog.NoRemote().Info("Log sharing is not enabled")
	}

	applog.LogStartupInfo(info)

	if err = adapterInstance.Start(); err != nil {
		applog.Error("Failed to start adapter", zap.Error(err))
	}
}
