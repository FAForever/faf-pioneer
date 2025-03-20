package main

import (
	"bufio"
	"faf-pioneer/applog"
	"faf-pioneer/faf"
	"faf-pioneer/gpgnet"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

	// Client starts an own GPG-Net server that used to communicate between FAF-Client and FAF.exe.
	// So for that we need to create GpgNetServer and start listening on gpgNetClientPort.

	adapterToFafClient := make(chan gpgnet.Message)
	fafClientToAdapter := make(chan gpgnet.Message)

	server := faf.NewGpgNetLauncherServer(ctx, info, info.GpgNetClientPort)
	fafProcess := NewGameProcess(ctx, info)

	adapterConnected := func() {
		if err := fafProcess.Start(); err != nil {
			applog.Error("Failed to start game process", zap.Error(err))
			return
		}

		// Let's wait for process to exit in another routine,
		// so we can do graceful shutdown for launcher emulator process.
		go func() {
			_ = fafProcess.cmd.Wait()
			cancel()
		}()
	}

	go func() {
		err := server.Listen(adapterToFafClient, fafClientToAdapter, adapterConnected)
		if err != nil {
			applog.Fatal("Failed to connect to GPG-Net server", zap.Error(err))
		}
	}()

	cr := util.NewCancelableIoReader(ctx, os.Stdin)
	scanner := bufio.NewScanner(cr)

	for scanner.Scan() {
		value := scanner.Text()
		applog.Debug("Entered command", zap.String("rawCommand", value))

		if strings.HasPrefix(value, "host") {
			applog.Info("Sending host game messages to the adapter/game")

			if server.GetGameState() != gpgnet.GameStateIde {
				applog.Warn("Game is not in Idle state yet, wait and retry")
				continue
			}

			server.SendMessagesToGame(
				//gpgnet.NewCreateLobbyMessage(
				//	gpgnet.LobbyInitModeNormal,
				//	// LocalGameUdpPort
				//	int32(info.GameUdpPort),
				//	"UserA",
				//	// PlayerId
				//	int32(info.UserId),
				//),
				gpgnet.NewHostGameMessage(""),
			)
			continue
		}

		if strings.HasPrefix(value, "accept") {
			applog.Info("Emulating that UserB trying to connect to our hosted game")

			server.SendMessagesToGame(gpgnet.NewConnectToPeerMessage(
				"UserB",
				2,
				"127.0.0.1:14081",
			))
			continue
		}

		if strings.HasPrefix(value, "join") {
			applog.Info("First joining stage sending JoinGameMessage")

			server.SendMessagesToGame(
				//gpgnet.NewCreateLobbyMessage(
				//	gpgnet.LobbyInitModeNormal,
				//	// LocalGameUdpPort
				//	int32(info.GameUdpPort),
				//	"UserB",
				//	// PlayerId
				//	int32(info.UserId),
				//),
				gpgnet.NewJoinGameMessage(
					"UserA",
					1,
					// fmt.Sprintf("127.0.0.1:%d", int32(info.GpgNetPort)),
					fmt.Sprintf("127.0.0.1:%d", 14080),
				),
			)
			continue
		}

		if strings.HasPrefix(value, "connect_to") {
			applog.Info("Sending join game messages to the adapter/game")

			// connect_to UserA 1 14080
			// connect_to UserB 2 14081

			args := strings.Split(value, " ")[1:]
			user := args[0]
			uid, _ := strconv.Atoi(args[1])
			port, _ := strconv.Atoi(args[2])

			server.SendMessagesToGame(
				gpgnet.NewConnectToPeerMessage(
					user,
					int32(uid),
					fmt.Sprintf("127.0.0.1:%d", int32(port)),
				),
			)

			continue
		}

		if strings.HasPrefix(value, "quit") {
			_ = server.Close()
			return
		}

		// User B (second user) should run:
		// Send JoinGameMessage to UDP port of second player `fmt.Sprintf("127.0.0.1:%d", 14080)`
		// Commands:
		// join

		// User A (host) should run:
		// Send HostGameMessage to FAF.exe
		// Commands:
		// host
		// accept (or `connect_to UserB 2 14081`)
	}
}
