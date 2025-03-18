package main

import (
	"faf-pioneer/applog"
	"faf-pioneer/faf"
	"faf-pioneer/gpgnet"
	"faf-pioneer/launcher"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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

	// Client starts an own GPG-Net server that used to communicate between FAF-Client and FAF.exe.
	// So for that we need to create GpgNetServer and start listening on gpgNetClientPort.

	adapterToFafClient := make(chan gpgnet.Message)
	fafClientToAdapter := make(chan gpgnet.Message)

	server := faf.NewGpgNetLauncherServer(ctx, info.GpgNetClientPort)

	err := server.Listen(adapterToFafClient, fafClientToAdapter)
	if err != nil {
		applog.Fatal("Failed to connect to GPG-Net server", zap.Error(err))
	}

	/*
		cr := util.NewCancelableIoReader(ctx, os.Stdin)
		scanner := bufio.NewScanner(cr)
		fmt.Printf("Enter command: ")

		for scanner.Scan() {

				value := scanner.Text()

				// TODO: Not actually used for testing, CreateLobby already automated in
				// 		 `GpgNetLauncherClient::processMessage`.


				// `Create Lobby` command for testing.
				if strings.HasPrefix(value, "create") {
					// Receive GameState=Idle "hello" from game
					gameStateLobby := <-adapterToFafClient

					var createGameLobbyMessage faf.GpgMessage = &faf.CreateLobbyMessage{
						Command:          "CreateLobby",
						LobbyInitMode:    0,
						LobbyPort:        60001,
						LocalPlayerName:  "p4block",
						LocalPlayerId:    1, //18746,
						UnknownParameter: 1,
					}

					fafClientToAdapter <- &createGameLobbyMessage
					gameStateLobby = <-adapterToFafClient

					var hostGameMessage faf.GpgMessage = &faf.HostGameMessage{
						Command: "HostGame",
						MapName: "",
					}
					fafClientToAdapter <- &hostGameMessage

					applog.Info("GameStateLobby", zap.Any("state", gameStateLobby))

					var connectToPeerMessage faf.GpgMessage = &faf.ConnectToPeerMessage{
						Command:           "ConnectToPeer",
						RemotePlayerId:    2,
						RemotePlayerLogin: "Brutus5000",
						Destination:       "127.0.0.1:60002",
					}
					fafClientToAdapter <- &connectToPeerMessage
				}
		}
	*/
}
