package main

import (
	"bufio"
	"faf-pioneer/faf"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"fmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	defer cancel()

	info := launcher.NewInfoFromFlags()
	util.SetupLogging(info.UserId, info.GameId)
	defer util.ShutdownLogging()

	if err := info.Validate(); err != nil {
		logrus.Fatalf("%v", err)
	}

	// Client starts an own GPG-Net server that used to communicate between FAF-Client and FAF.exe.
	// So for that we need to create GpgNetServer and start listening on gpgNetClientPort.

	adapterToFafClient := make(chan *faf.GpgMessage)
	fafClientToAdapter := make(chan *faf.GpgMessage)

	server := faf.NewGpgNetLauncherServer(ctx, info.GpgNetClientPort)
	err := server.Listen(adapterToFafClient, fafClientToAdapter)
	if err != nil {
		logrus.Fatalf("Failed to connect to GPG-Net server: %v", err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("Enter command: ")

	for scanner.Scan() {
		value := scanner.Text()

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

			logrus.WithField("state", gameStateLobby).Info("GameStateLobby")

			var connectToPeerMessage faf.GpgMessage = &faf.ConnectToPeerMessage{
				Command:           "ConnectToPeer",
				RemotePlayerId:    2,
				RemotePlayerLogin: "Brutus5000",
				Destination:       "127.0.0.1:60002",
			}
			fafClientToAdapter <- &connectToPeerMessage
		}
	}
}
