package forgedalliance

import (
	"log"
	"testing"
)

// This test starts the game and sends some mock lobby server messages to get it to initialize out of its blackscreen state
func TestStandalone(t *testing.T) {
	gpgNetServer := NewGpgNetServer(31000)

	gameToAdapter := make(chan *GpgMessage)
	adapterToGame := make(chan *GpgMessage)

	go gpgNetServer.Listen(gameToAdapter, adapterToGame)

	log.Println("GpgNet TCP server started, please start the game now")

	log.Println("Forged Alliance started successfully!")

	// Receive GameState=Idle "hello" from game
	gameStateLobby := <-gameToAdapter

	// Send message to game to create the lobby and receive acknowledgement
	var createGameLobbyMessage GpgMessage = &CreateLobbyMessage{
		Command:          "CreateLobby",
		LobbyInitMode:    0,
		LobbyPort:        60000,
		LocalPlayerName:  "p4block",
		LocalPlayerId:    18746,
		UnknownParameter: 1,
	}
	adapterToGame <- &createGameLobbyMessage
	gameStateLobby = <-gameToAdapter

	// Send mapname (optional, it will use a fallback default map if empty)
	// This message is required else game is stuck on Connecting...
	var message GpgMessage = &HostGameMessage{
		Command: "HostGame",
		MapName: "",
	}
	adapterToGame <- &message
	gameStateLobby = <-gameToAdapter

	// Send more things if desired..
	// var message..

	log.Printf("GameStateLobby: %v", gameStateLobby)

}
