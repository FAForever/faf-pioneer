package faf

import (
	"faf-pioneer/webrtc"
	"log/slog"
	"testing"
)

type MockPeerHandler struct{}
type MockPeer struct {
	playerId uint
}

func (*MockPeer) IsOfferer() bool {
	return false
}

func (p *MockPeer) PeerId() uint {
	return p.playerId
}

func (p *MockPeerHandler) AddPeerIfMissing(playerId uint) webrtc.PeerMeta {
	slog.Info("AddPeerIfMissing", slog.Any("playerId", playerId))
	return &MockPeer{playerId: playerId}
}

// This test starts the game and sends some mock lobby server messages to get it to initialize out of its blackscreen gameState
func TestAdapter2Game(t *testing.T) {
	gpgNetServer := NewGpgNetServer(&MockPeerHandler{}, 21001)

	gameToAdapter := make(chan *GpgMessage)
	adapterToGame := make(chan *GpgMessage)

	go gpgNetServer.Listen(gameToAdapter, adapterToGame)

	slog.Info("GpgNet TCP server started, please start the game now")

	slog.Info("Forged Alliance started successfully!")

	// Receive GameState=Idle "hello" from game
	gameStateLobby := <-gameToAdapter

	// Send message to game to create the lobby and receive acknowledgement
	var createGameLobbyMessage GpgMessage = &CreateLobbyMessage{
		Command:          "CreateLobby",
		LobbyInitMode:    0,
		LobbyPort:        60001,
		LocalPlayerName:  "p4block",
		LocalPlayerId:    1, //18746,
		UnknownParameter: 1,
	}
	adapterToGame <- &createGameLobbyMessage
	gameStateLobby = <-gameToAdapter

	// Send mapname (optional, it will use a fallback default map if empty)
	// This message is required else game is stuck on Connecting...
	var hostGameMessage GpgMessage = &HostGameMessage{
		Command: "HostGame",
		MapName: "",
	}
	adapterToGame <- &hostGameMessage

	slog.Info("GameStateLobby", slog.Any("gameState", gameStateLobby))

	var conectToPeerMessage GpgMessage = &ConnectToPeerMessage{
		Command:           "ConnectToPeer",
		RemotePlayerId:    2,
		RemotePlayerLogin: "Brutus5000",
		Destination:       "127.0.0.1:60002",
	}
	adapterToGame <- &conectToPeerMessage

}
