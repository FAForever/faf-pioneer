package gpgnet

import "fmt"

type LobbyInitMode = int32

const (
	// LobbyInitModeNormal is a normal lobby.
	LobbyInitModeNormal LobbyInitMode = 0
	// LobbyInitModeAuto skip lobby screen (e.g. ranked).
	LobbyInitModeAuto LobbyInitMode = 1
)

type CreateLobbyMessage struct {
	LobbyInitMode    LobbyInitMode
	LobbyPort        int32
	LocalPlayerName  string
	LocalPlayerId    int32
	UnknownParameter int32
}

func NewCreateLobbyMessage(
	lobbyInitMode LobbyInitMode,
	lobbyPort int32,
	playerName string,
	playerId int32,
) Message {
	return &CreateLobbyMessage{
		LobbyInitMode:    lobbyInitMode,
		LobbyPort:        lobbyPort,
		LocalPlayerName:  playerName,
		LocalPlayerId:    playerId,
		UnknownParameter: 1,
	}
}

func (m *CreateLobbyMessage) GetCommand() string {
	return MessageCommandCreateLobby
}

func (m *CreateLobbyMessage) GetArgs() []interface{} {
	return []interface{}{
		m.LobbyInitMode,
		m.LobbyPort,
		m.LocalPlayerName,
		m.LocalPlayerId,
		m.UnknownParameter,
	}
}

const createLobbyMessageArgs = 5

func (m *CreateLobbyMessage) Build(args []interface{}) (Message, error) {
	if len(args) < createLobbyMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), createLobbyMessageArgs)
	}

	m.LobbyInitMode = args[0].(int32)
	m.LobbyPort = args[1].(int32)
	m.LocalPlayerName = args[2].(string)
	m.LocalPlayerId = args[3].(int32)
	m.UnknownParameter = args[4].(int32)
	return m, nil
}
