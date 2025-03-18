package gpgnet

import "fmt"

type LobbyInitMode = int

const (
	// LobbyInitModeNormal is a normal lobby.
	LobbyInitModeNormal LobbyInitMode = 0
	// LobbyInitModeAuto skip lobby screen (e.g. ranked).
	LobbyInitModeAuto LobbyInitMode = 1
)

type CreateLobbyMessage struct {
	LobbyInitMode    LobbyInitMode
	LobbyPort        uint16
	LocalPlayerName  string
	LocalPlayerId    uint32
	UnknownParameter int
}

func NewCreateLobbyMessage(
	lobbyInitMode LobbyInitMode,
	lobbyPort uint16,
	playerName string,
	playerId uint32,
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

func (m *CreateLobbyMessage) Build(args []interface{}) error {
	if len(args) < createLobbyMessageArgs {
		return fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), createLobbyMessageArgs)
	}

	m.LobbyInitMode = args[0].(int)
	m.LobbyPort = args[1].(uint16)
	m.LocalPlayerName = args[2].(string)
	m.LocalPlayerId = args[3].(uint32)
	m.UnknownParameter = args[4].(int)
	return nil
}
