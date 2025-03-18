package gpgnet

import "fmt"

type GameState = string

const (
	GameStateNone      GameState = "None"
	GameStateIde       GameState = "Idle"
	GameStateLobby     GameState = "Lobby"
	GameStateLaunching GameState = "Launching"
	GameStateEnded     GameState = "Ended"
)

type GameStateMessage struct {
	State GameState
}

func NewGameStateMessage(
	state GameState,
) Message {
	return &GameStateMessage{
		State: state,
	}
}

func (m *GameStateMessage) GetCommand() string {
	return MessageCommandGameState
}

func (m *GameStateMessage) GetArgs() []interface{} {
	return []interface{}{
		m.State,
	}
}

const gameStateMessageArgs = 1

func (m *GameStateMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameStateMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameStateMessageArgs)
	}

	m.State = args[0].(GameState)
	return m, nil
}
