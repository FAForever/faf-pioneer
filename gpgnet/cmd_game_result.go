package gpgnet

import "fmt"

type GameResultMessage struct {
	TeamId int32
	Result string
}

func (m *GameResultMessage) GetCommand() string {
	return MessageCommandGameResult
}

func (m *GameResultMessage) GetArgs() []interface{} {
	return []interface{}{
		m.TeamId,
		m.Result,
	}
}

const gameResultMessageArgs = 2

func (m *GameResultMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameResultMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameResultMessageArgs)
	}

	m.TeamId = args[0].(int32)
	m.Result = args[1].(string)
	return m, nil
}
