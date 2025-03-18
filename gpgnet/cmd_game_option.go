package gpgnet

import "fmt"

type GameOptionKind = string

const (
	GameOptionKindShare GameOptionKind = "Share"
)

type GameOptionMessage struct {
	Kind GameOptionKind
}

type GameOptionShareCondition = string

const (
	GameOptionShareConditionFullShare        GameOptionShareCondition = "FullShare"
	GameOptionShareConditionShareUntilDeath  GameOptionShareCondition = "ShareUntilDeath"
	GameOptionShareConditionPartialShare     GameOptionShareCondition = "PartialShare"
	GameOptionShareConditionTransferToKiller GameOptionShareCondition = "TransferToKiller"
	GameOptionShareConditionDefectors        GameOptionShareCondition = "Defectors"
	GameOptionShareConditionCivilianDeserter GameOptionShareCondition = "CivilianDeserter"
	GameOptionShareConditionCivilianUnknown  GameOptionShareCondition = "unknown"
)

func (m *GameOptionMessage) GetCommand() string {
	return MessageCommandGameOption
}

func (m *GameOptionMessage) GetArgs() []interface{} {
	return []interface{}{
		m.Kind,
	}
}

const gameOptionMessageArgs = 1

func (m *GameOptionMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameOptionMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameOptionMessageArgs)
	}

	m.Kind = args[0].(string)
	switch m.Kind {
	case GameOptionKindShare:
		cmd := &GameOptionShareMessage{
			GameOptionMessage: *m,
		}
		return cmd.Build(args[1:])
	default:
		// All other unknown or unmapped packets should be built "as is".
		return m, nil
	}
}

type GameOptionShareMessage struct {
	GameOptionMessage
	Condition GameOptionShareCondition
}

func (m *GameOptionShareMessage) GetArgs() []interface{} {
	return append(m.GameOptionMessage.GetArgs(), []interface{}{
		m.Condition,
	})
}

const gameOptionShareMessageArgs = 1

func (m *GameOptionShareMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameOptionShareMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameOptionShareMessageArgs)
	}

	m.Condition = args[0].(GameOptionShareCondition)
	return m, nil
}
