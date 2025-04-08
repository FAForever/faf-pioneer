package gpgnet

import "fmt"

type GameOptionKind = string

const (
	GameOptionKindShare    GameOptionKind = "Share"
	GameOptionKindUnranked GameOptionKind = "Unranked"
)

type GameOptionMessage struct {
	Kind    GameOptionKind
	RawArgs []interface{}
}

func (m *GameOptionMessage) GetCommand() string {
	return MessageCommandGameOption
}

func (m *GameOptionMessage) GetArgs() []interface{} {
	if m.RawArgs != nil {
		return m.RawArgs
	}

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
	case GameOptionKindUnranked:
		cmd := &GameOptionUnrankedMessage{
			GameOptionMessage: *m,
		}
		return cmd.Build(args[1:])
	default:
		// All other unknown or unmapped packets should be built "as is".
		m.RawArgs = args
		return m, nil
	}
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

type GameOptionShareMessage struct {
	GameOptionMessage
	Condition GameOptionShareCondition
}

func (m *GameOptionShareMessage) GetArgs() []interface{} {
	return append(m.GameOptionMessage.GetArgs(), []interface{}{
		m.Condition,
	}...)
}

const gameOptionShareMessageArgs = 1

func (m *GameOptionShareMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameOptionShareMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameOptionShareMessageArgs)
	}

	m.Condition = args[0].(GameOptionShareCondition)
	return m, nil
}

type GameOptionValueYesNo = string

const (
	GameOptionValueYes GameOptionValueYesNo = "Yes"
	GameOptionValueNo  GameOptionValueYesNo = "No"
)

type GameOptionValueOffOn = string

const (
	GameOptionValueOn  GameOptionValueOffOn = "On"
	GameOptionValueOff GameOptionValueOffOn = "Off"
)

type GameOptionUnrankedMessage struct {
	GameOptionMessage
	IsUnranked GameOptionValueYesNo
}

func (m *GameOptionUnrankedMessage) GetArgs() []interface{} {
	return append(m.GameOptionMessage.GetArgs(), []interface{}{
		m.IsUnranked,
	}...)
}

const gameOptionUnrankedMessageArgs = 1

func (m *GameOptionUnrankedMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameOptionUnrankedMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameOptionUnrankedMessageArgs)
	}

	m.IsUnranked = args[0].(GameOptionValueYesNo)
	return m, nil
}

type GameOptionAutoTeamsCondition = string

const (
	GameOptionAutoTeamsConditionNone         GameOptionAutoTeamsCondition = "none"
	GameOptionAutoTeamsConditionManual       GameOptionAutoTeamsCondition = "manual"
	GameOptionAutoTeamsConditionTopVsBottom  GameOptionAutoTeamsCondition = "tvsb"
	GameOptionAutoTeamsConditionLeftVsRight  GameOptionAutoTeamsCondition = "lvsr"
	GameOptionAutoTeamsConditionEvenVsUneven GameOptionAutoTeamsCondition = "pvsi"
	GameOptionAutoTeamsConditionUnknown      GameOptionAutoTeamsCondition = "unknown"
)

type GameOptionAutoTeamsMessage struct {
	GameOptionMessage
	Value GameOptionAutoTeamsCondition
}

func (m *GameOptionAutoTeamsMessage) GetArgs() []interface{} {
	return append(m.GameOptionMessage.GetArgs(), []interface{}{
		m.Value,
	}...)
}

const gameOptionAutoTeamsMessageArgs = 1

func (m *GameOptionAutoTeamsMessage) Build(args []interface{}) (Message, error) {
	if len(args) < gameOptionAutoTeamsMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), gameOptionAutoTeamsMessageArgs)
	}

	m.Value = args[0].(GameOptionAutoTeamsCondition)
	return m, nil
}
