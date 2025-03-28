package gpgnet

import (
	"encoding/json"
	"faf-pioneer/applog"
	"fmt"
	"go.uber.org/zap"
)

type JsonStatsEntryGeneral struct {
	LastUpdateTick uint64 `json:"lastupdatetick"`
	Score          uint64 `json:"score"`
	CurrentCap     uint16 `json:"currentcap"`
	CurrentUnits   uint16 `json:"currentunits"`
}

type JsonStatsEntry struct {
	Type     string                `json:"type"`
	Name     string                `json:"name"`
	Faction  int                   `json:"faction"`
	Defeated float32               `json:"defeated"`
	General  JsonStatsEntryGeneral `json:"general"`
}

type JsonStatsData struct {
	Stats []JsonStatsEntry `json:"stats"`
}

type JsonStatsMessage struct {
	Data JsonStatsData
}

func NewJsonStatsMessage(
	data JsonStatsData,
) Message {
	return &JsonStatsMessage{
		Data: data,
	}
}

func (m *JsonStatsMessage) GetCommand() string {
	return MessageCommandJsonStats
}

func (m *JsonStatsMessage) GetArgs() []interface{} {
	data, err := json.Marshal(m.Data)
	if err != nil {
		applog.Error("Failed to marshal JsonStatsMessage", zap.Error(err))
		return []interface{}{
			"{}",
		}
	}

	return []interface{}{
		string(data),
	}
}

const jsonStatsMessageArgs = 1

func (m *JsonStatsMessage) Build(args []interface{}) (Message, error) {
	if len(args) < jsonStatsMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), jsonStatsMessageArgs)
	}

	m.Data = JsonStatsData{}

	stringData := args[0].(string)
	err := json.Unmarshal([]byte(stringData), &m.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json stats message: %w", err)
	}

	return m, nil
}
