package gpgnet

import (
	"encoding/json"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
)

type JsonStatsEntryCounter struct {
	Mass   float64 `json:"mass"`
	Count  int64   `json:"count"`
	Energy float64 `json:"energy"`
}

type JsonStatsEntryGeneral struct {
	LastUpdateTick uint64                `json:"lastupdatetick"`
	Score          float64               `json:"score"`
	CurrentCap     float64               `json:"currentcap"`
	CurrentUnits   float64               `json:"currentunits"`
	Lost           JsonStatsEntryCounter `json:"lost"`
	Kills          JsonStatsEntryCounter `json:"kills"`
	Built          JsonStatsEntryCounter `json:"built"`
}

type JsonUnitCounters struct {
	Lost  int64 `json:"lost"`
	Kills int64 `json:"kills"`
	Built int64 `json:"built"`
}

type JsonBlueprintStats struct {
	Kills        int64   `json:"kills"`
	Built        int64   `json:"built"`
	Lost         int64   `json:"lost"`
	LowestHealth float64 `json:"lowest_health"`
}

type JsonResourceIn struct {
	Total       float64 `json:"total"`
	Reclaimed   float64 `json:"reclaimed,omitempty"`
	ReclaimRate float64 `json:"reclaimRate,omitempty"`
	Rate        float64 `json:"rate"`
}

type JsonResourceOut struct {
	Total  float64 `json:"total"`
	Rate   float64 `json:"rate"`
	Excess float64 `json:"excess,omitempty"`
}

type JsonResourceStorage struct {
	StoredEnergy float64 `json:"storedEnergy"`
	MaxEnergy    float64 `json:"maxEnergy"`
	MaxMass      float64 `json:"maxMass"`
	StoredMass   float64 `json:"storedMass"`
}

type JsonResources struct {
	MassIn    JsonResourceIn      `json:"massin"`
	EnergyOut JsonResourceOut     `json:"energyout"`
	Storage   JsonResourceStorage `json:"storage"`
	EnergyIn  JsonResourceIn      `json:"energyin"`
	MassOut   JsonResourceOut     `json:"massout"`
}

type JsonStatsEntry struct {
	Type       string                        `json:"type"`
	Name       string                        `json:"name"`
	Faction    int                           `json:"faction"`
	Defeated   float64                       `json:"defeated"`
	General    JsonStatsEntryGeneral         `json:"general"`
	Units      map[string]JsonUnitCounters   `json:"units"`
	Blueprints map[string]JsonBlueprintStats `json:"blueprints"`
	Resources  JsonResources                 `json:"resources"`
}

type JsonStatsData struct {
	Stats []JsonStatsEntry `json:"stats"`
}

type JsonStatsMessage struct {
	Raw json.RawMessage
}

func NewJsonStatsMessage(
	data JsonStatsData,
) (error, Message) {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal JsonStatsMessage: %v", err), nil
	}
	return nil, &JsonStatsMessage{Raw: b}
}

func (m *JsonStatsMessage) GetCommand() string {
	return MessageCommandJsonStats
}

func (m *JsonStatsMessage) GetArgs() []interface{} {
	if len(m.Raw) == 0 {
		return []interface{}{"{}"}
	}
	return []interface{}{
		string(m.Raw),
	}
}

const jsonStatsMessageArgs = 1

func (m *JsonStatsMessage) Build(args []interface{}) (Message, error) {
	if len(args) < jsonStatsMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), jsonStatsMessageArgs)
	}

	switch v := args[0].(type) {
	case string:
		m.Raw = json.RawMessage(v)
	case []byte:
		m.Raw = v
	default:
		return nil, fmt.Errorf("unexpected arg[0] type %T for JsonStatsMessage", v)
	}
	return m, nil
}

func (m *JsonStatsMessage) GetData() (JsonStatsData, error) {
	var out JsonStatsData
	if len(m.Raw) == 0 {
		return out, nil
	}

	if err := json.Unmarshal(m.Raw, &out); err == nil {
		return out, nil
	}

	norm, err := util.NormalizeJSONKeysToLower(m.Raw)
	if err != nil {
		applog.Warn("Getting JsonStatsMessage data failed at NormalizeJSONKeysToLower", zap.Error(err))
		return out, json.Unmarshal(m.Raw, &out)
	}
	if err = json.Unmarshal(norm, &out); err != nil {
		return out, err
	}
	return out, nil
}
