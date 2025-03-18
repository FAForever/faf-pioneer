package gpgnet

import "fmt"

type HostGameMessage struct {
	MapName string
}

func NewHostGameMessage(
	mapName string,
) Message {
	return &HostGameMessage{
		MapName: mapName,
	}
}

func (m *HostGameMessage) GetCommand() string {
	return MessageCommandHostGame
}

func (m *HostGameMessage) GetArgs() []interface{} {
	return []interface{}{
		m.MapName,
	}
}

const hostGameMessageArgs = 1

func (m *HostGameMessage) Build(args []interface{}) (Message, error) {
	if len(args) < hostGameMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), hostGameMessageArgs)
	}

	m.MapName = args[0].(string)
	return m, nil
}
