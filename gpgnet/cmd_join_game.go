package gpgnet

import "fmt"

type JoinGameMessage struct {
	RemotePlayerLogin string
	RemotePlayerId    int32
	Destination       string
}

func NewJoinGameMessage(
	remotePlayerLogin string,
	remotePlayerId int32,
	destination string,
) Message {
	return &JoinGameMessage{
		RemotePlayerLogin: remotePlayerLogin,
		RemotePlayerId:    remotePlayerId,
		Destination:       destination,
	}
}

func (m *JoinGameMessage) GetCommand() string {
	return MessageCommandJoinGame
}

func (m *JoinGameMessage) GetArgs() []interface{} {
	return []interface{}{
		m.Destination,
		m.RemotePlayerLogin,
		m.RemotePlayerId,
	}
}

const joinGameMessageArgs = 3

func (m *JoinGameMessage) Build(args []interface{}) (Message, error) {
	if len(args) < joinGameMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), connectToPeerMessageArgs)
	}

	m.Destination = args[0].(string)
	m.RemotePlayerLogin = args[1].(string)
	m.RemotePlayerId = args[2].(int32)
	return m, nil
}
