package gpgnet

import "fmt"

type DisconnectFromPeerMessage struct {
	BaseMessage
	RemotePlayerId int32
	sentHandler    *func()
}

func NewDisconnectFromPeerMessage(
	remotePlayerId int32,
) Message {
	return &DisconnectFromPeerMessage{
		RemotePlayerId: remotePlayerId,
	}
}

func (m *DisconnectFromPeerMessage) GetCommand() string {
	return MessageCommandDisconnectFromPeer
}

func (m *DisconnectFromPeerMessage) GetArgs() []interface{} {
	return []interface{}{
		m.RemotePlayerId,
	}
}

const disconnectFromPeerMessageArgs = 1

func (m *DisconnectFromPeerMessage) Build(args []interface{}) (Message, error) {
	if len(args) < disconnectFromPeerMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), disconnectFromPeerMessageArgs)
	}

	m.RemotePlayerId = args[0].(int32)
	return m, nil
}
