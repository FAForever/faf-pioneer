package gpgnet

import "fmt"

type ConnectToPeerMessage struct {
	RemotePlayerLogin string
	RemotePlayerId    int32
	Destination       string
}

func NewConnectToPeerMessage(
	remotePlayerLogin string,
	remotePlayerId int32,
	destination string,
) Message {
	return &ConnectToPeerMessage{
		RemotePlayerLogin: remotePlayerLogin,
		RemotePlayerId:    remotePlayerId,
		Destination:       destination,
	}
}

func (m *ConnectToPeerMessage) GetCommand() string {
	return MessageCommandConnectToPeer
}

func (m *ConnectToPeerMessage) GetArgs() []interface{} {
	return []interface{}{
		m.Destination,
		m.RemotePlayerLogin,
		m.RemotePlayerId,
	}
}

const connectToPeerMessageArgs = 3

func (m *ConnectToPeerMessage) Build(args []interface{}) (Message, error) {
	if len(args) < connectToPeerMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), connectToPeerMessageArgs)
	}

	m.Destination = args[0].(string)
	m.RemotePlayerLogin = args[1].(string)
	m.RemotePlayerId = args[2].(int32)
	return m, nil
}
