package gpgnet

import "fmt"

type ConnectToPeerMessage struct {
	RemotePlayerLogin string
	RemotePlayerId    uint
	Destination       string
}

func NewConnectToPeerMessage(
	remotePlayerLogin string,
	remotePlayerId uint,
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

func (m *ConnectToPeerMessage) Build(args []interface{}) error {
	if len(args) < connectToPeerMessageArgs {
		return fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), connectToPeerMessageArgs)
	}

	m.RemotePlayerLogin = args[1].(string)
	m.RemotePlayerId = args[2].(uint)
	m.Destination = args[3].(string)
	return nil
}
