package gpgnet

import (
	"faf-pioneer/applog"
	"fmt"
	"go.uber.org/zap"
)

type MessageCommand = string

const (
	MessageCommandCreateLobby        MessageCommand = "CreateLobby"
	MessageCommandHostGame           MessageCommand = "HostGame"
	MessageCommandJoinGame           MessageCommand = "JoinGame"
	MessageCommandConnectToPeer      MessageCommand = "ConnectToPeer"
	MessageCommandDisconnectFromPeer MessageCommand = "DisconnectFromPeer"
	MessageCommandGameState          MessageCommand = "GameState"
	MessageCommandGameEnded          MessageCommand = "GameEnded"
	MessageCommandGameFull           MessageCommand = "GameFull"
)

type Message interface {
	GetCommand() MessageCommand
	GetArgs() []interface{}
	Build(args []interface{}) error
}

type BaseMessage struct {
	Command MessageCommand
	Args    []interface{}
}

func (m *BaseMessage) GetCommand() MessageCommand {
	return m.Command
}

func (m *BaseMessage) GetArgs() []interface{} {
	return m.Args
}

func (m *BaseMessage) Build(_ []interface{}) error {
	return fmt.Errorf("should not be called for base message")
}

var messagesRegistry = map[MessageCommand]func() Message{
	MessageCommandCreateLobby:        func() Message { return new(CreateLobbyMessage) },
	MessageCommandHostGame:           func() Message { return new(HostGameMessage) },
	MessageCommandJoinGame:           func() Message { return new(JoinGameMessage) },
	MessageCommandConnectToPeer:      func() Message { return new(ConnectToPeerMessage) },
	MessageCommandDisconnectFromPeer: func() Message { return new(DisconnectFromPeerMessage) },
	MessageCommandGameState:          func() Message { return new(GameStateMessage) },
	MessageCommandGameEnded:          func() Message { return new(GameEndedMessage) },
	MessageCommandGameFull:           func() Message { return new(GameFullMessage) },
}

func (m *BaseMessage) TryParse() (Message, error) {
	constructor, exists := messagesRegistry[m.Command]
	if !exists {
		return m, nil
	}

	msg := constructor()
	if err := msg.Build(m.Args); err != nil {
		applog.Error("Failed to build GPG-Net message",
			zap.String("command", m.Command),
			zap.Error(err),
		)
		return m, err
	}
	return msg, nil
}
