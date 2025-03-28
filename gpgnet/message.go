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
	MessageCommandGameOption         MessageCommand = "GameOption"
	MessageCommandChat               MessageCommand = "Chat"
	MessageCommandJsonStats          MessageCommand = "JsonStats"
	MessageCommandGameResult         MessageCommand = "GameResult"
)

type Message interface {
	GetCommand() MessageCommand
	GetArgs() []interface{}
	Build(args []interface{}) (Message, error)
}

type BaseMessage struct {
	Command     MessageCommand
	Args        []interface{}
	SentHandler *func()
}

func (m *BaseMessage) GetCommand() MessageCommand {
	return m.Command
}

func (m *BaseMessage) GetArgs() []interface{} {
	return m.Args
}

func (m *BaseMessage) SetSentHandler(fn func()) {
	m.SentHandler = &fn
}

func (m *BaseMessage) CallSentHandler() {
	if m.SentHandler != nil {
		(*m.SentHandler)()
	}
}

func (m *BaseMessage) Build(_ []interface{}) (Message, error) {
	return m, fmt.Errorf("should not be called for base message")
}

type messageBuilder = func(args []interface{}) (Message, error)

var messagesRegistry = map[MessageCommand]messageBuilder{
	MessageCommandCreateLobby:        new(CreateLobbyMessage).Build,
	MessageCommandHostGame:           new(HostGameMessage).Build,
	MessageCommandJoinGame:           new(JoinGameMessage).Build,
	MessageCommandConnectToPeer:      new(ConnectToPeerMessage).Build,
	MessageCommandDisconnectFromPeer: new(DisconnectFromPeerMessage).Build,
	MessageCommandGameState:          new(GameStateMessage).Build,
	MessageCommandGameEnded:          new(GameEndedMessage).Build,
	MessageCommandGameFull:           new(GameFullMessage).Build,
	MessageCommandGameOption:         new(GameOptionMessage).Build,
	MessageCommandChat:               new(ChatMessage).Build,
	MessageCommandJsonStats:          new(JsonStatsMessage).Build,
	MessageCommandGameResult:         new(GameResultMessage).Build,
}

func (m *BaseMessage) TryParse() (Message, error) {
	builder, exists := messagesRegistry[m.Command]
	if !exists {
		return m, nil
	}

	msg, err := builder(m.Args)
	if err != nil {
		applog.Error("Failed to build GPG-Net message",
			zap.String("command", m.Command),
			zap.Error(err),
		)
	}
	return msg, err
}
