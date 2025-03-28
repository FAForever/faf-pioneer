package gpgnet

import "fmt"

type ChatMessage struct {
	Message string
}

func (m *ChatMessage) GetCommand() string {
	return MessageCommandChat
}

func (m *ChatMessage) GetArgs() []interface{} {
	return []interface{}{
		m.Message,
	}
}

const chatMessageArgs = 1

func (m *ChatMessage) Build(args []interface{}) (Message, error) {
	if len(args) < chatMessageArgs {
		return m, fmt.Errorf("not enough arguments to parse (%d < %d)", len(args), chatMessageArgs)
	}

	m.Message = args[0].(string)
	return m, nil
}
