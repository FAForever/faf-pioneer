package gpgnet

type GameEndedMessage struct {
}

func NewGameEndedMessage() Message {
	return &GameEndedMessage{}
}

func (m *GameEndedMessage) GetCommand() string {
	return MessageCommandGameEnded
}

func (m *GameEndedMessage) GetArgs() []interface{} {
	return []interface{}{}
}

func (m *GameEndedMessage) Build(_ []interface{}) (Message, error) {
	return m, nil
}
