package gpgnet

type GameFullMessage struct {
}

func NewGameFullMessage() Message {
	return &GameFullMessage{}
}

func (m *GameFullMessage) GetCommand() string {
	return MessageCommandGameFull
}

func (m *GameFullMessage) GetArgs() []interface{} {
	return []interface{}{}
}

func (m *GameFullMessage) Build(_ []interface{}) error {
	return nil
}
