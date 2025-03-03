package forgedalliance

import (
	"log"
)

type MessageType string

type GpgMessage interface {
	GetCommand() string
	GetArgs() []interface{}
}

type GenericGpgMessage struct {
	Command string
	Args    []interface{}
}

func (m *GenericGpgMessage) GetCommand() string {
	return m.Command
}

func (m *GenericGpgMessage) GetArgs() []interface{} {
	return m.Args
}

type CreateLobbyMessage struct {
	Command          string
	LobbyInitMode    int
	LobbyPort        uint16
	LocalPlayerName  string
	LocalPlayerId    uint32
	UnknownParameter int
}

func (m *CreateLobbyMessage) GetCommand() string {
	return m.Command
}

func (m *CreateLobbyMessage) GetArgs() []interface{} {
	return []interface{}{m.LobbyInitMode, m.LobbyPort, m.LocalPlayerName, m.LocalPlayerId, m.UnknownParameter}
}

type HostGameMessage struct {
	Command string
	MapName string
}

func (m *HostGameMessage) GetCommand() string {
	return m.Command
}

func (m *HostGameMessage) GetArgs() []interface{} {
	return []interface{}{m.MapName}
}

type JoinGameMessage struct {
	Command           string
	RemotePlayerLogin string
	RemotePlayerId    uint32
	Destination       string
}

func (m *JoinGameMessage) GetCommand() string {
	return m.Command
}

func (m *JoinGameMessage) GetArgs() []interface{} {
	return []interface{}{m.Destination, m.RemotePlayerLogin, m.RemotePlayerId}
}

type ConnectToPeerMessage struct {
	Command           string
	RemotePlayerLogin string
	RemotePlayerId    uint32
	Destination       string
}

func (m *ConnectToPeerMessage) GetCommand() string {
	return m.Command
}

func (m *ConnectToPeerMessage) GetArgs() []interface{} {
	return []interface{}{m.Destination, m.RemotePlayerLogin, m.RemotePlayerId}
}

type DisconnectFromPeerMessage struct {
	Command        string
	RemotePlayerId uint32
}

func (m *DisconnectFromPeerMessage) GetCommand() string {
	return m.Command
}

func (m *DisconnectFromPeerMessage) GetArgs() []interface{} {
	return []interface{}{m.RemotePlayerId}
}

type GameStateMessage struct {
	Command string
	State   string
}

func (m *GameStateMessage) GetCommand() string {
	return m.Command
}

func (m *GameStateMessage) GetArgs() []interface{} {
	return []interface{}{m.State}
}

type GameEndedMessage struct {
	Command string
}

func (m *GameEndedMessage) GetCommand() string {
	return m.Command
}

func (m *GameEndedMessage) GetArgs() []interface{} {
	return []interface{}{}
}

func (m *GenericGpgMessage) TryParse() GpgMessage {
	switch m.Command {
	case "CreateLobby":
		if len(m.Args) < 5 {
			log.Println("Error: Not enough arguments to parse CreateLobbyMessage")
			return m
		}

		return &CreateLobbyMessage{
			Command:          m.Command,
			LobbyInitMode:    m.Args[0].(int),
			LobbyPort:        m.Args[1].(uint16),
			LocalPlayerName:  m.Args[2].(string),
			LocalPlayerId:    m.Args[3].(uint32),
			UnknownParameter: m.Args[4].(int),
		}

	case "HostGame":
		if len(m.Args) < 1 {
			log.Println("Error: Not enough arguments to parse HostGameMessage")
			return m
		}

		return &HostGameMessage{
			Command: m.Command,
			MapName: m.Args[0].(string),
		}

	case "JoinGame":
		if len(m.Args) < 3 {
			log.Println("Error: Not enough arguments to parse JoinGameMessage")
			return m
		}

		return &JoinGameMessage{
			Command:           m.Command,
			RemotePlayerLogin: m.Args[1].(string),
			RemotePlayerId:    m.Args[2].(uint32),
			Destination:       m.Args[0].(string),
		}
	case "ConnectToPeer":
		if len(m.Args) < 3 {
			log.Println("Error: Not enough arguments to parse ConnectToPeerMessage")
			return m
		}

		return &ConnectToPeerMessage{
			Command:           m.Command,
			RemotePlayerLogin: m.Args[1].(string),
			RemotePlayerId:    m.Args[2].(uint32),
			Destination:       m.Args[0].(string),
		}
	case "DisconnectFromPeer":
		if len(m.Args) < 1 {
			log.Println("Error: Not enough arguments to parse DisconnectFromPeerMessage")
			return m
		}

		return &DisconnectFromPeerMessage{
			Command:        m.Command,
			RemotePlayerId: m.Args[0].(uint32),
		}
	case "GameState":
		if len(m.Args) < 1 {
			log.Println("Error: Not enough arguments to parse GameStateMessage")
			return m
		}

		return &GameStateMessage{
			Command: m.Command,
			State:   m.Args[0].(string),
		}
	case "GameEnded":
		return &GameEndedMessage{
			Command: m.Command,
		}
	default:
		return m
	}
}
