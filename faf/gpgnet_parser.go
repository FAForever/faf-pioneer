package faf

import (
	"fmt"
)

type MessageType string

type GpgMessage interface {
	GetCommand() string
	GetArgs() []interface{}
}

type GpgMessageCommand = string

const (
	GpgMessageCommandCreateLobby        GpgMessageCommand = "CreateLobby"
	GpgMessageCommandHostGame           GpgMessageCommand = "HostGame"
	GpgMessageCommandJoinGame           GpgMessageCommand = "JoinGame"
	GpgMessageCommandConnectToPeer      GpgMessageCommand = "ConnectToPeer"
	GpgMessageCommandDisconnectFromPeer GpgMessageCommand = "DisconnectFromPeer"
	GpgMessageCommandGameState          GpgMessageCommand = "GameState"
	GpgMessageCommandGameEnded          GpgMessageCommand = "GameEnded"
	GpgMessageCommandGameFull           GpgMessageCommand = "GameFull"
)

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

type LobbyInitMode = int

const (
	// LobbyInitModeNormal is a normal lobby.
	LobbyInitModeNormal LobbyInitMode = 0
	// LobbyInitModeAuto skip lobby screen (e.g. ranked).
	LobbyInitModeAuto LobbyInitMode = 1
)

type CreateLobbyMessage struct {
	Command          string
	LobbyInitMode    LobbyInitMode
	LobbyPort        uint16
	LocalPlayerName  string
	LocalPlayerId    uint32
	UnknownParameter int
}

func NewCreateLobbyMessage(lobbyInitMode LobbyInitMode, lobbyPort uint16, playerName string, playerId uint32) GpgMessage {
	return &CreateLobbyMessage{
		Command:          GpgMessageCommandCreateLobby,
		LobbyInitMode:    lobbyInitMode,
		LobbyPort:        lobbyPort,
		LocalPlayerName:  playerName,
		LocalPlayerId:    playerId,
		UnknownParameter: 1,
	}
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
	RemotePlayerId    uint
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
	RemotePlayerId    uint
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
	RemotePlayerId uint
}

func (m *DisconnectFromPeerMessage) GetCommand() string {
	return m.Command
}

func (m *DisconnectFromPeerMessage) GetArgs() []interface{} {
	return []interface{}{m.RemotePlayerId}
}

type GameState = string

const (
	GameStateDisconnected GameState = "Disconnected"
	GameStateIde          GameState = "Idle"
	GameStateLobby        GameState = "Lobby"
	GameStateLaunching    GameState = "Launching"
	GameStateEnded        GameState = "Ended"
)

type GameStateMessage struct {
	Command string
	State   GameState
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

type GameFullMessage struct {
	Command string
}

func (m *GameFullMessage) GetCommand() string {
	return m.Command
}

func (m *GameFullMessage) GetArgs() []interface{} {
	return []interface{}{}
}

func (m *GenericGpgMessage) TryParse() (GpgMessage, error) {
	switch m.Command {
	case GpgMessageCommandCreateLobby:
		if len(m.Args) < 5 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &CreateLobbyMessage{
			Command:          m.Command,
			LobbyInitMode:    m.Args[0].(int),
			LobbyPort:        m.Args[1].(uint16),
			LocalPlayerName:  m.Args[2].(string),
			LocalPlayerId:    m.Args[3].(uint32),
			UnknownParameter: m.Args[4].(int),
		}, nil

	case GpgMessageCommandHostGame:
		if len(m.Args) < 1 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &HostGameMessage{
			Command: m.Command,
			MapName: m.Args[0].(string),
		}, nil

	case GpgMessageCommandJoinGame:
		if len(m.Args) < 3 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &JoinGameMessage{
			Command:           m.Command,
			RemotePlayerLogin: m.Args[1].(string),
			RemotePlayerId:    m.Args[2].(uint),
			Destination:       m.Args[0].(string),
		}, nil

	case GpgMessageCommandConnectToPeer:
		if len(m.Args) < 3 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &ConnectToPeerMessage{
			Command:           m.Command,
			RemotePlayerLogin: m.Args[1].(string),
			RemotePlayerId:    m.Args[2].(uint),
			Destination:       m.Args[0].(string),
		}, nil

	case GpgMessageCommandDisconnectFromPeer:
		if len(m.Args) < 1 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &DisconnectFromPeerMessage{
			Command:        m.Command,
			RemotePlayerId: m.Args[0].(uint),
		}, nil

	case GpgMessageCommandGameState:
		if len(m.Args) < 1 {
			return m, fmt.Errorf("not enough arguments to parse %s", m.Command)
		}

		return &GameStateMessage{
			Command: m.Command,
			State:   m.Args[0].(string),
		}, nil

	case GpgMessageCommandGameEnded:
		return &GameEndedMessage{
			Command: m.Command,
		}, nil

	default:
		return m, nil
	}
}
