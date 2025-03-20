package icebreaker

import (
	"encoding/json"
	"fmt"
	"github.com/pion/webrtc/v4"
	"strconv"
)

type EventMessage interface {
	GetSenderId() uint
	GetRecipientId() *uint
}

type BaseEvent struct {
	EventType   string `json:"eventType"`
	GameID      uint64 `json:"gameId"`
	SenderID    uint   `json:"senderId"`
	RecipientID *uint  `json:"recipientId,omitempty"`
}

type ConnectedMessage struct {
	BaseEvent
}

func (e ConnectedMessage) String() string {
	recipient := "nil"
	if e.RecipientID != nil {
		recipient = strconv.Itoa(int(*e.RecipientID))
	}

	return fmt.Sprintf(
		"ConnectedMessage { GameId=%d, SenderId=%d, RecipientId=%s }",
		e.GameID,
		e.SenderID,
		recipient,
	)
}
func (e ConnectedMessage) GetSenderId() uint     { return e.SenderID }
func (e ConnectedMessage) GetRecipientId() *uint { return e.RecipientID }

type CandidatesMessage struct {
	BaseEvent
	Session    *webrtc.SessionDescription `json:"session"`
	Candidates []webrtc.ICECandidate      `json:"candidates"`
}

func (e CandidatesMessage) String() string {
	recipient := "nil"
	if e.RecipientID != nil {
		recipient = strconv.Itoa(int(*e.RecipientID))
	}

	return fmt.Sprintf(
		"CandidatesMessage { GameId=%d, SenderId=%d, RecipientId=%s }",
		e.GameID,
		e.SenderID,
		recipient,
	)
}
func (e CandidatesMessage) GetSenderId() uint     { return e.SenderID }
func (e CandidatesMessage) GetRecipientId() *uint { return e.RecipientID }

func ParseEventMessage(message string) (EventMessage, error) {
	// First, decode into a generic map to extract eventType
	var data = []byte(message)
	var raw BaseEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	// Based on eventType, unmarshal into the correct struct
	switch raw.EventType {
	case "connected":
		var msg ConnectedMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "candidates":
		var msg CandidatesMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	default:
		return nil, fmt.Errorf("unknown eventType: %s", raw.EventType)
	}
}
