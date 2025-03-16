package icebreaker

import (
	"encoding/json"
	"faf-pioneer/util"
	"fmt"
	"log/slog"
	"resty.dev/v3"
	"strconv"
)

type Client struct {
	apiRoot      string
	gameId       uint64
	accessToken  string
	sessionToken string
	httpClient   *resty.Client
}

func NewClient(apiRoot string, gameId uint64, accessToken string) *Client {
	return &Client{
		apiRoot:      apiRoot,
		gameId:       gameId,
		accessToken:  accessToken,
		sessionToken: "",
		httpClient:   resty.New(),
	}
}

func (c *Client) withSessionToken() error {
	if c.sessionToken != "" {
		return nil
	}

	url := c.apiRoot + "/session/token"

	requestData := SessionTokenRequest{
		GameId: c.gameId,
	}

	var result SessionTokenResponse

	// Make the POST request with JSON payload and Authorization header
	resp, err := c.httpClient.R().
		SetHeader("Authorization", "Bearer "+c.accessToken).
		SetHeader("Content-Type", "application/json").
		SetBody(requestData).
		SetResult(&result).
		Post(url)

	if err != nil {
		return fmt.Errorf("fetching session token failed: %v", err)
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("fetching session token failed: %v", resp.Status())
	}

	c.sessionToken = result.Jwt

	return nil
}

func (c *Client) GetGameSession() (*SessionGameResponse, error) {
	slog.Info("Getting game session id")
	err := c.withSessionToken()

	if err != nil {
		return nil, err
	}

	// Construct the URL with the gameId
	url := c.apiRoot + "/session/game/" + strconv.FormatUint(c.gameId, 10)

	var result SessionGameResponse

	// Create a new HTTP request
	resp, err := c.httpClient.R().
		SetHeader("Authorization", "Bearer "+c.sessionToken).
		SetResult(&result).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("fetching game session failed: %v", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("fetching game session failed: %v", resp.Status())
	}

	return &result, nil
}

func (c *Client) SendEvent(msg EventMessage) error {
	err := c.withSessionToken()

	if err != nil {
		return err
	}

	url := c.apiRoot + "/session/game/" + strconv.FormatUint(c.gameId, 10) + "/events"

	m, _ := json.Marshal(msg)
	slog.Debug("Event body", slog.String("body", string(m)))

	// Make the POST request with JSON payload and Authorization header
	resp, err := c.httpClient.R().
		SetHeader("Authorization", "Bearer "+c.sessionToken).
		SetHeader("Content-Type", "application/json").
		SetBody(msg).
		Post(url)

	if err != nil {
		return fmt.Errorf("posting session event failed: %v", err)
	}

	if resp.StatusCode() != 204 {
		return fmt.Errorf("posting session event failed: %v", resp.Status())
	}

	return nil
}

func (c *Client) Listen(channel chan EventMessage) error {
	err := c.withSessionToken()

	if err != nil {
		return err
	}

	url := c.apiRoot + "/session/game/" + strconv.FormatUint(c.gameId, 10) + "/events"

	eventSource := resty.NewEventSource().
		SetURL(url).
		SetHeader("Authorization", "Bearer "+c.sessionToken).
		OnMessage(func(message any) {
			restyEvent, ok := message.(*resty.Event)
			if !ok {
				slog.Error("Invalid event format")
				return
			}

			event, err := ParseEventMessage(restyEvent.Data)
			if err != nil {
				slog.Error("Error parsing event", util.ErrorAttr(err))
				return
			}

			switch e := event.(type) {
			case *ConnectedMessage:
				slog.Debug("Handling a ConnectedMessage", slog.Any("message", e))
			case *CandidatesMessage:
				slog.Debug("Handling a CandidatesMessage", slog.Any("message", e))
			default:
				slog.Warn("Unknown event type", slog.Any("message", e))
			}

			channel <- event
		}, nil)

	slog.Info("Listening for server side events", slog.String("url", url))

	err = eventSource.Get()

	if err != nil {
		return fmt.Errorf("could not attach to message event endpoint: %s", err)
	}

	return nil
}
