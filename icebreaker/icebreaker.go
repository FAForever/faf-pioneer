package icebreaker

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"resty.dev/v3"
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

	url := fmt.Sprintf("%s/session/token", c.apiRoot)

	requestData := SessionTokenRequest{
		GameId: c.gameId,
	}

	var result SessionTokenResponse

	// Make the POST request with JSON payload and Authorization header
	resp, err := c.httpClient.R().
		SetAuthToken(c.accessToken).
		SetContentType("application/json").
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
	logrus.Info("Getting game session id")
	err := c.withSessionToken()

	if err != nil {
		return nil, err
	}

	// Construct the URL with the gameId
	url := fmt.Sprintf("%s/session/game/%d", c.apiRoot, c.gameId)

	var result SessionGameResponse

	// Create a new HTTP request
	resp, err := c.httpClient.R().
		SetAuthToken(c.accessToken).
		SetContentType("application/json").
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

	url := fmt.Sprintf("%s/session/game/%d/events", c.apiRoot, c.gameId)

	m, _ := json.Marshal(msg)
	logrus.WithField("body", string(m)).Debug("Event body")

	// Make the POST request with JSON payload and Authorization header
	resp, err := c.httpClient.R().
		SetAuthToken(c.sessionToken).
		SetContentType("application/json").
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

	url := fmt.Sprintf("%s/session/game/%d/events", c.apiRoot, c.gameId)

	eventSource := resty.NewEventSource().
		SetURL(url).
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", c.sessionToken)).
		OnMessage(func(message any) {
			restyEvent, ok := message.(*resty.Event)
			if !ok {
				logrus.Error("Invalid event format")
				return
			}

			event, err := ParseEventMessage(restyEvent.Data)
			if err != nil {
				logrus.WithError(err).Error("Error parsing event")
				return
			}

			switch e := event.(type) {
			case *ConnectedMessage:
				logrus.WithField("message", e).Debugf("Handling a %s", e.EventType)
			case *CandidatesMessage:
				logrus.WithField("message", e).Debugf("Handling a %s", e.EventType)
			default:
				logrus.WithField("message", e).Debug("Handling unknown event type")
			}

			channel <- event
		}, nil)

	logrus.WithField("url", url).Info("Listening for server side events")

	err = eventSource.Get()

	if err != nil {
		return fmt.Errorf("could not attach to message event endpoint: %s", err)
	}

	return nil
}
