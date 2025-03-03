package icebreaker

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"resty.dev/v3"
)

type Client struct {
	apiRoot      string
	gameId       uint64
	accessToken  string
	sessionToken string
	httpClient   *resty.Client
}

func NewClient(apiRoot string, gameId uint64, accessToken string) Client {
	return Client{
		apiRoot:      apiRoot,
		gameId:       gameId,
		accessToken:  accessToken,
		sessionToken: "",
		httpClient:   resty.New(),
	}
}

type SessionTokenRequest struct {
	GameId uint64 `json:"gameId"`
}

type SessionTokenResponse struct {
	Jwt string `json:"jwt"`
}

type SessionGameResponse struct {
	Id      string `json:"id"`
	Servers []struct {
		Id         string   `json:"id"`
		Username   string   `json:"username,omitempty"`
		Credential string   `json:"credential,omitempty"`
		Urls       []string `json:"urls"`
	} `json:"servers"`
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
	log.Printf("Getting game session id %d\n", c.gameId)
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
	log.Printf("Event body: %s\n", m)

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
				log.Fatal("Invalid event format")
				return
			}

			event, err := ParseEventMessage(restyEvent.Data)
			if err != nil {
				log.Print("Error parsing event:", err)
				return
			}

			switch e := event.(type) {
			case *ConnectedMessage:
				log.Println("Handling a ConnectedMessage:", e)
			case *CandidatesMessage:
				log.Println("Handling a CandidatesMessage:", e)
			default:
				log.Println("Unknown event type:", e)
			}

			channel <- event
		}, nil)

	log.Printf("Listening for server side events on %s\n", url)

	err = eventSource.Get()

	if err != nil {
		return fmt.Errorf("could not attach to message event endpoint: %s", err)
	}

	return nil
}
