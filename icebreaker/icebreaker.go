package icebreaker

import (
	"context"
	"encoding/json"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"net"
	"net/http"
	"resty.dev/v3"
	"sync"
	"time"
)

const defaultAddressRegistrationTimeout = 5 * time.Second

type addressRegistrationClient struct {
	addressFamily string
	httpClient    *resty.Client
}

type Client struct {
	apiRoot                    string
	gameId                     uint64
	accessToken                string
	sessionToken               string
	httpClient                 *resty.Client
	addressRegistrationClients []addressRegistrationClient
	addressRegistrationTimeout time.Duration
	ctx                        context.Context
}

func NewClient(ctx context.Context, apiRoot string, gameId uint64, accessToken string) *Client {
	c := &Client{
		apiRoot:      apiRoot,
		gameId:       gameId,
		accessToken:  accessToken,
		sessionToken: "",
		httpClient:   newHTTPClient(accessToken, ""),
		addressRegistrationClients: []addressRegistrationClient{
			{addressFamily: "IPv4", httpClient: newHTTPClient(accessToken, "tcp4")},
			{addressFamily: "IPv6", httpClient: newHTTPClient(accessToken, "tcp6")},
		},
		addressRegistrationTimeout: defaultAddressRegistrationTimeout,
		ctx:                        ctx,
	}

	return c
}

func newHTTPClient(accessToken string, network string) *resty.Client {
	client := resty.New()
	if network != "" {
		transport, err := client.HTTPTransport()
		if err != nil {
			panic(fmt.Sprintf("creating address-family HTTP client: %v", err))
		}
		dialContext := transport.DialContext
		transport.DialContext = func(ctx context.Context, _ string, address string) (net.Conn, error) {
			return dialContext(ctx, network, address)
		}
	}

	client.AddRequestMiddleware(func(_ *resty.Client, r *resty.Request) error {
		h, err := extractHMACFromJWT(accessToken)
		if err != nil {
			applog.Debug("Failed to extract HMAC from JWT", zap.Error(err))
		}
		if h != "" {
			r.SetHeader("X-HMAC", h)
		}
		return nil
	})

	return client
}

// WriteLogEntryToRemote implements applog.RemoteLogSender interface that could be used in logging
// as a remote log server. This integrates with applog.remoteSink and allows logger to buffer all
// the log entries we produce and send to ICE-Breaker (which will forward it to log storage backend).
func (c *Client) WriteLogEntryToRemote(entries []*applog.LogEntry) error {
	if c.sessionToken == "" {
		return nil
	}

	url := fmt.Sprintf("%s/session/game/%d/logs", c.apiRoot, c.gameId)

	// Here we should use `OnlyLocal()` otherwise it will cause stack overflow:
	// calling debug which is calling remoteWrite, which is again calling debug and remoteWrite.
	//
	// `applog.NoRemote().Debug("Sending remote log entry!")`

	logEntries := NewLogMessagesFromAppLogEntries(entries)

	apiCallJob := util.DelayedCancelContextWithJob(c.ctx, applog.AsyncSinkShutdownTimeout)
	defer apiCallJob.Done()

	resp, err := c.httpClient.R().
		SetContext(apiCallJob.GetContext()).
		SetAuthToken(c.sessionToken).
		SetContentType("application/json").
		SetBody(logEntries).
		Post(url)

	applog.NoRemote().Debug("Log entries are sent to remote server",
		zap.Int("entriesCount", len(logEntries)),
		zap.Any("entries", logEntries),
	)

	if err != nil {
		return fmt.Errorf("fetching session token failed: %v", err)
	}

	if resp.StatusCode() != 204 {
		return fmt.Errorf("failed to upload logs: %v", resp.Status())
	}

	return nil
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
		SetContext(c.ctx).
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
	applog.Info("Getting game session id from ICE-Breaker API")
	err := c.withSessionToken()

	if err != nil {
		return nil, err
	}

	// Construct the URL with the gameId
	url := fmt.Sprintf("%s/session/game/%d", c.apiRoot, c.gameId)

	var result SessionGameResponse

	// Create a new HTTP request
	resp, err := c.httpClient.R().
		SetContext(c.ctx).
		SetAuthToken(c.sessionToken).
		SetContentType("application/json").
		SetResult(&result).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("fetching game session failed: %v", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("fetching game session failed: %v", resp.Status())
	}

	c.registerAddresses()

	return &result, nil
}

func (c *Client) registerAddresses() {
	url := fmt.Sprintf("%s/session/game/%d/addresses", c.apiRoot, c.gameId)

	var waitGroup sync.WaitGroup
	for _, registrationClient := range c.addressRegistrationClients {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			ctx, cancel := context.WithTimeout(c.ctx, c.addressRegistrationTimeout)
			defer cancel()

			resp, err := registrationClient.httpClient.R().
				SetContext(ctx).
				SetAuthToken(c.sessionToken).
				Post(url)
			if err != nil {
				applog.Debug(
					"Could not register client address with ICE-Breaker API",
					zap.String("addressFamily", registrationClient.addressFamily),
					zap.Error(err),
				)
				return
			}
			if resp.StatusCode() != http.StatusNoContent {
				applog.Debug(
					"ICE-Breaker API rejected client address registration",
					zap.String("addressFamily", registrationClient.addressFamily),
					zap.String("status", resp.Status()),
				)
			}
		}()
	}
	waitGroup.Wait()
}

func (c *Client) SendEvent(msg EventMessage) error {
	err := c.withSessionToken()

	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/session/game/%d/events", c.apiRoot, c.gameId)

	m, _ := json.Marshal(msg)
	applog.Debug("Sending event to ICE-Breaker API", zap.String("body", string(m)))

	// Make the POST request with JSON payload and Authorization header
	resp, err := c.httpClient.R().
		SetContext(c.ctx).
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

func (c *Client) Listen(channel chan EventMessage) (bool, error) {
	err := c.withSessionToken()

	if err != nil {
		return false, err
	}

	url := fmt.Sprintf("%s/session/game/%d/events", c.apiRoot, c.gameId)
	connected := false

	eventSource := resty.NewSSESource().
		SetURL(url).
		SetContext(c.ctx).
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", c.sessionToken)).
		OnOpen(func(_ string, _ http.Header) {
			connected = true
		}).
		OnMessage(func(message any) {
			restyEvent, ok := message.(*resty.SSE)
			if !ok {
				applog.Error(
					"Invalid event format received from ICE-Breaker event",
					zap.Any("message", message),
				)
				return
			}

			event, parseErr := ParseEventMessage(restyEvent.Data)
			if parseErr != nil {
				applog.Error(
					"Failed parsing event received from ICE-Breaker event",
					zap.Any("message", message),
					zap.Error(parseErr),
				)
				return
			}

			switch e := event.(type) {
			case *ConnectedMessage:
				applog.Debug("Handing ICE-Breaker API event",
					zap.Any("event", e),
					zap.String("eventType", e.EventType),
				)
			case *CandidatesMessage:
				applog.Debug("Handing ICE-Breaker API event",
					zap.Any("event", e),
					zap.String("eventType", e.EventType),
				)
			case *PeerClosingMessage:
				applog.Debug("Handing ICE-Breaker API event",
					zap.Any("event", e),
					zap.String("eventType", e.EventType),
				)
			default:
				applog.Debug("Handing unknown ICE-Breaker API event",
					zap.Any("event", e),
				)
			}

			channel <- event
		}, nil)

	hmac, err := extractHMACFromJWT(c.accessToken)
	if err != nil {
		applog.Debug("Failed to extract HMAC from JWT", zap.Error(err))
	}
	if hmac != "" {
		eventSource.AddHeader("X-HMAC", hmac)
	}

	applog.Info("Listening for ICE-Breaker API (server-side) events", zap.String("url", url))

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-c.ctx.Done():
			eventSource.Close()
		case <-done:
		}
	}()

	err = eventSource.Get()

	if err != nil {
		return connected, fmt.Errorf("could not attach to message event endpoint: %w", err)
	}

	return connected, nil
}
