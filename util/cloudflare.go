package util

import (
	"fmt"
	"resty.dev/v3"
)

type CloudFlareCredentials struct {
	Username   string `json:"username"`
	Credential string `json:"credential"`
}

func GenerateCloudFlareTurnCredentials() (*CloudFlareCredentials, error) {
	client := resty.New()

	var result CloudFlareCredentials
	resp, err := client.R().SetResult(result).Get("https://speed.cloudflare.com/turn-creds")
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("CF API error: %s", resp.String())
	}

	return &result, nil
}
