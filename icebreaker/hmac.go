package icebreaker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

func extractHMACFromJWT(accessToken string) (string, error) {
	if accessToken == "" {
		return "", fmt.Errorf("empty string")
	}

	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("wrong length/format")
	}

	// Extract payload, JWT usually consists of `<header>.<payload>.<signature>`.
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("failed to decode payload: %w", err)
	}

	var claims struct {
		Ext struct {
			HMAC string `json:"hmac"`
		} `json:"ext"`
	}

	if err = json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return claims.Ext.HMAC, nil
}
