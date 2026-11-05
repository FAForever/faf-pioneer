package peering

import (
	"fmt"
	"strings"

	"faf-pioneer/icebreaker"

	"github.com/pion/stun/v3"
)

// ParseIceServers converts the icebreaker session servers to pion STUN/TURN URIs and attaches
// the TURN credentials.
func ParseIceServers(servers []icebreaker.SessionGameResponseServer) ([]*stun.URI, error) {
	var uris []*stun.URI
	for _, server := range servers {
		for _, rawUrl := range server.Urls {
			// for Java being Java reasons we unfortunately raped the URLs and need to convert it back.
			uri, err := stun.ParseURI(strings.ReplaceAll(rawUrl, "://", ":"))
			if err != nil {
				return nil, fmt.Errorf("invalid ice server url %q: %w", rawUrl, err)
			}

			if uri.Scheme == stun.SchemeTypeTURN || uri.Scheme == stun.SchemeTypeTURNS {
				uri.Username = server.Username
				uri.Password = server.Credential
			}
			uris = append(uris, uri)
		}
	}
	return uris, nil
}
