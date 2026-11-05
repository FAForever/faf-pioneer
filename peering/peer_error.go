package peering

import (
	"context"
)

type PeerError struct {
	Context context.Context
	Err     error
}

func (e *PeerError) Error() string {
	return e.Err.Error()
}
