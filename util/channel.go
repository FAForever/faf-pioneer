package util

import "context"

func RedirectChannelWithContext[T interface{}](ctx context.Context, from <-chan T, to chan T) {
	defer func() {
		if r := recover(); r != nil {
			// Log panic but don't crash the entire application
			// This can happen if 'to' channel is closed while we're sending
		}
	}()

	for {
		select {
		case msg, ok := <-from:
			if !ok {
				return
			}
			select {
			case to <- msg:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
