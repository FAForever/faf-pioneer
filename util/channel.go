package util

import "context"

func RedirectChannelWithContext[T interface{}](ctx context.Context, from <-chan T, to chan T) {
	for {
		select {
		case msg, ok := <-from:
			if !ok {
				return
			}
			to <- msg
		case <-ctx.Done():
			return
		}
	}
}
