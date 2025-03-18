package util

import (
	"context"
	"net"
)

func NetAcceptWithContext(ctx context.Context, listener net.Listener) (net.Conn, error) {
	type result struct {
		conn net.Conn
		err  error
	}
	resetCh := make(chan result, 1)

	go func() {
		conn, err := listener.Accept()
		resetCh <- result{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resetCh:
		return res.conn, res.err
	}
}
