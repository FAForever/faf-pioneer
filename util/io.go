package util

import (
	"context"
	"io"
)

type CancelableIoReader struct {
	ctx context.Context
	r   io.Reader
}

func NewCancelableIoReader(ctx context.Context, r io.Reader) *CancelableIoReader {
	return &CancelableIoReader{
		ctx: ctx,
		r:   r,
	}
}

func (cr *CancelableIoReader) Read(p []byte) (int, error) {
	select {
	case <-cr.ctx.Done():
		return 0, cr.ctx.Err()
	default:
		return cr.r.Read(p)
	}
}
