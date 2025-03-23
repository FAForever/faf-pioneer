package applog

import (
	"bytes"
	"context"
	"sync"
	"time"
)

const (
	maxLines      = 100 // Flush logs after 1000 lines
	flushInterval = 30 * time.Second
	maxRetries    = 3 // Retry up to 3 times on failure
	retryDelay    = 2 * time.Second
)

type RemoteSend func([]byte) error

type remoteWriteSyncer struct {
	ctx        context.Context
	mu         sync.Mutex
	buffer     bytes.Buffer
	lineCount  int
	flushTimer *time.Ticker
	cancel     context.CancelFunc
	remoteSend RemoteSend
}

func newRemoteWriteSyncer(ctx context.Context, handleFunc func([]byte) error) *remoteWriteSyncer {
	newCtx, cancel := context.WithCancel(ctx)
	syncer := &remoteWriteSyncer{
		ctx:        newCtx,
		flushTimer: time.NewTicker(flushInterval),
		cancel:     cancel,
		remoteSend: handleFunc,
	}

	// Background goroutine for periodic flushing
	go syncer.periodicFlush()

	return syncer
}

func (b *remoteWriteSyncer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buffer.Write(p)
	b.buffer.WriteByte('\n')
	b.lineCount++

	if b.lineCount >= maxLines {
		go b.flush()
	}

	return len(p), nil
}

func (b *remoteWriteSyncer) Sync() error {
	// We sync ourselves, thank you very much
	return nil
}

func (b *remoteWriteSyncer) periodicFlush() {
	for {
		select {
		case <-b.flushTimer.C:
			b.flush()
		case <-b.ctx.Done():
			b.flush()
			b.flushTimer.Stop()
			return
		}
	}
}

func (b *remoteWriteSyncer) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lineCount == 0 {
		return
	}

	// Attempt to send logs
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := b.remoteSend(b.buffer.Bytes()); err == nil {
			b.buffer.Reset()
			b.lineCount = 0
			return
		}
		time.Sleep(retryDelay)
	}
}
