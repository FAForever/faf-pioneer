package util

import (
	"context"
	"sync"
	"time"
)

type ContextJob struct {
	jobDone chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
}

func (cj *ContextJob) Done() {
	cj.once.Do(func() {
		close(cj.jobDone)
	})
}

func (cj *ContextJob) GetContext() context.Context {
	return cj.ctx
}

func DelayedCancelContextWithJob(
	parent context.Context,
	maxDelay time.Duration,
) *ContextJob {
	jobDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Wait for parent context to be canceled.
		<-parent.Done()
		// Now either wait for job finish using a channel signaling,
		// or just timeout after max delay duration.
		select {
		case <-jobDone:
			cancel()
		case <-time.After(maxDelay):
			cancel()
		}
	}()

	return &ContextJob{
		ctx:     ctx,
		cancel:  cancel,
		jobDone: jobDone,
	}
}
