package applog

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync"
	"time"
)

type remoteSink struct {
	sender    RemoteLogSender
	entryChan chan *LogEntry
	quit      chan struct{}
	wg        sync.WaitGroup
	batch     []*LogEntry
	batchSize int
	encoder   zapcore.Encoder
}

func newRemoteSink(sink RemoteLogSender, bufferSize int, encoderConfig zapcore.EncoderConfig) *remoteSink {
	s := &remoteSink{
		sender:    sink,
		entryChan: make(chan *LogEntry, bufferSize),
		quit:      make(chan struct{}),
		encoder:   zapcore.NewJSONEncoder(encoderConfig),
	}

	s.wg.Add(1)
	go s.process()
	return s
}

func (rs *remoteSink) Write(entry *LogEntry) error {
	select {
	case rs.entryChan <- entry:
		return nil
	default:
		return fmt.Errorf("remote log buffer overflow (capacity: %d)", cap(rs.entryChan))
	}
}

func (rs *remoteSink) process() {
	defer rs.wg.Done()
	for {
		select {
		case entry := <-rs.entryChan:
			entrySize := rs.getEntrySize(entry)
			if entrySize <= 0 {
				// Non-serializable entries we should drop instead, otherwise how we suppose
				// to send them to remote server.
				NoRemote().Error(
					"Failed to get log entry size for remoteSink, unserializable entry; dropping",
					zap.Any("entry", entry),
				)
				continue
			}

			rs.batch = append(rs.batch, entry)
			rs.batchSize += entrySize

			// If new message will overflow current limit, first we need to send everything
			// we already buffered.
			if rs.batchSize >= remoteSinkBatchSizeLimit {
				rs.flush()
			}
		case <-rs.quit:
			rs.flush()
			return
		}
	}
}

func (rs *remoteSink) getEntrySize(e *LogEntry) int {
	buf, err := rs.encoder.EncodeEntry(*e.Entry, e.Fields)
	if err != nil {
		return 0
	}

	return buf.Len()
}

func (rs *remoteSink) flush() {
	if len(rs.batch) <= 0 {
		return
	}

	err := rs.sender.WriteLogEntryToRemote(rs.batch)
	if err != nil {
		NoRemote().Debug("Failed to flush remoteSink", zap.Error(err))
		return
	}

	rs.batch = nil
	rs.batchSize = 0
}

func (rs *remoteSink) Shutdown(timeout time.Duration) {
	close(rs.quit)

	done := make(chan struct{})
	go func() {
		rs.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}
}
