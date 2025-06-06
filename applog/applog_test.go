package applog

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitializeCreatesLogFileAndSetsGlobals(t *testing.T) {
	// Create temporary directory and switch working one onto that.
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	assert.NoError(t, err, "could not get current working directory")

	// Make sure to set working directory to previous value at the end of test.
	defer func(dir string) {
		_ = os.Chdir(dir)
	}(origWd)

	err = os.Chdir(tmpDir)
	assert.NoError(t, err, "could not change current working directory to temporary one")

	userId := uint(42)
	gameId := uint64(1000)
	rawLogLevel := int(zapcore.InfoLevel)

	err = Initialize(userId, gameId, rawLogLevel, "")
	assert.NoError(t, err, fmt.Sprintf("could not initialize logger: %v", err))

	if logFile != nil {
		t.Cleanup(func() {
			_ = logFile.Close()
		})
	}

	// Make sure log file created.
	assert.NotNil(t, logFile, "logFile are not initialized (got nil value) after Initialize call")

	expectedLogPath := filepath.Join(tmpDir, "logs", fmt.Sprintf("game_%d_user_%d.log", gameId, userId))
	_, err = os.Stat(expectedLogPath)
	assert.NoError(t, err, fmt.Sprintf("expected log file to exist by path '%s'", expectedLogPath))

	assert.NotNil(t, globalLogger,
		"globalLogger are not initialized (got nil value) after Initialize call")

	// Make sure that asyncSinks have stdout and file sinks.
	assert.Equal(t, 2, len(asyncSinks),
		fmt.Sprintf("expected 2 async sinks but got %d", len(asyncSinks)))
}

func TestInitializeDoesNotFailOnValidDirectory(t *testing.T) {
	// Create temporary directory and switch working one onto that.
	tmpDir := t.TempDir()
	origWd, err := os.Getwd()
	assert.NoError(t, err, "could not get current working directory")

	// Make sure to set working directory to previous value at the end of test.
	defer func(dir string) {
		_ = os.Chdir(dir)
	}(origWd)

	err = os.Chdir(tmpDir)
	assert.NoError(t, err, "could not change current working directory to temporary one")

	userId := uint(42)
	gameId := uint64(1000)
	rawLogLevel := int(zapcore.DebugLevel)

	err = Initialize(userId, gameId, rawLogLevel, "")
	assert.NoError(t, err, fmt.Sprintf("could not initialize logger: %v", err))

	if logFile != nil {
		t.Cleanup(func() {
			_ = logFile.Close()
		})
	}

	assert.NotNil(t, globalLogger,
		"globalLogger are not initialized (got nil value) after Initialize call")
}

func TestLogLevelArg(t *testing.T) {
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.DebugLevel)), zap.DebugLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(-2), zap.InfoLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.InfoLevel)), zap.InfoLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.WarnLevel)), zap.WarnLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.ErrorLevel)), zap.ErrorLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.DPanicLevel)), zap.DPanicLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.PanicLevel)), zap.PanicLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zap.FatalLevel)), zap.FatalLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zapcore.InvalidLevel)), zap.InfoLevel)
	assert.Equal(t, safeGetLogLevelOrDefault(int(zapcore.InvalidLevel)+1), zap.InfoLevel)
}

type testRemoteSenderGlobal struct {
	mu    sync.Mutex
	count int32
}

func (trs *testRemoteSenderGlobal) WriteLogEntryToRemote(entries []*LogEntry) error {
	trs.mu.Lock()
	defer trs.mu.Unlock()
	trs.count += int32(len(entries))
	return nil
}

func (trs *testRemoteSenderGlobal) GetCount() int32 {
	trs.mu.Lock()
	defer trs.mu.Unlock()
	return trs.count
}

func TestLogToRemoteSinkDoesNothingWhenNotAccepting(t *testing.T) {
	// Create test remote sender that will count all entries came to `WriteLogEntryToRemote`.
	sender := &testRemoteSenderGlobal{}

	// Not set the remote sink instance to above one.
	remoteSinkInstance = newRemoteSink(sender, remoteSinkMaxLogEntriesBufferSize, getEncoderConfig())

	// Stop accepting new logs.
	atomic.StoreInt32(&acceptingLogs, 0)

	// Make sure global logger are initialized and not-nil.
	globalLogger = zap.New(zapcore.NewNopCore())
	setLogger(globalLogger)

	// Test every logging method with message and fields,
	// we shouldn't see any messages in `testRemoteSenderGlobal` as `logToRemoteSink` call should
	// exit when `acceptingLogs` set to 0.
	Info("should not be logged remotely", zap.String("key", "value"))
	Error("should not be logged remotely", zap.String("key", "value"))
	Warn("should not be logged remotely", zap.String("key", "value"))
	Debug("should not be logged remotely", zap.String("key", "value"))

	// Give time for `process` to execute and finish.
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, int32(0), sender.GetCount(),
		fmt.Sprintf(
			"expected that remoteSink won't accept any entry with acceptingLogs==0, "+
				"but it got %d instead", sender.GetCount()),
	)
}

func TestLoggingAfterShutdownNonBlocking(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	logger := zap.New(core, zap.AddCaller())
	setLogger(logger)

	// Stop accepting logs before the shutdown.
	atomic.StoreInt32(&acceptingLogs, 1)

	Shutdown()

	// Measure execution time of our log calls.
	start := time.Now()
	Info("post shutdown info", zap.String("k", "v"))
	Error("post shutdown error", zap.String("k", "v"))
	Warn("post shutdown warn", zap.String("k", "v"))
	Debug("post shutdown debug", zap.String("k", "v"))
	elapsed := time.Since(start)

	// Make sure we finish fast enough (less than 50ms).
	assert.LessOrEqual(t, elapsed, 50*time.Millisecond,
		fmt.Sprintf("logging methods took too long (%v) after Shutdown call", elapsed))

	// Make sure that after calling `Shutdown` there are no new log entries detected
	// by our observer.
	// Give some time for unexpected logging.
	time.Sleep(100 * time.Millisecond)
	logs := observed.All()
	assert.Equal(t, 0, len(logs), "expected no new entries after Shutdown call")
}

func TestShutdownSetsAcceptingLogs(t *testing.T) {
	core, _ := observer.New(zapcore.DebugLevel)
	logger := zap.New(core, zap.AddCaller())
	setLogger(logger)

	// Stop accepting logs before the shutdown.
	atomic.StoreInt32(&acceptingLogs, 1)

	Shutdown()

	assert.Equal(t, int32(0), atomic.LoadInt32(&acceptingLogs), "acceptingLogs should be 0 after Shutdown")
}

func TestShutdownDoesNotBlock(t *testing.T) {
	core, _ := observer.New(zapcore.DebugLevel)
	logger := zap.New(core, zap.AddCaller())
	setLogger(logger)

	// Stop accepting logs before the shutdown.
	atomic.StoreInt32(&acceptingLogs, 1)

	for i := 0; i < 1000; i++ {
		Info("pre shutdown info", zap.String("k", "v"), zap.Int("i", i))
	}

	done := make(chan struct{})
	go func() {
		Shutdown()
		close(done)
	}()

	go func() {
		for i := 0; i < 100; i++ {
			Info("post shutdown info", zap.String("k", "v"), zap.Int("i", i))
			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not complete in time, possible deadlock")
	}
}

func TestNoNewLogsAfterShutdown(t *testing.T) {
	fakeSender := &testRemoteSenderGlobal{}
	remoteSinkInstance = newRemoteSink(fakeSender, remoteSinkMaxLogEntriesBufferSize, getEncoderConfig())

	core, _ := observer.New(zapcore.DebugLevel)
	globalLogger = zap.New(core, zap.AddCaller())
	setLogger(globalLogger)

	// Stop accepting logs before the shutdown.
	atomic.StoreInt32(&acceptingLogs, 1)

	for i := 0; i < 10; i++ {
		Info("pre shutdown log", zap.Int("i", i))
	}

	done := make(chan struct{})
	go func() {
		Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not complete in time, possible deadlock")
	}

	// Shutdown should set `acceptingLogs` to 0 after 500 ms, wait a bit more of that time.
	time.Sleep(600 * time.Millisecond)

	// Try to send new logs.
	for i := 0; i < 10; i++ {
		Info("post shutdown log", zap.Int("i", i))
	}

	// Wait a little to ensure async calls for remoteSink could finish.
	time.Sleep(100 * time.Millisecond)

	// Проверяем, что fakeSender не получил никаких записей после shutdown.
	assert.Equal(t, int32(0), fakeSender.GetCount(),
		fmt.Sprintf(
			"expected no new logs after Shutdown call (500ms), "+
				"but fakeSender received %d entries instead", fakeSender.GetCount()),
	)
}
