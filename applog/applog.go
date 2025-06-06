package applog

import (
	"encoding/json"
	"faf-pioneer/build"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// remoteSinkMaxLogEntriesBufferSize is a maximum capacity of internal LogEntry queue,
	// for asyncSinks (STDOUT and File sinks).
	// asyncSink won't be able to store more than this amount temporary and will drop/discard any
	// new entries. However, this is not a "fixed" limit, queue will be empties by background `process` calls.
	asyncSinkMaxLogEntriesBufferSize = 1000
	// remoteSinkMaxLogEntriesBufferSize is a maximum capacity of internal LogEntry queue,
	//for remoteSinkInstance (remote sink).
	//asyncSink won't be able to store more than this amount temporary and will drop/discard any
	//new entries. However, this is not a "fixed" limit, queue will be empties by background `process` calls.
	remoteSinkMaxLogEntriesBufferSize = 1000
	// remoteSinkBatchSizeLimit is a limit of how many data should be sent to RemoteSinkDestination interface
	// when certain amount of messages are written to remote sender.
	remoteSinkBatchSizeLimit = 3 * 1024

	// AsyncSinkShutdownTimeout is how many seconds we will be waiting to flush and sync our asynchronous logging
	// sinks before application exit (on context cancel for example).
	// This value also used in `flush` for remoteSink as we can cancel context during the beginning of flush call,
	// and we wanted to have identical flush timeout for it as well.
	AsyncSinkShutdownTimeout = 3 * time.Second
)

type Logger = zap.Logger

type LogSink interface {
	Write(entry *LogEntry) error
	Close() error
}

type RemoteLogSender interface {
	WriteLogEntryToRemote(entries []*LogEntry) error
}

type baseSink interface {
	Shutdown(timeout time.Duration)
}

type LogEntry struct {
	Entry  *zapcore.Entry
	Fields []zap.Field
}

var (
	opts = []zap.Option{
		zap.AddCaller(),
	}
	logFile *os.File

	noRemoteLogger *zap.Logger
	stdoutCore     zapcore.Core
	fileCore       zapcore.Core

	acceptingLogs      int32 = 1
	mu                 sync.Mutex
	globalLogger       *zap.Logger
	asyncSinks         []baseSink
	remoteSinkInstance *remoteSink
)

func Info(msg string, fields ...zapcore.Field) {
	if atomic.LoadInt32(&acceptingLogs) == 0 {
		return
	}
	if globalLogger == nil {
		return
	}
	entry := globalLogger.WithOptions(zap.AddCallerSkip(1)).Check(zapcore.InfoLevel, msg)
	if entry == nil {
		return
	}

	entry.Write(fields...)
	logToRemoteSink(entry, fields)
}

func Warn(msg string, fields ...zapcore.Field) {
	if atomic.LoadInt32(&acceptingLogs) == 0 {
		return
	}
	if globalLogger == nil {
		return
	}
	entry := globalLogger.WithOptions(zap.AddCallerSkip(1)).Check(zapcore.WarnLevel, msg)
	if entry == nil {
		return
	}
	entry.Write(fields...)
	logToRemoteSink(entry, fields)
}

func Debug(msg string, fields ...zapcore.Field) {
	if atomic.LoadInt32(&acceptingLogs) == 0 {
		return
	}
	if globalLogger == nil {
		return
	}
	entry := globalLogger.WithOptions(zap.AddCallerSkip(1)).Check(zapcore.DebugLevel, msg)
	if entry == nil {
		return
	}
	entry.Write(fields...)
	logToRemoteSink(entry, fields)
}

func Error(msg string, fields ...zapcore.Field) {
	if atomic.LoadInt32(&acceptingLogs) == 0 {
		return
	}
	if globalLogger == nil {
		return
	}
	entry := globalLogger.WithOptions(zap.AddCallerSkip(1)).Check(zapcore.ErrorLevel, msg)
	if entry == nil {
		return
	}
	entry.Write(fields...)
	logToRemoteSink(entry, fields)
}

func logToRemoteSink(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
	if entry == nil || remoteSinkInstance == nil {
		return
	}

	err := remoteSinkInstance.Write(&LogEntry{
		Entry: &zapcore.Entry{
			Time:    entry.Time,
			Level:   entry.Level,
			Caller:  entry.Caller,
			Message: entry.Message,
		},
		Fields: fields,
	})
	if err != nil {
		NoRemote().Error("remote sink write failed", zap.Error(err))
	}
}

func NoRemote() *zap.Logger {
	if noRemoteLogger == nil {
		// Here we're creating a same configuration but without remoteSink.
		// This is required to log errors in remoteSink events or inside sender's process loops.
		// Otherwise, for remoteSink for example, calling same methods like `applog.Info` will cause
		// a recursive stack overflow issue, which we wanted to avoid.
		// For that purposes we are creating separate "safe" logger that we can use.
		aggregation := zapcore.NewTee(stdoutCore, fileCore)
		noRemoteLogger = zap.New(aggregation, opts...)
	}
	return noRemoteLogger
}

func LogStartupInfo(launchArgs interface{}) {
	buildInfo := build.GetBuildInfo()
	buildCommit := "unknown"
	if buildInfo != nil {
		buildCommit = buildInfo.CommitHash
	}

	Info("Application started",
		zap.String("buildCommit", buildCommit),
		zap.Any("launchArgs", launchArgs),
	)
}

func Initialize(userId uint, gameId uint64, rawLogLevel int, logPath string) error {
	encoderConfig := getEncoderConfig()
	logLevel := safeGetLogLevelOrDefault(rawLogLevel)

	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)
	stdoutCore = zapcore.NewCore(jsonEncoder, zapcore.AddSync(os.Stdout), logLevel)

	stdoutAsync := newAsyncSink(stdoutCore, asyncSinkMaxLogEntriesBufferSize)
	fileAsync, fileErr := initializeFileLogger(userId, gameId, jsonEncoder, logLevel, logPath)

	asyncSinks = make([]baseSink, 0, 2)
	asyncSinks = append(asyncSinks, stdoutAsync)
	cores := make([]zapcore.Core, 0, 2)
	cores = append(cores, stdoutCore)

	if fileErr == nil {
		asyncSinks = append(asyncSinks, fileAsync)
		cores = append(cores, fileAsync)
	}

	aggCore := zapcore.NewTee(cores...)
	globalLogger = zap.New(aggCore, opts...).With(
		zap.Uint("localUserId", userId),
		zap.Uint64("localGameId", gameId),
	)

	setLogger(globalLogger)
	return fileErr
}

func initializeFileLogger(
	userId uint,
	gameId uint64,
	jsonEncoder zapcore.Encoder,
	logLevel zapcore.Level,
	logPath string,
) (*asyncSink, error) {
	logDir, err := resolveLogPath(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve log path '%s': %w", logPath, err)
	}

	logFilename := filepath.Join(
		logDir,
		fmt.Sprintf("game_%d_user_%d.log",
			gameId,
			userId,
		),
	)

	err = os.MkdirAll(filepath.Dir(logFilename), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err = os.OpenFile(logFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	fileCore = zapcore.NewCore(jsonEncoder, zapcore.AddSync(logFile), logLevel)
	fileAsync := newAsyncSink(fileCore, asyncSinkMaxLogEntriesBufferSize)

	return fileAsync, nil
}

func resolveLogPath(logPath string) (string, error) {
	if logPath != "" {
		info, err := os.Stat(logPath)
		if err != nil || !info.IsDir() {
			log.Printf(
				"Selected log path '%s' does not exist or is not a directory, falling back to working directory\n",
				logPath,
			)
			logPath = ""
		}
	}

	if logPath == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("failed to get current working directory: %w", err)
		}
		logPath = filepath.Join(wd, "logs")
	}

	return logPath, nil
}

func SetRemoteLogSender(sender RemoteLogSender) {
	if remoteSinkInstance != nil {
		var foundIndex = -1
		for i, sink := range asyncSinks {
			if sink == remoteSinkInstance {
				foundIndex = i
				break
			}
		}

		if foundIndex != -1 {
			slices.Delete(asyncSinks, foundIndex, foundIndex+1)
		}
	}

	remoteSinkInstance = newRemoteSink(sender, remoteSinkMaxLogEntriesBufferSize, getEncoderConfig())
	asyncSinks = append(asyncSinks, remoteSinkInstance)
}

func Shutdown() {
	// Shutdown all the sinks that we have.
	mu.Lock()
	for _, sink := range asyncSinks {
		sink.Shutdown(AsyncSinkShutdownTimeout)
	}
	mu.Unlock()

	// Let our logger write more logs for 0.5 second if we have something pending after
	// main context was canceled.
	syncDone := make(chan error, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&acceptingLogs, 0)
		syncDone <- globalLogger.Sync()
	}()

	// Give sync and accepting log setter 3 seconds to finish everything and then shutdown.
	select {
	case <-syncDone:
	case <-time.After(3 * time.Second):
	}

	if logFile != nil {
		_ = logFile.Close()
	}
}

func safeGetLogLevelOrDefault(level int) zapcore.Level {
	converted := zapcore.Level(level)
	if converted < zapcore.DebugLevel || converted >= zapcore.InvalidLevel {
		return zapcore.InfoLevel
	}

	return converted
}

func getEncoderConfig() zapcore.EncoderConfig {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		// Use UTC timezone for logs.
		enc.AppendString(t.UTC().Format(time.RFC3339))
	}

	return encoderConfig
}

func setLogger(instance *Logger) {
	if instance == nil {
		return
	}
	globalLogger = instance
	zap.ReplaceGlobals(globalLogger)
}

func ExtractFieldValue(field zap.Field) (string, error) {
	enc := zapcore.NewMapObjectEncoder()
	field.AddTo(enc)
	val, ok := enc.Fields[field.Key]
	if !ok {
		return "", fmt.Errorf("field %q is not supported by zap.Encoder", field.Key)
	}

	switch value := val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value), nil
	case string:
		return value, nil
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
