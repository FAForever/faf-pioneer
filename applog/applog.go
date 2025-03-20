package applog

import (
	"context"
	"faf-pioneer/build"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"path/filepath"
	"time"
)

type Logger = zap.Logger

func Info(msg string, fields ...zapcore.Field) {
	def.WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(msg string, fields ...zapcore.Field) {
	def.WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Debug(msg string, fields ...zapcore.Field) {
	def.WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Error(msg string, fields ...zapcore.Field) {
	def.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Fatal(msg string, fields ...zapcore.Field) {
	def.WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

func GetLogger() *Logger {
	return def
}

func FromContext(ctx context.Context) *Logger {
	return def.With(getFields(ctx)...)
}

func Initialize(userId uint, gameId uint64) {
	workdir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get current working directory: %v", err)
	}

	logFilename := filepath.Join(
		workdir,
		"logs",
		fmt.Sprintf("game_%d_user_%d_%s.log",
			userId,
			gameId,
			time.Now().Format("2006-01-02_15-04-05"),
		),
	)

	err = os.MkdirAll(filepath.Dir(logFilename), os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	logFile, err = os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file '%s': %v", logFilename, err)
	}

	buildInfo := build.GetBuildInfo()
	buildCommit := "unknown"
	if buildInfo != nil {
		buildCommit = buildInfo.CommitHash
	}

	l := newLogger(opts...).With(
		zap.String("buildCommit", buildCommit),
		zap.Uint("localUserId", userId),
		zap.Uint64("localGameId", gameId))

	setLogger(l)
}

func Shutdown() {
	if logFile != nil {
		_ = logFile.Close()
	}
}

type logFieldKey struct{}

func getFields(ctx context.Context) []zap.Field {
	fields, ok := ctx.Value(logFieldKey{}).([]zap.Field)
	if !ok {
		return nil
	}
	return fields
}

func mergeFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	current := getFields(ctx)
	result := make([]zap.Field, 0, len(current)+len(fields))
	seen := make(map[string]struct{}, len(current)+len(fields))
	for _, v := range fields {
		seen[v.Key] = struct{}{}
		result = append(result, v)
	}
	for _, v := range current {
		if _, ok := seen[v.Key]; ok {
			continue
		}
		seen[v.Key] = struct{}{}
		result = append(result, v)
	}
	return result
}

func AddFields(ctx context.Context, fields ...zap.Field) context.Context {
	fm := mergeFields(ctx, fields...)
	return context.WithValue(ctx, logFieldKey{}, fm)
}

var (
	opts = []zap.Option{
		zap.AddCaller(),
	}
	def     = newLogger(opts...)
	logFile *os.File
)

func newLogger(opts ...zap.Option) *Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC1123)

	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	consoleSyncer := zapcore.AddSync(os.Stdout)
	fileSyncer := zapcore.AddSync(logFile)
	logLevel := zapcore.DebugLevel

	consoleCore := zapcore.NewCore(jsonEncoder, consoleSyncer, logLevel)
	fileCore := zapcore.NewCore(jsonEncoder, fileSyncer, logLevel)

	combinedCore := zapcore.NewTee(consoleCore, fileCore)
	logger := zap.New(combinedCore, opts...)

	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	return logger
}

func setLogger(l *Logger) {
	def = l
	zap.ReplaceGlobals(def)
}
