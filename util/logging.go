package util

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"log/slog"
	"os"
	"time"
)

type DefaultFieldsHook struct {
	Fields logrus.Fields
}

func (hook *DefaultFieldsHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook *DefaultFieldsHook) Fire(entry *logrus.Entry) error {
	for key, value := range hook.Fields {
		if _, exists := entry.Data[key]; !exists {
			entry.Data[key] = value
		}
	}

	return nil
}

func SetupLogging(userId uint, gameId uint64) {
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})

	logFilename := fmt.Sprintf("Game_%d_User_%d_%s.log",
		userId,
		gameId,
		time.Now().Format("2006-01-02_15-04-05"),
	)

	logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file '%s': %v", logFilename, err)
	}

	defer func(logFile *os.File) {
		_ = logFile.Close()
	}(logFile)

	mw := io.MultiWriter(os.Stdout, logFile)
	logrus.SetOutput(mw)

	launcherField := logrus.Fields{
		"userId": userId,
		"gameId": gameId,
	}

	logrus.AddHook(&DefaultFieldsHook{Fields: launcherField})
}

func ErrorAttr(err error) slog.Attr {
	return slog.Any("error", err)
}
