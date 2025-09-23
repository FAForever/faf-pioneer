package util

import (
	"context"
	"faf-pioneer/applog"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

func PtrValueOrDef[T any](value *T, def T) T {
	if value == nil {
		return def
	}
	return *value
}

func WrapAppContextCancelExitMessage(ctx context.Context, appName string) {
	ctxErr := ctx.Err()
	if ctxErr != nil {
		applog.Info(fmt.Sprintf("%s exited; context cancelled", appName), zap.Error(ctxErr))
		return
	}

	applog.Info(fmt.Sprintf("%s exited", appName))
}

func DataToHex(buffer []byte) string {
	var parts []string
	for _, b := range buffer {
		parts = append(parts, fmt.Sprintf("%02X", b))
	}
	return strings.Join(parts, " ")
}

func HexStrToData(hexStr string) []byte {
	parts := strings.Split(hexStr, " ")
	data := make([]byte, len(parts))
	for i, part := range parts {
		b, err := strconv.ParseUint(part, 16, 8)
		if err != nil {
			return nil
		}
		data[i] = byte(b)
	}
	return data
}
