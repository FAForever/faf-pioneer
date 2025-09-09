package icebreaker

import (
	"faf-pioneer/applog"
	"time"
)

type SessionTokenRequest struct {
	GameId uint64 `json:"gameId"`
}

type SessionTokenResponse struct {
	Jwt string `json:"jwt"`
}

type SessionGameResponse struct {
	Id         string                      `json:"id"`
	ForceRelay bool                        `json:"forceRelay"`
	Servers    []SessionGameResponseServer `json:"servers"`
}

type SessionGameResponseServer struct {
	Id         string   `json:"id"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
	Urls       []string `json:"urls"`
}

type LogsMessageRequest struct {
	Timestamp time.Time              `json:"timestamp"`
	Message   string                 `json:"message"`
	MetaData  map[string]interface{} `json:"metaData"`
}

// NewLogMessagesFromAppLogEntries is to convert our application log entries wrapper (applog.LogEntry)
// that contains log messages and fields (zap.Entry).
func NewLogMessagesFromAppLogEntries(entries []*applog.LogEntry) []LogsMessageRequest {
	ret := make([]LogsMessageRequest, 0, len(entries))

	for _, entry := range entries {
		requestEntry := LogsMessageRequest{
			Timestamp: entry.Entry.Time,
			Message:   entry.Entry.Message,
			MetaData: map[string]interface{}{
				"level":  entry.Entry.Level.String(),
				"caller": entry.Entry.Caller.String(),
			},
		}

		for _, field := range entry.Fields {
			data, err := applog.ExtractFieldValue(field)
			if err != nil {
				continue
			}

			requestEntry.MetaData[field.Key] = data
		}

		ret = append(ret, requestEntry)
	}

	return ret
}
