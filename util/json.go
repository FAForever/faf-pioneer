package util

import (
	"encoding/json"
	"strings"
)

func NormalizeJSONKeysToLower(b []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	lower := lowercaseKeys(v)
	return json.Marshal(lower)
}

func lowercaseKeys(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[strings.ToLower(k)] = lowercaseKeys(val)
		}
		return out
	case []any:
		for i := range x {
			x[i] = lowercaseKeys(x[i])
		}
		return x
	default:
		return v
	}
}
