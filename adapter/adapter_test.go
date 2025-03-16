package adapter

import (
	"os"
	"strconv"
	"testing"
)

// This test connects 2 adapters
func TestAdapter2Adapter(t *testing.T) {
	userId, _ := strconv.ParseUint(os.Getenv("USER_ID"), 10, 64)
	accessToken := os.Getenv("ACCESS_TOKEN")
	apiRoot := os.Getenv("API_ROOT")

	Start(
		uint(userId),
		100,
		accessToken,
		apiRoot,
		50000,
		50001,
		60000)
}
