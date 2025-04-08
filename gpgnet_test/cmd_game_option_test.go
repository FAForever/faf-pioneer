package gpgnet_test

import (
	"bufio"
	"bytes"
	"faf-pioneer/faf"
	"faf-pioneer/gpgnet"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestRoundTrip_GameOptionUnrankedMessage writes game packet GameOptionUnrankedMessage to a StreamWriter,
// then read it from a StreamReader and make sure that after TryParse we will get correct data and types.
func TestRoundTrip_GameOptionUnrankedMessage(t *testing.T) {
	original := &gpgnet.GameOptionUnrankedMessage{
		GameOptionMessage: gpgnet.GameOptionMessage{Kind: gpgnet.GameOptionKindUnranked},
		IsUnranked:        gpgnet.GameOptionValueNo,
	}

	buf := new(bytes.Buffer)

	// Write message to a FAF stream.
	writer := faf.NewFaStreamWriter(bufio.NewWriter(buf))
	err := writer.WriteMessage(original)
	assert.NoError(t, err, "WriteMessage failed")

	// Read message back from a FAF stream.
	reader := faf.NewFaStreamReader(bufio.NewReader(buf))
	cmd, err := reader.ReadString()
	assert.NoError(t, err, "ReadString failed")
	assert.Equal(t, string(gpgnet.MessageCommandGameOption), cmd,
		fmt.Sprintf("Expected command %q, got %s", gpgnet.MessageCommandGameOption, cmd))

	args, err := reader.ReadChunks()
	assert.NoError(t, err, "ReadChunks failed")

	// Create BaseMessage and try to parse our GameOptionMessage into it.
	baseMsg := &gpgnet.BaseMessage{
		Command: cmd,
		Args:    args,
	}
	parsed, err := baseMsg.TryParse()
	assert.NoError(t, err, "TryParse failed")

	unranked, ok := parsed.(*gpgnet.GameOptionUnrankedMessage)
	assert.True(t, ok, fmt.Sprintf("Expected *GameOptionUnrankedMessage, got %T", parsed))

	assert.Equal(t, gpgnet.GameOptionKindUnranked, unranked.Kind,
		fmt.Sprintf("Expected Kind %q, got %q", gpgnet.GameOptionKindUnranked, unranked.Kind))
	assert.Equal(t, gpgnet.GameOptionValueNo, unranked.IsUnranked,
		fmt.Sprintf("Expected IsUnranked %q, got %q", gpgnet.GameOptionValueNo, unranked.IsUnranked))
}

// TestRoundTrip_GameOptionMessage testing write and read of base GameOptionMessage with unknown kind type.
func TestRoundTrip_GameOptionMessage(t *testing.T) {
	orig := &gpgnet.GameOptionMessage{
		Kind: "Test",
	}

	buf := new(bytes.Buffer)
	writer := faf.NewFaStreamWriter(bufio.NewWriter(buf))
	err := writer.WriteMessage(orig)
	assert.NoError(t, err, "WriteMessage failed")

	reader := faf.NewFaStreamReader(bufio.NewReader(buf))
	cmd, err := reader.ReadString()
	assert.NoError(t, err, "ReadString failed")
	assert.Equal(t, string(gpgnet.MessageCommandGameOption), cmd,
		fmt.Sprintf("Expected command %q, got %s", gpgnet.MessageCommandGameOption, cmd))

	args, err := reader.ReadChunks()
	assert.NoError(t, err, "ReadChunks failed")

	baseMsg := &gpgnet.BaseMessage{
		Command: cmd,
		Args:    args,
	}
	parsed, err := baseMsg.TryParse()
	assert.NoError(t, err, "TryParse failed")

	msg, ok := parsed.(*gpgnet.GameOptionMessage)
	assert.True(t, ok, fmt.Sprintf("Expected *GameOptionUnrankedMessage, got %T", parsed))

	assert.Equal(t, "Test", msg.Kind, fmt.Sprintf("Expected Kind 'Test', got %q", msg.Kind))
}

// TestGameOptionMessage_UnknownType_RandomArgs tests Build behaviour for GameOptionMessage with
// unknown Kind and unknown Args (data). We expect that all the arguments will be read "as is"
// and passed to future network processing without any changes, so that unknown game packets or
// messages from mods will work.
func TestGameOptionMessage_UnknownType_RandomArgs(t *testing.T) {
	// First element is a `Kind`.
	// All other - random arguments.
	testArgs := []interface{}{
		"UnknownType",
		"foo",
		123,
		"bar",
		456,
	}

	baseMsg := &gpgnet.GameOptionMessage{}
	parsed, err := baseMsg.Build(testArgs)
	assert.NoError(t, err, "GameOptionMessage build failed")

	msg, ok := parsed.(*gpgnet.GameOptionMessage)
	assert.True(t, ok, fmt.Sprintf("Expected *GameOptionUnrankedMessage, got %T", parsed))

	assert.Equal(t, len(testArgs), len(msg.GetArgs()),
		fmt.Sprintf("Expected %d arguments, got %d", len(testArgs), len(msg.GetArgs())))

	for i, v := range testArgs {
		assert.Equal(t, v, msg.GetArgs()[i],
			fmt.Sprintf("Argument %d: expected %v, got %v", i, v, msg.GetArgs()[i]))
	}
}
