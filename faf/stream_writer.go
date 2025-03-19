package faf

import (
	"bufio"
	"encoding/binary"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"fmt"
	"go.uber.org/zap"
	"sync"
)

// StreamWriter writes messages in a format compatible with the FA stream protocol.
type StreamWriter struct {
	w  *bufio.Writer
	mu sync.Mutex
}

// NewFaStreamWriter creates a new writer for the given output stream.
func NewFaStreamWriter(w *bufio.Writer) *StreamWriter {
	applog.Debug("A new faf.StreamWriter opened")
	return &StreamWriter{
		w: w,
	}
}

// writeString writes a string with its length prefix.
func (w *StreamWriter) writeString(s string) error {
	// Write the length of the string
	if err := binary.Write(w.w, binary.LittleEndian, int32(len(s))); err != nil {
		return err
	}
	// Write the string bytes
	_, err := w.w.Write([]byte(s))
	return err
}

// writeArgs writes a list of arguments with their types.
func (w *StreamWriter) writeArgs(args []interface{}) error {
	// Write number of arguments
	if err := binary.Write(w.w, binary.LittleEndian, int32(len(args))); err != nil {
		return err
	}

	// Iterate over each argument and write it
	for index, arg := range args {
		switch v := arg.(type) {
		case int:
			w.w.WriteByte(FaStreamConstants.FieldTypes.INT)
			binary.Write(w.w, binary.LittleEndian, int32(v))
		case uint:
			w.w.WriteByte(FaStreamConstants.FieldTypes.INT)
			binary.Write(w.w, binary.LittleEndian, int32(v))
		case uint16:
			w.w.WriteByte(FaStreamConstants.FieldTypes.INT)
			binary.Write(w.w, binary.LittleEndian, int32(v))
		case uint32:
			w.w.WriteByte(FaStreamConstants.FieldTypes.INT)
			binary.Write(w.w, binary.LittleEndian, v)
		case int32:
			w.w.WriteByte(FaStreamConstants.FieldTypes.INT)
			binary.Write(w.w, binary.LittleEndian, v)
		case string:
			w.w.WriteByte(FaStreamConstants.FieldTypes.STRING)
			if err := w.writeString(v); err != nil {
				return err
			}
		default:
			return fmt.Errorf("Unexpected type %T in arguments (index %d)\n", v, index)
		}
	}
	return nil
}

// WriteMessage writes a GpgMessage to the output stream.
func (w *StreamWriter) WriteMessage(message gpgnet.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	applog.Debug("Writing message to FA stream",
		zap.String("command", message.GetCommand()),
		zap.Any("message", message),
	)

	if err := w.writeString(message.GetCommand()); err != nil {
		return err
	}
	if err := w.writeArgs(message.GetArgs()); err != nil {
		return err
	}

	return w.w.Flush()
}

// Close closes the StreamWriter.
func (w *StreamWriter) Close() error {
	applog.Debug("Closing faf.StreamWriter")
	return w.w.Flush()
}

// FaStreamConstants defines constants for field types.
var FaStreamConstants = struct {
	FieldTypes struct {
		INT             byte
		STRING          byte
		FOLLOWUP_STRING byte
	}
}{
	FieldTypes: struct {
		INT             byte
		STRING          byte
		FOLLOWUP_STRING byte
	}{
		INT:             0x00,
		STRING:          0x01,
		FOLLOWUP_STRING: 0x02,
	},
}
