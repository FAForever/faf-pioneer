package faf

import (
	"encoding/binary"
	"faf-pioneer/applog"
	"fmt"
	"io"
)

// StreamReader wraps an io.Reader for our binary protocol.
type StreamReader struct {
	r io.Reader
}

// NewFaStreamReader wraps the given reader.
// You can pass a bufio.Reader here if desired.
func NewFaStreamReader(r io.Reader) *StreamReader {
	applog.Debug("A new faf.StreamReader opened")
	return &StreamReader{r: r}
}

// ReadChunks reads the chunks from the input and returns them as a slice of interface{}.
func (f *StreamReader) ReadChunks() ([]interface{}, error) {
	// Read the number of chunks (int32, little endian)
	var numberOfChunks int32
	if err := binary.Read(f.r, binary.LittleEndian, &numberOfChunks); err != nil {
		return nil, fmt.Errorf("error reading number of chunks: %w", err)
	}

	if numberOfChunks > MaxChunkSize {
		return nil, fmt.Errorf("too many chunks: %d", numberOfChunks)
	}

	chunks := make([]interface{}, 0, numberOfChunks)

	for i := 0; i < int(numberOfChunks); i++ {
		// Read the field type (1 byte)
		var fieldType FieldType
		if err := binary.Read(f.r, binary.LittleEndian, &fieldType); err != nil {
			return nil, fmt.Errorf("error reading field type: %w", err)
		}

		switch fieldType {
		case IntType:
			// Read an int32 in little endian
			var val int32
			if err := binary.Read(f.r, binary.LittleEndian, &val); err != nil {
				return nil, fmt.Errorf("error reading int value: %w", err)
			}
			chunks = append(chunks, val)
		default:
			// Read a string (prefixed by its length as int32)
			s, err := f.ReadString()
			if err != nil {
				return nil, fmt.Errorf("error reading string: %w", err)
			}
			// Replace "/t" with "\t" and "/n" with "\n"
			s = replaceSpecial(s)
			chunks = append(chunks, s)
		}
	}

	return chunks, nil
}

// ReadString reads a length-prefixed string from the input.
func (f *StreamReader) ReadString() (string, error) {
	// First, read the length (int32)
	var size int32
	if err := binary.Read(f.r, binary.LittleEndian, &size); err != nil {
		return "", fmt.Errorf("error reading string length: %w", err)
	}

	// Read 'size' bytes
	buf := make([]byte, size)
	if _, err := io.ReadFull(f.r, buf); err != nil {
		return "", fmt.Errorf("error reading string bytes: %w", err)
	}

	return string(buf), nil
}
