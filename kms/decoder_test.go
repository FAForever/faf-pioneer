package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"testing"
)

// Define the structure based on your description
type CreateLobbyMessage struct {
	Command          string
	NumArguments     uint32
	Arguments        []interface{} // To hold arguments, which could be strings or ints
	LobbyInitMode    uint32
	LobbyPort        uint32
	LocalPlayerName  string
	LocalPlayerId    uint32
	UnknownParameter uint32
}

func decodeCreateLobby(data []byte) (*CreateLobbyMessage, error) {
	var message CreateLobbyMessage

	// Read the first 4 bytes to get the length of the command
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short to read command length")
	}
	commandLength := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	// Ensure we have enough data to read the full command
	if len(data) < int(commandLength) {
		return nil, fmt.Errorf("data too short to read full command")
	}

	// Read Command (length as specified by the first 4 bytes)
	message.Command = string(data[:commandLength])
	data = data[commandLength:]

	// Read the number of arguments (next 4 bytes)
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short to read number of arguments")
	}
	message.NumArguments = binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	// Now, decode each argument based on the magic byte
	for i := uint32(0); i < message.NumArguments; i++ {
		// Read the magic byte (type specifier)
		if len(data) < 1 {
			return nil, fmt.Errorf("data too short to read magic byte")
		}
		magicByte := data[0]
		data = data[1:]

		switch magicByte {
		case 0: // Int (uint32)
			if len(data) < 4 {
				return nil, fmt.Errorf("data too short to read uint32 value")
			}
			value := binary.LittleEndian.Uint32(data[:4])
			data = data[4:]

			if message.LobbyPort == 0 {
				message.LobbyPort = value
			} else if message.LocalPlayerId == 0 {
				message.LocalPlayerId = value
			} else if message.UnknownParameter == 0 {
				message.UnknownParameter = value
			}

		case 1, 2: // String (prefix length + actual string)
			if len(data) < 4 {
				return nil, fmt.Errorf("data too short to read string length")
			}

			// Read the length of the string (4 bytes)
			strLength := binary.LittleEndian.Uint32(data[:4])
			data = data[4:]

			// Ensure we have enough data for the string itself
			if len(data) < int(strLength) {
				return nil, fmt.Errorf("data too short to read string data")
			}

			// Read the string
			str := string(data[:strLength])
			data = data[strLength:]

			// Assign the string to the LocalPlayerName
			if message.LocalPlayerName == "" {
				message.LocalPlayerName = str
			} else {
				// If it’s not the first string, append it to Arguments
				message.Arguments = append(message.Arguments, str)
			}

		default:
			return nil, fmt.Errorf("unknown magic byte: %v", magicByte)
		}
	}

	// Return the decoded message
	return &message, nil
}

func Test_Decoder(t *testing.T) {
	// Use default hex string directly in the test
	hexData := "0b0000004372656174654c6f6262790500000000000000000008ea000001070000007034626c6f636b00aad102000001000000"

	// Convert hex string to binary data
	binaryData, err := hex.DecodeString(hexData)
	if err != nil {
		t.Fatalf("Invalid hexadecimal data provided: %v", err)
	}

	// Decode the data for CreateLobby
	decodedMessage, err := decodeCreateLobby(binaryData)
	if err != nil {
		t.Fatalf("Error decoding CreateLobby data: %v", err)
	}

	// Print the decoded message
	log.Printf("Decoded CreateLobby Message: %+v\n", decodedMessage)
}
