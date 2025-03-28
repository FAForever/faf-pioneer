package util

import (
	"faf-pioneer/gpgnet"
	"fmt"
	"os"
	"sync"
)

type PacketLogger struct {
	file *os.File
	mu   sync.Mutex
}

func NewPacketLogger(filename string) (*PacketLogger, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	header := "" +
		"----+-----------+----+-------+-------+-------+-------+-------+-----------\n" +
		"Dir | Type      | ?? | Seq   | AckSq | LSimB | RSimB | PaLen | Payload   \n" +
		"----+-----------+----+-------+-------+-------+-------+-------+-----------\n"

	if _, err = file.WriteString(header); err != nil {
		return nil, err
	}

	return &PacketLogger{file: file}, nil
}

func (pl *PacketLogger) LogPacket(direction string, data []byte) error {
	packet, err := gpgnet.ParseGamePacket(data)
	if err != nil {
		return fmt.Errorf("failed to parse packet: %w", err)
	}

	payloadStr := DataToHex(packet.Payload)

	// Generate by template:
	// dir | Type      | ?? | Seq   | AckSq | LSimB | RSimB | PLen  | Payload
	line := fmt.Sprintf("%3s | %-9s | %02X | %-5d | %-5d | %-5d | %-5d | %-5d | %s\n",
		direction,
		packetTypeToString(packet.Header.Type),
		packet.Header.UnknownFlag,
		packet.Header.Sequence,
		packet.Header.AckSequence,
		packet.Header.SimBeat,
		packet.Header.RemoteSimBeat,
		packet.Header.PayloadLength,
		payloadStr,
	)

	pl.mu.Lock()
	defer pl.mu.Unlock()
	_, err = pl.file.WriteString(line)
	return err
}

func packetTypeToString(packetType gpgnet.PacketType) string {
	switch packetType {
	case gpgnet.PacketTypeConnect:
		return "CONNECT"
	case gpgnet.PacketTypeAnswer:
		return "ANSWER"
	case gpgnet.PacketTypeResetSerial:
		return "RST-SER"
	case gpgnet.PacketTypeSerialReset:
		return "SER-RST"
	case gpgnet.PacketTypeData:
		return "DATA"
	case gpgnet.PacketTypeAck:
		return "ACK"
	case gpgnet.PacketTypeKeepalive:
		return "KEEPALIVE"
	case gpgnet.PacketTypeGoodbye:
		return "GOODBYE"
	case gpgnet.PacketTypeNatTraversal:
		return "NAT-TRVL"
	default:
		return fmt.Sprintf("%02X %02X %02X %02X",
			uint8(packetType),
			uint8(packetType>>8),
			uint8(packetType>>16),
			uint8(packetType>>24))
	}
}
