package moho

import (
	"encoding/binary"
	"faf-pioneer/applog"
	"faf-pioneer/util"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
	"unsafe"
)

type CompressionState = byte

const (
	CompressionStateDisabled CompressionState = 0
	CompressionStateEnabled  CompressionState = 1
)

const (
	PacketMaxSize = 512
	HeaderSize    = 15
	MaxPayload    = PacketMaxSize - HeaderSize // 497
)

type State = byte

const (
	StateConnect      State = 0
	StateAnswer       State = 1
	StateResetSerial  State = 2
	StateSerialReset  State = 3
	StateData         State = 4
	StateAck          State = 5
	StateKeepalive    State = 6
	StateGoodbye      State = 7
	StateNatTraversal State = 8
)

func StateToString(s State) string {
	switch s {
	case StateConnect:
		return "CONNECT"
	case StateAnswer:
		return "ANSWER"
	case StateResetSerial:
		return "RESETSERIAL"
	case StateSerialReset:
		return "SERIALRESET"
	case StateData:
		return "DATA"
	case StateAck:
		return "ACK"
	case StateKeepalive:
		return "KEEPALIVE"
	case StateGoodbye:
		return "GOODBYE"
	case StateNatTraversal:
		return "NATTRAVERSAL"
	default:
		return fmt.Sprintf("%02x", s)
	}
}

type PacketMeta struct {
	SentTime    int64 // +0
	ResendCount int32 // +8
	Size        int32 // +12  (общий wire size = header + payload)
}

// PacketHeader in Engine has size of 15 bytes (`pack(push, 1)` in C++)
// Use only with Marshal/Unmarshal methods to have correct in/out size!
type PacketHeader struct {
	State                  State  // +0 (1)
	EarlyMask              uint32 // +1..+4
	SerialNumber           uint16 // +5..+6
	InResponseTo           uint16 // +7..+8
	SequenceNumber         uint16 // +9..+10
	ExpectedSequenceNumber uint16 // +11..+12
	PayloadLength          uint16 // +13..+14
}

func (h *PacketHeader) Unmarshal(b []byte) error {
	if len(b) < HeaderSize {
		return fmt.Errorf("packet header is shorter than %d bytes", HeaderSize)
	}
	h.State = b[0]
	h.EarlyMask = binary.LittleEndian.Uint32(b[1:5])
	h.SerialNumber = binary.LittleEndian.Uint16(b[5:7])
	h.InResponseTo = binary.LittleEndian.Uint16(b[7:9])
	h.SequenceNumber = binary.LittleEndian.Uint16(b[9:11])
	h.ExpectedSequenceNumber = binary.LittleEndian.Uint16(b[11:13])
	h.PayloadLength = binary.LittleEndian.Uint16(b[13:15])
	return nil
}

func (h *PacketHeader) Marshal(b []byte) {
	b[0] = h.State
	binary.LittleEndian.PutUint32(b[1:5], h.EarlyMask)
	binary.LittleEndian.PutUint16(b[5:7], h.SerialNumber)
	binary.LittleEndian.PutUint16(b[7:9], h.InResponseTo)
	binary.LittleEndian.PutUint16(b[9:11], h.SequenceNumber)
	binary.LittleEndian.PutUint16(b[11:13], h.ExpectedSequenceNumber)
	binary.LittleEndian.PutUint16(b[13:15], h.PayloadLength)
}

type Packet struct {
	PacketMeta
	data [PacketMaxSize]byte
}

func (p *Packet) Header() (h PacketHeader) {
	_ = h.Unmarshal(p.data[:HeaderSize])
	return
}
func (p *Packet) SetHeader(h PacketHeader) {
	h.Marshal(p.data[:HeaderSize])
	p.Size = int32(HeaderSize + int(h.PayloadLength))
}
func (p *Packet) Payload() []byte {
	h := p.Header()
	return p.data[HeaderSize : HeaderSize+int(h.PayloadLength)]
}
func (p *Packet) Bytes() []byte {
	return p.data[:p.Size]
}

func (p *Packet) LogPacket(dirType string, receiveOrSentTime int64, includePayload bool) {
	age := receiveOrSentTime - p.SentTime

	header := p.Header()

	fields := []zapcore.Field{
		zap.Int64("age", age),
		zap.Int32("length", p.Size),
		zap.Int32("resendCount", p.ResendCount),
		zap.String("type", StateToString(header.State)),
		zap.String("earlyMask", fmt.Sprintf("%08x", header.EarlyMask)),
		zap.Uint16("serialNumber", header.SerialNumber),
		zap.Uint16("inResponseTo", header.InResponseTo),
		zap.Uint16("sequenceNumber", header.SequenceNumber),
		zap.Uint16("expectedSequenceNumber", header.ExpectedSequenceNumber),
		zap.Uint16("payloadLength", header.PayloadLength),
	}

	if includePayload {
		fields = append(fields, zap.String("payload", util.DataToHex(p.Payload())))
	}

	applog.Debug(dirType, fields...)
}

// SetPayloadSize sets header and data size (Header.PayloadLength & Meta.Size)
func (p *Packet) SetPayloadSize(n int) {
	if n < 0 || n > MaxPayload {
		n = MaxPayload
	}
	h := p.Header()
	h.PayloadLength = uint16(n)
	p.SetHeader(h)
}

func PacketAs[T any](p *Packet) (*T, error) {
	var zero T
	size := int(unsafe.Sizeof(zero))
	if size > MaxPayload {
		return nil, fmt.Errorf("object size %d is larger than allowed packet payload %d", size, MaxPayload)
	}
	return (*T)(unsafe.Pointer(&p.data[HeaderSize])), nil
}

func WritePacketBody[T any](p *Packet, v T) {
	size := int(unsafe.Sizeof(v))
	if size > MaxPayload {
		panic("body too large")
	}
	dst := p.Payload()[:size:size]
	src := unsafe.Slice((*byte)(unsafe.Pointer(&v)), size)
	copy(dst, src)
	p.SetPayloadSize(size)
}

type BodyConnect struct {
	Protocol    uint32   // ENetProtocolType
	Time        int64    // Game time
	Comp        byte     // ENetCompressionMethod (0/1)
	SenderNonce [32]byte // Sender nonce
} // 4 + 8 + 1 + 32 = 45

type BodyAnswer struct {
	Protocol      uint32   // ENetProtocolType
	Time          int64    // Game time
	Comp          byte     // ENetCompressionMethod (0/1)
	SenderNonce   [32]byte // Sender nonce
	ReceiverNonce [32]byte // Receiver nonce
} // 77

func NewPacketWithBody[T any](
	meta PacketMeta,
	header PacketHeader,
	body T,
) *Packet {
	var p Packet
	p.PacketMeta = meta
	p.SetHeader(header)
	WritePacketBody(&p, body)
	return &p
}

func Parse(buf []byte) (*Packet, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("packet is too short (%d < %d)", len(buf), HeaderSize)
	}
	var p Packet
	copy(p.data[:], buf[:min(len(buf), PacketMaxSize)])

	var h PacketHeader
	if err := h.Unmarshal(p.data[:HeaderSize]); err != nil {
		return nil, err
	}
	want := HeaderSize + int(h.PayloadLength)
	if len(buf) != want {
		return nil, fmt.Errorf("payload length mismatch (had = %d, expected = %d)", len(buf), want)
	}
	p.Size = int32(want)
	return &p, nil
}

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
		"----+-----------+----+--------+--------+--------+--------+-------+-----------\n" +
		"Dir | Type      | EM | Serial | InRsTo | SeqNu  | ESeqNu | PaLen | Payload   \n" +
		"----+-----------+----+--------+--------+--------+--------+-------+-----------\n"

	if _, err = file.WriteString(header); err != nil {
		return nil, err
	}

	return &PacketLogger{file: file}, nil
}

func (pl *PacketLogger) LogPacket(direction string, data []byte) error {
	packet, err := Parse(data)
	if err != nil {
		return fmt.Errorf("failed to parse packet: %w", err)
	}

	payloadStr := util.DataToHex(packet.Payload())
	header := packet.Header()

	// Generate by template:
	// dir | Type      | EM | Seq   | AckSq | LSimB | RSimB | PLen  | Payload
	line := fmt.Sprintf("%3s | %-9s | %02X | %-6d | %-6d | %-6d | %-6d | %-5d | %s\n",
		direction,
		StateToString(header.State),
		header.EarlyMask,
		header.SerialNumber,
		header.InResponseTo,
		header.SequenceNumber,
		header.ExpectedSequenceNumber,
		header.PayloadLength,
		payloadStr,
	)

	pl.mu.Lock()
	defer pl.mu.Unlock()
	_, err = pl.file.WriteString(line)
	return err
}
