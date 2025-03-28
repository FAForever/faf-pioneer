package gpgnet

import (
	"encoding/binary"
	"fmt"
)

type PacketType = uint32

const (
	PacketTypeConnect      PacketType = 0x00000000 // CONNECT
	PacketTypeAnswer       PacketType = 0x00000001 // ANSWER
	PacketTypeResetSerial  PacketType = 0x00000002 // RESETSERIAL
	PacketTypeSerialReset  PacketType = 0x00000003 // SERIALRESET
	PacketTypeData         PacketType = 0x00000004 // DATA
	PacketTypeAck          PacketType = 0x00000005 // ACK
	PacketTypeKeepalive    PacketType = 0x00000006 // KEEPALIVE
	PacketTypeGoodbye      PacketType = 0x00000007 // GOODBYE
	PacketTypeNatTraversal PacketType = 0x00000008 // NATTRAVERSAL
)

// PacketHeader is a game network packet description with type, payload length, sequence and more.
// Had size of 15 bytes.
type PacketHeader struct {
	Type          PacketType // (0x00, offset 0 ) 4 bytes: type of the packet.
	UnknownFlag   byte       // (0x04, offset 4 ) 1 byte: unknown flag.
	Sequence      uint16     // (0x05, offset 5 ) 2 byte: sequence number of a packet.
	AckSequence   uint16     // (0x07, offset 7 ) 2 byte: sequence number that we're acked (answer to a seq. number)
	SimBeat       uint16     // (0x09, offset 9 ) 2 bytes: local simulation beat.
	RemoteSimBeat uint16     // (0x0B, offset 11) 2 bytes: last accepted/confirmed remote peer simulation beat.
	PayloadLength uint16     // (0x0D, offset 13) 2 bytes: payload length.
}

// GamePacket is main game network packet structure with a payload data.
type GamePacket struct {
	Header  PacketHeader // (0x00, offset: 0 ) 15 bytes: packet header information.
	Payload []byte       // (0x0F, offset: 15): `Header.PayloadLength` bytes, our payload.
}

// GamePacketContainer – container used by game within `recvfrom` (sub_128BBF0).
// Size of structure - 28 bytes.
// Layout is the following:
//   - 0x00–0x03: Unknown1 (container[0])
//   - 0x04–0x07: Unknown2 (container[1])
//   - 0x08–0x0F: ReceivedTime (container[2] and container[3])
//   - 0x0C–0x13: Unknown3 (container[4])
//   - 0x14–0x15: ReceivedBytes (container[5])
//   - 0x16-....: Packet (container[6])
type GamePacketContainer struct {
	Unknown1      [4]byte    // (0x00, offset 0 ): unknown (should be expected seq and connection state somewhere here).
	Unknown2      [4]byte    // (0x04, offset 4 ): unknown.
	ReceivedTime  int64      // (0x08, offset 8 ): low and high 32 bits of receive packet time.
	Unknown3      [8]byte    // (0x0C, offset 12): unknown.
	ReceivedBytes int32      // (0x14, offset 20): raw amount of bytes received by `recvfrom`.
	Packet        GamePacket // (0x16, offset 24): packet itself.
}

/*
GamePacketContainer
---------------------------------------------------------------
  *DWORD    *CHAR
---------------------------------------------------------------
[0  -  ] [0  -  3]   ├─ [0  -  3] 0x00–0x03: Unknown1	     [4]byte
[1  -  ] [4  -  7]   ├─ [4  -  7] 0x04–0x07: Unknown2	     [4]byte
[2  -  ] [8  – 15]   ├─ [8  - 11] 0x08–0x0F: ReceivedTime   int64
[3  - 4] [12 - 19]   ├─ [12 - 19] 0x0C–0x13: Unknown3       [8]byte
[5  -  ] [20 – 23]   ├─ [20 - 23] 0x14–0x15: ReceivedBytes  int32
[6  -  ] [24 –   ]   └─ 0x16-    : Packet (GamePacket)
[6  -  ] [24 –   ]      ├─ 0x00–0x0E: Header (PacketHeader, 15 bytes)
[6  -  ] [24 – 27]      │       ├─ [0  -  3] 0x00–0x03: Type          PacketType
[6  -  ] [28 – 28]      │       ├─ [4  -  4] 0x04     : UnknownFlag   byte
[6  -  ] [29 – 30]      │       ├─ [5  -  6] 0x05–0x06: Sequence      uint16
[6  -  ] [31 – 32]      │       ├─ [7  -  8] 0x07–0x08: AckSequence   uint16
[6  -  ] [33 – 34]      │       ├─ [9  - 10] 0x09–0x0A: SimBeat       uint16
[6  -  ] [35 – 36]      │       ├─ [11 - 12] 0x0B–0x0C: RemoteSimBeat uint16
[6  -  ] [37 – 38]      │       └─ [13 - 14] 0x0D–0x0E: PayloadLength uint16
[6  -  ] [39 –   ]      └─ 0x0F: Payload []byte

ANSWER packet (77 bytes, if take +15 of header = 92 which is game checking):
[6  -  ] [39 –  42] 0x00–0x04 (offset 0 ), 4 bytes: connection protocol?
[   -  ] [43 –  50] - it's checking for this value <= CNetUDPConnection + 147) as int64/uint64.
[   -  ] [43 –  46] 0x04–0x08 (offset 4 ), 4 bytes:
[   -  ] [47 –  50] 0x08–0x0B (offset 8 ), 4 bytes:
[   -  ] [51 –  51] 0x0C–0x0C (offset 12), 1 byte: compression method (0/1)
[   -  ] [52 –  83] 0x0D–0x2C (offset 13), 32 bytes: sender nonce?
[   -  ] [84 – 116] 0x2D–0x74 (offset 45), 32 bytes: receiver nonce?

Other packets are using Moho::CMessageStream (gpg::BinaryReader) after deflate (flate decompression).
*/

// ConnectPacketPayload packet of type PacketTypeConnect.
// Used to establish initial connection with peer.
// Could be sent up to 4-5 packets (other peer answer only to first or to any of those, that he received).
type ConnectPacketPayload struct {
	Protocol    uint32           // (0x00, offset: 0) 4 bytes: number of connection protocol (currently 2 is supported).
	UserToken   [4]byte          // (0x04, offset: 4) 4 bytes: unique sender token, different in ANSWER packet.
	SharedToken [4]byte          // (0x08, offset: 8) 4 bytes: token that are shared in ANSWER packet back to sender.
	Compression CompressionState // (0x0C, offset: 12) 1 byte: compression status.
	SenderNonce [32]byte         // (0x0D, offset: 13) 32 bytes: sender nonce?
}

func newConnectPacketPayload(data []byte) ConnectPacketPayload {
	return ConnectPacketPayload{
		Protocol:    binary.LittleEndian.Uint32(data[0:4]),
		UserToken:   [4]byte(data[4:8]),
		SharedToken: [4]byte(data[8:12]),
		Compression: data[12],
		SenderNonce: [32]byte(data[13:44]),
	}
}

type AnswerPacketPayload struct {
	ConnectPacketPayload          // Same fields
	ReceiverNonce        [32]byte // (0x2D, offset 45), 32 bytes: receiver nonce?
}

func newAnswerPacketPayload(data []byte) AnswerPacketPayload {
	return AnswerPacketPayload{
		ConnectPacketPayload: newConnectPacketPayload(data),
		ReceiverNonce:        [32]byte(data[45:77]),
	}
}

type CompressionState = byte

const (
	CompressionStateDisabled CompressionState = 0
	CompressionStateEnabled  CompressionState = 1
)

type ConnectionState int32

const (
	StatePending ConnectionState = iota
	StateConnecting
	StateAnswering
	StateEstablished
	StateTimedOut
	StateErrored
)

func ParseGamePacket(data []byte) (*GamePacket, error) {
	if len(data) < 15 {
		return nil, fmt.Errorf("packet too short: %d bytes", len(data))
	}
	hdr := PacketHeader{
		Type:          binary.LittleEndian.Uint32(data[0:4]),
		UnknownFlag:   data[4],
		Sequence:      binary.LittleEndian.Uint16(data[5:7]),
		AckSequence:   binary.LittleEndian.Uint16(data[7:9]),
		SimBeat:       binary.LittleEndian.Uint16(data[9:11]),
		RemoteSimBeat: binary.LittleEndian.Uint16(data[11:13]),
		PayloadLength: binary.LittleEndian.Uint16(data[13:15]),
	}

	if hdr.Type > PacketTypeNatTraversal {
		return nil, fmt.Errorf("invalid packet type: %d", hdr.Type)
	}

	if len(data) != int(15+hdr.PayloadLength) {
		return nil, fmt.Errorf(
			"payload length mismatch: got %d, header says %d",
			len(data)-15,
			hdr.PayloadLength)
	}

	return &GamePacket{
		Header:  hdr,
		Payload: data[15:],
	}, nil
}
