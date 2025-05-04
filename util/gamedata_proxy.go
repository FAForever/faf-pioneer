package util

import (
	"context"
	"encoding/binary"
	"faf-pioneer/applog"
	"fmt"
	"go.uber.org/zap"
	"net"
	"unsafe"
)

var loopbackIpv6Addr = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

const (
	receiveBufferSize = 512
)

type GameUDPProxy struct {
	ctx                  context.Context
	ctxCancel            context.CancelFunc
	localAddr            *net.UDPAddr
	proxyAddr            *net.UDPAddr
	conn                 *net.UDPConn
	dataToGameChannel    <-chan []byte
	dataFromGameChannel  chan<- []byte
	gameMessagesSent     uint32
	gameMessagesReceived uint32
	gameMessagesDropped  uint32
	gameBytesSent        uint64
	gameBytesReceived    uint64
	packetLogger         *PacketLogger
}

func NewGameUDPProxy(
	ctx context.Context,
	localPort,
	proxyPort uint,
	dataFromGameChannel chan<- []byte,
	dataToGameChannel <-chan []byte,
) (*GameUDPProxy, error) {
	if localPort == 0 {
		return nil, fmt.Errorf("local port cannot be 0")
	}
	if proxyPort == 0 {
		return nil, fmt.Errorf("proxy port cannot be 0")
	}

	// localPort is where FAF.exe will create lobby and listen for UDP game data
	localAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", localPort))
	if err != nil {
		return nil, err
	}

	proxyAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		return nil, err
	}

	// Listening on proxyPort and then sending everything back to localPort, which is a FAF.exe
	// UDP game port.
	conn, err := net.ListenUDP("udp", proxyAddr)
	if err != nil {
		return nil, err
	}

	contextWrapper, cancel := context.WithCancel(ctx)

	proxy := &GameUDPProxy{
		ctx:                 contextWrapper,
		ctxCancel:           cancel,
		localAddr:           localAddr,
		proxyAddr:           proxyAddr,
		conn:                conn,
		dataToGameChannel:   dataToGameChannel,
		dataFromGameChannel: dataFromGameChannel,
	}

	applog.Debug("Running game UDP proxy",
		zap.Uint("localPort", localPort),
		zap.Uint("proxyPort", proxyPort))

	// For protocol investigation.
	// proxy.packetLogger, err = NewPacketLogger(fmt.Sprintf(
	// 	"packets_%s.log",
	// 	time.Now().Format("2006_01_02-15_04_05"),
	// ))
	// if err != nil {
	// 	applog.Debug("Failed to create packet logger in game UDP proxy", zap.Error(err))
	// }

	go proxy.receiveLoop()
	go proxy.sendLoop()

	return proxy, nil
}

func (p *GameUDPProxy) Close() {
	p.ctxCancel()
	err := p.conn.Close()
	if err != nil {
		applog.Warn("Error closing UDP connection", zap.Error(err))
	}

	close(p.dataFromGameChannel)
}

func (p *GameUDPProxy) receiveLoop() {
	buffer := make([]byte, receiveBufferSize)
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			n, addr, err := p.conn.ReadFromUDP(buffer)
			if err != nil {
				applog.Warn("Error reading data from game", zap.Error(err))
				continue
			}

			// Uncomment for debug: never in production.
			// DumpPacket(buffer[:n], addr, "UDP proxy read data from peer", DumpDirectionFromPeer)
			// if p.packetLogger != nil {
			// 	_ = p.packetLogger.LogPacket("<-", buffer[:n])
			// }

			// We should not have below debug log calls here for prod releases,
			// it may cause additional performance degradation which we wanted to avoid.
			// TODO: Remove `applog.Debug` calls after testing.

			// Since this is a data that we receive from a game,
			// we can only make sure that src-IP is a loopback one.

			if len(addr.IP) == net.IPv4len {
				numericIp := binary.BigEndian.Uint32(addr.IP)
				// Checks that IP starts with a 127, basically 127.0.0.0/8 check,
				// mean if it's not local IP we do allowance check, otherwise just pass that packet.
				if (numericIp >> 24) != 127 {
					applog.Debug(
						"Received UDP proxy packet from non-local address; dropping v4 packet",
						zap.String("receivedFrom", addr.String()),
					)

					p.gameMessagesDropped++
					continue
				}
			} else if len(addr.IP) == net.IPv6len {
				// Checks that it is ::1 loopback address,
				// mean if it's not local IP we do allowance check, otherwise just pass that packet.
				if *(*[16]byte)(unsafe.Pointer(&addr.IP[0])) != loopbackIpv6Addr {
					applog.Debug(
						"Received UDP proxy packet from non-local address; dropping v6 packet",
						zap.String("receivedFrom", addr.String()),
					)

					p.gameMessagesDropped++
					continue
				}
			} else {
				// Just to the sake of safety and checks, let's ignore that weird packet
				// of an unknown protocol.
				continue
			}

			select {
			case p.dataFromGameChannel <- buffer[:n]:
			case <-p.ctx.Done():
				return
			default:
				applog.Debug("Dropping game packet, data from game channel busy or closed",
					zap.Int("bytes", n),
				)
			}

			p.gameMessagesReceived++
			p.gameBytesReceived += uint64(n)
		}
	}
}

func (p *GameUDPProxy) sendLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case data, ok := <-p.dataToGameChannel:
			if !ok {
				return
			}

			// Uncomment for debug: never in production.
			// DumpPacket(data, p.localAddr, "UDP proxy forwarding data to game", DumpDirectionToGame)
			// if p.packetLogger != nil {
			// 	_ = p.packetLogger.LogPacket("->", data)
			// }

			_, err := p.conn.WriteToUDP(data, p.localAddr)
			if err != nil {
				applog.Warn("Error forwarding data to game", zap.Error(err))
				continue
			}

			p.gameMessagesSent++
			p.gameBytesSent += uint64(len(data))
		}
	}
}
