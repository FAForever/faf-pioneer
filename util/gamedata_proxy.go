package util

import (
	"faf-pioneer/applog"
	"fmt"
	"go.uber.org/zap"
	"net"
)

type GameUDPProxy struct {
	localAddr            *net.UDPAddr
	proxyAddr            *net.UDPAddr
	conn                 *net.UDPConn
	dataToGameChannel    <-chan []byte
	dataFromGameChannel  chan<- []byte
	closed               bool
	gameMessagesSent     uint32
	gameMessagesReceived uint32
}

func NewGameUDPProxy(localPort, proxyPort uint, dataFromGameChannel chan<- []byte, dataToGameChannel <-chan []byte) (*GameUDPProxy, error) {
	localAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", localPort))
	if err != nil {
		return nil, err
	}

	proxyAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", proxyAddr)
	if err != nil {
		return nil, err
	}

	proxy := &GameUDPProxy{
		localAddr:           localAddr,
		proxyAddr:           proxyAddr,
		conn:                conn,
		dataToGameChannel:   dataToGameChannel,
		dataFromGameChannel: dataFromGameChannel,
		closed:              false,
	}

	go proxy.receiveLoop()
	go proxy.sendLoop()

	return proxy, nil
}

func (p *GameUDPProxy) Close() {
	p.closed = true
	err := p.conn.Close()
	if err != nil {
		applog.Warn("Error closing UDP connection", zap.Error(err))
	}

	close(p.dataFromGameChannel)
}

func (p *GameUDPProxy) receiveLoop() {
	buffer := make([]byte, 1500)
	for !p.closed {
		n, _, err := p.conn.ReadFromUDP(buffer)
		if err != nil {
			applog.Warn("Error reading data from game", zap.Error(err))
			continue
		}
		p.dataFromGameChannel <- buffer[:n]
		p.gameMessagesReceived++
	}
}

func (p *GameUDPProxy) sendLoop() {
	for data := range p.dataToGameChannel {
		if p.closed {
			return
		}

		// Send data back to local UDP socket
		_, err := p.conn.WriteToUDP(data, p.localAddr)
		if err != nil {
			applog.Warn("Error forwarding data to game", zap.Error(err))
		}
		p.gameMessagesSent++
	}
}
