package util

import (
	"fmt"
	"net"
)

func GetFreeUdpPort() (uint, error) {
	addr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		return 0, fmt.Errorf("dns failed: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return 0, err
	}

	defer func(conn *net.UDPConn) {
		_ = conn.Close()
	}(conn)

	return (uint)(conn.LocalAddr().(*net.UDPAddr).Port), nil
}
