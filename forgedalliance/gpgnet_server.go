package forgedalliance

import (
	"bufio"
	"log"
	"net"
	"strconv"
)

type GpgNetServer struct {
	port        uint
	tcpSocket   *net.Listener
	currentConn *net.Conn
	state       string
}

func NewGpgNetServer(port uint) *GpgNetServer {
	return &GpgNetServer{
		port:  port,
		state: "disconnected",
	}
}

func (s *GpgNetServer) Listen(gameToAdapter chan<- *GpgMessage, adapterToGame <-chan *GpgMessage) error {
	tcpSocket, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(int(s.port)))
	if err != nil {
		return err
	}

	s.tcpSocket = &tcpSocket

	for {
		conn, err := tcpSocket.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}

		s.currentConn = &conn

		log.Println("New client connected:", conn.RemoteAddr())

		// Wrap the connection in a buffered reader.
		bufferReader := bufio.NewReader(conn)
		faStreamreader := NewFaStreamReader(bufferReader)

		go func() {
			log.Println("Waiting for incoming GpgNet message")

			for {
				// Read one message from the connection.
				command, err := faStreamreader.ReadString()
				if err != nil {
					log.Printf("error parsing command from %s: %v\n", conn.RemoteAddr(), err)
					continue
				}

				chunks, err := faStreamreader.ReadChunks()
				if err != nil {
					log.Printf("error parsing command from %s: %v", conn.RemoteAddr(), err)
					continue
				}

				unparsedMsg := GenericGpgMessage{
					Command: command,
					Args:    chunks,
				}

				parsedMsg := unparsedMsg.TryParse()

				gameToAdapter <- &parsedMsg
			}
		}()

		go func() {
			bufferedWriter := bufio.NewWriter(conn)
			faStreamWriter := NewFaStreamWriter(bufferedWriter)

			log.Println("Waiting for GpgNet messages to be sent")

			for msg := range adapterToGame {
				faStreamWriter.WriteMessage(*msg)
			}
		}()
	}
}

func (s *GpgNetServer) Close() {
	(*s.tcpSocket).Close()
}
