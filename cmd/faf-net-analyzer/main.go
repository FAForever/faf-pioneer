package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/klauspost/compress/flate"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type PacketInfo struct {
	Seq    int
	Length int
}

func extractPayloadAndSeq(line string) (payloadHex string, seq string, err error) {
	parts := strings.Split(line, "|")
	if len(parts) < 5 {
		return "", "", fmt.Errorf("not enought fields in packet dump row")
	}
	payloadHex = strings.TrimSpace(parts[len(parts)-1])
	seq = strings.TrimSpace(parts[3])
	return payloadHex, seq, nil
}

func dataToHex(buffer []byte) string {
	var parts []string
	for _, b := range buffer {
		parts = append(parts, fmt.Sprintf("%02X", b))
	}
	return strings.Join(parts, " ")
}

type OutputBlock struct {
	Data       []byte
	ReceivedAt time.Time
}

func readerGoroutine(r io.Reader, outCh chan<- OutputBlock) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			block := make([]byte, n)
			copy(block, buf[:n])
			outCh <- OutputBlock{Data: block, ReceivedAt: time.Now()}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading data from deflate: %v", err)
			}
			close(outCh)
			return
		}
	}
}

func main() {
	file, err := os.Open("G:\\projects\\faf-pioneer\\packets_2025_03_26-23_05_50.log")
	if err != nil {
		log.Fatalf("unable to open a file: %v", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	scanner := bufio.NewScanner(file)
	lineCount := 0
	// Skip first 3 lines of file header.
	for scanner.Scan() {
		lineCount++
		if lineCount >= 3 {
			break
		}
	}

	// Pipe for data transfer to deflate.
	pr, pw := io.Pipe()

	// Game uses `flate` from zlib using 1 << 14 window size, but for `deflate`
	// this doesn't really matter, so just create default `flate` reader.
	// One important thing there is that game also didn't include zlib flate header bytes, only
	// processing raw packets payload into GPGNetStream data.

	deflateReader := flate.NewReader(pr)
	defer func(deflateReader io.ReadCloser) {
		_ = deflateReader.Close()
	}(deflateReader)

	outCh := make(chan OutputBlock, 10)
	go readerGoroutine(deflateReader, outCh)

	var pendingPackets []PacketInfo

	for scanner.Scan() {
		line := scanner.Text()
		// Take only incoming `DATA` packets into account.
		if !strings.HasPrefix(line, " -> | DATA") {
			continue
		}

		payloadHex, seqStr, err := extractPayloadAndSeq(line)
		if err != nil {
			continue
		}

		cleanHex := strings.ReplaceAll(payloadHex, " ", "")
		if cleanHex == "" {
			continue
		}

		data, err := hex.DecodeString(cleanHex)
		if err != nil {
			log.Printf("Hex decode for row '%q', are failed: %v", payloadHex, err)
			continue
		}
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			log.Printf("Failed to parse packet Seq number: %v", err)
			continue
		}

		log.Printf("Data of a DATA packet [Seq = '%d', Size = '%d' (bytes)]", seq, len(data))
		_, err = pw.Write(data)
		if err != nil {
			log.Printf("Failed writing data into pipe: %v", err)
			break
		}

		pendingPackets = append(pendingPackets, PacketInfo{Seq: seq, Length: len(data)})
		// Wait for deflate to process.
		time.Sleep(100 * time.Millisecond)

		select {
		case block, ok := <-outCh:
			if ok {
				printUsedPacketData(pendingPackets, block)
				pendingPackets = nil
			}
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Failed to scan line from input file: %v", err)
	}

	_ = pw.Close()

	// Output all left packets.
	for block := range outCh {
		printUsedPacketData(pendingPackets, block)
	}
}

func printUsedPacketData(pendingPackets []PacketInfo, block OutputBlock) {
	fmt.Printf("\n=== Decompressed (%d bytes) at %v ===\n", len(block.Data), block.ReceivedAt)
	fmt.Printf("Output data: %s\n", dataToHex(block.Data))
	fmt.Println("Packets that were used to form a valid decompression sequence:")
	fmt.Println("+------+----------------------+")
	fmt.Println("| Seq  | Length (bytes)       |")
	fmt.Println("+------+----------------------+")
	for _, p := range pendingPackets {
		fmt.Printf("| %4d | %20d |\n", p.Seq, p.Length)
	}
	fmt.Println("+------+----------------------+")
}
