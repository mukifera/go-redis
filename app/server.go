package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var res []byte

	read := make(chan byte, 1<<14)
	go readFromConnection(conn, read)

	for {

		raw_call := decode(read)
		call, ok := raw_call.([]interface{})
		if !ok {
			fmt.Fprintln(os.Stderr, "expected command as array")
			continue
		}
		command, ok := call[0].(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "expected command name as string")
			continue
		}

		switch command {
		case "PING":
			res = encodeSimpleString("PONG")
			writeToConnection(conn, res)
		case "ECHO":
			key, ok := call[1].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "expected command name as string")
				continue
			}
			res = encodeBulkString(key)
			writeToConnection(conn, res)
		default:
			fmt.Fprintf(os.Stderr, "unknown command %s\n", command)
		}
	}
}

func writeToConnection(conn net.Conn, data []byte) {
	_, err := conn.Write(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write to connection: %v\n", err)
		return
	}
}

func readFromConnection(conn net.Conn, out chan<- byte) {
	defer close(out)

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err == io.EOF {
			continue
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read from connection: %v\n", err)
			return
		}
		for i := 0; i < n; i++ {
			out <- buf[i]
		}
	}
}
