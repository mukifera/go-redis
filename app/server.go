package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var req []byte
	var res []byte
	var err error

	for {
		req = make([]byte, 1024)
		_, err = conn.Read(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read from connection")
			os.Exit(1)
		}

		res = []byte("+PONG\r\n")
		_, err = conn.Write(res)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to write to connection")
			os.Exit(1)
		}
	}
}
