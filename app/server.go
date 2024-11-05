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
	conn, err := l.Accept()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
		os.Exit(1)
	}

	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to read from connection")
		os.Exit(1)
	}

	response := []byte("+PONG\r\n")
	_, err = conn.Write(response)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to write from connection")
		os.Exit(1)
	}

}
