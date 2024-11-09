package main

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestServerStarts(t *testing.T) {
	t.Skip()
	signal := make(chan struct{})
	go startServer(serverFlags{
		dir:        "",
		dbfilename: "",
		port:       "6379",
		replicaof:  "",
	}, signal)
	defer close(signal)

	time.Sleep(time.Millisecond * 200)

	ln, err := net.Listen("tcp", "0.0.0.0:6379")
	if err == nil {
		ln.Close()
		t.Fatalf("Server did not start")
	}
}

func TestServerRespondsToPing(t *testing.T) {
	signal := make(chan struct{})
	go startServer(serverFlags{
		dir:        "",
		dbfilename: "",
		port:       "6379",
		replicaof:  "",
	}, signal)
	defer close(signal)

	time.Sleep(time.Millisecond * 200)

	conn, err := net.Dial("tcp", "0.0.0.0:6379")
	if err != nil {
		t.Fatalf("Cannot listen to port 8888: %v\n", err)
	}

	command := generateCommand("PING")
	writeToConnection(conn, command)

	buf := make([]byte, 7)
	n, err := conn.Read(buf)
	if n != 7 || err != nil {
		t.Fatalf("Invalid response to ping\n")
	}

	expected := "+PONG\r\n"
	actual := string(buf)
	if expected != actual {
		t.Fatalf("Expected: %s\nGot: %s\n", strconv.Quote(expected), strconv.Quote(actual))
	}

}
