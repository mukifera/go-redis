package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	var store redisStore
	store.init()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn, &store)
	}
}

func handleConnection(conn net.Conn, store *redisStore) {
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
		command = strings.ToUpper(command)

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
		case "SET":
			if len(call) != 3 && len(call) != 5 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to SET command")
				continue
			}
			var err error
			expiry := -1
			if len(call) == 5 {
				flag, ok := call[3].(string)
				if !ok {
					fmt.Fprintln(os.Stderr, "expected flag to be a string")
					continue
				}
				flag = strings.ToUpper(flag)
				if flag == "PX" {
					expiry_str, ok := call[4].(string)
					if !ok {
						fmt.Fprintln(os.Stderr, "expected an expiry value")
						continue
					}
					expiry, err = strconv.Atoi(expiry_str)
					if err != nil {
						fmt.Fprintf(os.Stderr, "expected expiry value to be an integer: %v\n", err)
						continue
					}
				}
			}
			store.set(call[1], call[2], expiry)
			res = encodeSimpleString("OK")
			writeToConnection(conn, res)
		case "GET":
			if len(call) != 2 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to GET command")
				continue
			}
			value, ok := store.get(call[1])
			if !ok {
				res = encodeNullBulkString()
			} else {
				res = encode(value)
			}
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
