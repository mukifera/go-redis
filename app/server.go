package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {

	dir_ptr := flag.String("dir", "", "the directory of the RDB config file")
	dbfilename_ptr := flag.String("dbfilename", "", "the name of the RDB config file")
	flag.Parse()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	var store *redisStore
	store = new(redisStore)
	store.init()
	rdb_file := ""
	if dir_ptr != nil && *dir_ptr != "" {
		store.params["dir"] = *dir_ptr
		rdb_file = filepath.Join(rdb_file, *dir_ptr)
	}
	if dbfilename_ptr != nil && *dbfilename_ptr != "" {
		store.params["dbfilename"] = *dbfilename_ptr
		rdb_file = filepath.Join(rdb_file, *dbfilename_ptr)
	}

	store, err = readRDBFile(rdb_file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn, store)
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
			res = encodeBulkString(&key)
			writeToConnection(conn, res)
		case "SET":
			if len(call) != 3 && len(call) != 5 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to SET command")
				continue
			}
			var err error
			var expiry uint64
			key, ok := call[1].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "key must be a string")
				continue
			}
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
					expiry, err = strconv.ParseUint(expiry_str, 10, 64)
					if err != nil {
						fmt.Fprintf(os.Stderr, "expected expiry value to be an integer: %v\n", err)
						continue
					}
				}
				store.setWithExpiry(key, call[2], expiry)
			} else {
				store.set(key, call[2])
			}
			res = encodeSimpleString("OK")
			writeToConnection(conn, res)
		case "GET":
			if len(call) != 2 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to GET command")
				continue
			}
			key, ok := call[1].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "key must be a string")
				continue
			}
			value, ok := store.get(key)
			if !ok {
				res = encodeBulkString(nil)
			} else {
				res = encode(value)
			}
			writeToConnection(conn, res)
		case "CONFIG":
			if len(call) != 3 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
				continue
			}
			sub, ok := call[1].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "expected a string subcommand to CONFIG command")
				continue
			}
			sub = strings.ToUpper(sub)
			if sub != "GET" {
				fmt.Fprintln(os.Stderr, "invalid use of the CONFIG GET command")
				continue
			}
			param, ok := call[2].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "expected a string param")
				continue
			}
			value, ok := store.getParam(param)
			vals := []interface{}{&param}
			if !ok {
				vals = append(vals, nil)
			} else {
				vals = append(vals, &value)
			}
			res = encode(vals)
			writeToConnection(conn, res)

		case "KEYS":
			if len(call) != 2 {
				fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
				continue
			}

			search, ok := call[1].(string)
			if !ok {
				fmt.Fprintf(os.Stderr, "expected a string search parameter")
			}

			keys := store.getKeys(search)
			res = encode(keys)
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
