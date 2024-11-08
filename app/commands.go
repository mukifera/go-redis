package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func handleCommand(call []interface{}, conn net.Conn, store *redisStore) {

	command, ok := call[0].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}
	command = strings.ToUpper(command)

	switch command {
	case "PING":
		handlePingCommand(conn)
	case "ECHO":
		handleEchoCommand(call, conn)
	case "SET":
		handleSetCommand(call, conn, store)
	case "GET":
		handleGetCommand(call, conn, store)
	case "CONFIG":
		handleConfigCommand(call, conn, store)
	case "KEYS":
		handleKeysCommand(call, conn, store)
	case "INFO":
		handleInfoCommand(call, conn, store)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s\n", command)
	}
}

func handlePingCommand(conn net.Conn) {
	res := encodeSimpleString("PONG")
	writeToConnection(conn, res)
}

func handleEchoCommand(call []interface{}, conn net.Conn) {
	key, ok := call[1].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}
	res := encodeBulkString(&key)
	writeToConnection(conn, res)
}

func handleSetCommand(call []interface{}, conn net.Conn, store *redisStore) {

	if len(call) != 3 && len(call) != 5 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to SET command")
		return
	}
	var err error
	var expiry uint64
	key, ok := call[1].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "key must be a string")
		return
	}
	if len(call) == 5 {
		flag, ok := call[3].(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "expected flag to be a string")
			return
		}
		flag = strings.ToUpper(flag)
		if flag == "PX" {
			expiry_str, ok := call[4].(string)
			if !ok {
				fmt.Fprintln(os.Stderr, "expected an expiry value")
				return
			}
			expiry, err = strconv.ParseUint(expiry_str, 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "expected expiry value to be an integer: %v\n", err)
				return
			}
		}
		store.setWithExpiry(key, call[2], expiry)
	} else {
		store.set(key, call[2])
	}
	res := encodeSimpleString("OK")
	writeToConnection(conn, res)
}

func handleGetCommand(call []interface{}, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to GET command")
		return
	}
	key, ok := call[1].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "key must be a string")
		return
	}
	value, ok := store.get(key)
	var res []byte
	if !ok {
		res = encodeBulkString(nil)
	} else {
		res = encode(value)
	}
	writeToConnection(conn, res)
}

func handleConfigCommand(call []interface{}, conn net.Conn, store *redisStore) {
	if len(call) != 3 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
		return
	}
	sub, ok := call[1].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected a string subcommand to CONFIG command")
		return
	}
	sub = strings.ToUpper(sub)
	if sub != "GET" {
		fmt.Fprintln(os.Stderr, "invalid use of the CONFIG GET command")
		return
	}
	param, ok := call[2].(string)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected a string param")
		return
	}
	value, ok := store.getParam(param)
	vals := []interface{}{&param}
	if !ok {
		vals = append(vals, nil)
	} else {
		vals = append(vals, &value)
	}
	res := encode(vals)
	writeToConnection(conn, res)
}

func handleKeysCommand(call []interface{}, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
		return
	}

	search, ok := call[1].(string)
	if !ok {
		fmt.Fprintf(os.Stderr, "expected a string search parameter")
	}

	keys := store.getKeys(search)
	res := encode(keys)
	writeToConnection(conn, res)

}

func handleInfoCommand(call []interface{}, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to INFO command")
		return
	}

	arg, ok := call[1].(string)
	if !ok {
		fmt.Fprintf(os.Stderr, "expected a string argument for INFO")
	}
	if arg != "replication" {
		return
	}

	role := "master"
	if _, ok := store.getParam("replicaof"); ok {
		role = "slave"
	}
	strs := []string{"role:" + role}
	if role == "master" {
		master_replid, _ := store.getParam("master_replid")
		master_repl_offset, _ := store.getParam("master_repl_offset")
		strs = append(strs, "master_replid:"+master_replid)
		strs = append(strs, "master_repl_offset:"+master_repl_offset)
	}

	info := strings.Join(strs, "\r\n")
	res := encodeBulkString(&info)
	writeToConnection(conn, res)
}
