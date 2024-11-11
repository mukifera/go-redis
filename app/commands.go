package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func handleCommand(call respArray, conn net.Conn, store *redisStore) {

	command, ok := call[0].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}

	switch strings.ToUpper(string(command)) {
	case "PING":
		handlePingCommand(conn)
	case "ECHO":
		handleEchoCommand(call, conn)
	case "SET":
		handleSetCommand(call, conn, store)
		propagateToReplicas(call, store)
	case "GET":
		handleGetCommand(call, conn, store)
	case "CONFIG":
		handleConfigCommand(call, conn, store)
	case "KEYS":
		handleKeysCommand(call, conn, store)
	case "INFO":
		handleInfoCommand(call, conn, store)
	case "REPLCONF":
		handleReplconfCommand(call, conn, store)
	case "PSYNC":
		handlePsyncCommand(conn, store)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s\n", command)
	}
}

func handlePingCommand(conn net.Conn) {
	res := respSimpleString("PONG")
	writeToConnection(conn, res.encode())
}

func handleEchoCommand(call respArray, conn net.Conn) {
	key, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}
	res := respBulkString(key)
	writeToConnection(conn, res.encode())
}

func handleSetCommand(call respArray, conn net.Conn, store *redisStore) {

	if len(call) != 3 && len(call) != 5 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to SET command")
		return
	}
	var err error
	var expiry uint64
	key, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "key must be a string")
		return
	}
	value, ok := call[2].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "value must be a string")
	}
	if len(call) == 5 {
		flag, ok := call[3].(respBulkString)
		if !ok {
			fmt.Fprintln(os.Stderr, "expected flag to be a string")
			return
		}
		if strings.ToUpper(string(flag)) == "PX" {
			expiry_str, ok := call[4].(respBulkString)
			if !ok {
				fmt.Fprintln(os.Stderr, "expected an expiry value")
				return
			}
			expiry, err = strconv.ParseUint(string(expiry_str), 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "expected expiry value to be an integer: %v\n", err)
				return
			}
		}
		store.setWithExpiry(string(key), value, expiry)
	} else {
		store.set(string(key), call[2])
	}
	res := respSimpleString("OK")
	writeToConnection(conn, res.encode())
}

func handleGetCommand(call respArray, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to GET command")
		return
	}
	key, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "key must be a string")
		return
	}
	res, ok := store.get(string(key))
	if !ok {
		res = respNullBulkString{}
	}
	writeToConnection(conn, res.encode())
}

func handleConfigCommand(call respArray, conn net.Conn, store *redisStore) {
	if len(call) != 3 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
		return
	}
	sub, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected a string subcommand to CONFIG command")
		return
	}
	if strings.ToUpper(string(sub)) != "GET" {
		fmt.Fprintln(os.Stderr, "invalid use of the CONFIG GET command")
		return
	}
	param, ok := call[2].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected a string param")
		return
	}
	value, ok := store.getParam(string(param))
	var vals respArray = []respObject{param}
	if !ok {
		vals = append(vals, nil)
	} else {
		bulk_str := respBulkString(value)
		vals = append(vals, bulk_str)
	}
	writeToConnection(conn, vals.encode())
}

func handleKeysCommand(call respArray, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to CONFIG command")
		return
	}

	search, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintf(os.Stderr, "expected a string search parameter")
	}

	keys := store.getKeys(string(search))
	var res respArray = make([]respObject, len(keys))
	for i := 0; i < len(keys); i++ {
		res[i] = respBulkString(keys[i])
	}
	writeToConnection(conn, res.encode())

}

func handleInfoCommand(call respArray, conn net.Conn, store *redisStore) {
	if len(call) != 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to INFO command")
		return
	}

	arg, ok := call[1].(respBulkString)
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
	res := respBulkString(info)
	writeToConnection(conn, res.encode())
}

func handleReplconfCommand(call respArray, conn net.Conn, store *redisStore) {
	if len(call) < 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to REPLCONF command")
		return
	}
	sub, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintf(os.Stderr, "expected a string subcommand for REPLCONF")
		return
	}
	if sub == "listening-port" {
		store.addReplica(conn)
	}
	res := respSimpleString("OK")
	writeToConnection(conn, res.encode())
}

func handlePsyncCommand(conn net.Conn, store *redisStore) error {
	strs := make([]string, 3)
	strs[0] = "FULLRESYNC"
	ok := true
	strs[1], ok = store.getParam("master_replid")
	if !ok {
		return errors.New("no master_replid found")
	}
	strs[2], ok = store.getParam("master_repl_offset")
	if !ok {
		return errors.New("no master_repl_offset found")
	}
	res := respSimpleString(strings.Join(strs, " "))
	writeToConnection(conn, res.encode())
	sendCurrentState(conn)
	return nil
}

func sendCurrentState(conn net.Conn) {
	data := generateRDBFile(nil)
	res := respBulkString(data).encode()
	res = res[:len(res)-2]
	writeToConnection(conn, res)
}

func propagateToReplicas(call respArray, store *redisStore) {
	res := call.encode()
	for _, conn := range store.replicas {
		writeToConnection(conn, res)
	}
}
