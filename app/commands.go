package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func handleCommand(call respArray, conn *redisConn, store *redisStore) {

	command, ok := respToString(call[0])
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}

	fmt.Printf("Received command %v\n", call)

	switch strings.ToUpper(string(command)) {
	case "PING":
		handlePingCommand(conn, store)
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
	case "WAIT":
		handleWaitCommand(call, conn, store)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %v\n", call)
	}
}

func handlePingCommand(conn *redisConn, store *redisStore) {
	res := respSimpleString("PONG")
	if store.master == nil || conn.conn != store.master.conn {
		writeToConnection(conn, res.encode())
	}
}

func handleEchoCommand(call respArray, conn *redisConn) {
	key, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintln(os.Stderr, "expected command name as string")
		return
	}
	res := respBulkString(key)
	writeToConnection(conn, res.encode())
}

func handleSetCommand(call respArray, conn *redisConn, store *redisStore) {

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
	if store.master == nil || conn.conn != store.master.conn {
		writeToConnection(conn, res.encode())
	}
}

func handleGetCommand(call respArray, conn *redisConn, store *redisStore) {
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
	if store.master == nil || conn.conn != store.master.conn {
		writeToConnection(conn, res.encode())
	}
}

func handleConfigCommand(call respArray, conn *redisConn, store *redisStore) {
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

func handleKeysCommand(call respArray, conn *redisConn, store *redisStore) {
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

func handleInfoCommand(call respArray, conn *redisConn, store *redisStore) {
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

func handleReplconfCommand(call respArray, conn *redisConn, store *redisStore) {
	if len(call) < 2 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to REPLCONF command")
		return
	}
	sub, ok := call[1].(respBulkString)
	if !ok {
		fmt.Fprintf(os.Stderr, "expected a string subcommand for REPLCONF")
		return
	}

	var res []byte
	switch strings.ToUpper(string(sub)) {
	case "LISTENING-PORT":
		_, ok := respToString(call[2])
		if !ok {
			fmt.Fprintf(os.Stderr, "invalid listening port")
			return
		}
		_, ok = conn.conn.RemoteAddr().(*net.TCPAddr)
		if !ok {
			fmt.Fprintf(os.Stderr, "invalid TCP host")
			return
		}

		store.addReplica(conn)
		res = respSimpleString("OK").encode()

	case "GETACK":
		conn.mu.Lock()
		res = generateCommand("REPLCONF", "ACK", strconv.Itoa(conn.offset))
		fmt.Printf("offset = %d\n", conn.offset)
		conn.mu.Unlock()

	case "ACK":
		num, ok := respToInt(call[2])
		if !ok {
			fmt.Fprintf(os.Stderr, "invalid response to ACK")
			return
		}
		conn.mu.Lock()
		conn.offset = num
		fmt.Printf("offset for replica %v is %d\n", conn.conn.RemoteAddr(), conn.offset)
		conn.mu.Unlock()
		return

	default:
		res = respSimpleString("OK").encode()
	}
	writeToConnection(conn, res)
}

func handlePsyncCommand(conn *redisConn, store *redisStore) error {
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
	conn.mu.Lock()
	conn.total_propagated = 0
	conn.offset = 0
	conn.mu.Unlock()

	conn.ticker = time.NewTicker(200 * time.Millisecond)
	conn.stopChan = make(chan bool)
	go sendAcksToReplica(conn)

	return nil
}

func handleWaitCommand(call respArray, conn *redisConn, store *redisStore) {
	if len(call) != 3 {
		fmt.Fprintln(os.Stderr, "invalid number of arguments to WAIT command")
		return
	}

	numreplicas, ok := respToInt(call[1])
	if !ok {
		fmt.Fprintln(os.Stderr, "expected numreplicas to be an integer")
		return
	}

	timeout, ok := respToInt(call[2])
	if !ok {
		fmt.Fprintln(os.Stderr, "expected timeout to be an integer")
		return
	}

	for _, replica := range store.replicas {
		sendAckToReplica(replica)
	}

	timer := time.After(time.Duration(timeout * int(time.Millisecond)))
	replicatation_count := 0
	timed_out := false
	update_replication_count := func() {
		replicatation_count = 0
		for _, replica := range store.replicas {
			replica.mu.Lock()
			if replica.expected_offset == replica.offset {
				replicatation_count++
			}
			replica.mu.Unlock()
		}
	}
	for replicatation_count < numreplicas && !timed_out {
		select {
		case <-timer:
			timed_out = true
		default:
			update_replication_count()
		}
	}
	update_replication_count()

	res := respInteger(replicatation_count)
	writeToConnection(conn, res.encode())
}

func sendCurrentState(conn *redisConn) {
	data := generateRDBFile(nil)
	res := respBulkString(data).encode()
	res = res[:len(res)-2]
	writeToConnection(conn, res)
}

func propagateToReplicas(call respArray, store *redisStore) {
	res := call.encode()
	for _, conn := range store.replicas {
		fmt.Printf("Propagating %v to replica %v\n", call, conn.conn.LocalAddr())
		writeToConnection(conn, res)
		conn.mu.Lock()
		conn.total_propagated += len(res)
		conn.expected_offset = conn.total_propagated
		fmt.Printf("sent %d bytes to replica %v: %s\n", len(res), conn.conn.RemoteAddr(), strconv.Quote(string(res)))
		fmt.Printf("total_propagated = %d, offset = %d\n", conn.total_propagated, conn.offset)
		conn.mu.Unlock()
	}
}

func sendAcksToReplica(conn *redisConn) {
	defer conn.ticker.Stop()
	for {
		select {
		case <-conn.ticker.C:
			sendAckToReplica(conn)
		case <-conn.stopChan:
			return
		}
	}
}

func sendAckToReplica(conn *redisConn) {
	conn.mu.Lock()
	if conn.offset == conn.expected_offset {
		conn.mu.Unlock()
		return
	}
	conn.expected_offset = conn.total_propagated
	res := generateCommand("REPLCONF", "GETACK", "*")
	writeToConnection(conn, res)
	conn.total_propagated += len(res)
	conn.mu.Unlock()
}
