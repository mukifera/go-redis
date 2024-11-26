package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type commandHandlerFunc func(respArray, *redisConn, *redisStore) respObject
type commandHandlerFuncs map[string]commandHandlerFunc

func getCommandName(call respArray) (string, bool) {
	command, ok := respToString(call[0])
	if !ok {
		return "", false
	}

	return strings.ToUpper(command), true
}

func getRespArrayCall(obj respObject) respArray {
	switch typed := obj.(type) {
	case respArray:
		return typed
	case respSimpleString, respBulkString:
		call := []respObject{typed}
		return call
	default:
		fmt.Fprintf(os.Stderr, "invalid command %v\n", obj)
		return respArray{}
	}
}

func handleCommand(call respArray, conn *redisConn, store *redisStore) respObject {

	command, ok := getCommandName(call)
	if !ok {
		return respSimpleError("expected command name as string")
	}

	conn.mu.Lock()
	if conn.multi && command != "EXEC" && command != "DISCARD" {
		fmt.Printf("Queued command %v\n", call)
		conn.queued = append(conn.queued, call)
		conn.mu.Unlock()
		return respSimpleString("QUEUED")
	}
	conn.mu.Unlock()

	fmt.Printf("Received command %v\n", call)

	var handlers commandHandlerFuncs = commandHandlerFuncs{
		"PING":     handlePingCommand,
		"ECHO":     handleEchoCommand,
		"SET":      handleSetCommand,
		"GET":      handleGetCommand,
		"CONFIG":   handleConfigCommand,
		"KEYS":     handleKeysCommand,
		"INFO":     handleInfoCommand,
		"REPLCONF": handleReplconfCommand,
		"PSYNC":    handlePsyncCommand,
		"WAIT":     handleWaitCommand,
		"TYPE":     handleTypeCommand,
		"XADD":     handleXaddCommand,
		"XRANGE":   handleXrangeCommand,
		"XREAD":    handleXreadCommand,
		"INCR":     handleIncrCommand,
		"MULTI":    handleMultiCommand,
		"EXEC":     handleExecCommand,
		"DISCARD":  handleDiscardCommand,
	}

	handler, ok := handlers[command]
	if !ok {
		return respSimpleError(fmt.Sprintf("unknown command %v\n", call))
	}

	return handler(call, conn, store)
}

func handlePingCommand(_ respArray, conn *redisConn, store *redisStore) respObject {
	if store.master == nil || conn.conn != store.master.conn {
		return respSimpleString("PONG")
	}
	return nil
}

func handleEchoCommand(call respArray, conn *redisConn, _ *redisStore) respObject {
	key, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("expected command name as string")
	}
	return respBulkString(key)
}

func handleSetCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 3 && len(call) != 5 {
		return respSimpleError("invalid number of arguments to SET command")
	}
	var err error
	var expiry uint64
	key, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("key must be a string")
	}
	value, ok := call[2].(respBulkString)
	if !ok {
		return respSimpleError("value must be a string")
	}
	if len(call) == 5 {
		flag, ok := call[3].(respBulkString)
		if !ok {
			return respSimpleError("expected flag to be a string")
		}
		if strings.ToUpper(string(flag)) == "PX" {
			expiry_str, ok := call[4].(respBulkString)
			if !ok {
				return respSimpleError("expected an expiry value")
			}
			expiry, err = strconv.ParseUint(string(expiry_str), 10, 64)
			if err != nil {
				return respSimpleError(fmt.Sprintf("expected expiry value to be an integer: %v\n", err))
			}
		}
		store.setWithExpiry(string(key), value, expiry)
	} else {
		store.set(string(key), call[2])
	}

	if store.master == nil || conn.conn != store.master.conn {
		return respSimpleString("OK")
	}
	return nil
}

func handleGetCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 2 {
		return respSimpleError("invalid number of arguments to GET command")
	}
	key, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("key must be a string")
	}
	res, ok := store.get(string(key))
	if !ok {
		res = respNullBulkString{}
	}
	if store.master == nil || conn.conn != store.master.conn {
		return res
	}
	return nil
}

func handleConfigCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 3 {
		return respSimpleError("invalid number of arguments to CONFIG command")
	}
	sub, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("expected a string subcommand to CONFIG command")
	}
	if strings.ToUpper(string(sub)) != "GET" {
		return respSimpleError("invalid use of the CONFIG GET command")
	}
	param, ok := call[2].(respBulkString)
	if !ok {
		return respSimpleError("expected a string param")
	}
	value, ok := store.getParam(string(param))
	var vals respArray = []respObject{param}
	if !ok {
		vals = append(vals, nil)
	} else {
		bulk_str := respBulkString(value)
		vals = append(vals, bulk_str)
	}
	return vals
}

func handleKeysCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 2 {
		return respSimpleError("invalid number of arguments to CONFIG command")
	}

	search, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("expected a string search parameter")
	}

	keys := store.getKeys(string(search))
	var res respArray = make([]respObject, len(keys))
	for i := 0; i < len(keys); i++ {
		res[i] = respBulkString(keys[i])
	}
	return res
}

func handleInfoCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 2 {
		return respSimpleError("invalid number of arguments to INFO command")
	}

	arg, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("expected a string argument for INFO")
	}
	if arg != "replication" {
		return nil
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
	return res
}

func handleReplconfCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) < 2 {
		return respSimpleError("invalid number of arguments to REPLCONF command")
	}
	sub, ok := call[1].(respBulkString)
	if !ok {
		return respSimpleError("expected a string subcommand for REPLCONF")
	}

	var res respObject
	switch strings.ToUpper(string(sub)) {
	case "LISTENING-PORT":
		_, ok := respToString(call[2])
		if !ok {
			return respSimpleError("invalid listening port")
		}
		_, ok = conn.conn.RemoteAddr().(*net.TCPAddr)
		if !ok {
			return respSimpleError("invalid TCP host")
		}

		store.addReplica(conn)
		res = respSimpleString("OK")

	case "GETACK":
		conn.mu.Lock()
		res = generateCommand("REPLCONF", "ACK", strconv.Itoa(conn.offset))
		fmt.Printf("offset = %d\n", conn.offset)
		conn.mu.Unlock()

	case "ACK":
		num, ok := respToInt(call[2])
		if !ok {
			return respSimpleError("invalid response to ACK")
		}
		conn.mu.Lock()
		conn.offset = num
		fmt.Printf("offset for replica %v is %d\n", conn.conn.RemoteAddr(), conn.offset)
		conn.mu.Unlock()
		return nil

	default:
		res = respSimpleString("OK")
	}
	return res
}

func handlePsyncCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	strs := make([]string, 3)
	strs[0] = "FULLRESYNC"
	ok := true
	strs[1], ok = store.getParam("master_replid")
	if !ok {
		return respSimpleError("no master_replid found")
	}
	strs[2], ok = store.getParam("master_repl_offset")
	if !ok {
		return respSimpleError("no master_repl_offset found")
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

func handleWaitCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 3 {
		return respSimpleError("invalid number of arguments to WAIT command")
	}

	numreplicas, ok := respToInt(call[1])
	if !ok {
		return respSimpleError("expected numreplicas to be an integer")
	}

	timeout, ok := respToInt(call[2])
	if !ok {
		return respSimpleError("expected timeout to be an integer")
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
	return res
}

func handleTypeCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 2 {
		return respSimpleError("invalid number of arguments to TYPE command")
	}

	key, ok := respToString(call[1])
	if !ok {
		return respSimpleError("expected a string value for key")
	}

	value_type := store.typeOfValue(key)
	res := respSimpleString(value_type)
	return res
}

func handleXaddCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) < 3 {
		return respSimpleError("invalid number of arguments to XADD command")
	}

	key, ok := respToString(call[1])
	if !ok {
		return respSimpleError("expected a string stream key")
	}

	raw_stream, ok := store.get(key)
	stream := &respStream{}
	if ok {
		stream, ok = raw_stream.(*respStream)
		if !ok {
			return respSimpleError("key has a non stream value type")
		}
	}

	id, ok := respToString(call[2])
	if !ok {
		return respSimpleError("expected a string entry id")
	}

	id, err := processStreamID(stream, id)
	if err != nil {
		res := respSimpleError(err.Error())
		return res
	}

	data := make(map[string]respObject)
	if (len(call)-3)%2 != 0 {
		return respSimpleError("expected a list of key/value pairs")
	}

	for i := 3; i < len(call); i += 2 {
		data_key, ok := respToString(call[i])
		if !ok {
			return respSimpleError("expected stream entry keys to be strings")
		}
		data[data_key] = call[i+1]
	}

	stream.mu.Lock()
	stream.addEntry(id, data)
	stream.mu.Unlock()
	store.set(key, stream)

	res := respBulkString(id)
	return res
}

func handleXrangeCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 4 {
		return respSimpleError("ERR invalid number of arguments to XRANGE command")
	}

	key, ok := respToString(call[1])
	if !ok {
		res := respSimpleError("ERR expected a string key")
		return res
	}

	stream_raw, ok := store.get(key)
	if !ok {
		return respSimpleError("ERR key does not exist in store")
	}
	stream, ok := stream_raw.(*respStream)
	if !ok {
		return respSimpleError("ERR key does not hold a stream value")
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	from_id, ok := respToString(call[2])
	if !ok {
		return respSimpleError("ERR the start argument is not a valid string")
	}

	to_id, ok := respToString(call[3])
	if !ok {
		return respSimpleError("ERR the end argument is not a valid string")
	}

	var from_index int
	if from_id == "-" {
		from_index = 0
	} else {
		from_index = streamLowerBound(stream, from_id)
	}

	var to_index int
	if to_id == "+" {
		to_index = len(stream.entries) - 1
	} else {
		to_index = streamUpperBound(stream, to_id)
	}

	res := respArray{}
	for i := from_index; i <= to_index; i++ {
		entry := respArray{}
		entry = append(entry, respBulkString(stream.entries[i].id))

		data := respArray{}
		for k, v := range stream.entries[i].data {
			data = append(data, respBulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		res = append(res, entry)
	}

	return res
}

func handleXreadCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) < 4 || len(call)%2 != 0 {
		return respSimpleError("ERR invalid number of arguments to XREAD command")
	}

	keys_and_ids := call[2:]
	sub, ok := respToString(call[1])
	var timer <-chan time.Time

	is_blocking := ok && sub == "block"

	if is_blocking {
		keys_and_ids = call[4:]

		timeout, ok := respToInt(call[2])
		if !ok {
			return respSimpleError("ERR expected timeout to be a number")
		}

		if timeout == 0 {
			timer = nil
		} else {
			timer = time.After(time.Duration(timeout) * time.Millisecond)
		}
	}

	num_of_streams := len(keys_and_ids) / 2

	keys := make([]string, 0)
	streams := make([]*respStream, 0)
	ids := make([]string, 0)

	for i := 0; i < num_of_streams; i++ {
		key, ok := respToString(keys_and_ids[i])
		if !ok {
			return respSimpleError("ERR expected a string for stream key")
		}

		id, ok := respToString(keys_and_ids[i+num_of_streams])
		if !ok {
			return respSimpleError("ERR expected a string for stream key")
		}

		stream_raw, ok := store.get(key)
		if !ok {
			return respSimpleError("ERR key does not exist in store")
		}
		stream, ok := stream_raw.(*respStream)
		if !ok {
			return respSimpleError("ERR key does not hold a stream value")
		}

		if id == "$" {
			stream.mu.Lock()
			id = stream.entries[len(stream.entries)-1].id
			stream.mu.Unlock()
		}

		keys = append(keys, key)
		streams = append(streams, stream)
		ids = append(ids, id)

	}

	var res respObject
	if is_blocking {
		res = blockStreamsRead(keys, streams, ids, timer)
	} else {
		res = readFromStreams(keys, streams, ids)
	}

	return res
}

func handleIncrCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	if len(call) != 2 {
		return respSimpleError("invalid number of commands to INCR command")
	}

	key, ok := respToString(call[1])
	if !ok {
		return respSimpleError("expected a string key")
	}

	value_raw, key_exists := store.get(key)

	var value int

	if key_exists {
		stored_value, is_a_number := respToInt(value_raw)

		if !is_a_number {
			return respSimpleError("ERR value is not an integer or out of range")
		}

		value = stored_value + 1
	} else {
		value = 1
	}

	str := respBulkString(strconv.Itoa(value))
	store.set(key, str)

	return respInteger(value)
}

func handleMultiCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	conn.mu.Lock()
	conn.multi = true
	conn.mu.Unlock()
	return respSimpleString("OK")
}

func handleExecCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	conn.mu.Lock()
	if !conn.multi {
		conn.mu.Unlock()
		return respSimpleError("ERR EXEC without MULTI")
	}
	conn.multi = false
	conn.mu.Unlock()

	res := respArray{}
	for _, sub_call := range conn.queued {
		sub := acceptCommand(sub_call, conn, store)
		res = append(res, sub)
	}

	return res
}

func handleDiscardCommand(call respArray, conn *redisConn, store *redisStore) respObject {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.multi {
		return respSimpleError("ERR DISCARD without MULTI")
	}
	conn.multi = false
	conn.queued = make([]respObject, 0)
	return respSimpleString("OK")
}

func blockStreamsRead(keys []string, streams []*respStream, ids []string, timer <-chan time.Time) respObject {
	for {
		select {
		case <-timer:
			return respNullBulkString{}
		default:
			reads := respArray{}
			for i := 0; i < len(streams); i++ {
				key := keys[i]
				stream := streams[i]
				id := ids[i]

				entries := readStreamEntries(stream, id)

				if len(entries) == 0 {
					continue
				}

				stream_read := respArray{}
				stream_read = append(stream_read, respBulkString(key))
				stream_read = append(stream_read, entries)

				reads = append(reads, stream_read)
			}

			if len(reads) != 0 {
				return reads
			}
		}
	}
}

func readFromStreams(keys []string, streams []*respStream, ids []string) respArray {
	reads := respArray{}

	for i := 0; i < len(streams); i++ {
		key := keys[i]
		stream := streams[i]
		id := ids[i]

		read := readFromStream(key, stream, id)

		reads = append(reads, read)
	}

	return reads
}

func readFromStream(key string, stream *respStream, id string) respArray {
	entries := readStreamEntries(stream, id)

	stream_read := respArray{}
	stream_read = append(stream_read, respBulkString(key))
	stream_read = append(stream_read, entries)

	return stream_read
}

func readStreamEntries(stream *respStream, id string) respArray {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	from_index := streamLowerBound(stream, id)
	if from_index < len(stream.entries) && stream.entries[from_index].id == id {
		from_index++
	}

	entries := respArray{}
	for j := from_index; j < len(stream.entries); j++ {
		entry := respArray{}
		entry = append(entry, respBulkString(stream.entries[j].id))

		data := respArray{}
		for k, v := range stream.entries[j].data {
			data = append(data, respBulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		entries = append(entries, entry)
	}

	return entries
}

func streamLowerBound(stream *respStream, id string) int {
	low := -1
	high := len(stream.entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.entries[mid].id) != 1 {
			high = mid
		} else {
			low = mid
		}
	}
	return high
}

func streamUpperBound(stream *respStream, id string) int {
	low := -1
	high := len(stream.entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.entries[mid].id) != -1 {
			low = mid
		} else {
			high = mid
		}
	}
	return low
}

func processStreamID(stream *respStream, id string) (string, error) {

	if id == "*" {
		current_timestamp := time.Now().UnixMilli()
		return strconv.Itoa(int(current_timestamp)) + "-0", nil
	}

	top_id := "0-0"
	if len(stream.entries) != 0 {
		top_id = stream.entries[len(stream.entries)-1].id
	}

	id_split := strings.Split(id, "-")
	if len(id_split) != 2 {
		return "", fmt.Errorf("invalid id format")
	}

	id_ms, err := strconv.Atoi(id_split[0])
	if err != nil {
		return "", fmt.Errorf("ERR The ID %s does not have a valid timestamp", id)
	}

	if id_split[1] == "*" {

		top_split := strings.Split(top_id, "-")
		top_ms, _ := strconv.Atoi(top_split[0])
		top_seq, _ := strconv.Atoi(top_split[1])

		if id_ms < top_ms {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if id_ms == top_ms {
			id = id_split[0] + "-" + strconv.Itoa(top_seq+1)
		} else {
			id = id_split[0] + "-0"
		}

		return id, nil
	}

	_, err = strconv.Atoi(id_split[1])
	if err != nil {
		return "", fmt.Errorf("ERR The ID %s does not have a valid sequence number", id)
	}

	if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	} else if compareStreamIDs(id, top_id) != 1 {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return id, nil
}

func compareStreamIDs(a string, b string) int {
	if a == b {
		return 0
	}
	a_split := strings.Split(a, "-")
	b_split := strings.Split(b, "-")

	a_ms, _ := strconv.Atoi(a_split[0])
	a_seq, _ := strconv.Atoi(a_split[1])
	b_ms, _ := strconv.Atoi(b_split[0])
	b_seq, _ := strconv.Atoi(b_split[1])

	if a_ms == b_ms {
		if a_seq < b_seq {
			return -1
		}
		return 1
	}
	if a_ms < b_ms {
		return -1
	}
	return 1
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
	res := generateCommand("REPLCONF", "GETACK", "*").encode()
	writeToConnection(conn, res)
	conn.total_propagated += len(res)
	fmt.Printf("Propagated %d bytes to replica %v: %v\n", len(res), conn.conn.RemoteAddr(), strconv.Quote(string(res)))
	conn.mu.Unlock()
}
