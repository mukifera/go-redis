package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type commandHandlerFunc func(resp.Array, *core.Conn, *core.Store) resp.Object
type commandHandlerFuncs map[string]commandHandlerFunc

func getCommandName(call resp.Array) (string, bool) {
	command, ok := resp.ToString(call[0])
	if !ok {
		return "", false
	}

	return strings.ToUpper(command), true
}

func getRespArrayCall(obj resp.Object) resp.Array {
	switch typed := obj.(type) {
	case resp.Array:
		return typed
	case resp.SimpleString, resp.BulkString:
		call := []resp.Object{typed}
		return call
	default:
		fmt.Fprintf(os.Stderr, "invalid command %v\n", obj)
		return resp.Array{}
	}
}

func handleCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {

	command, ok := getCommandName(call)
	if !ok {
		return resp.SimpleError("expected command name as string")
	}

	conn.Mu.Lock()
	if conn.Multi && command != "EXEC" && command != "DISCARD" {
		fmt.Printf("Queued command %v\n", call)
		conn.Queued = append(conn.Queued, call)
		conn.Mu.Unlock()
		return resp.SimpleString("QUEUED")
	}
	conn.Mu.Unlock()

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
		return resp.SimpleError(fmt.Sprintf("unknown command %v\n", call))
	}

	return handler(call, conn, store)
}

func handlePingCommand(_ resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if store.Master == nil || conn.Conn != store.Master.Conn {
		return resp.SimpleString("PONG")
	}
	return nil
}

func handleEchoCommand(call resp.Array, conn *core.Conn, _ *core.Store) resp.Object {
	key, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected command name as string")
	}
	return resp.BulkString(key)
}

func handleSetCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 3 && len(call) != 5 {
		return resp.SimpleError("invalid number of arguments to SET command")
	}
	var err error
	var expiry uint64
	key, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("key must be a string")
	}
	value, ok := call[2].(resp.BulkString)
	if !ok {
		return resp.SimpleError("value must be a string")
	}
	if len(call) == 5 {
		flag, ok := call[3].(resp.BulkString)
		if !ok {
			return resp.SimpleError("expected flag to be a string")
		}
		if strings.ToUpper(string(flag)) == "PX" {
			expiry_str, ok := call[4].(resp.BulkString)
			if !ok {
				return resp.SimpleError("expected an expiry value")
			}
			expiry, err = strconv.ParseUint(string(expiry_str), 10, 64)
			if err != nil {
				return resp.SimpleError(fmt.Sprintf("expected expiry value to be an integer: %v\n", err))
			}
		}
		store.SetWithExpiry(string(key), value, expiry)
	} else {
		store.Set(string(key), call[2])
	}

	if store.Master == nil || conn.Conn != store.Master.Conn {
		return resp.SimpleString("OK")
	}
	return nil
}

func handleGetCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 2 {
		return resp.SimpleError("invalid number of arguments to GET command")
	}
	key, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("key must be a string")
	}
	res, ok := store.Get(string(key))
	if !ok {
		res = resp.NullBulkString{}
	}
	if store.Master == nil || conn.Conn != store.Master.Conn {
		return res
	}
	return nil
}

func handleConfigCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 3 {
		return resp.SimpleError("invalid number of arguments to CONFIG command")
	}
	sub, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected a string subcommand to CONFIG command")
	}
	if strings.ToUpper(string(sub)) != "GET" {
		return resp.SimpleError("invalid use of the CONFIG GET command")
	}
	param, ok := call[2].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected a string param")
	}
	value, ok := store.GetParam(string(param))
	var vals resp.Array = []resp.Object{param}
	if !ok {
		vals = append(vals, nil)
	} else {
		bulk_str := resp.BulkString(value)
		vals = append(vals, bulk_str)
	}
	return vals
}

func handleKeysCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 2 {
		return resp.SimpleError("invalid number of arguments to CONFIG command")
	}

	search, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected a string search parameter")
	}

	keys := store.GetKeys(string(search))
	var res resp.Array = make([]resp.Object, len(keys))
	for i := 0; i < len(keys); i++ {
		res[i] = resp.BulkString(keys[i])
	}
	return res
}

func handleInfoCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 2 {
		return resp.SimpleError("invalid number of arguments to INFO command")
	}

	arg, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected a string argument for INFO")
	}
	if arg != "replication" {
		return nil
	}

	role := "master"
	if _, ok := store.GetParam("replicaof"); ok {
		role = "slave"
	}
	strs := []string{"role:" + role}
	if role == "master" {
		master_replid, _ := store.GetParam("master_replid")
		master_repl_offset, _ := store.GetParam("master_repl_offset")
		strs = append(strs, "master_replid:"+master_replid)
		strs = append(strs, "master_repl_offset:"+master_repl_offset)
	}

	info := strings.Join(strs, "\r\n")
	res := resp.BulkString(info)
	return res
}

func handleReplconfCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) < 2 {
		return resp.SimpleError("invalid number of arguments to REPLCONF command")
	}
	sub, ok := call[1].(resp.BulkString)
	if !ok {
		return resp.SimpleError("expected a string subcommand for REPLCONF")
	}

	var res resp.Object
	switch strings.ToUpper(string(sub)) {
	case "LISTENING-PORT":
		_, ok := resp.ToString(call[2])
		if !ok {
			return resp.SimpleError("invalid listening port")
		}
		_, ok = conn.Conn.RemoteAddr().(*net.TCPAddr)
		if !ok {
			return resp.SimpleError("invalid TCP host")
		}

		store.AddReplica(conn)
		res = resp.SimpleString("OK")

	case "GETACK":
		conn.Mu.Lock()
		res = generateCommand("REPLCONF", "ACK", strconv.Itoa(conn.Offset))
		fmt.Printf("offset = %d\n", conn.Offset)
		conn.Mu.Unlock()

	case "ACK":
		num, ok := resp.ToInt(call[2])
		if !ok {
			return resp.SimpleError("invalid response to ACK")
		}
		conn.Mu.Lock()
		conn.Offset = num
		fmt.Printf("offset for replica %v is %d\n", conn.Conn.RemoteAddr(), conn.Offset)
		conn.Mu.Unlock()
		return nil

	default:
		res = resp.SimpleString("OK")
	}
	return res
}

func handlePsyncCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	strs := make([]string, 3)
	strs[0] = "FULLRESYNC"
	ok := true
	strs[1], ok = store.GetParam("master_replid")
	if !ok {
		return resp.SimpleError("no master_replid found")
	}
	strs[2], ok = store.GetParam("master_repl_offset")
	if !ok {
		return resp.SimpleError("no master_repl_offset found")
	}
	res := resp.SimpleString(strings.Join(strs, " "))
	conn.Write(res.Encode())
	sendCurrentState(conn)
	conn.Mu.Lock()
	conn.Total_propagated = 0
	conn.Offset = 0
	conn.Mu.Unlock()

	conn.Ticker = time.NewTicker(200 * time.Millisecond)
	conn.StopChan = make(chan bool)
	go sendAcksToReplica(conn)

	return nil
}

func handleWaitCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 3 {
		return resp.SimpleError("invalid number of arguments to WAIT command")
	}

	numreplicas, ok := resp.ToInt(call[1])
	if !ok {
		return resp.SimpleError("expected numreplicas to be an integer")
	}

	timeout, ok := resp.ToInt(call[2])
	if !ok {
		return resp.SimpleError("expected timeout to be an integer")
	}

	for _, replica := range store.Replicas {
		sendAckToReplica(replica)
	}

	timer := time.After(time.Duration(timeout * int(time.Millisecond)))
	replicatation_count := 0
	timed_out := false
	update_replication_count := func() {
		replicatation_count = 0
		for _, replica := range store.Replicas {
			replica.Mu.Lock()
			if replica.Expected_offset == replica.Offset {
				replicatation_count++
			}
			replica.Mu.Unlock()
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

	res := resp.Integer(replicatation_count)
	return res
}

func handleTypeCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 2 {
		return resp.SimpleError("invalid number of arguments to TYPE command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		return resp.SimpleError("expected a string value for key")
	}

	value_type := store.TypeOfValue(key)
	res := resp.SimpleString(value_type)
	return res
}

func handleXaddCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) < 3 {
		return resp.SimpleError("invalid number of arguments to XADD command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		return resp.SimpleError("expected a string stream key")
	}

	raw_stream, ok := store.Get(key)
	stream := &resp.Stream{}
	if ok {
		stream, ok = raw_stream.(*resp.Stream)
		if !ok {
			return resp.SimpleError("key has a non stream value type")
		}
	}

	id, ok := resp.ToString(call[2])
	if !ok {
		return resp.SimpleError("expected a string entry id")
	}

	id, err := processStreamID(stream, id)
	if err != nil {
		res := resp.SimpleError(err.Error())
		return res
	}

	data := make(map[string]resp.Object)
	if (len(call)-3)%2 != 0 {
		return resp.SimpleError("expected a list of key/value pairs")
	}

	for i := 3; i < len(call); i += 2 {
		data_key, ok := resp.ToString(call[i])
		if !ok {
			return resp.SimpleError("expected stream entry keys to be strings")
		}
		data[data_key] = call[i+1]
	}

	stream.Mu.Lock()
	stream.AddEntry(id, data)
	stream.Mu.Unlock()
	store.Set(key, stream)

	res := resp.BulkString(id)
	return res
}

func handleXrangeCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 4 {
		return resp.SimpleError("ERR invalid number of arguments to XRANGE command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		res := resp.SimpleError("ERR expected a string key")
		return res
	}

	stream_raw, ok := store.Get(key)
	if !ok {
		return resp.SimpleError("ERR key does not exist in store")
	}
	stream, ok := stream_raw.(*resp.Stream)
	if !ok {
		return resp.SimpleError("ERR key does not hold a stream value")
	}

	stream.Mu.Lock()
	defer stream.Mu.Unlock()

	from_id, ok := resp.ToString(call[2])
	if !ok {
		return resp.SimpleError("ERR the start argument is not a valid string")
	}

	to_id, ok := resp.ToString(call[3])
	if !ok {
		return resp.SimpleError("ERR the end argument is not a valid string")
	}

	var from_index int
	if from_id == "-" {
		from_index = 0
	} else {
		from_index = streamLowerBound(stream, from_id)
	}

	var to_index int
	if to_id == "+" {
		to_index = len(stream.Entries) - 1
	} else {
		to_index = streamUpperBound(stream, to_id)
	}

	res := resp.Array{}
	for i := from_index; i <= to_index; i++ {
		entry := resp.Array{}
		entry = append(entry, resp.BulkString(stream.Entries[i].Id))

		data := resp.Array{}
		for k, v := range stream.Entries[i].Data {
			data = append(data, resp.BulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		res = append(res, entry)
	}

	return res
}

func handleXreadCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) < 4 || len(call)%2 != 0 {
		return resp.SimpleError("ERR invalid number of arguments to XREAD command")
	}

	keys_and_ids := call[2:]
	sub, ok := resp.ToString(call[1])
	var timer <-chan time.Time

	is_blocking := ok && sub == "block"

	if is_blocking {
		keys_and_ids = call[4:]

		timeout, ok := resp.ToInt(call[2])
		if !ok {
			return resp.SimpleError("ERR expected timeout to be a number")
		}

		if timeout == 0 {
			timer = nil
		} else {
			timer = time.After(time.Duration(timeout) * time.Millisecond)
		}
	}

	num_of_streams := len(keys_and_ids) / 2

	keys := make([]string, 0)
	streams := make([]*resp.Stream, 0)
	ids := make([]string, 0)

	for i := 0; i < num_of_streams; i++ {
		key, ok := resp.ToString(keys_and_ids[i])
		if !ok {
			return resp.SimpleError("ERR expected a string for stream key")
		}

		id, ok := resp.ToString(keys_and_ids[i+num_of_streams])
		if !ok {
			return resp.SimpleError("ERR expected a string for stream key")
		}

		stream_raw, ok := store.Get(key)
		if !ok {
			return resp.SimpleError("ERR key does not exist in store")
		}
		stream, ok := stream_raw.(*resp.Stream)
		if !ok {
			return resp.SimpleError("ERR key does not hold a stream value")
		}

		if id == "$" {
			stream.Mu.Lock()
			id = stream.Entries[len(stream.Entries)-1].Id
			stream.Mu.Unlock()
		}

		keys = append(keys, key)
		streams = append(streams, stream)
		ids = append(ids, id)

	}

	var res resp.Object
	if is_blocking {
		res = blockStreamsRead(keys, streams, ids, timer)
	} else {
		res = readFromStreams(keys, streams, ids)
	}

	return res
}

func handleIncrCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 2 {
		return resp.SimpleError("invalid number of commands to INCR command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		return resp.SimpleError("expected a string key")
	}

	value_raw, key_exists := store.Get(key)

	var value int

	if key_exists {
		stored_value, is_a_number := resp.ToInt(value_raw)

		if !is_a_number {
			return resp.SimpleError("ERR value is not an integer or out of range")
		}

		value = stored_value + 1
	} else {
		value = 1
	}

	str := resp.BulkString(strconv.Itoa(value))
	store.Set(key, str)

	return resp.Integer(value)
}

func handleMultiCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	conn.Mu.Lock()
	conn.Multi = true
	conn.Mu.Unlock()
	return resp.SimpleString("OK")
}

func handleExecCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	conn.Mu.Lock()
	if !conn.Multi {
		conn.Mu.Unlock()
		return resp.SimpleError("ERR EXEC without MULTI")
	}
	conn.Multi = false
	conn.Mu.Unlock()

	res := resp.Array{}
	for _, sub_call := range conn.Queued {
		sub := acceptCommand(sub_call, conn, store)
		res = append(res, sub)
	}

	return res
}

func handleDiscardCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	conn.Mu.Lock()
	defer conn.Mu.Unlock()
	if !conn.Multi {
		return resp.SimpleError("ERR DISCARD without MULTI")
	}
	conn.Multi = false
	conn.Queued = make([]resp.Object, 0)
	return resp.SimpleString("OK")
}

func blockStreamsRead(keys []string, streams []*resp.Stream, ids []string, timer <-chan time.Time) resp.Object {
	for {
		select {
		case <-timer:
			return resp.NullBulkString{}
		default:
			reads := resp.Array{}
			for i := 0; i < len(streams); i++ {
				key := keys[i]
				stream := streams[i]
				id := ids[i]

				entries := readStreamEntries(stream, id)

				if len(entries) == 0 {
					continue
				}

				stream_read := resp.Array{}
				stream_read = append(stream_read, resp.BulkString(key))
				stream_read = append(stream_read, entries)

				reads = append(reads, stream_read)
			}

			if len(reads) != 0 {
				return reads
			}
		}
	}
}

func readFromStreams(keys []string, streams []*resp.Stream, ids []string) resp.Array {
	reads := resp.Array{}

	for i := 0; i < len(streams); i++ {
		key := keys[i]
		stream := streams[i]
		id := ids[i]

		read := readFromStream(key, stream, id)

		reads = append(reads, read)
	}

	return reads
}

func readFromStream(key string, stream *resp.Stream, id string) resp.Array {
	entries := readStreamEntries(stream, id)

	stream_read := resp.Array{}
	stream_read = append(stream_read, resp.BulkString(key))
	stream_read = append(stream_read, entries)

	return stream_read
}

func readStreamEntries(stream *resp.Stream, id string) resp.Array {
	stream.Mu.Lock()
	defer stream.Mu.Unlock()

	from_index := streamLowerBound(stream, id)
	if from_index < len(stream.Entries) && stream.Entries[from_index].Id == id {
		from_index++
	}

	entries := resp.Array{}
	for j := from_index; j < len(stream.Entries); j++ {
		entry := resp.Array{}
		entry = append(entry, resp.BulkString(stream.Entries[j].Id))

		data := resp.Array{}
		for k, v := range stream.Entries[j].Data {
			data = append(data, resp.BulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		entries = append(entries, entry)
	}

	return entries
}

func streamLowerBound(stream *resp.Stream, id string) int {
	low := -1
	high := len(stream.Entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.Entries[mid].Id) != 1 {
			high = mid
		} else {
			low = mid
		}
	}
	return high
}

func streamUpperBound(stream *resp.Stream, id string) int {
	low := -1
	high := len(stream.Entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.Entries[mid].Id) != -1 {
			low = mid
		} else {
			high = mid
		}
	}
	return low
}

func processStreamID(stream *resp.Stream, id string) (string, error) {

	if id == "*" {
		current_timestamp := time.Now().UnixMilli()
		return strconv.Itoa(int(current_timestamp)) + "-0", nil
	}

	top_id := "0-0"
	if len(stream.Entries) != 0 {
		top_id = stream.Entries[len(stream.Entries)-1].Id
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

func sendCurrentState(conn *core.Conn) {
	data := rdb.GenerateFile(nil)
	res := resp.BulkString(data).Encode()
	res = res[:len(res)-2]
	conn.Write(res)
}

func propagateToReplicas(call resp.Array, store *core.Store) {
	res := call.Encode()
	for _, conn := range store.Replicas {
		fmt.Printf("Propagating %v to replica %v\n", call, conn.Conn.LocalAddr())
		conn.Write(res)
		conn.Mu.Lock()
		conn.Total_propagated += len(res)
		conn.Expected_offset = conn.Total_propagated
		fmt.Printf("sent %d bytes to replica %v: %s\n", len(res), conn.Conn.RemoteAddr(), strconv.Quote(string(res)))
		fmt.Printf("total_propagated = %d, offset = %d\n", conn.Total_propagated, conn.Offset)
		conn.Mu.Unlock()
	}
}

func sendAcksToReplica(conn *core.Conn) {
	defer conn.Ticker.Stop()
	for {
		select {
		case <-conn.Ticker.C:
			sendAckToReplica(conn)
		case <-conn.StopChan:
			return
		}
	}
}

func sendAckToReplica(conn *core.Conn) {
	conn.Mu.Lock()
	if conn.Offset == conn.Expected_offset {
		conn.Mu.Unlock()
		return
	}
	conn.Expected_offset = conn.Total_propagated
	res := generateCommand("REPLCONF", "GETACK", "*").Encode()
	conn.Write(res)
	conn.Total_propagated += len(res)
	fmt.Printf("Propagated %d bytes to replica %v: %v\n", len(res), conn.Conn.RemoteAddr(), strconv.Quote(string(res)))
	conn.Mu.Unlock()
}