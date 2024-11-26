package commands

import (
	"fmt"
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

func GetCommandName(call resp.Array) (string, bool) {
	command, ok := resp.ToString(call[0])
	if !ok {
		return "", false
	}

	return strings.ToUpper(command), true
}

func GetRespArrayCall(obj resp.Object) resp.Array {
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

func HandleCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {

	command, ok := GetCommandName(call)
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
		command := GetRespArrayCall(sub_call)
		sub := HandleCommand(command, conn, store)
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

func sendCurrentState(conn *core.Conn) {
	data := rdb.GenerateFile(nil)
	res := resp.BulkString(data).Encode()
	res = res[:len(res)-2]
	conn.Write(res)
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
	res := Generate("REPLCONF", "GETACK", "*").Encode()
	conn.Write(res)
	conn.Total_propagated += len(res)
	fmt.Printf("Propagated %d bytes to replica %v: %v\n", len(res), conn.Conn.RemoteAddr(), strconv.Quote(string(res)))
	conn.Mu.Unlock()
}

func Generate(strs ...string) resp.Array {
	arr := resp.Array(make([]resp.Object, len(strs)))
	for i := 0; i < len(arr); i++ {
		bulk_str := resp.BulkString(strs[i])
		arr[i] = &bulk_str
	}
	return arr
}
