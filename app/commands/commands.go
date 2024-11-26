package commands

import (
	"fmt"
	"os"
	"strings"

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

func sendCurrentState(conn *core.Conn) {
	data := rdb.GenerateFile(nil)
	res := resp.BulkString(data).Encode()
	res = res[:len(res)-2]
	conn.Write(res)
}

func Generate(strs ...string) resp.Array {
	arr := resp.Array(make([]resp.Object, len(strs)))
	for i := 0; i < len(arr); i++ {
		bulk_str := resp.BulkString(strs[i])
		arr[i] = &bulk_str
	}
	return arr
}
