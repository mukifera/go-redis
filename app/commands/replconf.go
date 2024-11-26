package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

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
		res = Generate("REPLCONF", "ACK", strconv.Itoa(conn.Offset))
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
