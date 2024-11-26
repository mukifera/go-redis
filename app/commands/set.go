package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

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
