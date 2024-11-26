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
