package commands

import (
	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

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
