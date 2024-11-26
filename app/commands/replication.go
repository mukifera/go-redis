package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

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
