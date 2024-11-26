package commands

import (
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
