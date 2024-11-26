package core

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type connRelationType byte

var ConnRelationTypeEnum = struct {
	NORMAL  connRelationType
	MASTER  connRelationType
	REPLICA connRelationType
}{
	NORMAL:  0,
	MASTER:  1,
	REPLICA: 2,
}

type Conn struct {
	Conn             net.Conn
	ByteChan         chan byte
	StopChan         chan bool
	Ticker           *time.Ticker
	Offset           int
	Expected_offset  int
	Total_propagated int
	Multi            bool
	Queued           []resp.Object
	Relation         connRelationType
	Mu               sync.Mutex
}

func NewConn(conn net.Conn, relation_type connRelationType) *Conn {
	return &Conn{
		Conn:             conn,
		ByteChan:         make(chan byte, 1<<14),
		StopChan:         make(chan bool),
		Ticker:           nil,
		Offset:           0,
		Expected_offset:  0,
		Total_propagated: 0,
		Multi:            false,
		Queued:           make([]resp.Object, 0),
		Relation:         relation_type,
		Mu:               sync.Mutex{},
	}
}

func (conn *Conn) Write(data []byte) {
	current := 0
	for current < len(data) {
		n, err := conn.Conn.Write(data[current:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write to connection: %v\n", err)
			return
		}
		current += n
	}
	if conn.Relation != ConnRelationTypeEnum.REPLICA {
		fmt.Printf("sent %d bytes to %v: %s\n", len(data), conn.Conn.RemoteAddr(), strconv.Quote(string(data)))
	}
}
