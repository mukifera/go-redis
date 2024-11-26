package core

import (
	"net"
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
