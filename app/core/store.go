package core

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Store struct {
	dict     map[string]resp.Object
	expiry   map[interface{}]int64
	params   map[string]string
	Replicas []*Conn
	Master   *Conn
	mu       sync.Mutex
}

func (s *Store) Init() {
	s.dict = make(map[string]resp.Object)
	s.expiry = make(map[interface{}]int64)
	s.params = make(map[string]string)
	s.Replicas = make([]*Conn, 0)
}

func (s *Store) Set(key string, value resp.Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
}

func (s *Store) SetWithExpiry(key string, value resp.Object, expiry uint64) {
	s.SetWithAbsoluteExpiry(key, value, uint64(time.Now().Add(time.Duration(expiry)*time.Millisecond).UnixMilli()))
}

func (s *Store) SetWithAbsoluteExpiry(key string, value resp.Object, expiry uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
	s.expiry[key] = int64(expiry)
}

func (s *Store) Get(key string) (resp.Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, in_dict := s.dict[key]
	expiry, in_expiry := s.expiry[key]
	if in_dict && in_expiry && time.Now().UnixMilli() > expiry {
		delete(s.dict, key)
		delete(s.expiry, key)
		return nil, false
	}
	return value, in_dict
}

func (s *Store) SetParam(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params[key] = value
}

func (s *Store) GetParam(key string) (string, bool) {
	value, ok := s.params[key]
	return value, ok
}

func (s *Store) GetKeys(_ string) []string {
	keys := make([]string, len(s.dict))
	i := 0
	for key := range s.dict {
		keys[i] = key
		i++
	}
	return keys
}

func (s *Store) AddReplica(conn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Replicas = append(s.Replicas, conn)
	conn.Relation = ConnRelationTypeEnum.REPLICA
}

func (s *Store) TypeOfValue(key string) string {
	value, ok := s.Get(key)
	if !ok {
		return "none"
	}
	switch value.(type) {
	case resp.SimpleString, resp.BulkString:
		return "string"
	case resp.Stream, *resp.Stream:
		return "stream"
	default:
		return "unknown"
	}
}

func (store *Store) PropagateToReplicas(call resp.Array) {
	res := call.Encode()
	for _, conn := range store.Replicas {
		fmt.Printf("Propagating %v to replica %v\n", call, conn.Conn.LocalAddr())
		conn.Write(res)
		conn.Mu.Lock()
		conn.Total_propagated += len(res)
		conn.Expected_offset = conn.Total_propagated
		fmt.Printf("sent %d bytes to replica %v: %s\n", len(res), conn.Conn.RemoteAddr(), strconv.Quote(string(res)))
		fmt.Printf("total_propagated = %d, offset = %d\n", conn.Total_propagated, conn.Offset)
		conn.Mu.Unlock()
	}
}
