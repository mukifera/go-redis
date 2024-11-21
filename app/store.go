package main

import (
	"net"
	"sync"
	"time"
)

type connRelationType byte

var connRelationTypeEnum = struct {
	NORMAL  connRelationType
	MASTER  connRelationType
	REPLICA connRelationType
}{
	NORMAL:  0,
	MASTER:  1,
	REPLICA: 2,
}

type redisConn struct {
	conn             net.Conn
	byteChan         chan byte
	stopChan         chan bool
	ticker           *time.Ticker
	offset           int
	expected_offset  int
	total_propagated int
	relation         connRelationType
	mu               sync.Mutex
}

type redisStore struct {
	dict     map[string]respObject
	expiry   map[interface{}]int64
	params   map[string]string
	replicas []*redisConn
	master   *redisConn
	mu       sync.Mutex
}

func newRedisConn(conn net.Conn, relation_type connRelationType) *redisConn {
	return &redisConn{
		conn:             conn,
		byteChan:         make(chan byte, 1<<14),
		stopChan:         make(chan bool),
		ticker:           nil,
		offset:           0,
		expected_offset:  0,
		total_propagated: 0,
		relation:         relation_type,
		mu:               sync.Mutex{},
	}
}

func (s *redisStore) init() {
	s.dict = make(map[string]respObject)
	s.expiry = make(map[interface{}]int64)
	s.params = make(map[string]string)
	s.replicas = make([]*redisConn, 0)
}

func (s *redisStore) set(key string, value respObject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
}

func (s *redisStore) setWithExpiry(key string, value respObject, expiry uint64) {
	s.setWithAbsoluteExpiry(key, value, uint64(time.Now().Add(time.Duration(expiry)*time.Millisecond).UnixMilli()))
}

func (s *redisStore) setWithAbsoluteExpiry(key string, value respObject, expiry uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
	s.expiry[key] = int64(expiry)
}

func (s *redisStore) get(key string) (respObject, bool) {
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

func (s *redisStore) setParam(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.params[key] = value
}

func (s *redisStore) getParam(key string) (string, bool) {
	value, ok := s.params[key]
	return value, ok
}

func (s *redisStore) getKeys(_ string) []string {
	keys := make([]string, len(s.dict))
	i := 0
	for key := range s.dict {
		keys[i] = key
		i++
	}
	return keys
}

func (s *redisStore) addReplica(conn *redisConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas = append(s.replicas, conn)
	conn.relation = connRelationTypeEnum.REPLICA
}

func (s *redisStore) typeOfValue(key string) string {
	value, ok := s.get(key)
	if !ok {
		return "none"
	}
	switch value.(type) {
	case respSimpleString, respBulkString:
		return "string"
	default:
		return "unknown"
	}
}
