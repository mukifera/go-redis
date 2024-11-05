package main

import (
	"sync"
	"time"
)

type redisStore struct {
	dict   map[interface{}]interface{}
	expiry map[interface{}]int64
	mu     sync.Mutex
}

func (s *redisStore) init() {
	s.dict = make(map[interface{}]interface{})
	s.expiry = make(map[interface{}]int64)
}

func (s *redisStore) set(key interface{}, value interface{}, expiry int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
	if expiry != -1 {
		s.expiry[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond).UnixMilli()
	} else {
		s.expiry[key] = -1
	}
}

func (s *redisStore) get(key interface{}) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.dict[key]
	if ok && s.expiry[key] != -1 && time.Now().UnixMilli() > s.expiry[key] {
		delete(s.dict, key)
		delete(s.expiry, key)
		return nil, false
	}
	return value, ok
}
