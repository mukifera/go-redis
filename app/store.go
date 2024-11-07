package main

import (
	"sync"
	"time"
)

type redisStore struct {
	dict   map[interface{}]interface{}
	expiry map[interface{}]int64
	params map[string]string
	mu     sync.Mutex
}

func (s *redisStore) init() {
	s.dict = make(map[interface{}]interface{})
	s.expiry = make(map[interface{}]int64)
	s.params = make(map[string]string)
}

func (s *redisStore) set(key interface{}, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
}

func (s *redisStore) setWithExpiry(key interface{}, value interface{}, expiry uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
	s.expiry[key] = time.Now().Add(time.Duration(expiry) * time.Millisecond).UnixMilli()
}

func (s *redisStore) get(key interface{}) (interface{}, bool) {
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
