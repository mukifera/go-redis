package main

import "sync"

type redisStore struct {
	dict map[interface{}]interface{}
	mu   sync.Mutex
}

func (s *redisStore) set(key interface{}, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dict[key] = value
}

func (s *redisStore) get(key interface{}) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.dict[key]
	return value, ok
}
