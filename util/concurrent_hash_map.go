package util

import (
	"hash/fnv"
	"sync"
)

type ConcurrentHashMap struct {
	shards []*SharedMap
}

type SharedMap struct {
	items map[string]interface{}
	mu    sync.RWMutex
}

func NewConcurrentHashMap(shardCount int, capacity int) *ConcurrentHashMap {
	if shardCount <= 0 {
		shardCount = 32
	}
	m := &ConcurrentHashMap{
		shards: make([]*SharedMap, shardCount),
	}
	shardCap := capacity / shardCount
	for i := 0; i < shardCount; i++ {
		m.shards[i] = &SharedMap{
			items: make(map[string]interface{}, shardCap),
		}
	}
	return m
}

func (m *ConcurrentHashMap) getShardIndex(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(m.shards)))
}

func (m *ConcurrentHashMap) Get(key string) (interface{}, bool) {
	shard := m.shards[m.getShardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, ok := shard.items[key]
	return val, ok
}

func (m *ConcurrentHashMap) Set(key string, value interface{}) {
	shard := m.shards[m.getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.items[key] = value
}

func (m *ConcurrentHashMap) Remove(key string) {
	shard := m.shards[m.getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.items, key)
}

func (m *ConcurrentHashMap) Count() int {
	count := 0
	for _, shard := range m.shards {
		shard.mu.RLock()
		count += len(shard.items)
		shard.mu.RUnlock()
	}
	return count
}
