package store

import (
	"context"
	"sort"
	"sync"
)

type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (m *MemoryStore) Set(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := make([]byte, len(value))
	copy(v, value)
	m.data[key] = v
	return nil
}

func (m *MemoryStore) Put(ctx context.Context, key string, value []byte) error {
	return m.Set(ctx, key, value)
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryStore) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		if len(prefix) == 0 || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	start := 0
	if cursor != "" {
		for i, key := range keys {
			if key > cursor {
				start = i
				break
			}
			if i == len(keys)-1 {
				start = len(keys)
			}
		}
	}
	if limit <= 0 || start+limit >= len(keys) {
		return keys[start:], "", nil
	}
	return keys[start : start+limit], keys[start+limit-1], nil
}
