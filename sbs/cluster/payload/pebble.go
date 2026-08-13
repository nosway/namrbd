package payload

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/store"
)

type PebbleStore struct {
	db *pebble.DB
}

func OpenPebbleStore(path string) (*PebbleStore, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{db: db}, nil
}

func (s *PebbleStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PebbleStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	raw, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

func (s *PebbleStore) Put(_ context.Context, key string, value []byte) error {
	return s.db.Set([]byte(key), append([]byte(nil), value...), pebble.Sync)
}

func (s *PebbleStore) Delete(_ context.Context, key string) error {
	return s.db.Delete([]byte(key), pebble.Sync)
}

func (s *PebbleStore) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()

	keys := make([]string, 0)
	nextCursor := ""
	started := cursor == ""
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if !started {
			if key <= cursor {
				continue
			}
			started = true
		}
		keys = append(keys, key)
		if limit > 0 && len(keys) >= limit {
			nextCursor = key
			break
		}
	}
	if limit > 0 && len(keys) < limit {
		nextCursor = ""
	}
	return keys, nextCursor, nil
}

var _ store.ObjectStore = (*PebbleStore)(nil)

type ReplicaStores struct {
	mu     sync.RWMutex
	stores map[string]*PebbleStore
}

func OpenReplicaStores(root string, replicaIDs []string) (*ReplicaStores, error) {
	if root == "" {
		return nil, fmt.Errorf("root is required")
	}
	uniqueIDs := append([]string(nil), replicaIDs...)
	sort.Strings(uniqueIDs)
	storeSet := &ReplicaStores{
		stores: make(map[string]*PebbleStore, len(uniqueIDs)),
	}
	for _, replicaID := range uniqueIDs {
		if replicaID == "" {
			storeSet.Close()
			return nil, fmt.Errorf("replica id is required")
		}
		payloadStore, err := OpenPebbleStore(filepath.Join(root, "replicas", replicaID))
		if err != nil {
			storeSet.Close()
			return nil, err
		}
		storeSet.stores[replicaID] = payloadStore
	}
	return storeSet, nil
}

func (s *ReplicaStores) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for replicaID, payloadStore := range s.stores {
		if err := payloadStore.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close replica %q: %w", replicaID, err)
		}
	}
	s.stores = map[string]*PebbleStore{}
	return firstErr
}

func (s *ReplicaStores) ObjectStores() map[string]store.ObjectStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]store.ObjectStore, len(s.stores))
	for replicaID, payloadStore := range s.stores {
		out[replicaID] = payloadStore
	}
	return out
}
