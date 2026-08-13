package metadata

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

type PebbleKV struct {
	db *pebble.DB
	mu sync.Mutex
}

func OpenPebbleKV(path string) (*PebbleKV, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleKV{db: db}, nil
}

func (kv *PebbleKV) Close() error {
	if kv == nil || kv.db == nil {
		return nil
	}
	return kv.db.Close()
}

func (kv *PebbleKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	raw, closer, err := kv.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

func (kv *PebbleKV) Set(_ context.Context, key string, value []byte) error {
	return kv.db.Set([]byte(key), append([]byte(nil), value...), pebble.Sync)
}

func (kv *PebbleKV) Delete(_ context.Context, key string) error {
	return kv.db.Delete([]byte(key), pebble.Sync)
}

func (kv *PebbleKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	batch := kv.db.NewBatch()
	defer batch.Close()

	tx := &pebbleTxn{
		db:     kv.db,
		batch:  batch,
		writes: make(map[string][]byte),
	}
	if err := fn(tx); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (kv *PebbleKV) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	iter, err := kv.db.NewIter(&pebble.IterOptions{
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

type pebbleTxn struct {
	db     *pebble.DB
	batch  *pebble.Batch
	writes map[string][]byte
}

func (tx *pebbleTxn) Get(_ context.Context, key string) ([]byte, bool, error) {
	if value, ok := tx.writes[key]; ok {
		return append([]byte(nil), value...), true, nil
	}
	raw, closer, err := tx.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

func (tx *pebbleTxn) Set(_ context.Context, key string, value []byte) error {
	buf := append([]byte(nil), value...)
	tx.writes[key] = buf
	return tx.batch.Set([]byte(key), buf, nil)
}

func (tx *pebbleTxn) Delete(_ context.Context, key string) error {
	delete(tx.writes, key)
	return tx.batch.Delete([]byte(key), nil)
}
