package payload

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/payload/compress"
)

type PebbleStore struct {
	db *pebble.DB

	compressionMu sync.RWMutex
	codec         compress.Codec
	policy        func(string) bool
	decodeLegacy  bool
}

func OpenPebbleStore(path string) (*PebbleStore, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{
		db:     db,
		codec:  compress.CodecNone,
		policy: defaultCompressionKeyPolicy,
	}, nil
}

func (s *PebbleStore) SetCompressionCodec(codec compress.Codec) error {
	if !codec.Valid() {
		return fmt.Errorf("invalid compression codec %d", codec)
	}
	if s == nil {
		return nil
	}
	s.compressionMu.Lock()
	defer s.compressionMu.Unlock()
	s.codec = codec
	return nil
}

func (s *PebbleStore) CompressionCodec() compress.Codec {
	if s == nil {
		return compress.CodecNone
	}
	s.compressionMu.RLock()
	defer s.compressionMu.RUnlock()
	return s.codec
}

func (s *PebbleStore) SetCompressionKeyPolicy(policy func(string) bool) {
	if s == nil {
		return
	}
	if policy == nil {
		policy = defaultCompressionKeyPolicy
	}
	s.compressionMu.Lock()
	defer s.compressionMu.Unlock()
	s.policy = policy
}

// SetLegacyCompressionDecoding is an explicit migration switch. Legacy v1
// used a two-byte marker and cannot be distinguished safely from arbitrary
// values beginning with "NC", so it is never auto-detected by default.
func (s *PebbleStore) SetLegacyCompressionDecoding(enabled bool) {
	if s == nil {
		return
	}
	s.compressionMu.Lock()
	defer s.compressionMu.Unlock()
	s.decodeLegacy = enabled
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

	_, policy, decodeLegacy := s.compressionConfig()
	if policy(key) && compress.IsEnvelope(raw) {
		decompressed, _, err := compress.DecompressPayload(raw)
		if err != nil {
			return nil, false, fmt.Errorf("transparent decompress error for key %q: %w", key, err)
		}
		return decompressed, true, nil
	}
	if policy(key) && decodeLegacy && compress.IsLegacyEnvelope(raw) {
		decompressed, _, err := compress.DecompressPayload(raw)
		if err != nil {
			return nil, false, fmt.Errorf("transparent legacy decompress error for key %q: %w", key, err)
		}
		return decompressed, true, nil
	}

	return append([]byte(nil), raw...), true, nil
}

func (s *PebbleStore) Put(_ context.Context, key string, value []byte) error {
	valToStore := value
	codec, policy, decodeLegacy := s.compressionConfig()
	if codec != compress.CodecNone && policy(key) {
		switch {
		case compress.IsEnvelope(value):
			if _, _, err := compress.DecompressPayload(value); err != nil {
				return fmt.Errorf("validate pre-encoded payload for key %q: %w", key, err)
			}
		case decodeLegacy && compress.IsLegacyEnvelope(value):
			if _, _, err := compress.DecompressPayload(value); err != nil {
				return fmt.Errorf("validate legacy pre-encoded payload for key %q: %w", key, err)
			}
		default:
			compressed, _, err := compress.CompressPayload(value, codec)
			if err != nil {
				return fmt.Errorf("compress payload for key %q: %w", key, err)
			}
			valToStore = compressed
		}
	}
	return s.db.Set([]byte(key), append([]byte(nil), valToStore...), pebble.Sync)
}

func (s *PebbleStore) compressionConfig() (compress.Codec, func(string) bool, bool) {
	if s == nil {
		return compress.CodecNone, defaultCompressionKeyPolicy, false
	}
	s.compressionMu.RLock()
	defer s.compressionMu.RUnlock()
	policy := s.policy
	if policy == nil {
		policy = defaultCompressionKeyPolicy
	}
	return s.codec, policy, s.decodeLegacy
}

func defaultCompressionKeyPolicy(key string) bool {
	return strings.Contains(key, ":chk:") ||
		(strings.HasPrefix(key, "replicas/") && strings.Contains(key, "/chunks/"))
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
