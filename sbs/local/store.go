package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/store"
)

type objectStore struct {
	metadataPath string
	legacyDB     *pebble.DB
	shards       map[string]*pebble.DB
	shardPaths   map[string]string
	closers      []*pebble.DB
}

type shardSnapshot struct {
	StoreID                   string `json:"store_id"`
	ShardID                   uint32 `json:"shard_id"`
	Path                      string `json:"path"`
	ChunkKeys                 int    `json:"chunk_keys"`
	PebbleFiles               int    `json:"pebble_files"`
	PebbleDiskUsageBytes      uint64 `json:"pebble_disk_usage_bytes"`
	CompactionPendingBytes    uint64 `json:"compaction_pending_bytes"`
	CompactionInProgressBytes uint64 `json:"compaction_in_progress_bytes"`
}

type storeRuntimeSnapshot struct {
	CapacityBytes             uint64
	AvailableBytes            uint64
	UsedBytes                 uint64
	PebbleDiskUsageBytes      uint64
	CompactionPendingBytes    uint64
	CompactionInProgressBytes uint64
}

func newObjectStore(metadataPath string, legacyDB *pebble.DB, stores []StoreSpec) (*objectStore, error) {
	out := &objectStore{
		metadataPath: filepath.Clean(metadataPath),
		legacyDB:     legacyDB,
		shards:       make(map[string]*pebble.DB),
		shardPaths:   make(map[string]string),
	}
	for _, spec := range stores {
		for shardID := 0; shardID < spec.Shards; shardID++ {
			path := shardPath(spec.Path, shardID)
			key := shardKey(spec.ID, uint32(shardID))
			if filepath.Clean(spec.Path) == out.metadataPath && spec.ID == DefaultStoreID && spec.Shards == 1 && shardID == 0 {
				out.shardPaths[key] = out.metadataPath
				out.shards[key] = legacyDB
				continue
			}
			out.shardPaths[key] = path
			if err := os.MkdirAll(path, 0o755); err != nil {
				out.Close()
				return nil, err
			}
			db, err := pebble.Open(path, &pebble.Options{})
			if err != nil {
				out.Close()
				return nil, err
			}
			out.shards[key] = db
			out.closers = append(out.closers, db)
		}
	}
	return out, nil
}

func (s *objectStore) Close() error {
	var firstErr error
	for _, db := range s.closers {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.closers = nil
	return firstErr
}

func (s *objectStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	target, internalKey, err := s.routeKey(key)
	if err != nil {
		return nil, false, err
	}
	raw, closer, err := target.Get([]byte(internalKey))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

func (s *objectStore) Put(_ context.Context, key string, value []byte) error {
	target, internalKey, err := s.routeKey(key)
	if err != nil {
		return err
	}
	return target.Set([]byte(internalKey), value, pebble.Sync)
}

func (s *objectStore) Delete(_ context.Context, key string) error {
	target, internalKey, err := s.routeKey(key)
	if err != nil {
		return err
	}
	return target.Delete([]byte(internalKey), pebble.Sync)
}

func (s *objectStore) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	target, internalPrefix, internalCursor, restore, err := s.routePrefix(prefix, cursor)
	if err != nil {
		return nil, "", err
	}
	iter, err := target.NewIter(&pebble.IterOptions{
		LowerBound: []byte(internalPrefix),
		UpperBound: []byte(internalPrefix + "\xff"),
	})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()

	var out []string
	nextCursor := ""
	started := internalCursor == ""
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if !started {
			if key <= internalCursor {
				continue
			}
			started = true
		}
		publicKey := restore(key)
		out = append(out, publicKey)
		if limit > 0 && len(out) >= limit {
			nextCursor = publicKey
			break
		}
	}
	if len(out) < limit {
		nextCursor = ""
	}
	return out, nextCursor, nil
}

func (s *objectStore) ShardSnapshots() ([]shardSnapshot, error) {
	snapshots := make([]shardSnapshot, 0, len(s.shardPaths))
	keys := make([]string, 0, len(s.shardPaths))
	for key := range s.shardPaths {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		storeID, shardID, err := parseShardKey(key)
		if err != nil {
			return nil, err
		}
		path := s.shardPaths[key]
		chunkKeys, err := countDBChunkKeys(s.shards[key])
		if err != nil {
			return nil, err
		}
		pebbleFiles, err := countPebbleFiles(path)
		if err != nil {
			return nil, err
		}
		pebbleDiskUsageBytes, compactionPendingBytes, compactionInProgressBytes := pebbleDBRuntimeStats(s.shards[key])
		snapshots = append(snapshots, shardSnapshot{
			StoreID:                   storeID,
			ShardID:                   shardID,
			Path:                      path,
			ChunkKeys:                 chunkKeys,
			PebbleFiles:               pebbleFiles,
			PebbleDiskUsageBytes:      pebbleDiskUsageBytes,
			CompactionPendingBytes:    compactionPendingBytes,
			CompactionInProgressBytes: compactionInProgressBytes,
		})
	}
	return snapshots, nil
}

func (s *objectStore) StoreRuntimeSnapshots(stores []StoreSpec) (map[string]storeRuntimeSnapshot, error) {
	out := make(map[string]storeRuntimeSnapshot, len(stores))
	for _, spec := range stores {
		capacityBytes, availableBytes, err := filesystemCapacity(spec.Path)
		if err != nil {
			return nil, err
		}
		snapshot := storeRuntimeSnapshot{
			CapacityBytes:  capacityBytes,
			AvailableBytes: availableBytes,
		}
		for shardID := 0; shardID < spec.Shards; shardID++ {
			key := shardKey(spec.ID, uint32(shardID))
			db := s.shards[key]
			if db == nil {
				continue
			}
			usage, pending, inProgress := pebbleDBRuntimeStats(db)
			snapshot.PebbleDiskUsageBytes += usage
			snapshot.UsedBytes += usage
			snapshot.CompactionPendingBytes += pending
			snapshot.CompactionInProgressBytes += inProgress
		}
		out[spec.ID] = snapshot
	}
	return out, nil
}

func (s *objectStore) routeKey(key string) (*pebble.DB, string, error) {
	route, err := parseObjectKey(key)
	if err != nil {
		return nil, "", err
	}
	if !route.isChunk && !route.isObject {
		return s.legacyDB, key, nil
	}
	db, err := s.dbForRoute(route)
	if err != nil {
		return nil, "", err
	}
	if route.isObject {
		return db, remapObjectKey(route.prefix, route.objectSuffix), nil
	}
	return db, remapChunkKey(route.prefix, route.chunkSuffix), nil
}

func (s *objectStore) routePrefix(prefix, cursor string) (*pebble.DB, string, string, func(string) string, error) {
	route, err := parseObjectKey(prefix)
	if err != nil {
		return nil, "", "", nil, err
	}
	if !route.isChunk && !route.isObject {
		return s.legacyDB, prefix, cursor, func(key string) string { return key }, nil
	}
	db, err := s.dbForRoute(route)
	if err != nil {
		return nil, "", "", nil, err
	}
	if route.isObject {
		internalPrefix := remapObjectKey(route.prefix, route.objectSuffix)
		internalCursor := ""
		if cursor != "" {
			cursorRoute, err := parseObjectKey(cursor)
			if err != nil {
				return nil, "", "", nil, err
			}
			internalCursor = remapObjectKey(cursorRoute.prefix, cursorRoute.objectSuffix)
		}
		return db, internalPrefix, internalCursor, func(key string) string {
			return restoreObjectKey(route.prefix, route.storeID, route.shardID, key)
		}, nil
	}
	internalPrefix := remapChunkKey(route.prefix, route.chunkSuffix)
	internalCursor := ""
	if cursor != "" {
		cursorRoute, err := parseObjectKey(cursor)
		if err != nil {
			return nil, "", "", nil, err
		}
		internalCursor = remapChunkKey(cursorRoute.prefix, cursorRoute.chunkSuffix)
	}
	return db, internalPrefix, internalCursor, func(key string) string {
		return restoreChunkKey(route.prefix, route.storeID, route.shardID, key)
	}, nil
}

func (s *objectStore) dbForRoute(route parsedObjectKey) (*pebble.DB, error) {
	if route.storeID == "" {
		return s.legacyDB, nil
	}
	key := shardKey(route.storeID, route.shardID)
	db, ok := s.shards[key]
	if !ok {
		if route.storeID == DefaultStoreID && route.shardID == 0 && len(s.shards) == 1 {
			for _, sole := range s.shards {
				return sole, nil
			}
		}
		return nil, fmt.Errorf("unknown store/shard route %s", key)
	}
	return db, nil
}

type parsedObjectKey struct {
	prefix       string
	storeID      string
	shardID      uint32
	chunkSuffix  string
	objectSuffix string
	isChunk      bool
	isObject     bool
}

func parseObjectKey(key string) (parsedObjectKey, error) {
	if idx := strings.Index(key, ":store:"); idx >= 0 {
		prefix := key[:idx]
		rest := key[idx+len(":store:"):]
		storeID, rest, ok := strings.Cut(rest, ":shard:")
		if !ok {
			return parsedObjectKey{}, fmt.Errorf("invalid store-qualified key %q", key)
		}
		shardRaw, objectRaw, objectOK := strings.Cut(rest, ":obj:")
		shardID64, err := strconv.ParseUint(strings.TrimSpace(shardRaw), 10, 32)
		if objectOK {
			if err != nil {
				return parsedObjectKey{}, fmt.Errorf("invalid shard id in key %q", key)
			}
			return parsedObjectKey{
				prefix:       prefix,
				storeID:      strings.TrimSpace(storeID),
				shardID:      uint32(shardID64),
				objectSuffix: objectRaw,
				isObject:     true,
			}, nil
		}
		shardRaw, chunkRaw, ok := strings.Cut(rest, ":chk:")
		if !ok {
			return parsedObjectKey{}, fmt.Errorf("invalid store-qualified key %q", key)
		}
		shardID64, err = strconv.ParseUint(strings.TrimSpace(shardRaw), 10, 32)
		if err != nil {
			return parsedObjectKey{}, fmt.Errorf("invalid shard id in key %q", key)
		}
		return parsedObjectKey{
			prefix:      prefix,
			storeID:     strings.TrimSpace(storeID),
			shardID:     uint32(shardID64),
			chunkSuffix: chunkRaw,
			isChunk:     true,
		}, nil
	}
	if strings.Contains(key, ":obj:") {
		parts := strings.SplitN(key, ":obj:", 2)
		return parsedObjectKey{
			prefix:       parts[0],
			objectSuffix: parts[1],
			isObject:     true,
		}, nil
	}
	if strings.Contains(key, ":chk:") {
		parts := strings.SplitN(key, ":chk:", 2)
		return parsedObjectKey{
			prefix:      parts[0],
			chunkSuffix: parts[1],
			isChunk:     true,
		}, nil
	}
	return parsedObjectKey{}, nil
}

func remapChunkKey(prefix, chunkSuffix string) string {
	return "volumes/" + prefix + "/chunks/" + chunkSuffix
}

func remapObjectKey(prefix, objectSuffix string) string {
	return "volumes/" + prefix + "/objects/" + objectSuffix
}

func restoreChunkKey(prefix, storeID string, shardID uint32, internalKey string) string {
	const marker = "/chunks/"
	if strings.HasPrefix(internalKey, "volumes/") && strings.Contains(internalKey, marker) {
		trim := strings.TrimPrefix(internalKey, "volumes/")
		parts := strings.SplitN(trim, marker, 2)
		if len(parts) == 2 {
			if storeID == "" {
				return store.BuildChunkKey(prefix, mustParseUint(parts[1]))
			}
			return prefix + ":store:" + storeID + ":shard:" + strconv.FormatUint(uint64(shardID), 10) + ":chk:" + parts[1]
		}
	}
	return internalKey
}

func restoreObjectKey(prefix, storeID string, shardID uint32, internalKey string) string {
	const marker = "/objects/"
	if strings.HasPrefix(internalKey, "volumes/") && strings.Contains(internalKey, marker) {
		trim := strings.TrimPrefix(internalKey, "volumes/")
		parts := strings.SplitN(trim, marker, 2)
		if len(parts) == 2 {
			if storeID == "" {
				return prefix + ":obj:" + parts[1]
			}
			return prefix + ":store:" + storeID + ":shard:" + strconv.FormatUint(uint64(shardID), 10) + ":obj:" + parts[1]
		}
	}
	return internalKey
}

func shardPath(path string, shardID int) string {
	return filepath.Join(path, fmt.Sprintf("shard-%04d", shardID))
}

func shardKey(storeID string, shardID uint32) string {
	return storeID + "/" + strconv.FormatUint(uint64(shardID), 10)
}

func parseShardKey(key string) (string, uint32, error) {
	storeID, shardRaw, ok := strings.Cut(key, "/")
	if !ok {
		return "", 0, fmt.Errorf("invalid shard key %q", key)
	}
	shardID, err := strconv.ParseUint(shardRaw, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("invalid shard key %q", key)
	}
	return storeID, uint32(shardID), nil
}

func countDBChunkKeys(db *pebble.DB) (int, error) {
	const prefix = "volumes/"
	const marker = "/chunks/"
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if !strings.Contains(string(iter.Key()), marker) {
			continue
		}
		count++
	}
	return count, nil
}

func countPebbleFiles(path string) (int, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		count++
	}
	return count, nil
}

func pebbleDBRuntimeStats(db *pebble.DB) (diskUsageBytes, compactionPendingBytes, compactionInProgressBytes uint64) {
	if db == nil {
		return 0, 0, 0
	}
	metrics := db.Metrics()
	if metrics == nil {
		return 0, 0, 0
	}
	diskUsageBytes = metrics.DiskSpaceUsage()
	compactionPendingBytes = metrics.Compact.EstimatedDebt
	if metrics.Compact.InProgressBytes > 0 {
		compactionInProgressBytes = uint64(metrics.Compact.InProgressBytes)
	}
	return diskUsageBytes, compactionPendingBytes, compactionInProgressBytes
}

func mustParseUint(v string) uint64 {
	var out uint64
	for _, ch := range v {
		out = out*10 + uint64(ch-'0')
	}
	return out
}
