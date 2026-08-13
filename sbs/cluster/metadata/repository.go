package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/volumeid"
)

var ErrNotFound = errors.New("metadata record not found")
var ErrCASConflict = errors.New("metadata compare-and-set conflict")

type kvStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error)
}

type KV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error)
}

type transactionalKV interface {
	RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error
}

type kvReadWriter interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type kvBatchReader interface {
	BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)
}

type cachedKVReadWriter struct {
	base       kvReadWriter
	entries    map[string]cachedKVEntry
	dirty      map[string]cachedKVEntry
	dirtyOrder []string
}

type cachedKVEntry struct {
	value []byte
	found bool
}

func newCachedKVReadWriter(base kvReadWriter) *cachedKVReadWriter {
	return &cachedKVReadWriter{
		base:    base,
		entries: make(map[string]cachedKVEntry),
		dirty:   make(map[string]cachedKVEntry),
	}
}

func (c *cachedKVReadWriter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if entry, ok := c.entries[key]; ok {
		if !entry.found {
			return nil, false, nil
		}
		return append([]byte(nil), entry.value...), true, nil
	}
	value, found, err := c.base.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}
	entry := cachedKVEntry{found: found}
	if found {
		entry.value = append([]byte(nil), value...)
	}
	c.entries[key] = entry
	if !found {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (c *cachedKVReadWriter) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	missing := make([]string, 0, len(keys))
	missingSeen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if entry, ok := c.entries[key]; ok {
			if entry.found {
				out[key] = append([]byte(nil), entry.value...)
			}
			continue
		}
		if _, ok := missingSeen[key]; ok {
			continue
		}
		missingSeen[key] = struct{}{}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return out, nil
	}
	if batcher, ok := c.base.(kvBatchReader); ok {
		values, err := batcher.BatchGet(ctx, missing)
		if err != nil {
			return nil, err
		}
		for _, key := range missing {
			value, found := values[key]
			entry := cachedKVEntry{found: found}
			if found {
				entry.value = append([]byte(nil), value...)
				out[key] = append([]byte(nil), value...)
			}
			c.entries[key] = entry
		}
		return out, nil
	}
	for _, key := range missing {
		value, found, err := c.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		entry := cachedKVEntry{found: found}
		if found {
			entry.value = append([]byte(nil), value...)
			out[key] = append([]byte(nil), value...)
		}
		c.entries[key] = entry
	}
	return out, nil
}

func (c *cachedKVReadWriter) Set(ctx context.Context, key string, value []byte) error {
	entry := cachedKVEntry{
		value: append([]byte(nil), value...),
		found: true,
	}
	c.entries[key] = entry
	if _, ok := c.dirty[key]; !ok {
		c.dirtyOrder = append(c.dirtyOrder, key)
	}
	c.dirty[key] = entry
	return nil
}

func (c *cachedKVReadWriter) Delete(ctx context.Context, key string) error {
	entry := cachedKVEntry{found: false}
	c.entries[key] = entry
	if _, ok := c.dirty[key]; !ok {
		c.dirtyOrder = append(c.dirtyOrder, key)
	}
	c.dirty[key] = entry
	return nil
}

func (c *cachedKVReadWriter) DirtyCount() int {
	if c == nil {
		return 0
	}
	return len(c.dirty)
}

func (c *cachedKVReadWriter) Flush(ctx context.Context) error {
	for _, key := range c.dirtyOrder {
		entry, ok := c.dirty[key]
		if !ok {
			continue
		}
		if !entry.found {
			if err := c.base.Delete(ctx, key); err != nil {
				return err
			}
			continue
		}
		if err := c.base.Set(ctx, key, entry.value); err != nil {
			return err
		}
	}
	c.dirty = make(map[string]cachedKVEntry)
	c.dirtyOrder = nil
	return nil
}

type ReadWriter interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

func RunInTransaction(ctx context.Context, store any, fn func(tx ReadWriter) error) error {
	runner, ok := store.(interface {
		RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error
	})
	if !ok {
		return fmt.Errorf("store does not support transactions")
	}
	return runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		return fn(tx)
	})
}

type Repository struct {
	kv                              kvStore
	root                            string
	mu                              sync.Mutex
	appendOnlyRevisionMu            sync.Mutex
	lastAppendOnlyRevision          uint64
	nativeAllocationFastPath        bool
	asyncWriteMutationFinalize      bool
	nativeAllocationVolumeMu        sync.RWMutex
	nativeAllocationVolumesObserved map[string]struct{}
}

func NewRepository(kv kvStore, root string) *Repository {
	root = strings.TrimSpace(root)
	root = strings.Trim(root, "/")
	if root == "" {
		root = "sbs/cluster"
	}
	return &Repository{
		kv:                              kv,
		root:                            root,
		nativeAllocationVolumesObserved: make(map[string]struct{}),
	}
}

func (r *Repository) SetNativeAllocationFastPath(enabled bool) {
	if r == nil {
		return
	}
	r.nativeAllocationFastPath = enabled
}

func (r *Repository) SetAsyncWriteMutationFinalize(enabled bool) {
	if r == nil {
		return
	}
	r.asyncWriteMutationFinalize = enabled
}

func (r *Repository) SetSkipNormalizedExtentRevisionBump(enabled bool) {
	r.SetNativeAllocationFastPath(enabled)
}

func (r *Repository) PutVolumeState(ctx context.Context, rec VolumeState) error {
	return r.putJSON(ctx, volumeStateKey(r.root, rec.VolumeID), rec)
}

func (r *Repository) DeleteVolumeState(ctx context.Context, volumeID string) error {
	r.forgetNativeAllocationVolume(volumeID)
	return r.kv.Delete(ctx, volumeStateKey(r.root, volumeID))
}

func (r *Repository) GetVolumeState(ctx context.Context, volumeID string) (VolumeState, error) {
	var rec VolumeState
	if err := r.getJSON(ctx, volumeStateKey(r.root, volumeID), &rec); err != nil {
		return VolumeState{}, err
	}
	return rec, nil
}

func (r *Repository) PutVolumeSpec(ctx context.Context, rec VolumeSpecRecord) error {
	return r.putJSON(ctx, volumeSpecKey(r.root, rec.VolumeID), rec)
}

func (r *Repository) GetVolumeSpec(ctx context.Context, volumeID string) (VolumeSpecRecord, error) {
	var rec VolumeSpecRecord
	if err := r.getJSON(ctx, volumeSpecKey(r.root, volumeID), &rec); err != nil {
		return VolumeSpecRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ExpandVolume(ctx context.Context, volumeID string, targetSizeBytes uint64) (VolumeSpecRecord, VolumeSpecRecord, VolumeState, error) {
	if targetSizeBytes == 0 {
		return VolumeSpecRecord{}, VolumeSpecRecord{}, VolumeState{}, fmt.Errorf("target_size_bytes is required")
	}
	volumeID = mustCanonicalVolumeID(volumeID)
	var oldSpec VolumeSpecRecord
	var newSpec VolumeSpecRecord
	var newState VolumeState
	update := func(store kvReadWriter) error {
		state, err := readVolumeState(ctx, store, r.root, volumeID)
		if err != nil {
			return err
		}
		spec, err := readVolumeSpec(ctx, store, r.root, volumeID)
		if err != nil {
			return err
		}
		if spec.BlockSize == 0 {
			return fmt.Errorf("volume %q has invalid block_size=0", volumeID)
		}
		if targetSizeBytes%uint64(spec.BlockSize) != 0 {
			return fmt.Errorf("target_size_bytes must be aligned to block_size")
		}
		if targetSizeBytes < spec.SizeBytes {
			return fmt.Errorf("target_size_bytes must be greater than current size_bytes")
		}
		oldSpec = spec
		if targetSizeBytes == spec.SizeBytes {
			newSpec = spec
			newState = state
			return nil
		}
		spec.SizeBytes = targetSizeBytes
		state.Revision++
		if state.Revision == 0 {
			state.Revision = 1
		}
		if err := writeVolumeSpec(ctx, store, r.root, spec); err != nil {
			return err
		}
		if err := writeVolumeState(ctx, store, r.root, state); err != nil {
			return err
		}
		newSpec = spec
		newState = state
		return nil
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		if err := txkv.RunInTransaction(ctx, update); err != nil {
			return VolumeSpecRecord{}, VolumeSpecRecord{}, VolumeState{}, err
		}
		return oldSpec, newSpec, newState, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := update(r.kv); err != nil {
		return VolumeSpecRecord{}, VolumeSpecRecord{}, VolumeState{}, err
	}
	return oldSpec, newSpec, newState, nil
}

func (r *Repository) DeleteVolumeSpec(ctx context.Context, volumeID string) error {
	return r.kv.Delete(ctx, volumeSpecKey(r.root, volumeID))
}

func (r *Repository) CreateSnapshotRecord(ctx context.Context, rec SnapshotRecord) (SnapshotRecord, bool, error) {
	if err := validateSnapshotID(rec.SnapshotID); err != nil {
		return SnapshotRecord{}, false, err
	}
	if rec.SourceVolumeID == "" {
		return SnapshotRecord{}, false, fmt.Errorf("source_volume_id is required")
	}
	sourceVolumeID, err := CanonicalVolumeID(rec.SourceVolumeID)
	if err != nil {
		return SnapshotRecord{}, false, fmt.Errorf("canonical source_volume_id: %w", err)
	}
	rec.SourceVolumeID = sourceVolumeID
	if rec.State == "" {
		rec.State = SnapshotStateAvailable
	}
	if rec.SnapshotRootID == "" {
		rec.SnapshotRootID = rec.SnapshotID
	}
	if rec.CreatedAtUnix == 0 {
		rec.CreatedAtUnix = time.Now().Unix()
	}
	if rec.UpdatedAtUnix == 0 {
		rec.UpdatedAtUnix = rec.CreatedAtUnix
	}

	if txkv, ok := r.kv.(transactionalKV); ok {
		var out SnapshotRecord
		var replay bool
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, replay, err = r.createSnapshotRecordWithStore(ctx, tx, rec)
			return err
		})
		return out, replay, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createSnapshotRecordWithStore(ctx, r.kv, rec)
}

func (r *Repository) createSnapshotRecordWithStore(ctx context.Context, store kvReadWriter, rec SnapshotRecord) (SnapshotRecord, bool, error) {
	if rec.IdempotencyKey != "" {
		var idem SnapshotIdempotencyRecord
		err := getJSONStore(ctx, store, snapshotIdempotencyKey(r.root, rec.SourceVolumeID, rec.IdempotencyKey), &idem)
		if err == nil {
			existing, err := readSnapshotRecord(ctx, store, r.root, idem.SnapshotID)
			return existing, true, err
		}
		if !errors.Is(err, ErrNotFound) {
			return SnapshotRecord{}, false, err
		}
	}

	if _, err := readSnapshotRecord(ctx, store, r.root, rec.SnapshotID); err == nil {
		return SnapshotRecord{}, false, ErrCASConflict
	} else if !errors.Is(err, ErrNotFound) {
		return SnapshotRecord{}, false, err
	}
	if err := writeSnapshotRecord(ctx, store, r.root, rec); err != nil {
		return SnapshotRecord{}, false, err
	}
	if err := putJSONStore(ctx, store, snapshotSourceIndexKey(r.root, rec.SourceVolumeID, rec.SnapshotID), snapshotSourceIndexRecord{
		SourceVolumeID: rec.SourceVolumeID,
		SnapshotID:     rec.SnapshotID,
		CreatedAtUnix:  rec.CreatedAtUnix,
	}); err != nil {
		return SnapshotRecord{}, false, err
	}
	if rec.IdempotencyKey != "" {
		if err := putJSONStore(ctx, store, snapshotIdempotencyKey(r.root, rec.SourceVolumeID, rec.IdempotencyKey), SnapshotIdempotencyRecord{
			SourceVolumeID:   rec.SourceVolumeID,
			IdempotencyKey:   rec.IdempotencyKey,
			SnapshotID:       rec.SnapshotID,
			CreatedAtUnix:    rec.CreatedAtUnix,
			LastObservedUnix: rec.UpdatedAtUnix,
		}); err != nil {
			return SnapshotRecord{}, false, err
		}
	}
	return rec, false, nil
}

func (r *Repository) GetSnapshotRecord(ctx context.Context, snapshotID string) (SnapshotRecord, error) {
	if err := validateSnapshotID(snapshotID); err != nil {
		return SnapshotRecord{}, err
	}
	return readSnapshotRecord(ctx, r.kv, r.root, snapshotID)
}

func (r *Repository) ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]SnapshotRecord, error) {
	var keys []string
	var err error
	if strings.TrimSpace(sourceVolumeID) != "" {
		keys, err = r.listAll(ctx, snapshotSourceIndexPrefix(r.root, sourceVolumeID))
	} else {
		keys, err = r.listAll(ctx, snapshotsPrefix(r.root))
	}
	if err != nil {
		return nil, err
	}

	out := make([]SnapshotRecord, 0, len(keys))
	seen := make(map[string]struct{})
	for _, key := range keys {
		var snapshotID string
		if strings.TrimSpace(sourceVolumeID) != "" {
			var idx snapshotSourceIndexRecord
			if err := r.getJSON(ctx, key, &idx); err != nil {
				return nil, err
			}
			snapshotID = idx.SnapshotID
		} else {
			if !strings.HasSuffix(key, "/record") {
				continue
			}
			snapshotID = snapshotIDFromRecordKey(key)
		}
		if snapshotID == "" {
			continue
		}
		if _, ok := seen[snapshotID]; ok {
			continue
		}
		seen[snapshotID] = struct{}{}
		rec, err := r.GetSnapshotRecord(ctx, snapshotID)
		if err != nil {
			return nil, err
		}
		if !includeDeleted && rec.State == SnapshotStateDeleted {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtUnix == out[j].CreatedAtUnix {
			return out[i].SnapshotID < out[j].SnapshotID
		}
		return out[i].CreatedAtUnix < out[j].CreatedAtUnix
	})
	return out, nil
}

func (r *Repository) MarkSnapshotState(ctx context.Context, snapshotID string, state SnapshotState, errorMessage string) (SnapshotRecord, error) {
	if err := validateSnapshotID(snapshotID); err != nil {
		return SnapshotRecord{}, err
	}
	if state == "" {
		return SnapshotRecord{}, fmt.Errorf("snapshot state is required")
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		var out SnapshotRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, err = markSnapshotStateWithStore(ctx, tx, r.root, snapshotID, state, errorMessage)
			return err
		})
		return out, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return markSnapshotStateWithStore(ctx, r.kv, r.root, snapshotID, state, errorMessage)
}

func (r *Repository) CreateCloneRecord(ctx context.Context, rec CloneRecord) (CloneRecord, bool, error) {
	if err := validateCloneID(rec.CloneID); err != nil {
		return CloneRecord{}, false, err
	}
	if err := validateSnapshotID(rec.SourceSnapshotID); err != nil {
		return CloneRecord{}, false, fmt.Errorf("source_snapshot_id: %w", err)
	}
	if rec.State == "" {
		rec.State = CloneStateAvailable
	}
	if rec.CreatedAtUnix == 0 {
		rec.CreatedAtUnix = time.Now().Unix()
	}
	if rec.UpdatedAtUnix == 0 {
		rec.UpdatedAtUnix = rec.CreatedAtUnix
	}

	if txkv, ok := r.kv.(transactionalKV); ok {
		var out CloneRecord
		var replay bool
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, replay, err = r.createCloneRecordWithStore(ctx, tx, rec)
			return err
		})
		return out, replay, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createCloneRecordWithStore(ctx, r.kv, rec)
}

func (r *Repository) createCloneRecordWithStore(ctx context.Context, store kvReadWriter, rec CloneRecord) (CloneRecord, bool, error) {
	if rec.IdempotencyKey != "" {
		var idem CloneIdempotencyRecord
		err := getJSONStore(ctx, store, cloneIdempotencyKey(r.root, rec.SourceSnapshotID, rec.IdempotencyKey), &idem)
		if err == nil {
			existing, err := readCloneRecord(ctx, store, r.root, idem.CloneID)
			return existing, true, err
		}
		if !errors.Is(err, ErrNotFound) {
			return CloneRecord{}, false, err
		}
	}

	source, err := readSnapshotRecord(ctx, store, r.root, rec.SourceSnapshotID)
	if err != nil {
		return CloneRecord{}, false, err
	}
	if source.State != SnapshotStateAvailable {
		return CloneRecord{}, false, fmt.Errorf("source snapshot %q is not available: state=%s", rec.SourceSnapshotID, source.State)
	}
	if rec.SizeBytes == 0 {
		rec.SizeBytes = source.SourceSizeBytes
	}
	if rec.SizeBytes < source.SourceSizeBytes {
		return CloneRecord{}, false, fmt.Errorf("clone size_bytes %d is smaller than source snapshot size_bytes %d", rec.SizeBytes, source.SourceSizeBytes)
	}
	rec.SourceVolumeID = source.SourceVolumeID
	rec.CloneBaseRootID = source.SnapshotRootID
	rec.AllocationChunkSizeBytes = source.AllocationChunkSizeBytes
	rec.AllocationPageSizeBytes = source.AllocationPageSizeBytes
	rec.SourceSizeBytes = source.SourceSizeBytes

	if _, err := readCloneRecord(ctx, store, r.root, rec.CloneID); err == nil {
		return CloneRecord{}, false, ErrCASConflict
	} else if !errors.Is(err, ErrNotFound) {
		return CloneRecord{}, false, err
	}
	if err := writeCloneRecord(ctx, store, r.root, rec); err != nil {
		return CloneRecord{}, false, err
	}
	if err := putJSONStore(ctx, store, cloneSourceSnapshotIndexKey(r.root, rec.SourceSnapshotID, rec.CloneID), cloneSourceIndexRecord{
		SourceSnapshotID: rec.SourceSnapshotID,
		SourceVolumeID:   rec.SourceVolumeID,
		CloneID:          rec.CloneID,
		CreatedAtUnix:    rec.CreatedAtUnix,
	}); err != nil {
		return CloneRecord{}, false, err
	}
	if err := putJSONStore(ctx, store, cloneSourceVolumeIndexKey(r.root, rec.SourceVolumeID, rec.CloneID), cloneSourceIndexRecord{
		SourceSnapshotID: rec.SourceSnapshotID,
		SourceVolumeID:   rec.SourceVolumeID,
		CloneID:          rec.CloneID,
		CreatedAtUnix:    rec.CreatedAtUnix,
	}); err != nil {
		return CloneRecord{}, false, err
	}
	if rec.IdempotencyKey != "" {
		if err := putJSONStore(ctx, store, cloneIdempotencyKey(r.root, rec.SourceSnapshotID, rec.IdempotencyKey), CloneIdempotencyRecord{
			SourceSnapshotID: rec.SourceSnapshotID,
			IdempotencyKey:   rec.IdempotencyKey,
			CloneID:          rec.CloneID,
			CreatedAtUnix:    rec.CreatedAtUnix,
			LastObservedUnix: rec.UpdatedAtUnix,
		}); err != nil {
			return CloneRecord{}, false, err
		}
	}
	source.CloneReferenceCount++
	source.UpdatedAtUnix = time.Now().Unix()
	if err := writeSnapshotRecord(ctx, store, r.root, source); err != nil {
		return CloneRecord{}, false, err
	}
	return rec, false, nil
}

func (r *Repository) GetCloneRecord(ctx context.Context, cloneID string) (CloneRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return CloneRecord{}, err
	}
	return readCloneRecord(ctx, r.kv, r.root, cloneID)
}

func (r *Repository) ListCloneRecords(ctx context.Context, sourceSnapshotID, sourceVolumeID string, includeDeleted bool) ([]CloneRecord, error) {
	var keys []string
	var err error
	switch {
	case strings.TrimSpace(sourceSnapshotID) != "":
		if err := validateSnapshotID(sourceSnapshotID); err != nil {
			return nil, err
		}
		keys, err = r.listAll(ctx, cloneSourceSnapshotIndexPrefix(r.root, sourceSnapshotID))
	case strings.TrimSpace(sourceVolumeID) != "":
		volumeID, canonErr := CanonicalVolumeID(sourceVolumeID)
		if canonErr != nil {
			return nil, fmt.Errorf("canonical source_volume_id: %w", canonErr)
		}
		keys, err = r.listAll(ctx, cloneSourceVolumeIndexPrefix(r.root, volumeID))
	default:
		keys, err = r.listAll(ctx, clonesPrefix(r.root))
	}
	if err != nil {
		return nil, err
	}

	out := make([]CloneRecord, 0, len(keys))
	seen := make(map[string]struct{})
	for _, key := range keys {
		var cloneID string
		if strings.TrimSpace(sourceSnapshotID) != "" || strings.TrimSpace(sourceVolumeID) != "" {
			var idx cloneSourceIndexRecord
			if err := r.getJSON(ctx, key, &idx); err != nil {
				return nil, err
			}
			cloneID = idx.CloneID
		} else {
			if !strings.HasSuffix(key, "/record") {
				continue
			}
			cloneID = cloneIDFromRecordKey(key)
		}
		if cloneID == "" {
			continue
		}
		if _, ok := seen[cloneID]; ok {
			continue
		}
		seen[cloneID] = struct{}{}
		rec, err := r.GetCloneRecord(ctx, cloneID)
		if err != nil {
			return nil, err
		}
		if !includeDeleted && rec.State == CloneStateDeleted {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAtUnix == out[j].CreatedAtUnix {
			return out[i].CloneID < out[j].CloneID
		}
		return out[i].CreatedAtUnix < out[j].CreatedAtUnix
	})
	return out, nil
}

func (r *Repository) MarkCloneState(ctx context.Context, cloneID string, state CloneState, errorMessage string) (CloneRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return CloneRecord{}, err
	}
	if state == "" {
		return CloneRecord{}, fmt.Errorf("clone state is required")
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		var out CloneRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, err = markCloneStateWithStore(ctx, tx, r.root, cloneID, state, errorMessage)
			return err
		})
		return out, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return markCloneStateWithStore(ctx, r.kv, r.root, cloneID, state, errorMessage)
}

func (r *Repository) MarkCloneMaterialized(ctx context.Context, cloneID, materializedVolumeID string) (CloneRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return CloneRecord{}, err
	}
	volumeID, err := CanonicalVolumeID(materializedVolumeID)
	if err != nil {
		return CloneRecord{}, fmt.Errorf("canonical materialized_volume_id: %w", err)
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		var out CloneRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, err = markCloneMaterializedWithStore(ctx, tx, r.root, cloneID, volumeID)
			return err
		})
		return out, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return markCloneMaterializedWithStore(ctx, r.kv, r.root, cloneID, volumeID)
}

func (r *Repository) DeleteCloneRecord(ctx context.Context, cloneID string) (CloneRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return CloneRecord{}, err
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		var out CloneRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			out, err = deleteCloneRecordWithStore(ctx, tx, r.root, cloneID)
			return err
		})
		return out, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return deleteCloneRecordWithStore(ctx, r.kv, r.root, cloneID)
}

func (r *Repository) ListVolumeStates(ctx context.Context) ([]VolumeState, error) {
	prefix := fmt.Sprintf("%s/volumes/", r.root)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]VolumeState, 0)
	seen := make(map[string]struct{})
	for _, key := range keys {
		if !strings.HasSuffix(key, "/meta/state") {
			continue
		}
		var rec VolumeState
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		if _, ok := seen[rec.VolumeID]; ok {
			continue
		}
		seen[rec.VolumeID] = struct{}{}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

func (r *Repository) PutExtentMapping(ctx context.Context, rec ExtentMappingRecord) error {
	return r.putJSON(ctx, extentMappingKey(r.root, rec.VolumeID, rec.ExtentID), rec)
}

func (r *Repository) PutAllocationPage(ctx context.Context, rec AllocationPageRecord) error {
	if err := r.putJSON(ctx, allocationPageKey(r.root, rec.VolumeID, rec.PageNo), rec); err != nil {
		return err
	}
	r.rememberNativeAllocationVolume(rec.VolumeID)
	return nil
}

func (r *Repository) PutSnapshotAllocationPage(ctx context.Context, snapshotID string, rec AllocationPageRecord) error {
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	return r.putJSON(ctx, snapshotAllocationPageKey(r.root, snapshotID, rec.PageNo), rec)
}

func (r *Repository) GetSnapshotAllocationPage(ctx context.Context, snapshotID string, pageNo uint64) (AllocationPageRecord, error) {
	if err := validateSnapshotID(snapshotID); err != nil {
		return AllocationPageRecord{}, err
	}
	var rec AllocationPageRecord
	if err := r.getJSON(ctx, snapshotAllocationPageKey(r.root, snapshotID, pageNo), &rec); err != nil {
		return AllocationPageRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListSnapshotAllocationPages(ctx context.Context, snapshotID string) ([]AllocationPageRecord, error) {
	if err := validateSnapshotID(snapshotID); err != nil {
		return nil, err
	}
	prefix := snapshotAllocationPagesPrefix(r.root, snapshotID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]AllocationPageRecord, 0, len(keys))
	for _, key := range keys {
		var rec AllocationPageRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return out, nil
}

func (r *Repository) CaptureSnapshotAllocationPages(ctx context.Context, snapshotID string, pages []AllocationPageRecord) error {
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			for _, page := range pages {
				if err := putJSONStore(ctx, tx, snapshotAllocationPageKey(r.root, snapshotID, page.PageNo), page); err != nil {
					return err
				}
			}
			return nil
		})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, page := range pages {
		if err := putJSONStore(ctx, r.kv, snapshotAllocationPageKey(r.root, snapshotID, page.PageNo), page); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) PutCloneDeltaAllocationPage(ctx context.Context, cloneID string, rec AllocationPageRecord) error {
	return r.PutCloneDeltaAllocationPages(ctx, cloneID, []AllocationPageRecord{rec})
}

func (r *Repository) PutCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []AllocationPageRecord) error {
	if err := validateCloneID(cloneID); err != nil {
		return err
	}
	if len(pages) == 0 {
		return nil
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			return putCloneDeltaAllocationPagesWithStore(ctx, tx, r.root, cloneID, pages)
		})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return putCloneDeltaAllocationPagesWithStore(ctx, r.kv, r.root, cloneID, pages)
}

func (r *Repository) CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []AllocationPageRecord) error {
	return r.PutCloneDeltaAllocationPages(ctx, cloneID, pages)
}

func (r *Repository) GetCloneDeltaAllocationPage(ctx context.Context, cloneID string, pageNo uint64) (AllocationPageRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return AllocationPageRecord{}, err
	}
	var rec AllocationPageRecord
	if err := r.getJSON(ctx, cloneDeltaAllocationPageKey(r.root, cloneID, pageNo), &rec); err != nil {
		return AllocationPageRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListCloneDeltaAllocationPages(ctx context.Context, cloneID string) ([]AllocationPageRecord, error) {
	if err := validateCloneID(cloneID); err != nil {
		return nil, err
	}
	prefix := cloneDeltaAllocationPagesPrefix(r.root, cloneID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]AllocationPageRecord, 0, len(keys))
	for _, key := range keys {
		var rec AllocationPageRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return out, nil
}

func (r *Repository) GetNextChunkID(ctx context.Context, volumeID string) (uint64, error) {
	if _, err := r.GetVolumeState(ctx, volumeID); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getNextChunkIDLocked(ctx, volumeID)
}

func (r *Repository) PutNextChunkID(ctx context.Context, volumeID string, nextID uint64) error {
	if _, err := r.GetVolumeState(ctx, volumeID); err != nil {
		return err
	}
	if nextID == 0 {
		nextID = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.putNextChunkIDLocked(ctx, volumeID, nextID)
}

func (r *Repository) AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	if _, err := r.GetVolumeState(ctx, volumeID); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	nextID, err := r.getNextChunkIDLocked(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	startID := nextID
	nextID += uint64(count)
	if err := r.putNextChunkIDLocked(ctx, volumeID, nextID); err != nil {
		return 0, err
	}
	return startID, nil
}

func (r *Repository) getNextChunkIDLocked(ctx context.Context, volumeID string) (uint64, error) {
	key := chunkNextIDKey(r.root, volumeID)
	nextID := uint64(1)
	if raw, found, err := r.kv.Get(ctx, key); err != nil {
		return 0, err
	} else if found {
		if err := json.Unmarshal(raw, &nextID); err != nil {
			return 0, err
		}
		if nextID == 0 {
			nextID = 1
		}
	}
	return nextID, nil
}

func (r *Repository) putNextChunkIDLocked(ctx context.Context, volumeID string, nextID uint64) error {
	payload, err := json.Marshal(nextID)
	if err != nil {
		return err
	}
	return r.kv.Set(ctx, chunkNextIDKey(r.root, volumeID), payload)
}

func (r *Repository) DeleteAllocationPage(ctx context.Context, volumeID string, pageNo uint64) error {
	r.forgetNativeAllocationVolume(volumeID)
	return r.kv.Delete(ctx, allocationPageKey(r.root, volumeID, pageNo))
}

func (r *Repository) GetAllocationPage(ctx context.Context, volumeID string, pageNo uint64) (AllocationPageRecord, error) {
	var rec AllocationPageRecord
	if err := r.getJSON(ctx, allocationPageKey(r.root, volumeID, pageNo), &rec); err != nil {
		return AllocationPageRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListAllocationPages(ctx context.Context, volumeID string) ([]AllocationPageRecord, error) {
	prefix := allocationPagesPrefix(r.root, volumeID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]AllocationPageRecord, 0, len(keys))
	for _, key := range keys {
		var rec AllocationPageRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return out, nil
}

// GetCompatibleAllocationPage returns the current compatibility allocation page
// view. If native allocation pages exist for the volume, they are always used.
// Otherwise the method synthesizes an allocation-like page from legacy extent
// mappings.
//
// This is a legacy adapter, not the Phase J/K substrate contract. New
// snapshot/clone/GC code should depend on explicit AllocationEntry /
// PhysicalObjectRef conversion rather than extending this fallback path.
func (r *Repository) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (AllocationPageRecord, error) {
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return AllocationPageRecord{}, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}

	page, err := r.GetAllocationPage(ctx, volumeID, pageNo)
	if err == nil {
		return page, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return AllocationPageRecord{}, err
	}

	pages, err := r.ListAllocationPages(ctx, volumeID)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	if len(pages) > 0 {
		for _, page := range pages {
			if page.PageNo == pageNo {
				return page, nil
			}
		}
		return zeroAllocationPage(volumeID, pageNo, pageBytes, chunkSizeBytes), nil
	}

	mappings, err := r.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	return synthesizeAllocationPageFromMappings(volumeID, pageNo, pageBytes, chunkSizeBytes, mappings), nil
}

// ListCompatibleAllocationPages returns native allocation pages when present. If
// the volume has no native allocation pages yet, legacy extent mappings are
// synthesized into allocation-like pages.
//
// This is a legacy adapter, not the Phase J/K substrate contract.
// Snapshot capture currently stores AllocationPageRecord copies for the replica
// backend MVP. Phase K should migrate the durable snapshot root toward explicit
// AllocationEntry / PhysicalObjectRef records so EC stripe/shard references do
// not depend on this compatibility shape.
func (r *Repository) ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]AllocationPageRecord, error) {
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}

	pages, err := r.ListAllocationPages(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if len(pages) > 0 {
		return pages, nil
	}

	mappings, err := r.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if len(mappings) == 0 {
		return nil, nil
	}

	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	pageNos := make(map[uint64]struct{})
	for _, mapping := range mappings {
		if mapping.LengthBytes == 0 {
			continue
		}
		startChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
		endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / uint64(chunkSizeBytes)
		startPage := startChunk / chunksPerPage
		endPage := endChunk / chunksPerPage
		for pageNo := startPage; pageNo <= endPage; pageNo++ {
			pageNos[pageNo] = struct{}{}
		}
	}

	sortedPageNos := make([]uint64, 0, len(pageNos))
	for pageNo := range pageNos {
		sortedPageNos = append(sortedPageNos, pageNo)
	}
	sort.Slice(sortedPageNos, func(i, j int) bool { return sortedPageNos[i] < sortedPageNos[j] })

	out := make([]AllocationPageRecord, 0, len(sortedPageNos))
	for _, pageNo := range sortedPageNos {
		out = append(out, synthesizeAllocationPageFromMappings(volumeID, pageNo, pageBytes, chunkSizeBytes, mappings))
	}
	return out, nil
}

func (r *Repository) DeleteExtentMapping(ctx context.Context, volumeID string, extentID uint64) error {
	return r.kv.Delete(ctx, extentMappingKey(r.root, volumeID, extentID))
}

func (r *Repository) GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (ExtentMappingRecord, error) {
	var rec ExtentMappingRecord
	if err := r.getJSON(ctx, extentMappingKey(r.root, volumeID, extentID), &rec); err != nil {
		return ExtentMappingRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListExtentMappings(ctx context.Context, volumeID string) ([]ExtentMappingRecord, error) {
	prefix := extentMappingsPrefix(r.root, volumeID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ExtentMappingRecord, 0, len(keys))
	for _, key := range keys {
		var rec ExtentMappingRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExtentID < out[j].ExtentID })
	return out, nil
}

func (r *Repository) PutReplicaSet(ctx context.Context, rec ReplicaSetState) error {
	return r.putJSON(ctx, replicaSetKey(r.root, rec.VolumeID, rec.ReplicaSetID), rec)
}

func (r *Repository) DeleteReplicaSet(ctx context.Context, volumeID, replicaSetID string) error {
	return r.kv.Delete(ctx, replicaSetKey(r.root, volumeID, replicaSetID))
}

func (r *Repository) GetReplicaSet(ctx context.Context, volumeID, replicaSetID string) (ReplicaSetState, error) {
	var rec ReplicaSetState
	if err := r.getJSON(ctx, replicaSetKey(r.root, volumeID, replicaSetID), &rec); err != nil {
		return ReplicaSetState{}, err
	}
	return rec, nil
}

func (r *Repository) ListReplicaSets(ctx context.Context, volumeID string) ([]ReplicaSetState, error) {
	prefix := replicaSetsPrefix(r.root, volumeID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ReplicaSetState, 0, len(keys))
	for _, key := range keys {
		var rec ReplicaSetState
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ReplicaSetID < out[j].ReplicaSetID })
	return out, nil
}

func (r *Repository) PutIdempotencyRecord(ctx context.Context, rec IdempotencyRecord) error {
	return r.putJSON(ctx, idempotencyKey(r.root, rec.VolumeID, rec.IdempotencyKey), rec)
}

func (r *Repository) PutMutationOperation(ctx context.Context, rec MutationOperationRecord) error {
	return r.putJSON(ctx, mutationOperationKey(r.root, rec.VolumeID, rec.OperationID), rec)
}

func (r *Repository) PutWriteIntent(ctx context.Context, record IdempotencyRecord, operation MutationOperationRecord) error {
	return r.PutWriteIntentBatch(ctx, []WriteIntentRecord{{
		IdempotencyRecord: record,
		MutationOperation: operation,
	}})
}

func (r *Repository) PutWriteIntentBatch(ctx context.Context, intents []WriteIntentRecord) error {
	if len(intents) == 0 {
		return nil
	}
	if hasDuplicateWriteIntentKeys(intents) {
		for _, intent := range intents {
			if err := r.putWriteIntent(ctx, intent.IdempotencyRecord, intent.MutationOperation); err != nil {
				return err
			}
		}
		return nil
	}
	apply := func(store kvReadWriter) error {
		for _, intent := range intents {
			if err := writeIdempotencyRecord(ctx, store, r.root, intent.IdempotencyRecord); err != nil {
				return err
			}
			if err := writeMutationOperation(ctx, store, r.root, intent.MutationOperation); err != nil {
				return err
			}
		}
		return nil
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, apply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return apply(r.kv)
}

func (r *Repository) putWriteIntent(ctx context.Context, record IdempotencyRecord, operation MutationOperationRecord) error {
	apply := func(store kvReadWriter) error {
		if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
			return err
		}
		return writeMutationOperation(ctx, store, r.root, operation)
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, apply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return apply(r.kv)
}

func hasDuplicateWriteIntentKeys(intents []WriteIntentRecord) bool {
	seen := make(map[string]struct{}, len(intents)*2)
	for _, intent := range intents {
		idemKey := mustCanonicalVolumeID(intent.IdempotencyRecord.VolumeID) + "\x00" + intent.IdempotencyRecord.IdempotencyKey
		if _, ok := seen["i:"+idemKey]; ok {
			return true
		}
		seen["i:"+idemKey] = struct{}{}
		mutationKey := mustCanonicalVolumeID(intent.MutationOperation.VolumeID) + "\x00" + intent.MutationOperation.OperationID
		if _, ok := seen["m:"+mutationKey]; ok {
			return true
		}
		seen["m:"+mutationKey] = struct{}{}
	}
	return false
}

func (r *Repository) DeleteMutationOperation(ctx context.Context, volumeID, operationID string) error {
	return r.kv.Delete(ctx, mutationOperationKey(r.root, volumeID, operationID))
}

func (r *Repository) GetMutationOperation(ctx context.Context, volumeID, operationID string) (MutationOperationRecord, error) {
	var rec MutationOperationRecord
	if err := r.getJSON(ctx, mutationOperationKey(r.root, volumeID, operationID), &rec); err != nil {
		return MutationOperationRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListMutationOperations(ctx context.Context, volumeID string) ([]MutationOperationRecord, error) {
	prefix := mutationOperationsPrefix(r.root, volumeID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]MutationOperationRecord, 0, len(keys))
	for _, key := range keys {
		var rec MutationOperationRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastUpdatedAtUnix == out[j].LastUpdatedAtUnix {
			return out[i].OperationID < out[j].OperationID
		}
		return out[i].LastUpdatedAtUnix < out[j].LastUpdatedAtUnix
	})
	return out, nil
}

func (r *Repository) FindMutationOperationByID(ctx context.Context, operationID string) (MutationOperationRecord, error) {
	keys, err := r.listAll(ctx, mutationOperationsRootPrefix(r.root))
	if err != nil {
		return MutationOperationRecord{}, err
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, "/"+operationID) {
			continue
		}
		var rec MutationOperationRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return MutationOperationRecord{}, err
		}
		return rec, nil
	}
	return MutationOperationRecord{}, ErrNotFound
}

func (r *Repository) ListAllMutationOperations(ctx context.Context) ([]MutationOperationRecord, error) {
	keys, err := r.listAll(ctx, mutationOperationsRootPrefix(r.root))
	if err != nil {
		return nil, err
	}
	out := make([]MutationOperationRecord, 0, len(keys))
	for _, key := range keys {
		if !strings.Contains(key, "/operations/") {
			continue
		}
		var rec MutationOperationRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastUpdatedAtUnix == out[j].LastUpdatedAtUnix {
			return out[i].OperationID < out[j].OperationID
		}
		return out[i].LastUpdatedAtUnix < out[j].LastUpdatedAtUnix
	})
	return out, nil
}

func (r *Repository) DeleteIdempotencyRecord(ctx context.Context, volumeID, idemKey string) error {
	return r.kv.Delete(ctx, idempotencyKey(r.root, volumeID, idemKey))
}

func (r *Repository) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (IdempotencyRecord, error) {
	var rec IdempotencyRecord
	if err := r.getJSON(ctx, idempotencyKeyKey(r.root, volumeID, idempotencyKey), &rec); err != nil {
		return IdempotencyRecord{}, err
	}
	return rec, nil
}

func (r *Repository) PutTopologyZone(ctx context.Context, rec TopologyZoneRecord) error {
	rec.ZoneID = strings.TrimSpace(rec.ZoneID)
	if rec.ZoneID == "" {
		return fmt.Errorf("zone_id is required")
	}
	if rec.Lifecycle == "" {
		rec.Lifecycle = TopologyZoneLifecycleActive
	}
	return r.putJSON(ctx, topologyZoneKey(r.root, rec.ZoneID), rec)
}

func (r *Repository) GetTopologyZone(ctx context.Context, zoneID string) (TopologyZoneRecord, error) {
	var rec TopologyZoneRecord
	if err := r.getJSON(ctx, topologyZoneKey(r.root, strings.TrimSpace(zoneID)), &rec); err != nil {
		return TopologyZoneRecord{}, err
	}
	return rec, nil
}

func (r *Repository) DeleteTopologyZone(ctx context.Context, zoneID string) error {
	return r.kv.Delete(ctx, topologyZoneKey(r.root, strings.TrimSpace(zoneID)))
}

func (r *Repository) ListTopologyZones(ctx context.Context) ([]TopologyZoneRecord, error) {
	keys, err := r.listAll(ctx, topologyZonesPrefix(r.root))
	if err != nil {
		return nil, err
	}
	out := make([]TopologyZoneRecord, 0, len(keys))
	for _, key := range keys {
		var rec TopologyZoneRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ZoneID < out[j].ZoneID })
	return out, nil
}

func (r *Repository) PutNodeMembership(ctx context.Context, rec NodeMembershipRecord) error {
	return r.putJSON(ctx, nodeMembershipKey(r.root, rec.NodeID), rec)
}

func (r *Repository) PutNodeHealthDetail(ctx context.Context, rec NodeHealthDetailRecord) error {
	return r.putJSON(ctx, nodeHealthDetailKey(r.root, rec.NodeID), rec)
}

func (r *Repository) DeleteNodeHealthDetail(ctx context.Context, nodeID string) error {
	return r.kv.Delete(ctx, nodeHealthDetailKey(r.root, nodeID))
}

func (r *Repository) GetNodeHealthDetail(ctx context.Context, nodeID string) (NodeHealthDetailRecord, error) {
	var rec NodeHealthDetailRecord
	if err := r.getJSON(ctx, nodeHealthDetailKey(r.root, nodeID), &rec); err != nil {
		return NodeHealthDetailRecord{}, err
	}
	return rec, nil
}

func (r *Repository) DeleteNodeMembership(ctx context.Context, nodeID string) error {
	return r.kv.Delete(ctx, nodeMembershipKey(r.root, nodeID))
}

func (r *Repository) GetNodeMembership(ctx context.Context, nodeID string) (NodeMembershipRecord, error) {
	var rec NodeMembershipRecord
	if err := r.getJSON(ctx, nodeMembershipKey(r.root, nodeID), &rec); err != nil {
		return NodeMembershipRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListNodeMemberships(ctx context.Context) ([]NodeMembershipRecord, error) {
	prefix := fmt.Sprintf("%s/nodes/", r.root)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]NodeMembershipRecord, 0, len(keys))
	for _, key := range keys {
		if !strings.HasSuffix(key, "/membership") {
			continue
		}
		var rec NodeMembershipRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (r *Repository) PutPlacementTransition(ctx context.Context, rec PlacementTransitionRecord) error {
	return r.putJSON(ctx, placementTransitionKey(r.root, rec.VolumeID, rec.PlacementRef), rec)
}

func (r *Repository) DeletePlacementTransition(ctx context.Context, volumeID, placementRef string) error {
	return r.kv.Delete(ctx, placementTransitionKey(r.root, volumeID, placementRef))
}

func (r *Repository) GetPlacementTransition(ctx context.Context, volumeID, placementRef string) (PlacementTransitionRecord, error) {
	var rec PlacementTransitionRecord
	if err := r.getJSON(ctx, placementTransitionKey(r.root, volumeID, placementRef), &rec); err != nil {
		return PlacementTransitionRecord{}, err
	}
	return rec, nil
}

func (r *Repository) ListPlacementTransitions(ctx context.Context, volumeID string) ([]PlacementTransitionRecord, error) {
	prefix := placementTransitionsPrefix(r.root, volumeID)
	keys, err := r.listAll(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]PlacementTransitionRecord, 0, len(keys))
	for _, key := range keys {
		var rec PlacementTransitionRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastProgressAtUnix == out[j].LastProgressAtUnix {
			return out[i].PlacementRef < out[j].PlacementRef
		}
		return out[i].LastProgressAtUnix < out[j].LastProgressAtUnix
	})
	return out, nil
}

type CommitWriteMetadataRequest struct {
	VolumeID                 string
	ExpectedEpoch            uint64
	ExpectedRevision         uint64
	IdempotencyKey           string
	ExpectedIdempotencyState IdempotencyResultState
	CommittedRevision        uint64
	AttachmentID             string
	Generation               uint64
	AllowMissingWriteIntent  bool
	AllocationPages          []AllocationPageRecord
	NormalizeExtentMappings  []uint64
	MutationOperationID      string
	ExpectedMutationState    MutationOperationState
	AffectedExtentIDs        []uint64
	AffectedPageNos          []uint64
	AffectedPageChunkRanges  []AllocationPageChunkRangeRecord
	RetiredPhysicalChunkIDs  []uint64
	MutationOperation        MutationOperationRecord
}

type CommitWriteStateRequest struct {
	VolumeID                 string
	ExpectedEpoch            uint64
	ExpectedRevision         uint64
	IdempotencyKey           string
	ExpectedIdempotencyState IdempotencyResultState
	CommittedRevision        uint64
	AttachmentID             string
	Generation               uint64
	AllowMissingWriteIntent  bool
}

type ApplyCommittedWriteEffectsRequest struct {
	VolumeID                string
	CommittedRevision       uint64
	AllocationPages         []AllocationPageRecord
	NormalizeExtentMappings []uint64
	MutationOperationID     string
	ExpectedMutationState   MutationOperationState
	AffectedExtentIDs       []uint64
	AffectedPageNos         []uint64
	AffectedPageChunkRanges []AllocationPageChunkRangeRecord
	RetiredPhysicalChunkIDs []uint64
	MutationOperation       MutationOperationRecord
}

type WriteIntentRecord struct {
	IdempotencyRecord IdempotencyRecord
	MutationOperation MutationOperationRecord
}

type AllocationPageChunkRangeRecord struct {
	PageNo     uint64
	StartChunk uint64
	EndChunk   uint64
}

type CommitECFullStripeWriteRequest struct {
	VolumeID                string
	ExpectedEpoch           uint64
	ExpectedRevision        uint64
	IdempotencyKey          string
	CommittedRevision       uint64
	PhysicalObject          PhysicalObjectRecord
	ECStripe                ECStripeRecord
	AllocationPages         []AllocationPageRecord
	MutationOperationID     string
	ExpectedMutationState   MutationOperationState
	AffectedPageNos         []uint64
	AffectedExtentIDs       []uint64
	RetiredPhysicalChunkIDs []uint64
	RetiredECObjects        []RetiredECObjectRef
	MutationOperation       MutationOperationRecord
}

type CommitECDiscardRequest struct {
	VolumeID                string
	ExpectedEpoch           uint64
	ExpectedRevision        uint64
	IdempotencyKey          string
	CommittedRevision       uint64
	AllocationPages         []AllocationPageRecord
	MutationOperationID     string
	ExpectedMutationState   MutationOperationState
	AffectedPageNos         []uint64
	AffectedExtentIDs       []uint64
	RetiredPhysicalChunkIDs []uint64
	RetiredECObjects        []RetiredECObjectRef
}

type RetiredECObjectRef struct {
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
}

type rangeLocalWriteStateRecord struct {
	VolumeID       string `json:"volume_id"`
	PageNo         uint64 `json:"page_no"`
	Revision       uint64 `json:"revision"`
	IdempotencyKey string `json:"idempotency_key"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
}

func (r ApplyCommittedWriteEffectsRequest) MatchesCommittedMutationOperation(operation MutationOperationRecord) bool {
	return operation.State == MutationOperationCommitted &&
		operation.AllocationRevision == r.CommittedRevision &&
		slices.Equal(operation.AffectedExtentIDs, r.AffectedExtentIDs) &&
		slices.Equal(operation.AffectedPageNos, r.AffectedPageNos) &&
		slices.Equal(operation.RetiredPhysicalChunkIDs, r.RetiredPhysicalChunkIDs)
}

func (r CommitWriteMetadataRequest) StateCommitRequest() CommitWriteStateRequest {
	return CommitWriteStateRequest{
		VolumeID:                 r.VolumeID,
		ExpectedEpoch:            r.ExpectedEpoch,
		ExpectedRevision:         r.ExpectedRevision,
		IdempotencyKey:           r.IdempotencyKey,
		ExpectedIdempotencyState: r.ExpectedIdempotencyState,
		CommittedRevision:        r.CommittedRevision,
		AttachmentID:             r.AttachmentID,
		Generation:               r.Generation,
		AllowMissingWriteIntent:  r.AllowMissingWriteIntent,
	}
}

func (r CommitWriteMetadataRequest) EffectsApplyRequest() ApplyCommittedWriteEffectsRequest {
	return ApplyCommittedWriteEffectsRequest{
		VolumeID:                r.VolumeID,
		CommittedRevision:       r.CommittedRevision,
		AllocationPages:         append([]AllocationPageRecord(nil), r.AllocationPages...),
		NormalizeExtentMappings: append([]uint64(nil), r.NormalizeExtentMappings...),
		MutationOperationID:     r.MutationOperationID,
		ExpectedMutationState:   r.ExpectedMutationState,
		AffectedExtentIDs:       append([]uint64(nil), r.AffectedExtentIDs...),
		AffectedPageNos:         append([]uint64(nil), r.AffectedPageNos...),
		AffectedPageChunkRanges: append([]AllocationPageChunkRangeRecord(nil), r.AffectedPageChunkRanges...),
		RetiredPhysicalChunkIDs: append([]uint64(nil), r.RetiredPhysicalChunkIDs...),
		MutationOperation:       cloneMutationOperationRecord(r.MutationOperation),
	}
}

func cloneApplyCommittedWriteEffectsRequests(in []ApplyCommittedWriteEffectsRequest) []ApplyCommittedWriteEffectsRequest {
	if len(in) == 0 {
		return nil
	}
	out := make([]ApplyCommittedWriteEffectsRequest, len(in))
	for i, req := range in {
		out[i] = cloneApplyCommittedWriteEffectsRequest(req)
	}
	return out
}

func cloneApplyCommittedWriteEffectsRequest(req ApplyCommittedWriteEffectsRequest) ApplyCommittedWriteEffectsRequest {
	req.AllocationPages = cloneAllocationPageRecords(req.AllocationPages)
	req.NormalizeExtentMappings = append([]uint64(nil), req.NormalizeExtentMappings...)
	req.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	req.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	req.AffectedPageChunkRanges = append([]AllocationPageChunkRangeRecord(nil), req.AffectedPageChunkRanges...)
	req.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	req.MutationOperation = cloneMutationOperationRecord(req.MutationOperation)
	return req
}

func cloneMutationOperationRecord(in MutationOperationRecord) MutationOperationRecord {
	in.AffectedExtentIDs = append([]uint64(nil), in.AffectedExtentIDs...)
	in.AffectedPageNos = append([]uint64(nil), in.AffectedPageNos...)
	in.CompletedPageNos = append([]uint64(nil), in.CompletedPageNos...)
	in.RetryPageWindows = append([]MutationPageWindowRecord(nil), in.RetryPageWindows...)
	in.RetiredPhysicalChunkIDs = append([]uint64(nil), in.RetiredPhysicalChunkIDs...)
	return in
}

func cloneAllocationPageRecords(in []AllocationPageRecord) []AllocationPageRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]AllocationPageRecord, len(in))
	for i, page := range in {
		page.Extents = cloneAllocationExtentRecords(page.Extents)
		out[i] = page
	}
	return out
}

func cloneAllocationExtentRecords(in []AllocationExtentRecord) []AllocationExtentRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]AllocationExtentRecord, len(in))
	for i, extent := range in {
		extent.Encryption = clonePayloadEncryptionHeader(extent.Encryption)
		out[i] = extent
	}
	return out
}

func (r *Repository) CommitWriteState(ctx context.Context, req CommitWriteStateRequest) (VolumeState, IdempotencyRecord, error) {
	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitWriteStateWithStore(ctx, tx, req)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitWriteStateWithStore(ctx, r.kv, req)
}

func (r *Repository) CommitAppendOnlyWriteState(ctx context.Context, req CommitWriteStateRequest) (VolumeState, IdempotencyRecord, error) {
	start := time.Now()
	var timings appendOnlyWriteStateTimings
	var err error
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("expected_epoch", req.ExpectedEpoch),
			structuredlog.F("expected_revision", req.ExpectedRevision),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("transaction_duration_ms", timings.transaction.Milliseconds()),
			structuredlog.F("batch_read_duration_ms", timings.batchRead.Milliseconds()),
			structuredlog.F("volume_state_read_duration_ms", timings.volumeStateRead.Milliseconds()),
			structuredlog.F("idempotency_read_duration_ms", timings.idempotencyRead.Milliseconds()),
			structuredlog.F("idempotency_write_duration_ms", timings.idempotencyWrite.Milliseconds()),
			structuredlog.F("append_revision_gen_duration_ms", timings.appendRevisionGen.Milliseconds()),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("sbs.metadata", "write_session_append_only_state_commit_phases_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.metadata", "write_session_append_only_state_commit_phases", fields...)
	}()

	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		phaseStart := time.Now()
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitAppendOnlyWriteStateWithStore(ctx, tx, req, &timings)
			return err
		})
		timings.transaction = time.Since(phaseStart)
		return state, record, err
	}

	phaseStart := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	state, record, err := r.commitAppendOnlyWriteStateWithStore(ctx, r.kv, req, &timings)
	timings.transaction = time.Since(phaseStart)
	return state, record, err
}

func (r *Repository) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req CommitWriteMetadataRequest) (VolumeState, IdempotencyRecord, error) {
	states, records, err := r.CommitAppendOnlyWriteMetadataBatch(ctx, []CommitWriteMetadataRequest{req})
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if len(states) != 1 || len(records) != 1 {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("append-only write metadata batch returned %d states and %d records for one request", len(states), len(records))
	}
	return states[0], records[0], nil
}

type committedWriteEffectsTimings struct {
	legacyBootstrap        time.Duration
	transaction            time.Duration
	transactionCallback    time.Duration
	transactionFlush       time.Duration
	idempotencyCheck       time.Duration
	allocationPagePersist  time.Duration
	extentMappingNormalize time.Duration
	mutationFinalize       time.Duration
	asyncMutationFinalize  int
	committedReplay        bool
	committedReplayCount   int
	transactionDirtyKeys   int
	normalizeStats         committedExtentNormalizeStats
}

func (t committedWriteEffectsTimings) transactionRunnerOverhead() time.Duration {
	overhead := t.transaction - t.transactionCallback
	if overhead < 0 {
		return 0
	}
	return overhead
}

type appendOnlyWriteStateTimings struct {
	transaction       time.Duration
	batchRead         time.Duration
	volumeStateRead   time.Duration
	idempotencyRead   time.Duration
	idempotencyWrite  time.Duration
	appendRevisionGen time.Duration
}

type committedExtentNormalizeStats struct {
	requested         int
	read              int
	written           int
	skipped           int
	alreadyNormalized int
	revisionAdvanced  int
	revisionPreserved int
}

func (s *committedExtentNormalizeStats) add(other committedExtentNormalizeStats) {
	s.requested += other.requested
	s.read += other.read
	s.written += other.written
	s.skipped += other.skipped
	s.alreadyNormalized += other.alreadyNormalized
	s.revisionAdvanced += other.revisionAdvanced
	s.revisionPreserved += other.revisionPreserved
}

type committedWriteMutationSnapshot struct {
	operation MutationOperationRecord
	found     bool
	committed bool
}

type committedWriteEffectsBatchItem struct {
	req      ApplyCommittedWriteEffectsRequest
	mutation committedWriteMutationSnapshot
}

type appendOnlyWriteStateSnapshot struct {
	state  VolumeState
	record IdempotencyRecord
}

type committedAllocationPageBatchKey struct {
	volumeID string
	pageNo   uint64
}

type committedAllocationPageBatchUpdate struct {
	req  ApplyCommittedWriteEffectsRequest
	page AllocationPageRecord
}

func (r *Repository) ApplyCommittedWriteEffects(ctx context.Context, req ApplyCommittedWriteEffectsRequest) (err error) {
	start := time.Now()
	var timings committedWriteEffectsTimings
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("mutation_operation_id", req.MutationOperationID),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
			structuredlog.F("committed_replay", timings.committedReplay),
			structuredlog.F("committed_replay_count", timings.committedReplayCount),
			structuredlog.F("legacy_bootstrap_duration_ms", timings.legacyBootstrap.Milliseconds()),
			structuredlog.F("transaction_duration_ms", timings.transaction.Milliseconds()),
			structuredlog.F("transaction_callback_duration_ms", timings.transactionCallback.Milliseconds()),
			structuredlog.F("transaction_flush_duration_ms", timings.transactionFlush.Milliseconds()),
			structuredlog.F("transaction_runner_overhead_duration_ms", timings.transactionRunnerOverhead().Milliseconds()),
			structuredlog.F("transaction_dirty_key_count", timings.transactionDirtyKeys),
			structuredlog.F("idempotency_check_duration_ms", timings.idempotencyCheck.Milliseconds()),
			structuredlog.F("allocation_page_persist_duration_ms", timings.allocationPagePersist.Milliseconds()),
			structuredlog.F("extent_mapping_normalize_duration_ms", timings.extentMappingNormalize.Milliseconds()),
			structuredlog.F("mutation_finalize_duration_ms", timings.mutationFinalize.Milliseconds()),
			structuredlog.F("async_mutation_finalize_count", timings.asyncMutationFinalize),
			structuredlog.F("normalize_extent_requested_count", timings.normalizeStats.requested),
			structuredlog.F("normalize_extent_read_count", timings.normalizeStats.read),
			structuredlog.F("normalize_extent_write_count", timings.normalizeStats.written),
			structuredlog.F("normalize_extent_skip_count", timings.normalizeStats.skipped),
			structuredlog.F("normalize_extent_already_normalized_count", timings.normalizeStats.alreadyNormalized),
			structuredlog.F("normalize_extent_revision_advanced_count", timings.normalizeStats.revisionAdvanced),
			structuredlog.F("normalize_extent_revision_preserved_count", timings.normalizeStats.revisionPreserved),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("sbs.metadata", "write_session_effects_apply_phases_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.metadata", "write_session_effects_apply_phases", fields...)
	}()

	phaseStart := time.Now()
	bootstrapPages, bootstrapExtentIDs, err := r.prepareLegacyAllocationBootstrap(ctx, CommitWriteMetadataRequest{
		VolumeID:        req.VolumeID,
		AllocationPages: req.AllocationPages,
	})
	timings.legacyBootstrap = time.Since(phaseStart)
	if err != nil {
		return err
	}
	req.AllocationPages = mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)
	req.NormalizeExtentMappings = mergeExtentIDs(bootstrapExtentIDs, req.NormalizeExtentMappings)

	if txkv, ok := r.kv.(transactionalKV); ok {
		phaseStart = time.Now()
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			callbackStart := time.Now()
			defer func() {
				timings.transactionCallback += time.Since(callbackStart)
			}()
			return r.applyCommittedWriteEffectsWithStore(ctx, tx, req, &timings)
		})
		timings.transaction = time.Since(phaseStart)
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applyCommittedWriteEffectsWithStore(ctx, r.kv, req, &timings)
}

func (r *Repository) applyPlacementChanges(ctx context.Context, req PlacementApplyRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	pages := placementApplyAllocationPages(req)
	effectsReq := ApplyCommittedWriteEffectsRequest{
		VolumeID:                req.VolumeID,
		CommittedRevision:       req.CommittedRevision,
		AllocationPages:         pages,
		NormalizeExtentMappings: append([]uint64(nil), req.NormalizeExtentIDs...),
		RetiredPhysicalChunkIDs: append([]uint64(nil), req.RetiredPhysicalChunkIDs...),
	}
	apply := func(store kvReadWriter) error {
		if err := r.persistCommittedAllocationPages(ctx, store, effectsReq); err != nil {
			return err
		}
		_, err := r.normalizeCommittedExtentMappings(ctx, store, effectsReq)
		return err
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, apply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return apply(r.kv)
}

func (r *Repository) ApplyCommittedWriteEffectsBatch(ctx context.Context, reqs []ApplyCommittedWriteEffectsRequest) (err error) {
	if len(reqs) == 0 {
		return nil
	}
	start := time.Now()
	prepared := make([]ApplyCommittedWriteEffectsRequest, len(reqs))
	var timings committedWriteEffectsTimings
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("request_count", len(reqs)),
			structuredlog.F("volume_id", reqs[0].VolumeID),
			structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
			structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
			structuredlog.F("legacy_bootstrap_duration_ms", timings.legacyBootstrap.Milliseconds()),
			structuredlog.F("transaction_duration_ms", timings.transaction.Milliseconds()),
			structuredlog.F("transaction_callback_duration_ms", timings.transactionCallback.Milliseconds()),
			structuredlog.F("transaction_flush_duration_ms", timings.transactionFlush.Milliseconds()),
			structuredlog.F("transaction_runner_overhead_duration_ms", timings.transactionRunnerOverhead().Milliseconds()),
			structuredlog.F("transaction_dirty_key_count", timings.transactionDirtyKeys),
			structuredlog.F("idempotency_check_duration_ms", timings.idempotencyCheck.Milliseconds()),
			structuredlog.F("allocation_page_persist_duration_ms", timings.allocationPagePersist.Milliseconds()),
			structuredlog.F("extent_mapping_normalize_duration_ms", timings.extentMappingNormalize.Milliseconds()),
			structuredlog.F("mutation_finalize_duration_ms", timings.mutationFinalize.Milliseconds()),
			structuredlog.F("async_mutation_finalize_count", timings.asyncMutationFinalize),
			structuredlog.F("committed_replay_count", timings.committedReplayCount),
			structuredlog.F("normalize_extent_requested_count", timings.normalizeStats.requested),
			structuredlog.F("normalize_extent_read_count", timings.normalizeStats.read),
			structuredlog.F("normalize_extent_write_count", timings.normalizeStats.written),
			structuredlog.F("normalize_extent_skip_count", timings.normalizeStats.skipped),
			structuredlog.F("normalize_extent_already_normalized_count", timings.normalizeStats.alreadyNormalized),
			structuredlog.F("normalize_extent_revision_advanced_count", timings.normalizeStats.revisionAdvanced),
			structuredlog.F("normalize_extent_revision_preserved_count", timings.normalizeStats.revisionPreserved),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("sbs.metadata", "write_session_effects_apply_batch_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.metadata", "write_session_effects_apply_batch", fields...)
	}()

	for i, req := range reqs {
		phaseStart := time.Now()
		bootstrapPages, bootstrapExtentIDs, err := r.prepareLegacyAllocationBootstrap(ctx, CommitWriteMetadataRequest{
			VolumeID:        req.VolumeID,
			AllocationPages: req.AllocationPages,
		})
		timings.legacyBootstrap += time.Since(phaseStart)
		if err != nil {
			return err
		}
		req.AllocationPages = mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)
		req.NormalizeExtentMappings = mergeExtentIDs(bootstrapExtentIDs, req.NormalizeExtentMappings)
		prepared[i] = req
	}

	if txkv, ok := r.kv.(transactionalKV); ok {
		var asyncFinalizeReqs []ApplyCommittedWriteEffectsRequest
		phaseStart := time.Now()
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			callbackStart := time.Now()
			defer func() {
				timings.transactionCallback += time.Since(callbackStart)
			}()
			store := newCachedKVReadWriter(tx)
			var err error
			asyncFinalizeReqs, err = r.applyCommittedWriteEffectsBatchWithStore(ctx, store, prepared, &timings)
			if err != nil {
				return err
			}
			timings.transactionDirtyKeys += store.DirtyCount()
			flushStart := time.Now()
			err = store.Flush(ctx)
			timings.transactionFlush += time.Since(flushStart)
			return err
		})
		timings.transaction = time.Since(phaseStart)
		if err == nil {
			r.finalizeWriteMutationsAsync(ctx, asyncFinalizeReqs)
		}
		return err
	}

	phaseStart := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	callbackStart := time.Now()
	store := newCachedKVReadWriter(r.kv)
	asyncFinalizeReqs, err := r.applyCommittedWriteEffectsBatchWithStore(ctx, store, prepared, &timings)
	if err != nil {
		timings.transactionCallback += time.Since(callbackStart)
		timings.transaction = time.Since(phaseStart)
		return err
	}
	timings.transactionDirtyKeys += store.DirtyCount()
	flushStart := time.Now()
	if err := store.Flush(ctx); err != nil {
		timings.transactionFlush += time.Since(flushStart)
		timings.transactionCallback += time.Since(callbackStart)
		timings.transaction = time.Since(phaseStart)
		return err
	}
	timings.transactionFlush += time.Since(flushStart)
	timings.transactionCallback += time.Since(callbackStart)
	timings.transaction = time.Since(phaseStart)
	r.finalizeWriteMutationsAsync(ctx, asyncFinalizeReqs)
	return nil
}

func (r *Repository) CommitAppendOnlyWriteMetadataBatch(ctx context.Context, reqs []CommitWriteMetadataRequest) (states []VolumeState, records []IdempotencyRecord, err error) {
	if len(reqs) == 0 {
		return nil, nil, nil
	}
	start := time.Now()
	prepared := make([]CommitWriteMetadataRequest, len(reqs))
	var timings committedWriteEffectsTimings
	var stateTimings appendOnlyWriteStateTimings
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("request_count", len(reqs)),
			structuredlog.F("volume_id", reqs[0].VolumeID),
			structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
			structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
			structuredlog.F("append_only_state_commit_duration_ms", stateTimings.transaction.Milliseconds()),
			structuredlog.F("append_only_state_batch_read_duration_ms", stateTimings.batchRead.Milliseconds()),
			structuredlog.F("append_only_state_volume_state_read_duration_ms", stateTimings.volumeStateRead.Milliseconds()),
			structuredlog.F("append_only_state_idempotency_read_duration_ms", stateTimings.idempotencyRead.Milliseconds()),
			structuredlog.F("append_only_state_idempotency_write_duration_ms", stateTimings.idempotencyWrite.Milliseconds()),
			structuredlog.F("append_only_state_revision_gen_duration_ms", stateTimings.appendRevisionGen.Milliseconds()),
			structuredlog.F("legacy_bootstrap_duration_ms", timings.legacyBootstrap.Milliseconds()),
			structuredlog.F("transaction_duration_ms", timings.transaction.Milliseconds()),
			structuredlog.F("transaction_callback_duration_ms", timings.transactionCallback.Milliseconds()),
			structuredlog.F("transaction_flush_duration_ms", timings.transactionFlush.Milliseconds()),
			structuredlog.F("transaction_runner_overhead_duration_ms", timings.transactionRunnerOverhead().Milliseconds()),
			structuredlog.F("transaction_dirty_key_count", timings.transactionDirtyKeys),
			structuredlog.F("idempotency_check_duration_ms", timings.idempotencyCheck.Milliseconds()),
			structuredlog.F("allocation_page_persist_duration_ms", timings.allocationPagePersist.Milliseconds()),
			structuredlog.F("extent_mapping_normalize_duration_ms", timings.extentMappingNormalize.Milliseconds()),
			structuredlog.F("mutation_finalize_duration_ms", timings.mutationFinalize.Milliseconds()),
			structuredlog.F("async_mutation_finalize_count", timings.asyncMutationFinalize),
			structuredlog.F("committed_replay_count", timings.committedReplayCount),
			structuredlog.F("normalize_extent_requested_count", timings.normalizeStats.requested),
			structuredlog.F("normalize_extent_read_count", timings.normalizeStats.read),
			structuredlog.F("normalize_extent_write_count", timings.normalizeStats.written),
			structuredlog.F("normalize_extent_skip_count", timings.normalizeStats.skipped),
			structuredlog.F("normalize_extent_already_normalized_count", timings.normalizeStats.alreadyNormalized),
			structuredlog.F("normalize_extent_revision_advanced_count", timings.normalizeStats.revisionAdvanced),
			structuredlog.F("normalize_extent_revision_preserved_count", timings.normalizeStats.revisionPreserved),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("sbs.metadata", "write_session_append_only_metadata_batch_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.metadata", "write_session_append_only_metadata_batch", fields...)
		// Preserve the existing Phase X attribution event while the batch envelope now
		// also includes append-only idempotency commit work.
		structuredlog.Info("sbs.metadata", "write_session_effects_apply_batch", fields...)
	}()

	for i, req := range reqs {
		phaseStart := time.Now()
		bootstrapPages, bootstrapExtentIDs, err := r.prepareLegacyAllocationBootstrap(ctx, CommitWriteMetadataRequest{
			VolumeID:        req.VolumeID,
			AllocationPages: req.AllocationPages,
		})
		timings.legacyBootstrap += time.Since(phaseStart)
		if err != nil {
			return nil, nil, err
		}
		req.AllocationPages = mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)
		req.NormalizeExtentMappings = mergeExtentIDs(bootstrapExtentIDs, req.NormalizeExtentMappings)
		prepared[i] = req
	}

	if txkv, ok := r.kv.(transactionalKV); ok {
		var asyncFinalizeReqs []ApplyCommittedWriteEffectsRequest
		phaseStart := time.Now()
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			callbackStart := time.Now()
			defer func() {
				timings.transactionCallback += time.Since(callbackStart)
			}()
			store := newCachedKVReadWriter(tx)
			var err error
			states, records, asyncFinalizeReqs, err = r.commitAppendOnlyWriteMetadataBatchWithStore(ctx, store, prepared, &timings, &stateTimings)
			if err != nil {
				return err
			}
			timings.transactionDirtyKeys += store.DirtyCount()
			flushStart := time.Now()
			err = store.Flush(ctx)
			timings.transactionFlush += time.Since(flushStart)
			return err
		})
		timings.transaction = time.Since(phaseStart)
		stateTimings.transaction = timings.transaction
		if err == nil {
			r.finalizeWriteMutationsAsync(ctx, asyncFinalizeReqs)
		}
		return states, records, err
	}

	phaseStart := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	callbackStart := time.Now()
	store := newCachedKVReadWriter(r.kv)
	var asyncFinalizeReqs []ApplyCommittedWriteEffectsRequest
	states, records, asyncFinalizeReqs, err = r.commitAppendOnlyWriteMetadataBatchWithStore(ctx, store, prepared, &timings, &stateTimings)
	if err != nil {
		timings.transactionCallback += time.Since(callbackStart)
		timings.transaction = time.Since(phaseStart)
		stateTimings.transaction = timings.transaction
		return nil, nil, err
	}
	timings.transactionDirtyKeys += store.DirtyCount()
	flushStart := time.Now()
	if err := store.Flush(ctx); err != nil {
		timings.transactionFlush += time.Since(flushStart)
		timings.transactionCallback += time.Since(callbackStart)
		timings.transaction = time.Since(phaseStart)
		stateTimings.transaction = timings.transaction
		return nil, nil, err
	}
	timings.transactionFlush += time.Since(flushStart)
	timings.transactionCallback += time.Since(callbackStart)
	timings.transaction = time.Since(phaseStart)
	stateTimings.transaction = timings.transaction
	r.finalizeWriteMutationsAsync(ctx, asyncFinalizeReqs)
	return states, records, nil
}

func (r *Repository) CommitWriteMetadata(ctx context.Context, req CommitWriteMetadataRequest) (VolumeState, IdempotencyRecord, error) {
	bootstrapPages, bootstrapExtentIDs, err := r.prepareLegacyAllocationBootstrap(ctx, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	allocationPages := mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)
	normalizeExtentIDs := mergeExtentIDs(bootstrapExtentIDs, req.NormalizeExtentMappings)

	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitWriteMetadataWithStore(ctx, tx, req, allocationPages, normalizeExtentIDs)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitWriteMetadataWithStore(ctx, r.kv, req, allocationPages, normalizeExtentIDs)
}

func (r *Repository) CommitPageScopedWriteMetadata(ctx context.Context, req CommitWriteMetadataRequest) (VolumeState, IdempotencyRecord, error) {
	bootstrapPages, bootstrapExtentIDs, err := r.prepareLegacyAllocationBootstrap(ctx, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	allocationPages := mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)
	normalizeExtentIDs := mergeExtentIDs(bootstrapExtentIDs, req.NormalizeExtentMappings)

	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitPageScopedWriteMetadataWithStore(ctx, tx, req, allocationPages, normalizeExtentIDs)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitPageScopedWriteMetadataWithStore(ctx, r.kv, req, allocationPages, normalizeExtentIDs)
}

func (r *Repository) CommitRangeLocalWriteState(ctx context.Context, req CommitWriteMetadataRequest) (VolumeState, IdempotencyRecord, error) {
	bootstrapPages, _, err := r.prepareLegacyAllocationBootstrap(ctx, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	allocationPages := mergeAllocationPagesByPageNo(bootstrapPages, req.AllocationPages)

	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err = txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitRangeLocalWriteStateWithStore(ctx, tx, req, allocationPages)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitRangeLocalWriteStateWithStore(ctx, r.kv, req, allocationPages)
}

func (r *Repository) CommitECFullStripeWrite(ctx context.Context, req CommitECFullStripeWriteRequest) (VolumeState, IdempotencyRecord, error) {
	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitECFullStripeWriteWithStore(ctx, tx, req)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitECFullStripeWriteWithStore(ctx, r.kv, req)
}

func (r *Repository) CommitECDiscard(ctx context.Context, req CommitECDiscardRequest) (VolumeState, IdempotencyRecord, error) {
	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var record IdempotencyRecord
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, record, err = r.commitECDiscardWithStore(ctx, tx, req)
			return err
		})
		return state, record, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitECDiscardWithStore(ctx, r.kv, req)
}

func (r *Repository) commitWriteMetadataWithStore(ctx context.Context, store kvReadWriter, req CommitWriteMetadataRequest, allocationPages []AllocationPageRecord, normalizeExtentIDs []uint64) (VolumeState, IdempotencyRecord, error) {
	stateReq := req.StateCommitRequest()
	effectsReq := req.EffectsApplyRequest()
	effectsReq.AllocationPages = allocationPages
	effectsReq.NormalizeExtentMappings = normalizeExtentIDs

	state, record, err := r.readAndValidateWriteCommitState(ctx, store, stateReq)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.persistCommittedWriteState(ctx, store, state, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.persistCommittedAllocationPages(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if _, err := r.normalizeCommittedExtentMappings(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.finalizeWriteMutationOperation(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (r *Repository) commitPageScopedWriteMetadataWithStore(ctx context.Context, store kvReadWriter, req CommitWriteMetadataRequest, allocationPages []AllocationPageRecord, normalizeExtentIDs []uint64) (VolumeState, IdempotencyRecord, error) {
	if len(allocationPages) == 0 {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("page-scoped write commit requires allocation pages")
	}
	state, record, err := r.readAndValidatePageScopedWriteCommitState(ctx, store, req.StateCommitRequest())
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	pages, committedRevision, err := r.preparePageScopedCommittedAllocationPages(ctx, store, allocationPages)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if committedRevision == 0 {
		committedRevision = req.CommittedRevision
	}
	record.Revision = committedRevision
	record.ResultState = IdempotencyCommitted
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	effectsReq := req.EffectsApplyRequest()
	effectsReq.CommittedRevision = committedRevision
	effectsReq.AllocationPages = pages
	effectsReq.NormalizeExtentMappings = normalizeExtentIDs
	if err := r.persistPageScopedCommittedAllocationPages(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if _, err := r.normalizeCommittedExtentMappings(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.finalizeWriteMutationOperation(ctx, store, effectsReq); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (r *Repository) commitRangeLocalWriteStateWithStore(ctx context.Context, store kvReadWriter, req CommitWriteMetadataRequest, allocationPages []AllocationPageRecord) (VolumeState, IdempotencyRecord, error) {
	if len(allocationPages) == 0 {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("range-local write state commit requires allocation pages")
	}
	state, record, err := r.readAndValidatePageScopedWriteCommitState(ctx, store, req.StateCommitRequest())
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	committedRevision, err := r.advanceRangeLocalWriteState(ctx, store, req.VolumeID, req.IdempotencyKey, allocationPages)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	record.Revision = committedRevision
	record.ResultState = IdempotencyCommitted
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (r *Repository) commitECFullStripeWriteWithStore(ctx context.Context, store kvReadWriter, req CommitECFullStripeWriteRequest) (VolumeState, IdempotencyRecord, error) {
	if len(req.AllocationPages) == 0 {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("ec full-stripe write commit requires allocation pages")
	}
	var err error
	var flushPrefetchStore *cachedKVReadWriter
	store, flushPrefetchStore, err = r.prefetchECFullStripeWriteCommitReads(ctx, store, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	stateReq := CommitWriteStateRequest{
		VolumeID:                 req.VolumeID,
		ExpectedEpoch:            req.ExpectedEpoch,
		ExpectedRevision:         req.ExpectedRevision,
		IdempotencyKey:           req.IdempotencyKey,
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        req.CommittedRevision,
	}
	state, record, err := r.readAndValidatePageScopedWriteCommitState(ctx, store, stateReq)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	pages, committedRevision, err := r.preparePageScopedCommittedAllocationPagesWithFloor(ctx, store, req.AllocationPages, req.CommittedRevision)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if committedRevision == 0 {
		committedRevision = req.CommittedRevision
	}

	operation, err := r.readECFullStripeCommitMutationOperation(ctx, store, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if operation.State != req.ExpectedMutationState {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}

	object := NormalizePhysicalObjectRecord(req.PhysicalObject)
	object.State = PhysicalObjectStateCommitted
	object.UpdatedAtUnix = time.Now().Unix()
	if err := ValidatePhysicalObjectRecord(object); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	stripe := NormalizeECStripeRecord(req.ECStripe)
	stripe.State = ECStripeStateCommitted
	stripe.UpdatedAtUnix = object.UpdatedAtUnix
	if err := ValidateECStripeRecord(stripe); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if stripe.ObjectID != object.ObjectID {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("ec stripe object_id=%q does not match physical object %q", stripe.ObjectID, object.ObjectID)
	}
	retiredObjects, err := r.prepareRetiredECObjects(ctx, store, req.VolumeID, object.ObjectID, req.RetiredECObjects)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	if err := putJSONStore(ctx, store, physicalObjectKey(r.root, object.VolumeID, object.ObjectID), object); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := putJSONStore(ctx, store, ecStripeKey(r.root, stripe.VolumeID, stripe.StripeID, stripe.StripeGeneration), stripe); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	for _, page := range pages {
		page.VolumeID = mustCanonicalVolumeID(page.VolumeID)
		if err := writeAllocationPage(ctx, store, r.root, page); err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
		r.rememberNativeAllocationVolume(page.VolumeID)
	}

	record.Revision = committedRevision
	record.ResultState = IdempotencyCommitted
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	operation.State = MutationOperationCommitted
	operation.AllocationRevision = committedRevision
	operation.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	operation.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	operation.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	operation.LastUpdatedAtUnix = time.Now().Unix()
	if err := writeMutationOperation(ctx, store, r.root, operation); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.writeRetiredECObjects(ctx, store, retiredObjects, operation.LastUpdatedAtUnix); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if flushPrefetchStore != nil {
		if err := flushPrefetchStore.Flush(ctx); err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
	}
	return state, record, nil
}

func (r *Repository) prefetchECFullStripeWriteCommitReads(ctx context.Context, store kvReadWriter, req CommitECFullStripeWriteRequest) (kvReadWriter, *cachedKVReadWriter, error) {
	if _, ok := store.(kvBatchReader); !ok {
		return store, nil, nil
	}
	cached, ok := store.(*cachedKVReadWriter)
	var flushStore *cachedKVReadWriter
	if !ok {
		cached = newCachedKVReadWriter(store)
		flushStore = cached
	}
	keys := make([]string, 0, 2+len(req.AllocationPages))
	seen := make(map[string]struct{}, 2+len(req.AllocationPages))
	addKey := func(key string) {
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	addKey(volumeStateKey(r.root, req.VolumeID))
	addKey(idempotencyKey(r.root, req.VolumeID, req.IdempotencyKey))
	for _, page := range req.AllocationPages {
		addKey(allocationPageKey(r.root, page.VolumeID, page.PageNo))
	}
	if len(keys) == 0 {
		return cached, flushStore, nil
	}
	if _, err := cached.BatchGet(ctx, keys); err != nil {
		return store, nil, err
	}
	return cached, flushStore, nil
}

func (r *Repository) readECFullStripeCommitMutationOperation(ctx context.Context, store kvReadWriter, req CommitECFullStripeWriteRequest) (MutationOperationRecord, error) {
	if operation, ok := ecFullStripeCommitMutationOperationFromRequest(req); ok {
		return operation, nil
	}
	operation, err := readMutationOperation(ctx, store, r.root, req.VolumeID, req.MutationOperationID)
	if err != nil {
		return MutationOperationRecord{}, err
	}
	if operation.State != req.ExpectedMutationState {
		return MutationOperationRecord{}, ErrCASConflict
	}
	return operation, nil
}

func ecFullStripeCommitMutationOperationFromRequest(req CommitECFullStripeWriteRequest) (MutationOperationRecord, bool) {
	operation := cloneMutationOperationRecord(req.MutationOperation)
	if req.MutationOperationID == "" || operation.OperationID != req.MutationOperationID {
		return MutationOperationRecord{}, false
	}
	if mustCanonicalVolumeID(operation.VolumeID) != mustCanonicalVolumeID(req.VolumeID) {
		return MutationOperationRecord{}, false
	}
	if operation.State != req.ExpectedMutationState {
		return MutationOperationRecord{}, false
	}
	if operation.IdempotencyKey != "" && operation.IdempotencyKey != req.IdempotencyKey {
		return MutationOperationRecord{}, false
	}
	if operation.WriterFencingEpoch != 0 && operation.WriterFencingEpoch != req.ExpectedEpoch {
		return MutationOperationRecord{}, false
	}
	return operation, true
}

func (r *Repository) commitECDiscardWithStore(ctx context.Context, store kvReadWriter, req CommitECDiscardRequest) (VolumeState, IdempotencyRecord, error) {
	if len(req.AllocationPages) == 0 {
		return VolumeState{}, IdempotencyRecord{}, fmt.Errorf("ec discard commit requires allocation pages")
	}
	stateReq := CommitWriteStateRequest{
		VolumeID:                 req.VolumeID,
		ExpectedEpoch:            req.ExpectedEpoch,
		ExpectedRevision:         req.ExpectedRevision,
		IdempotencyKey:           req.IdempotencyKey,
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        req.CommittedRevision,
	}
	state, record, err := r.readAndValidatePageScopedWriteCommitState(ctx, store, stateReq)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	pages, committedRevision, err := r.preparePageScopedCommittedAllocationPagesWithFloor(ctx, store, req.AllocationPages, req.CommittedRevision)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if committedRevision == 0 {
		committedRevision = req.CommittedRevision
	}

	operation, err := readMutationOperation(ctx, store, r.root, req.VolumeID, req.MutationOperationID)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if operation.State != req.ExpectedMutationState {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}
	retiredObjects, err := r.prepareRetiredECObjects(ctx, store, req.VolumeID, "", req.RetiredECObjects)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	for _, page := range pages {
		page.VolumeID = mustCanonicalVolumeID(page.VolumeID)
		if err := writeAllocationPage(ctx, store, r.root, page); err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
		r.rememberNativeAllocationVolume(page.VolumeID)
	}

	record.Revision = committedRevision
	record.ResultState = IdempotencyCommitted
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	nowUnix := time.Now().Unix()
	operation.State = MutationOperationCommitted
	operation.AllocationRevision = committedRevision
	operation.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	operation.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	operation.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	operation.LastUpdatedAtUnix = nowUnix
	if err := writeMutationOperation(ctx, store, r.root, operation); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.writeRetiredECObjects(ctx, store, retiredObjects, nowUnix); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return state, record, nil
}

type retiredECObjectRecords struct {
	object PhysicalObjectRecord
	stripe ECStripeRecord
}

func (r *Repository) prepareRetiredECObjects(ctx context.Context, store kvReadWriter, volumeID, currentObjectID string, refs []RetiredECObjectRef) ([]retiredECObjectRecords, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	canonVolumeID, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return nil, fmt.Errorf("invalid retired ec object volume_id %q: %w", volumeID, err)
	}
	currentObjectID = strings.TrimSpace(currentObjectID)
	out := make([]retiredECObjectRecords, 0, len(refs))
	for _, ref := range refs {
		ref.ObjectID = strings.TrimSpace(ref.ObjectID)
		ref.StripeID = strings.TrimSpace(ref.StripeID)
		if err := validatePhysicalObjectID(ref.ObjectID); err != nil {
			return nil, fmt.Errorf("retired ec object: %w", err)
		}
		if ref.ObjectID == currentObjectID {
			return nil, fmt.Errorf("retired ec object %q matches current object", ref.ObjectID)
		}
		if err := validateECStripeID(ref.StripeID); err != nil {
			return nil, fmt.Errorf("retired ec object %q: %w", ref.ObjectID, err)
		}
		if ref.StripeGeneration == 0 {
			return nil, fmt.Errorf("retired ec object %q stripe_generation is required", ref.ObjectID)
		}

		var object PhysicalObjectRecord
		if err := getJSONStore(ctx, store, physicalObjectKey(r.root, canonVolumeID, ref.ObjectID), &object); err != nil {
			return nil, err
		}
		object = NormalizePhysicalObjectRecord(object)
		if object.BackendType != PhysicalObjectBackendEC || object.EC == nil {
			return nil, fmt.Errorf("retired ec object %q is not an ec physical object", ref.ObjectID)
		}
		if object.EC.StripeID != ref.StripeID || object.EC.StripeGeneration != ref.StripeGeneration {
			return nil, fmt.Errorf("retired ec object %q descriptor stripe=%s/%d want=%s/%d",
				ref.ObjectID, object.EC.StripeID, object.EC.StripeGeneration, ref.StripeID, ref.StripeGeneration)
		}
		switch object.State {
		case PhysicalObjectStateCommitted, PhysicalObjectStateRetired:
		default:
			return nil, fmt.Errorf("retired ec object %q state=%q is not committed", ref.ObjectID, object.State)
		}

		var stripe ECStripeRecord
		if err := getJSONStore(ctx, store, ecStripeKey(r.root, canonVolumeID, ref.StripeID, ref.StripeGeneration), &stripe); err != nil {
			return nil, err
		}
		stripe = NormalizeECStripeRecord(stripe)
		if stripe.ObjectID != ref.ObjectID {
			return nil, fmt.Errorf("retired ec stripe %s/%d object_id=%q want=%q", ref.StripeID, ref.StripeGeneration, stripe.ObjectID, ref.ObjectID)
		}
		switch stripe.State {
		case ECStripeStateCommitted, ECStripeStateRetired:
		default:
			return nil, fmt.Errorf("retired ec stripe %s/%d state=%q is not committed", ref.StripeID, ref.StripeGeneration, stripe.State)
		}
		out = append(out, retiredECObjectRecords{object: object, stripe: stripe})
	}
	return out, nil
}

func (r *Repository) writeRetiredECObjects(ctx context.Context, store kvReadWriter, records []retiredECObjectRecords, updatedAtUnix int64) error {
	for _, rec := range records {
		rec.object.State = PhysicalObjectStateRetired
		rec.object.UpdatedAtUnix = updatedAtUnix
		rec.stripe.State = ECStripeStateRetired
		rec.stripe.UpdatedAtUnix = updatedAtUnix
		if err := putJSONStore(ctx, store, physicalObjectKey(r.root, rec.object.VolumeID, rec.object.ObjectID), rec.object); err != nil {
			return err
		}
		if err := putJSONStore(ctx, store, ecStripeKey(r.root, rec.stripe.VolumeID, rec.stripe.StripeID, rec.stripe.StripeGeneration), rec.stripe); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) commitWriteStateWithStore(ctx context.Context, store kvReadWriter, req CommitWriteStateRequest) (VolumeState, IdempotencyRecord, error) {
	state, record, err := r.readAndValidateWriteCommitState(ctx, store, req)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if err := r.persistCommittedWriteState(ctx, store, state, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (r *Repository) commitAppendOnlyWriteStateWithStore(ctx context.Context, store kvReadWriter, req CommitWriteStateRequest, timings *appendOnlyWriteStateTimings) (VolumeState, IdempotencyRecord, error) {
	state, record, err := r.readAndValidateAppendOnlyWriteCommitState(ctx, store, req, timings)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	phaseStart := time.Now()
	revision := r.nextAppendOnlyWriteRevision(req.CommittedRevision)
	if timings != nil {
		timings.appendRevisionGen += time.Since(phaseStart)
	}
	state.Revision = revision
	record.Revision = revision
	record.ResultState = IdempotencyCommitted
	phaseStart = time.Now()
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if timings != nil {
		timings.idempotencyWrite += time.Since(phaseStart)
	}
	return state, record, nil
}

func (r *Repository) nextAppendOnlyWriteRevision(floor uint64) uint64 {
	r.appendOnlyRevisionMu.Lock()
	defer r.appendOnlyRevisionMu.Unlock()
	next := uint64(time.Now().UnixNano())
	if next < floor {
		next = floor
	}
	if next <= r.lastAppendOnlyRevision {
		next = r.lastAppendOnlyRevision + 1
	}
	r.lastAppendOnlyRevision = next
	return next
}

func (r *Repository) advanceRangeLocalWriteState(ctx context.Context, store kvReadWriter, volumeID, idempotencyKey string, pages []AllocationPageRecord) (uint64, error) {
	pageNos := make([]uint64, 0, len(pages))
	initialRevisions := make(map[uint64]uint64, len(pages))
	for _, page := range pages {
		pageNos = append(pageNos, page.PageNo)
		if page.Revision > initialRevisions[page.PageNo] {
			initialRevisions[page.PageNo] = page.Revision
		}
	}
	sort.Slice(pageNos, func(i, j int) bool { return pageNos[i] < pageNos[j] })
	pageNos = slices.Compact(pageNos)

	var maxRevision uint64
	nowUnix := time.Now().Unix()
	for _, pageNo := range pageNos {
		current, err := readRangeLocalWriteState(ctx, store, r.root, volumeID, pageNo)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return 0, err
			}
			current = rangeLocalWriteStateRecord{
				VolumeID: mustCanonicalVolumeID(volumeID),
				PageNo:   pageNo,
				Revision: initialRevisions[pageNo],
			}
		}
		current.Revision++
		current.IdempotencyKey = idempotencyKey
		current.UpdatedAtUnix = nowUnix
		if current.Revision > maxRevision {
			maxRevision = current.Revision
		}
		if err := writeRangeLocalWriteState(ctx, store, r.root, current); err != nil {
			return 0, err
		}
	}
	return maxRevision, nil
}

func (r *Repository) hasObservedNativeAllocationVolume(volumeID string) bool {
	r.nativeAllocationVolumeMu.RLock()
	defer r.nativeAllocationVolumeMu.RUnlock()
	_, ok := r.nativeAllocationVolumesObserved[mustCanonicalVolumeID(volumeID)]
	return ok
}

func (r *Repository) rememberNativeAllocationVolume(volumeID string) {
	r.nativeAllocationVolumeMu.Lock()
	defer r.nativeAllocationVolumeMu.Unlock()
	r.nativeAllocationVolumesObserved[mustCanonicalVolumeID(volumeID)] = struct{}{}
}

func (r *Repository) forgetNativeAllocationVolume(volumeID string) {
	r.nativeAllocationVolumeMu.Lock()
	defer r.nativeAllocationVolumeMu.Unlock()
	delete(r.nativeAllocationVolumesObserved, mustCanonicalVolumeID(volumeID))
}

func (r *Repository) applyCommittedWriteEffectsWithStore(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest, timings *committedWriteEffectsTimings) error {
	phaseStart := time.Now()
	mutation, err := r.readCommittedWriteMutationSnapshot(ctx, store, req)
	if timings != nil {
		timings.idempotencyCheck += time.Since(phaseStart)
		timings.committedReplay = timings.committedReplay || mutation.committed
		if mutation.committed {
			timings.committedReplayCount++
		}
	}
	if err != nil {
		return err
	}
	if mutation.committed {
		return nil
	}
	phaseStart = time.Now()
	if err := r.persistCommittedAllocationPages(ctx, store, req); err != nil {
		return err
	}
	if timings != nil {
		timings.allocationPagePersist += time.Since(phaseStart)
	}
	phaseStart = time.Now()
	normalizeStats, err := r.normalizeCommittedExtentMappings(ctx, store, req)
	if err != nil {
		return err
	}
	if timings != nil {
		timings.extentMappingNormalize += time.Since(phaseStart)
		timings.normalizeStats.add(normalizeStats)
	}
	phaseStart = time.Now()
	if err := r.finalizeWriteMutationOperationFromSnapshot(ctx, store, req, mutation); err != nil {
		return err
	}
	if timings != nil {
		timings.mutationFinalize += time.Since(phaseStart)
	}
	return nil
}

func (r *Repository) applyCommittedWriteEffectsBatchWithStore(ctx context.Context, store kvReadWriter, reqs []ApplyCommittedWriteEffectsRequest, timings *committedWriteEffectsTimings) ([]ApplyCommittedWriteEffectsRequest, error) {
	if hasDuplicateMutationOperationIDs(reqs) {
		for _, req := range reqs {
			if err := r.applyCommittedWriteEffectsWithStore(ctx, store, req, timings); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	active := make([]committedWriteEffectsBatchItem, 0, len(reqs))
	for _, req := range reqs {
		phaseStart := time.Now()
		mutation, err := r.readCommittedWriteMutationSnapshot(ctx, store, req)
		if timings != nil {
			timings.idempotencyCheck += time.Since(phaseStart)
			timings.committedReplay = timings.committedReplay || mutation.committed
			if mutation.committed {
				timings.committedReplayCount++
			}
		}
		if err != nil {
			return nil, err
		}
		if mutation.committed {
			continue
		}
		active = append(active, committedWriteEffectsBatchItem{
			req:      req,
			mutation: mutation,
		})
	}
	if len(active) == 0 {
		return nil, nil
	}

	phaseStart := time.Now()
	if err := r.persistCommittedAllocationPagesForBatch(ctx, store, active); err != nil {
		return nil, err
	}
	if timings != nil {
		timings.allocationPagePersist += time.Since(phaseStart)
	}

	var asyncFinalizeReqs []ApplyCommittedWriteEffectsRequest
	for _, item := range active {
		phaseStart = time.Now()
		normalizeStats, err := r.normalizeCommittedExtentMappings(ctx, store, item.req)
		if err != nil {
			return nil, err
		}
		if timings != nil {
			timings.extentMappingNormalize += time.Since(phaseStart)
			timings.normalizeStats.add(normalizeStats)
		}

		if r.asyncWriteMutationFinalize {
			if err := validateWriteMutationSnapshotForFinalize(item.req, item.mutation); err != nil {
				return nil, err
			}
			if item.req.MutationOperationID != "" && !item.mutation.committed {
				asyncFinalizeReqs = append(asyncFinalizeReqs, cloneApplyCommittedWriteEffectsRequest(item.req))
			}
			continue
		}
		phaseStart = time.Now()
		if err := r.finalizeWriteMutationOperationFromSnapshot(ctx, store, item.req, item.mutation); err != nil {
			return nil, err
		}
		if timings != nil {
			timings.mutationFinalize += time.Since(phaseStart)
		}
	}
	if timings != nil {
		timings.asyncMutationFinalize += len(asyncFinalizeReqs)
	}
	return asyncFinalizeReqs, nil
}

func (r *Repository) commitAppendOnlyWriteStatesBatchWithStore(ctx context.Context, store kvReadWriter, reqs []CommitWriteStateRequest, timings *appendOnlyWriteStateTimings) ([]VolumeState, []IdempotencyRecord, error) {
	if len(reqs) == 0 {
		return nil, nil, nil
	}
	if hasDuplicateCommitIdempotencyKeys(reqs) {
		states := make([]VolumeState, 0, len(reqs))
		records := make([]IdempotencyRecord, 0, len(reqs))
		for _, req := range reqs {
			state, record, err := r.commitAppendOnlyWriteStateWithStore(ctx, store, req, timings)
			if err != nil {
				return nil, nil, err
			}
			states = append(states, state)
			records = append(records, record)
		}
		return states, records, nil
	}
	snapshots, err := r.readAndValidateAppendOnlyWriteCommitStatesBatch(ctx, store, reqs, timings)
	if err != nil {
		return nil, nil, err
	}
	states := make([]VolumeState, 0, len(reqs))
	records := make([]IdempotencyRecord, 0, len(reqs))
	for i, req := range reqs {
		state := snapshots[i].state
		record := snapshots[i].record
		phaseStart := time.Now()
		revision := r.nextAppendOnlyWriteRevision(req.CommittedRevision)
		if timings != nil {
			timings.appendRevisionGen += time.Since(phaseStart)
		}
		state.Revision = revision
		record.Revision = revision
		record.ResultState = IdempotencyCommitted
		phaseStart = time.Now()
		if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
			return nil, nil, err
		}
		if timings != nil {
			timings.idempotencyWrite += time.Since(phaseStart)
		}
		states = append(states, state)
		records = append(records, record)
	}
	return states, records, nil
}

func (r *Repository) commitAppendOnlyWriteMetadataBatchWithStore(ctx context.Context, store kvReadWriter, reqs []CommitWriteMetadataRequest, timings *committedWriteEffectsTimings, stateTimings *appendOnlyWriteStateTimings) ([]VolumeState, []IdempotencyRecord, []ApplyCommittedWriteEffectsRequest, error) {
	states := make([]VolumeState, 0, len(reqs))
	records := make([]IdempotencyRecord, 0, len(reqs))
	effectsReqs := make([]ApplyCommittedWriteEffectsRequest, 0, len(reqs))
	if hasDuplicateCommitMutationOperationIDs(reqs) {
		for _, req := range reqs {
			state, record, err := r.commitAppendOnlyWriteStateWithStore(ctx, store, req.StateCommitRequest(), stateTimings)
			if err != nil {
				return nil, nil, nil, err
			}
			effectsReq := req.EffectsApplyRequest()
			if record.Revision != 0 {
				effectsReq.CommittedRevision = record.Revision
			}
			if err := r.applyCommittedWriteEffectsWithStore(ctx, store, effectsReq, timings); err != nil {
				return nil, nil, nil, err
			}
			states = append(states, state)
			records = append(records, record)
		}
		return states, records, nil, nil
	}

	stateReqs := make([]CommitWriteStateRequest, len(reqs))
	for i, req := range reqs {
		stateReqs[i] = req.StateCommitRequest()
	}
	states, records, err := r.commitAppendOnlyWriteStatesBatchWithStore(ctx, store, stateReqs, stateTimings)
	if err != nil {
		return nil, nil, nil, err
	}
	for i, req := range reqs {
		effectsReq := req.EffectsApplyRequest()
		if records[i].Revision != 0 {
			effectsReq.CommittedRevision = records[i].Revision
		}
		effectsReqs = append(effectsReqs, effectsReq)
	}
	asyncFinalizeReqs, err := r.applyCommittedWriteEffectsBatchWithStore(ctx, store, effectsReqs, timings)
	if err != nil {
		return nil, nil, nil, err
	}
	return states, records, asyncFinalizeReqs, nil
}

func validateWriteMutationSnapshotForFinalize(req ApplyCommittedWriteEffectsRequest, mutation committedWriteMutationSnapshot) error {
	if req.MutationOperationID == "" || mutation.committed {
		return nil
	}
	if !mutation.found {
		return ErrNotFound
	}
	if mutation.operation.State != req.ExpectedMutationState {
		return ErrCASConflict
	}
	return nil
}

func (r *Repository) finalizeWriteMutationsAsync(ctx context.Context, reqs []ApplyCommittedWriteEffectsRequest) {
	if r == nil || !r.asyncWriteMutationFinalize || len(reqs) == 0 {
		return
	}
	reqs = cloneApplyCommittedWriteEffectsRequests(reqs)
	go func() {
		start := time.Now()
		finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err := r.finalizeWriteMutationsWithRetry(finalizeCtx, reqs)
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", reqs[0].VolumeID),
			structuredlog.F("request_count", len(reqs)),
			structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
			structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("sbs.metadata", "write_session_effects_mutation_finalize_async_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.metadata", "write_session_effects_mutation_finalize_async_completed", fields...)
	}()
}

func (r *Repository) finalizeWriteMutationsWithRetry(ctx context.Context, reqs []ApplyCommittedWriteEffectsRequest) error {
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		err = r.finalizeWriteMutationsOnce(ctx, reqs)
		if err == nil {
			return nil
		}
		if attempt == 5 {
			break
		}
		backoff := time.Duration(attempt*5) * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (r *Repository) finalizeWriteMutationsOnce(ctx context.Context, reqs []ApplyCommittedWriteEffectsRequest) error {
	apply := func(store kvReadWriter) error {
		for _, req := range reqs {
			if err := r.finalizeWriteMutationOperation(ctx, store, req); err != nil {
				return err
			}
		}
		return nil
	}
	if txkv, ok := r.kv.(transactionalKV); ok {
		return txkv.RunInTransaction(ctx, apply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return apply(r.kv)
}

func hasDuplicateMutationOperationIDs(reqs []ApplyCommittedWriteEffectsRequest) bool {
	seen := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		if req.MutationOperationID == "" {
			continue
		}
		if _, ok := seen[req.MutationOperationID]; ok {
			return true
		}
		seen[req.MutationOperationID] = struct{}{}
	}
	return false
}

func hasDuplicateCommitMutationOperationIDs(reqs []CommitWriteMetadataRequest) bool {
	seen := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		if req.MutationOperationID == "" {
			continue
		}
		if _, ok := seen[req.MutationOperationID]; ok {
			return true
		}
		seen[req.MutationOperationID] = struct{}{}
	}
	return false
}

func hasDuplicateCommitIdempotencyKeys(reqs []CommitWriteStateRequest) bool {
	seen := make(map[string]struct{}, len(reqs))
	for _, req := range reqs {
		key := mustCanonicalVolumeID(req.VolumeID) + "\x00" + req.IdempotencyKey
		if _, ok := seen[key]; ok {
			return true
		}
		seen[key] = struct{}{}
	}
	return false
}

func (r *Repository) readCommittedWriteMutationSnapshot(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest) (committedWriteMutationSnapshot, error) {
	if req.MutationOperationID == "" {
		return committedWriteMutationSnapshot{}, nil
	}
	if snapshot, ok := committedWriteMutationSnapshotFromRequest(req); ok {
		return snapshot, nil
	}
	operation, err := readMutationOperation(ctx, store, r.root, req.VolumeID, req.MutationOperationID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return committedWriteMutationSnapshot{}, nil
		}
		return committedWriteMutationSnapshot{}, err
	}
	return committedWriteMutationSnapshot{
		operation: operation,
		found:     true,
		committed: req.MatchesCommittedMutationOperation(operation),
	}, nil
}

func committedWriteMutationSnapshotFromRequest(req ApplyCommittedWriteEffectsRequest) (committedWriteMutationSnapshot, bool) {
	operation := cloneMutationOperationRecord(req.MutationOperation)
	if req.MutationOperationID == "" || operation.OperationID != req.MutationOperationID {
		return committedWriteMutationSnapshot{}, false
	}
	if mustCanonicalVolumeID(operation.VolumeID) != mustCanonicalVolumeID(req.VolumeID) {
		return committedWriteMutationSnapshot{}, false
	}
	if operation.State != req.ExpectedMutationState && !req.MatchesCommittedMutationOperation(operation) {
		return committedWriteMutationSnapshot{}, false
	}
	return committedWriteMutationSnapshot{
		operation: operation,
		found:     true,
		committed: req.MatchesCommittedMutationOperation(operation),
	}, true
}

func (r *Repository) readAndValidateWriteCommitState(ctx context.Context, store kvReadWriter, req CommitWriteStateRequest) (VolumeState, IdempotencyRecord, error) {
	state, err := readVolumeState(ctx, store, r.root, req.VolumeID)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if state.Epoch != req.ExpectedEpoch || state.Revision != req.ExpectedRevision {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}

	record, err := readIdempotencyRecord(ctx, store, r.root, req.VolumeID, req.IdempotencyKey)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}
	state.Revision = req.CommittedRevision
	record.Revision = req.CommittedRevision
	record.ResultState = IdempotencyCommitted
	return state, record, nil
}

func (r *Repository) readAndValidatePageScopedWriteCommitState(ctx context.Context, store kvReadWriter, req CommitWriteStateRequest) (VolumeState, IdempotencyRecord, error) {
	state, err := readVolumeState(ctx, store, r.root, req.VolumeID)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if state.Epoch != req.ExpectedEpoch {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}

	record, err := readIdempotencyRecord(ctx, store, r.root, req.VolumeID, req.IdempotencyKey)
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}
	return state, record, nil
}

func (r *Repository) readAndValidateAppendOnlyWriteCommitStatesBatch(ctx context.Context, store kvReadWriter, reqs []CommitWriteStateRequest, timings *appendOnlyWriteStateTimings) ([]appendOnlyWriteStateSnapshot, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	batcher, ok := store.(kvBatchReader)
	if !ok {
		snapshots := make([]appendOnlyWriteStateSnapshot, 0, len(reqs))
		for _, req := range reqs {
			state, record, err := r.readAndValidateAppendOnlyWriteCommitState(ctx, store, req, timings)
			if err != nil {
				return nil, err
			}
			snapshots = append(snapshots, appendOnlyWriteStateSnapshot{state: state, record: record})
		}
		return snapshots, nil
	}

	keys := make([]string, 0, len(reqs)*2)
	seenKeys := make(map[string]struct{}, len(reqs)*2)
	addKey := func(key string) {
		if _, ok := seenKeys[key]; ok {
			return
		}
		seenKeys[key] = struct{}{}
		keys = append(keys, key)
	}
	for _, req := range reqs {
		addKey(volumeStateKey(r.root, req.VolumeID))
		addKey(idempotencyKey(r.root, req.VolumeID, req.IdempotencyKey))
	}

	phaseStart := time.Now()
	values, err := batcher.BatchGet(ctx, keys)
	if timings != nil {
		timings.batchRead += time.Since(phaseStart)
	}
	if err != nil {
		return nil, err
	}

	stateByKey := make(map[string]VolumeState, len(reqs))
	snapshots := make([]appendOnlyWriteStateSnapshot, 0, len(reqs))
	for _, req := range reqs {
		stateKey := volumeStateKey(r.root, req.VolumeID)
		state, ok := stateByKey[stateKey]
		if !ok {
			stateRaw, found := values[stateKey]
			if !found {
				return nil, ErrNotFound
			}
			if err := json.Unmarshal(stateRaw, &state); err != nil {
				return nil, err
			}
			stateByKey[stateKey] = state
		}

		recordKey := idempotencyKey(r.root, req.VolumeID, req.IdempotencyKey)
		recordRaw, found := values[recordKey]
		var record IdempotencyRecord
		if !found {
			var created bool
			state, record, created, err = appendOnlyWriteCommitStateFromMissingIntent(req, state)
			if err != nil {
				return nil, err
			}
			if !created {
				return nil, ErrNotFound
			}
		} else if err := json.Unmarshal(recordRaw, &record); err != nil {
			return nil, err
		} else {
			state, record, err = validateAppendOnlyWriteCommitState(req, state, record)
		}
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, appendOnlyWriteStateSnapshot{state: state, record: record})
	}
	return snapshots, nil
}

func (r *Repository) readAndValidateAppendOnlyWriteCommitState(ctx context.Context, store kvReadWriter, req CommitWriteStateRequest, timings *appendOnlyWriteStateTimings) (VolumeState, IdempotencyRecord, error) {
	if batcher, ok := store.(kvBatchReader); ok {
		stateKey := volumeStateKey(r.root, req.VolumeID)
		recordKey := idempotencyKey(r.root, req.VolumeID, req.IdempotencyKey)
		phaseStart := time.Now()
		values, err := batcher.BatchGet(ctx, []string{stateKey, recordKey})
		if timings != nil {
			timings.batchRead += time.Since(phaseStart)
		}
		if err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
		stateRaw, ok := values[stateKey]
		if !ok {
			return VolumeState{}, IdempotencyRecord{}, ErrNotFound
		}
		var state VolumeState
		if err := json.Unmarshal(stateRaw, &state); err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
		recordRaw, ok := values[recordKey]
		if !ok {
			state, record, created, err := appendOnlyWriteCommitStateFromMissingIntent(req, state)
			if err != nil {
				return VolumeState{}, IdempotencyRecord{}, err
			}
			if !created {
				return VolumeState{}, IdempotencyRecord{}, ErrNotFound
			}
			return state, record, nil
		}
		var record IdempotencyRecord
		if err := json.Unmarshal(recordRaw, &record); err != nil {
			return VolumeState{}, IdempotencyRecord{}, err
		}
		return validateAppendOnlyWriteCommitState(req, state, record)
	}

	phaseStart := time.Now()
	state, err := readVolumeState(ctx, store, r.root, req.VolumeID)
	if timings != nil {
		timings.volumeStateRead += time.Since(phaseStart)
	}
	if err != nil {
		return VolumeState{}, IdempotencyRecord{}, err
	}

	phaseStart = time.Now()
	record, err := readIdempotencyRecord(ctx, store, r.root, req.VolumeID, req.IdempotencyKey)
	if timings != nil {
		timings.idempotencyRead += time.Since(phaseStart)
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			state, record, created, createErr := appendOnlyWriteCommitStateFromMissingIntent(req, state)
			if createErr != nil {
				return VolumeState{}, IdempotencyRecord{}, createErr
			}
			if created {
				return state, record, nil
			}
		}
		return VolumeState{}, IdempotencyRecord{}, err
	}
	return validateAppendOnlyWriteCommitState(req, state, record)
}

func validateAppendOnlyWriteCommitState(req CommitWriteStateRequest, state VolumeState, record IdempotencyRecord) (VolumeState, IdempotencyRecord, error) {
	if state.Epoch != req.ExpectedEpoch {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return VolumeState{}, IdempotencyRecord{}, ErrCASConflict
	}
	return state, record, nil
}

func appendOnlyWriteCommitStateFromMissingIntent(req CommitWriteStateRequest, state VolumeState) (VolumeState, IdempotencyRecord, bool, error) {
	if !req.AllowMissingWriteIntent {
		return VolumeState{}, IdempotencyRecord{}, false, nil
	}
	if req.ExpectedIdempotencyState != IdempotencyPending {
		return VolumeState{}, IdempotencyRecord{}, false, ErrCASConflict
	}
	if state.Epoch != req.ExpectedEpoch {
		return VolumeState{}, IdempotencyRecord{}, false, ErrCASConflict
	}
	record := IdempotencyRecord{
		IdempotencyKey: req.IdempotencyKey,
		VolumeID:       req.VolumeID,
		AttachmentID:   req.AttachmentID,
		Generation:     req.Generation,
		Epoch:          state.Epoch,
		Revision:       state.Revision,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}
	return state, record, true, nil
}

func (r *Repository) persistCommittedWriteState(ctx context.Context, store kvReadWriter, state VolumeState, record IdempotencyRecord) error {
	if err := writeVolumeState(ctx, store, r.root, state); err != nil {
		return err
	}
	if err := writeIdempotencyRecord(ctx, store, r.root, record); err != nil {
		return err
	}
	return nil
}

func (r *Repository) preparePageScopedCommittedAllocationPages(ctx context.Context, store kvReadWriter, pages []AllocationPageRecord) ([]AllocationPageRecord, uint64, error) {
	ordered := append([]AllocationPageRecord(nil), pages...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PageNo < ordered[j].PageNo })
	var maxRevision uint64
	for i := range ordered {
		page := ordered[i]
		current, err := readAllocationPage(ctx, store, r.root, page.VolumeID, page.PageNo)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return nil, 0, err
			}
			current = zeroAllocationPage(page.VolumeID, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
		}
		if current.PageBytes != page.PageBytes || current.ChunkSizeBytes != page.ChunkSizeBytes {
			return nil, 0, ErrCASConflict
		}
		if current.Revision != page.Revision {
			return nil, 0, ErrCASConflict
		}
		page.Revision = current.Revision + 1
		ordered[i] = page
		if page.Revision > maxRevision {
			maxRevision = page.Revision
		}
	}
	return ordered, maxRevision, nil
}

func (r *Repository) preparePageScopedCommittedAllocationPagesWithFloor(ctx context.Context, store kvReadWriter, pages []AllocationPageRecord, revisionFloor uint64) ([]AllocationPageRecord, uint64, error) {
	ordered, committedRevision, err := r.preparePageScopedCommittedAllocationPages(ctx, store, pages)
	if err != nil {
		return nil, 0, err
	}
	if committedRevision >= revisionFloor {
		return ordered, committedRevision, nil
	}
	for i := range ordered {
		if ordered[i].Revision < revisionFloor {
			ordered[i].Revision = revisionFloor
		}
	}
	return ordered, revisionFloor, nil
}

func (r *Repository) persistPageScopedCommittedAllocationPages(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest) error {
	for _, page := range req.AllocationPages {
		page.VolumeID = mustCanonicalVolumeID(page.VolumeID)
		if err := writeAllocationPage(ctx, store, r.root, page); err != nil {
			return err
		}
		r.rememberNativeAllocationVolume(page.VolumeID)
	}
	return nil
}

func (r *Repository) finalizeWriteMutationOperation(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest) error {
	mutation, err := r.readCommittedWriteMutationSnapshot(ctx, store, req)
	if err != nil {
		return err
	}
	return r.finalizeWriteMutationOperationFromSnapshot(ctx, store, req, mutation)
}

func (r *Repository) finalizeWriteMutationOperationFromSnapshot(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest, mutation committedWriteMutationSnapshot) error {
	if req.MutationOperationID == "" {
		return nil
	}
	if !mutation.found {
		return ErrNotFound
	}
	operation := mutation.operation
	if mutation.committed {
		return nil
	}
	if operation.State != req.ExpectedMutationState {
		return ErrCASConflict
	}
	operation.State = MutationOperationCommitted
	operation.AllocationRevision = req.CommittedRevision
	operation.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	operation.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	operation.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	operation.LastUpdatedAtUnix = time.Now().Unix()
	return writeMutationOperation(ctx, store, r.root, operation)
}

func (r *Repository) persistCommittedAllocationPages(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest) error {
	for _, page := range req.AllocationPages {
		page.VolumeID = mustCanonicalVolumeID(page.VolumeID)
		merged, err := r.mergeCommittedAllocationPage(ctx, store, req, page)
		if err != nil {
			return err
		}
		merged.Revision = req.CommittedRevision
		if err := writeAllocationPage(ctx, store, r.root, merged); err != nil {
			return err
		}
		r.rememberNativeAllocationVolume(merged.VolumeID)
	}
	return nil
}

func (r *Repository) persistCommittedAllocationPagesForBatch(ctx context.Context, store kvReadWriter, items []committedWriteEffectsBatchItem) error {
	groups := make(map[committedAllocationPageBatchKey][]committedAllocationPageBatchUpdate)
	order := make([]committedAllocationPageBatchKey, 0)
	for _, item := range items {
		for _, page := range item.req.AllocationPages {
			page.VolumeID = mustCanonicalVolumeID(page.VolumeID)
			key := committedAllocationPageBatchKey{volumeID: page.VolumeID, pageNo: page.PageNo}
			if _, ok := groups[key]; !ok {
				order = append(order, key)
			}
			groups[key] = append(groups[key], committedAllocationPageBatchUpdate{
				req:  item.req,
				page: page,
			})
		}
	}
	for _, key := range order {
		updates := groups[key]
		if len(updates) == 0 {
			continue
		}
		finalPage, err := r.mergeCommittedAllocationPageBatch(ctx, store, updates)
		if err != nil {
			return err
		}
		if err := writeAllocationPage(ctx, store, r.root, finalPage); err != nil {
			return err
		}
		r.rememberNativeAllocationVolume(finalPage.VolumeID)
	}
	return nil
}

func (r *Repository) mergeCommittedAllocationPageBatch(ctx context.Context, store kvReadWriter, updates []committedAllocationPageBatchUpdate) (AllocationPageRecord, error) {
	first := updates[0].page
	current, err := readAllocationPage(ctx, store, r.root, first.VolumeID, first.PageNo)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return AllocationPageRecord{}, err
		}
		current = zeroAllocationPage(first.VolumeID, first.PageNo, first.PageBytes, first.ChunkSizeBytes)
	}
	currentIDs, currentHeaders, err := expandAllocationChunkMappingsWithHeaders(current)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	finalPage := current
	for _, update := range updates {
		var err error
		currentIDs, currentHeaders, finalPage, err = r.mergeCommittedAllocationPageUpdateIntoChunks(ctx, store, currentIDs, currentHeaders, finalPage, update.req, update.page)
		if err != nil {
			return AllocationPageRecord{}, err
		}
	}
	pageStartChunk := finalPage.PageNo * uint64(finalPage.PageBytes/finalPage.ChunkSizeBytes)
	finalPage.Extents = compressAllocationChunkMappingsWithHeaders(pageStartChunk, currentIDs, currentHeaders)
	return finalPage, nil
}

func (r *Repository) mergeCommittedAllocationPage(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest, page AllocationPageRecord) (AllocationPageRecord, error) {
	current, err := readAllocationPage(ctx, store, r.root, page.VolumeID, page.PageNo)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return AllocationPageRecord{}, err
		}
		current = zeroAllocationPage(page.VolumeID, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	currentIDs, currentHeaders, err := expandAllocationChunkMappingsWithHeaders(current)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	currentIDs, currentHeaders, current, err = r.mergeCommittedAllocationPageUpdateIntoChunks(ctx, store, currentIDs, currentHeaders, current, req, page)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	pageStartChunk := current.PageNo * uint64(current.PageBytes/current.ChunkSizeBytes)
	current.Extents = compressAllocationChunkMappingsWithHeaders(pageStartChunk, currentIDs, currentHeaders)
	return current, nil
}

func (r *Repository) mergeCommittedAllocationPageUpdateIntoChunks(ctx context.Context, store kvReadWriter, currentIDs []uint64, currentHeaders []*PayloadEncryptionHeader, current AllocationPageRecord, req ApplyCommittedWriteEffectsRequest, page AllocationPageRecord) ([]uint64, []*PayloadEncryptionHeader, AllocationPageRecord, error) {
	if current.PageBytes != page.PageBytes || current.ChunkSizeBytes != page.ChunkSizeBytes {
		return nil, nil, AllocationPageRecord{}, fmt.Errorf("allocation page geometry mismatch: volume_id=%s page_no=%d current_page_bytes=%d current_chunk_size_bytes=%d page_bytes=%d chunk_size_bytes=%d",
			page.VolumeID, page.PageNo, current.PageBytes, current.ChunkSizeBytes, page.PageBytes, page.ChunkSizeBytes)
	}
	incomingIDs, incomingHeaders, err := expandAllocationChunkMappingsWithHeaders(page)
	if err != nil {
		return nil, nil, AllocationPageRecord{}, err
	}
	ranges, err := r.touchedAllocationPageChunkRanges(ctx, store, req, page)
	if err != nil {
		return nil, nil, AllocationPageRecord{}, err
	}
	if len(ranges) == 0 {
		ranges = []allocationPageChunkRange{{start: 0, end: uint64(len(incomingIDs))}}
	}
	retired := make(map[uint64]struct{}, len(req.RetiredPhysicalChunkIDs))
	for _, physicalChunkID := range req.RetiredPhysicalChunkIDs {
		if physicalChunkID == 0 {
			continue
		}
		retired[physicalChunkID] = struct{}{}
	}
	for _, rng := range ranges {
		for i := rng.start; i < rng.end && i < uint64(len(currentIDs)) && i < uint64(len(incomingIDs)); i++ {
			incomingPhysicalChunkID := incomingIDs[i]
			if incomingPhysicalChunkID != 0 {
				currentIDs[i] = incomingPhysicalChunkID
				currentHeaders[i] = clonePayloadEncryptionHeader(incomingHeaders[i])
				continue
			}
			currentPhysicalChunkID := currentIDs[i]
			if currentPhysicalChunkID == 0 {
				currentIDs[i] = 0
				currentHeaders[i] = nil
				continue
			}
			if _, ok := retired[currentPhysicalChunkID]; ok {
				currentIDs[i] = 0
				currentHeaders[i] = nil
			}
		}
	}
	pageStartChunk := page.PageNo * uint64(page.PageBytes/page.ChunkSizeBytes)
	current.Extents = compressAllocationChunkMappingsWithHeaders(pageStartChunk, currentIDs, currentHeaders)
	current.VolumeID = page.VolumeID
	current.PageNo = page.PageNo
	current.PageBytes = page.PageBytes
	current.ChunkSizeBytes = page.ChunkSizeBytes
	current.Revision = req.CommittedRevision
	return currentIDs, currentHeaders, current, nil
}

type allocationPageChunkRange struct {
	start uint64
	end   uint64
}

func (r *Repository) touchedAllocationPageChunkRanges(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest, page AllocationPageRecord) ([]allocationPageChunkRange, error) {
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	if ranges, found, err := affectedAllocationPageChunkRanges(req.AffectedPageChunkRanges, page); found || err != nil {
		return ranges, err
	}
	if len(req.NormalizeExtentMappings) == 0 {
		return nil, nil
	}
	pageStart := page.PageNo * uint64(page.PageBytes)
	pageEnd := pageStart + uint64(page.PageBytes)
	chunkSize := uint64(page.ChunkSizeBytes)
	ranges := make([]allocationPageChunkRange, 0)
	for _, extentID := range req.NormalizeExtentMappings {
		mapping, err := readExtentMapping(ctx, store, r.root, req.VolumeID, extentID)
		if err != nil {
			return nil, err
		}
		extentStart := mapping.LogicalOffset
		extentEnd := mapping.LogicalOffset + mapping.LengthBytes
		if extentEnd <= pageStart || extentStart >= pageEnd {
			continue
		}
		overlapStart := maxUint64(extentStart, pageStart)
		overlapEnd := minUint64(extentEnd, pageEnd)
		if overlapEnd <= overlapStart {
			continue
		}
		start := (overlapStart - pageStart) / chunkSize
		end := (overlapEnd - pageStart + chunkSize - 1) / chunkSize
		chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
		if end > chunksPerPage {
			end = chunksPerPage
		}
		if start < end {
			ranges = append(ranges, allocationPageChunkRange{start: start, end: end})
		}
	}
	return normalizeAllocationPageChunkRanges(ranges), nil
}

func affectedAllocationPageChunkRanges(records []AllocationPageChunkRangeRecord, page AllocationPageRecord) ([]allocationPageChunkRange, bool, error) {
	if len(records) == 0 {
		return nil, false, nil
	}
	chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
	ranges := make([]allocationPageChunkRange, 0)
	for _, record := range records {
		if record.PageNo != page.PageNo {
			continue
		}
		if record.StartChunk >= record.EndChunk {
			return nil, true, fmt.Errorf("invalid affected page chunk range: page_no=%d start_chunk=%d end_chunk=%d", record.PageNo, record.StartChunk, record.EndChunk)
		}
		if record.EndChunk > chunksPerPage {
			return nil, true, fmt.Errorf("affected page chunk range out of bounds: page_no=%d start_chunk=%d end_chunk=%d chunks_per_page=%d", record.PageNo, record.StartChunk, record.EndChunk, chunksPerPage)
		}
		ranges = append(ranges, allocationPageChunkRange{start: record.StartChunk, end: record.EndChunk})
	}
	if len(ranges) == 0 {
		return nil, false, nil
	}
	return normalizeAllocationPageChunkRanges(ranges), true, nil
}

func normalizeAllocationPageChunkRanges(ranges []allocationPageChunkRange) []allocationPageChunkRange {
	if len(ranges) == 0 {
		return nil
	}
	slices.SortFunc(ranges, func(a, b allocationPageChunkRange) int {
		switch {
		case a.start < b.start:
			return -1
		case a.start > b.start:
			return 1
		case a.end < b.end:
			return -1
		case a.end > b.end:
			return 1
		default:
			return 0
		}
	})
	out := ranges[:0]
	for _, rng := range ranges {
		if len(out) == 0 || rng.start > out[len(out)-1].end {
			out = append(out, rng)
			continue
		}
		if rng.end > out[len(out)-1].end {
			out[len(out)-1].end = rng.end
		}
	}
	return out
}

func (r *Repository) normalizeCommittedExtentMappings(ctx context.Context, store kvReadWriter, req ApplyCommittedWriteEffectsRequest) (committedExtentNormalizeStats, error) {
	stats := committedExtentNormalizeStats{requested: len(req.NormalizeExtentMappings)}
	for _, extentID := range req.NormalizeExtentMappings {
		mapping, err := readExtentMapping(ctx, store, r.root, req.VolumeID, extentID)
		if err != nil {
			return stats, err
		}
		stats.read++
		if mapping.ChunkID == 0 {
			stats.alreadyNormalized++
		}
		if r.canUseNativeAllocationFastPath(req.VolumeID) && mapping.ChunkID == 0 {
			if mapping.Revision < req.CommittedRevision {
				stats.skipped++
			} else {
				stats.revisionPreserved++
				stats.skipped++
			}
			continue
		}
		mapping.ChunkID = 0
		if mapping.Revision < req.CommittedRevision {
			mapping.Revision = req.CommittedRevision
			stats.revisionAdvanced++
		} else {
			stats.revisionPreserved++
		}
		if err := writeExtentMapping(ctx, store, r.root, mapping); err != nil {
			return stats, err
		}
		stats.written++
	}
	return stats, nil
}

func (r *Repository) canUseNativeAllocationFastPath(volumeID string) bool {
	return r.nativeAllocationFastPath && r.hasObservedNativeAllocationVolume(volumeID)
}

func (r *Repository) prepareLegacyAllocationBootstrap(ctx context.Context, req CommitWriteMetadataRequest) ([]AllocationPageRecord, []uint64, error) {
	if len(req.AllocationPages) == 0 {
		return nil, nil, nil
	}
	if r.hasObservedNativeAllocationVolume(req.VolumeID) {
		return nil, nil, nil
	}
	existingPages, err := r.ListAllocationPages(ctx, req.VolumeID)
	if err != nil {
		return nil, nil, err
	}
	if len(existingPages) > 0 {
		r.rememberNativeAllocationVolume(req.VolumeID)
		return nil, nil, nil
	}
	pageBytes := req.AllocationPages[0].PageBytes
	chunkSizeBytes := req.AllocationPages[0].ChunkSizeBytes
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, nil, fmt.Errorf("invalid allocation geometry for legacy bootstrap: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	pages, err := r.ListCompatibleAllocationPages(ctx, req.VolumeID, pageBytes, chunkSizeBytes)
	if err != nil {
		return nil, nil, err
	}
	mappings, err := r.ListExtentMappings(ctx, req.VolumeID)
	if err != nil {
		return nil, nil, err
	}
	extentIDs := make([]uint64, 0, len(mappings))
	for _, mapping := range mappings {
		extentIDs = append(extentIDs, mapping.ExtentID)
	}
	return pages, extentIDs, nil
}

func mergeAllocationPagesByPageNo(base, overlay []AllocationPageRecord) []AllocationPageRecord {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	byPageNo := make(map[uint64]AllocationPageRecord, len(base)+len(overlay))
	for _, page := range base {
		byPageNo[page.PageNo] = page
	}
	for _, page := range overlay {
		byPageNo[page.PageNo] = page
	}
	pageNos := make([]uint64, 0, len(byPageNo))
	for pageNo := range byPageNo {
		pageNos = append(pageNos, pageNo)
	}
	sort.Slice(pageNos, func(i, j int) bool { return pageNos[i] < pageNos[j] })
	out := make([]AllocationPageRecord, 0, len(pageNos))
	for _, pageNo := range pageNos {
		out = append(out, byPageNo[pageNo])
	}
	return out
}

func mergeExtentIDs(base, overlay []uint64) []uint64 {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(base)+len(overlay))
	out := make([]uint64, 0, len(base)+len(overlay))
	for _, extentID := range base {
		if _, ok := seen[extentID]; ok {
			continue
		}
		seen[extentID] = struct{}{}
		out = append(out, extentID)
	}
	for _, extentID := range overlay {
		if _, ok := seen[extentID]; ok {
			continue
		}
		seen[extentID] = struct{}{}
		out = append(out, extentID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

type CommitPrimaryFailoverRequest struct {
	VolumeID                 string
	ReplicaSetID             string
	ExpectedVolumeEpoch      uint64
	ExpectedReplicaSetEpoch  uint64
	ExpectedPrimaryReplicaID string
	NewPrimaryReplicaID      string
}

func (r *Repository) CommitPrimaryFailover(ctx context.Context, req CommitPrimaryFailoverRequest) (VolumeState, ReplicaSetState, error) {
	if txkv, ok := r.kv.(transactionalKV); ok {
		var state VolumeState
		var replicaSet ReplicaSetState
		err := txkv.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var err error
			state, err = readVolumeState(ctx, tx, r.root, req.VolumeID)
			if err != nil {
				return err
			}
			if state.Epoch != req.ExpectedVolumeEpoch {
				return ErrCASConflict
			}

			replicaSet, err = readReplicaSet(ctx, tx, r.root, req.VolumeID, req.ReplicaSetID)
			if err != nil {
				return err
			}
			if replicaSet.Epoch != req.ExpectedReplicaSetEpoch || replicaSet.PrimaryReplicaID != req.ExpectedPrimaryReplicaID {
				return ErrCASConflict
			}
			if err := applyPrimaryFailover(&state, &replicaSet, req); err != nil {
				return err
			}
			if err := writeVolumeState(ctx, tx, r.root, state); err != nil {
				return err
			}
			if err := writeReplicaSet(ctx, tx, r.root, replicaSet); err != nil {
				return err
			}
			return nil
		})
		return state, replicaSet, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state, err := readVolumeState(ctx, r.kv, r.root, req.VolumeID)
	if err != nil {
		return VolumeState{}, ReplicaSetState{}, err
	}
	if state.Epoch != req.ExpectedVolumeEpoch {
		return VolumeState{}, ReplicaSetState{}, ErrCASConflict
	}

	replicaSet, err := readReplicaSet(ctx, r.kv, r.root, req.VolumeID, req.ReplicaSetID)
	if err != nil {
		return VolumeState{}, ReplicaSetState{}, err
	}
	if replicaSet.Epoch != req.ExpectedReplicaSetEpoch || replicaSet.PrimaryReplicaID != req.ExpectedPrimaryReplicaID {
		return VolumeState{}, ReplicaSetState{}, ErrCASConflict
	}

	if err := applyPrimaryFailover(&state, &replicaSet, req); err != nil {
		return VolumeState{}, ReplicaSetState{}, err
	}

	if err := writeVolumeState(ctx, r.kv, r.root, state); err != nil {
		return VolumeState{}, ReplicaSetState{}, err
	}
	if err := writeReplicaSet(ctx, r.kv, r.root, replicaSet); err != nil {
		return VolumeState{}, ReplicaSetState{}, err
	}
	return state, replicaSet, nil
}

func applyPrimaryFailover(state *VolumeState, replicaSet *ReplicaSetState, req CommitPrimaryFailoverRequest) error {
	var found bool
	for _, replica := range replicaSet.Replicas {
		if replica.ReplicaID == req.NewPrimaryReplicaID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("new primary replica %q not found in replica set", req.NewPrimaryReplicaID)
	}

	state.Epoch++
	replicaSet.Epoch++
	replicaSet.PrimaryReplicaID = req.NewPrimaryReplicaID
	for i := range replicaSet.Replicas {
		switch replicaSet.Replicas[i].ReplicaID {
		case req.NewPrimaryReplicaID:
			replicaSet.Replicas[i].Role = ReplicaRolePrimary
		case req.ExpectedPrimaryReplicaID:
			replicaSet.Replicas[i].Role = ReplicaRoleSecondary
		}
	}
	return nil
}

func (r *Repository) putJSON(ctx context.Context, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.kv.Set(ctx, key, raw)
}

func putJSONStore(ctx context.Context, store kvReadWriter, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return store.Set(ctx, key, raw)
}

func (r *Repository) getJSON(ctx context.Context, key string, out any) error {
	return getJSONStore(ctx, r.kv, key, out)
}

func getJSONStore(ctx context.Context, store kvReadWriter, key string, out any) error {
	raw, found, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	return json.Unmarshal(raw, out)
}

func readVolumeState(ctx context.Context, store kvReadWriter, root, volumeID string) (VolumeState, error) {
	var rec VolumeState
	if err := getJSONStore(ctx, store, volumeStateKey(root, volumeID), &rec); err != nil {
		return VolumeState{}, err
	}
	return rec, nil
}

func writeVolumeState(ctx context.Context, store kvReadWriter, root string, rec VolumeState) error {
	return putJSONStore(ctx, store, volumeStateKey(root, rec.VolumeID), rec)
}

func readVolumeSpec(ctx context.Context, store kvReadWriter, root, volumeID string) (VolumeSpecRecord, error) {
	var rec VolumeSpecRecord
	if err := getJSONStore(ctx, store, volumeSpecKey(root, volumeID), &rec); err != nil {
		return VolumeSpecRecord{}, err
	}
	return rec, nil
}

func writeVolumeSpec(ctx context.Context, store kvReadWriter, root string, rec VolumeSpecRecord) error {
	return putJSONStore(ctx, store, volumeSpecKey(root, rec.VolumeID), rec)
}

func readReplicaSet(ctx context.Context, store kvReadWriter, root, volumeID, replicaSetID string) (ReplicaSetState, error) {
	var rec ReplicaSetState
	if err := getJSONStore(ctx, store, replicaSetKey(root, volumeID, replicaSetID), &rec); err != nil {
		return ReplicaSetState{}, err
	}
	return rec, nil
}

func writeReplicaSet(ctx context.Context, store kvReadWriter, root string, rec ReplicaSetState) error {
	return putJSONStore(ctx, store, replicaSetKey(root, rec.VolumeID, rec.ReplicaSetID), rec)
}

func readIdempotencyRecord(ctx context.Context, store kvReadWriter, root, volumeID, key string) (IdempotencyRecord, error) {
	var rec IdempotencyRecord
	if err := getJSONStore(ctx, store, idempotencyKey(root, volumeID, key), &rec); err != nil {
		return IdempotencyRecord{}, err
	}
	return rec, nil
}

func writeIdempotencyRecord(ctx context.Context, store kvReadWriter, root string, rec IdempotencyRecord) error {
	return putJSONStore(ctx, store, idempotencyKey(root, rec.VolumeID, rec.IdempotencyKey), rec)
}

func readExtentMapping(ctx context.Context, store kvReadWriter, root, volumeID string, extentID uint64) (ExtentMappingRecord, error) {
	var rec ExtentMappingRecord
	if err := getJSONStore(ctx, store, extentMappingKey(root, volumeID, extentID), &rec); err != nil {
		return ExtentMappingRecord{}, err
	}
	return rec, nil
}

func writeExtentMapping(ctx context.Context, store kvReadWriter, root string, rec ExtentMappingRecord) error {
	return putJSONStore(ctx, store, extentMappingKey(root, rec.VolumeID, rec.ExtentID), rec)
}

func readMutationOperation(ctx context.Context, store kvReadWriter, root, volumeID, operationID string) (MutationOperationRecord, error) {
	var rec MutationOperationRecord
	if err := getJSONStore(ctx, store, mutationOperationKey(root, volumeID, operationID), &rec); err != nil {
		return MutationOperationRecord{}, err
	}
	return rec, nil
}

func writeMutationOperation(ctx context.Context, store kvReadWriter, root string, rec MutationOperationRecord) error {
	return putJSONStore(ctx, store, mutationOperationKey(root, rec.VolumeID, rec.OperationID), rec)
}

func readAllocationPage(ctx context.Context, store kvReadWriter, root, volumeID string, pageNo uint64) (AllocationPageRecord, error) {
	var rec AllocationPageRecord
	if err := getJSONStore(ctx, store, allocationPageKey(root, volumeID, pageNo), &rec); err != nil {
		return AllocationPageRecord{}, err
	}
	return rec, nil
}

func writeAllocationPage(ctx context.Context, store kvReadWriter, root string, rec AllocationPageRecord) error {
	return putJSONStore(ctx, store, allocationPageKey(root, rec.VolumeID, rec.PageNo), rec)
}

func readRangeLocalWriteState(ctx context.Context, store kvReadWriter, root, volumeID string, pageNo uint64) (rangeLocalWriteStateRecord, error) {
	var rec rangeLocalWriteStateRecord
	if err := getJSONStore(ctx, store, rangeLocalWriteStateKey(root, volumeID, pageNo), &rec); err != nil {
		return rangeLocalWriteStateRecord{}, err
	}
	return rec, nil
}

func writeRangeLocalWriteState(ctx context.Context, store kvReadWriter, root string, rec rangeLocalWriteStateRecord) error {
	rec.VolumeID = mustCanonicalVolumeID(rec.VolumeID)
	return putJSONStore(ctx, store, rangeLocalWriteStateKey(root, rec.VolumeID, rec.PageNo), rec)
}

type snapshotSourceIndexRecord struct {
	SourceVolumeID string `json:"source_volume_id"`
	SnapshotID     string `json:"snapshot_id"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
}

type cloneSourceIndexRecord struct {
	SourceSnapshotID string `json:"source_snapshot_id"`
	SourceVolumeID   string `json:"source_volume_id"`
	CloneID          string `json:"clone_id"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
}

func readSnapshotRecord(ctx context.Context, store kvReadWriter, root, snapshotID string) (SnapshotRecord, error) {
	var rec SnapshotRecord
	if err := getJSONStore(ctx, store, snapshotRecordKey(root, snapshotID), &rec); err != nil {
		return SnapshotRecord{}, err
	}
	return rec, nil
}

func writeSnapshotRecord(ctx context.Context, store kvReadWriter, root string, rec SnapshotRecord) error {
	rec.SourceVolumeID = mustCanonicalVolumeID(rec.SourceVolumeID)
	return putJSONStore(ctx, store, snapshotRecordKey(root, rec.SnapshotID), rec)
}

func readCloneRecord(ctx context.Context, store kvReadWriter, root, cloneID string) (CloneRecord, error) {
	var rec CloneRecord
	if err := getJSONStore(ctx, store, cloneRecordKey(root, cloneID), &rec); err != nil {
		return CloneRecord{}, err
	}
	return rec, nil
}

func writeCloneRecord(ctx context.Context, store kvReadWriter, root string, rec CloneRecord) error {
	rec.SourceVolumeID = mustCanonicalVolumeID(rec.SourceVolumeID)
	return putJSONStore(ctx, store, cloneRecordKey(root, rec.CloneID), rec)
}

func validateSnapshotID(snapshotID string) error {
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return fmt.Errorf("snapshot_id is required")
	}
	if strings.Contains(snapshotID, "/") {
		return fmt.Errorf("snapshot_id must not contain '/'")
	}
	return nil
}

func validateCloneID(cloneID string) error {
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return fmt.Errorf("clone_id is required")
	}
	if strings.Contains(cloneID, "/") {
		return fmt.Errorf("clone_id must not contain '/'")
	}
	return nil
}

func markSnapshotStateWithStore(ctx context.Context, store kvReadWriter, root, snapshotID string, state SnapshotState, errorMessage string) (SnapshotRecord, error) {
	rec, err := readSnapshotRecord(ctx, store, root, snapshotID)
	if err != nil {
		return SnapshotRecord{}, err
	}
	rec.State = state
	rec.ErrorMessage = errorMessage
	rec.UpdatedAtUnix = time.Now().Unix()
	if err := writeSnapshotRecord(ctx, store, root, rec); err != nil {
		return SnapshotRecord{}, err
	}
	return rec, nil
}

func markCloneStateWithStore(ctx context.Context, store kvReadWriter, root, cloneID string, state CloneState, errorMessage string) (CloneRecord, error) {
	rec, err := readCloneRecord(ctx, store, root, cloneID)
	if err != nil {
		return CloneRecord{}, err
	}
	rec.State = state
	rec.ErrorMessage = errorMessage
	rec.UpdatedAtUnix = time.Now().Unix()
	if err := writeCloneRecord(ctx, store, root, rec); err != nil {
		return CloneRecord{}, err
	}
	return rec, nil
}

func markCloneMaterializedWithStore(ctx context.Context, store kvReadWriter, root, cloneID, materializedVolumeID string) (CloneRecord, error) {
	rec, err := readCloneRecord(ctx, store, root, cloneID)
	if err != nil {
		return CloneRecord{}, err
	}
	if rec.State == CloneStateDeleted {
		return CloneRecord{}, fmt.Errorf("clone %q is deleted", cloneID)
	}
	if rec.State == CloneStateFailed {
		return CloneRecord{}, fmt.Errorf("clone %q is failed: %s", cloneID, rec.ErrorMessage)
	}
	if rec.State == CloneStateMaterialized {
		if rec.MaterializedVolumeID != materializedVolumeID {
			return CloneRecord{}, ErrCASConflict
		}
		return rec, nil
	}
	source, err := readSnapshotRecord(ctx, store, root, rec.SourceSnapshotID)
	if err != nil {
		return CloneRecord{}, err
	}
	now := time.Now().Unix()
	rec.State = CloneStateMaterialized
	rec.MaterializedVolumeID = materializedVolumeID
	rec.ErrorMessage = ""
	rec.UpdatedAtUnix = now
	if err := writeCloneRecord(ctx, store, root, rec); err != nil {
		return CloneRecord{}, err
	}
	if source.CloneReferenceCount > 0 {
		source.CloneReferenceCount--
	}
	source.UpdatedAtUnix = now
	if err := writeSnapshotRecord(ctx, store, root, source); err != nil {
		return CloneRecord{}, err
	}
	return rec, nil
}

func deleteCloneRecordWithStore(ctx context.Context, store kvReadWriter, root, cloneID string) (CloneRecord, error) {
	rec, err := readCloneRecord(ctx, store, root, cloneID)
	if err != nil {
		return CloneRecord{}, err
	}
	if rec.State == CloneStateDeleted {
		return rec, nil
	}
	source, err := readSnapshotRecord(ctx, store, root, rec.SourceSnapshotID)
	if err != nil {
		return CloneRecord{}, err
	}
	previousState := rec.State
	rec.State = CloneStateDeleted
	rec.ErrorMessage = ""
	rec.UpdatedAtUnix = time.Now().Unix()
	if err := writeCloneRecord(ctx, store, root, rec); err != nil {
		return CloneRecord{}, err
	}
	if previousState != CloneStateMaterialized && source.CloneReferenceCount > 0 {
		source.CloneReferenceCount--
	}
	source.UpdatedAtUnix = rec.UpdatedAtUnix
	if err := writeSnapshotRecord(ctx, store, root, source); err != nil {
		return CloneRecord{}, err
	}
	return rec, nil
}

func putCloneDeltaAllocationPagesWithStore(ctx context.Context, store kvReadWriter, root, cloneID string, pages []AllocationPageRecord) error {
	clone, err := readCloneRecord(ctx, store, root, cloneID)
	if err != nil {
		return err
	}
	if clone.State != CloneStateAvailable {
		return fmt.Errorf("clone %q is not available: state=%s", cloneID, clone.State)
	}
	seen := make(map[uint64]struct{}, len(pages))
	deltaPageCount := clone.DeltaPageCount
	deltaObjectCount := clone.DeltaObjectCount
	for _, page := range pages {
		if _, ok := seen[page.PageNo]; ok {
			return fmt.Errorf("duplicate clone delta allocation page: clone_id=%s page_no=%d", cloneID, page.PageNo)
		}
		seen[page.PageNo] = struct{}{}
		if page.PageBytes != clone.AllocationPageSizeBytes || page.ChunkSizeBytes != clone.AllocationChunkSizeBytes {
			return fmt.Errorf("clone delta allocation page geometry mismatch: clone_id=%s page_no=%d page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d",
				cloneID, page.PageNo, page.PageBytes, page.ChunkSizeBytes, clone.AllocationPageSizeBytes, clone.AllocationChunkSizeBytes)
		}
		page.VolumeID = clone.CloneID
		key := cloneDeltaAllocationPageKey(root, cloneID, page.PageNo)
		existingObjectCount := uint64(0)
		if existing, err := readCloneDeltaAllocationPage(ctx, store, root, cloneID, page.PageNo); errors.Is(err, ErrNotFound) {
			deltaPageCount++
		} else if err != nil {
			return err
		} else {
			existingObjectCount = countAllocationPageObjects(existing)
		}
		if err := putJSONStore(ctx, store, key, page); err != nil {
			return err
		}
		newObjectCount := countAllocationPageObjects(page)
		if deltaObjectCount >= existingObjectCount {
			deltaObjectCount -= existingObjectCount
		} else {
			deltaObjectCount = 0
		}
		deltaObjectCount += newObjectCount
	}
	clone.DeltaPageCount = deltaPageCount
	clone.DeltaObjectCount = deltaObjectCount
	clone.UpdatedAtUnix = time.Now().Unix()
	return writeCloneRecord(ctx, store, root, clone)
}

func readCloneDeltaAllocationPage(ctx context.Context, store kvReadWriter, root, cloneID string, pageNo uint64) (AllocationPageRecord, error) {
	var rec AllocationPageRecord
	if err := getJSONStore(ctx, store, cloneDeltaAllocationPageKey(root, cloneID, pageNo), &rec); err != nil {
		return AllocationPageRecord{}, err
	}
	return rec, nil
}

func countAllocationPageObjects(page AllocationPageRecord) uint64 {
	var count uint64
	for _, extent := range page.Extents {
		if extent.Kind == AllocationKindData || extent.Kind == AllocationKindShared {
			count++
		}
	}
	return count
}

func (r *Repository) listAll(ctx context.Context, prefix string) ([]string, error) {
	cursor := ""
	var out []string
	for {
		keys, next, err := r.kv.List(ctx, prefix, cursor, 128)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func CanonicalVolumeID(volumeID string) (string, error) {
	parsed, err := volumeid.Parse(volumeID)
	if err != nil {
		return "", err
	}
	return volumeid.Format(parsed), nil
}

func volumeStateKey(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/meta/state", root, mustCanonicalVolumeID(volumeID))
}

func volumeSpecKey(root, volumeID string) string {
	return fmt.Sprintf("%s/admin/volumes/%s/spec", root, mustCanonicalVolumeID(volumeID))
}

func snapshotsPrefix(root string) string {
	return fmt.Sprintf("%s/snapshots/", root)
}

func snapshotRecordKey(root, snapshotID string) string {
	return fmt.Sprintf("%s%s/record", snapshotsPrefix(root), snapshotID)
}

func snapshotIDFromRecordKey(key string) string {
	key = strings.TrimSuffix(key, "/record")
	idx := strings.LastIndex(key, "/")
	if idx < 0 || idx == len(key)-1 {
		return ""
	}
	return key[idx+1:]
}

func snapshotSourceIndexPrefix(root, sourceVolumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/snapshots/", root, mustCanonicalVolumeID(sourceVolumeID))
}

func snapshotSourceIndexKey(root, sourceVolumeID, snapshotID string) string {
	return fmt.Sprintf("%s%s", snapshotSourceIndexPrefix(root, sourceVolumeID), snapshotID)
}

func snapshotIdempotencyKey(root, sourceVolumeID, idempotencyKey string) string {
	return fmt.Sprintf("%s/volumes/%s/snapshot_idem/%s", root, mustCanonicalVolumeID(sourceVolumeID), idempotencyKey)
}

func clonesPrefix(root string) string {
	return fmt.Sprintf("%s/clones/", root)
}

func cloneRecordKey(root, cloneID string) string {
	return fmt.Sprintf("%s%s/record", clonesPrefix(root), cloneID)
}

func cloneIDFromRecordKey(key string) string {
	key = strings.TrimSuffix(key, "/record")
	idx := strings.LastIndex(key, "/")
	if idx < 0 || idx == len(key)-1 {
		return ""
	}
	return key[idx+1:]
}

func cloneSourceSnapshotIndexPrefix(root, sourceSnapshotID string) string {
	return fmt.Sprintf("%s/snapshots/%s/clones/", root, sourceSnapshotID)
}

func cloneSourceSnapshotIndexKey(root, sourceSnapshotID, cloneID string) string {
	return fmt.Sprintf("%s%s", cloneSourceSnapshotIndexPrefix(root, sourceSnapshotID), cloneID)
}

func cloneSourceVolumeIndexPrefix(root, sourceVolumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/clones/", root, mustCanonicalVolumeID(sourceVolumeID))
}

func cloneSourceVolumeIndexKey(root, sourceVolumeID, cloneID string) string {
	return fmt.Sprintf("%s%s", cloneSourceVolumeIndexPrefix(root, sourceVolumeID), cloneID)
}

func cloneIdempotencyKey(root, sourceSnapshotID, idempotencyKey string) string {
	return fmt.Sprintf("%s/snapshots/%s/clone_idem/%s", root, sourceSnapshotID, idempotencyKey)
}

func cloneDeltaAllocationPagesPrefix(root, cloneID string) string {
	return fmt.Sprintf("%s%s/delta/allocation/pages/", clonesPrefix(root), cloneID)
}

func cloneDeltaAllocationPageKey(root, cloneID string, pageNo uint64) string {
	return fmt.Sprintf("%s%020d", cloneDeltaAllocationPagesPrefix(root, cloneID), pageNo)
}

func extentMappingsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/extents/", root, mustCanonicalVolumeID(volumeID))
}

func extentMappingKey(root, volumeID string, extentID uint64) string {
	return fmt.Sprintf("%s%020d", extentMappingsPrefix(root, volumeID), extentID)
}

func allocationPagesPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/allocation/pages/", root, mustCanonicalVolumeID(volumeID))
}

func allocationPageKey(root, volumeID string, pageNo uint64) string {
	return fmt.Sprintf("%s%020d", allocationPagesPrefix(root, volumeID), pageNo)
}

func snapshotAllocationPagesPrefix(root, snapshotID string) string {
	return fmt.Sprintf("%s%s/allocation/pages/", snapshotsPrefix(root), snapshotID)
}

func snapshotAllocationPageKey(root, snapshotID string, pageNo uint64) string {
	return fmt.Sprintf("%s%020d", snapshotAllocationPagesPrefix(root, snapshotID), pageNo)
}

func rangeLocalWriteStateKey(root, volumeID string, pageNo uint64) string {
	return fmt.Sprintf("%s/volumes/%s/write_state/pages/%020d", root, mustCanonicalVolumeID(volumeID), pageNo)
}

func replicaSetKey(root, volumeID, replicaSetID string) string {
	return fmt.Sprintf("%s/volumes/%s/replicasets/%s", root, mustCanonicalVolumeID(volumeID), replicaSetID)
}

func replicaSetsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/replicasets/", root, mustCanonicalVolumeID(volumeID))
}

func idempotencyKey(root, volumeID, idempotencyKey string) string {
	return idempotencyKeyKey(root, volumeID, idempotencyKey)
}

func idempotencyKeyKey(root, volumeID, key string) string {
	return fmt.Sprintf("%s/volumes/%s/idem/%s", root, mustCanonicalVolumeID(volumeID), key)
}

func topologyZonesPrefix(root string) string {
	return fmt.Sprintf("%s/topology/zones/", root)
}

func topologyZoneKey(root, zoneID string) string {
	return fmt.Sprintf("%s%s", topologyZonesPrefix(root), zoneID)
}

func nodeMembershipKey(root, nodeID string) string {
	return fmt.Sprintf("%s/nodes/%s/membership", root, nodeID)
}

func nodeHealthDetailKey(root, nodeID string) string {
	return fmt.Sprintf("%s/nodes/%s/health/detail", root, nodeID)
}

func placementTransitionKey(root, volumeID, placementRef string) string {
	return fmt.Sprintf("%s/volumes/%s/placements/%s/transition", root, mustCanonicalVolumeID(volumeID), placementRef)
}

func placementTransitionsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/placements/", root, mustCanonicalVolumeID(volumeID))
}

func mutationOperationsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/operations/", root, mustCanonicalVolumeID(volumeID))
}

func mutationOperationsRootPrefix(root string) string {
	return fmt.Sprintf("%s/volumes/", root)
}

func mutationOperationKey(root, volumeID, operationID string) string {
	return fmt.Sprintf("%s%s", mutationOperationsPrefix(root, volumeID), operationID)
}

func chunkNextIDKey(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/meta/next_chunk_id", root, mustCanonicalVolumeID(volumeID))
}

func zeroAllocationPage(volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) AllocationPageRecord {
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	return AllocationPageRecord{
		VolumeID:       mustCanonicalVolumeID(volumeID),
		PageNo:         pageNo,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
		Extents: []AllocationExtentRecord{
			{
				LogicalChunkStart: pageNo * chunksPerPage,
				ChunkCount:        uint32(chunksPerPage),
				Kind:              AllocationKindZero,
			},
		},
	}
}

func synthesizeAllocationPageFromMappings(volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32, mappings []ExtentMappingRecord) AllocationPageRecord {
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	pageStartChunk := pageNo * chunksPerPage
	pageEndChunk := pageStartChunk + chunksPerPage
	physicalChunkIDs := make([]uint64, int(chunksPerPage))

	for _, mapping := range mappings {
		if mapping.LengthBytes == 0 {
			continue
		}
		extentStartChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
		extentEndChunk := (mapping.LogicalOffset + mapping.LengthBytes + uint64(chunkSizeBytes) - 1) / uint64(chunkSizeBytes)
		if extentEndChunk <= pageStartChunk || extentStartChunk >= pageEndChunk {
			continue
		}
		overlapStart := maxUint64(extentStartChunk, pageStartChunk)
		overlapEnd := minUint64(extentEndChunk, pageEndChunk)
		for logicalChunk := overlapStart; logicalChunk < overlapEnd; logicalChunk++ {
			pageIndex := logicalChunk - pageStartChunk
			if mapping.ChunkID == 0 {
				physicalChunkIDs[pageIndex] = 0
				continue
			}
			physicalChunkIDs[pageIndex] = mapping.ChunkID + (logicalChunk - extentStartChunk)
		}
	}

	return AllocationPageRecord{
		VolumeID:       mustCanonicalVolumeID(volumeID),
		PageNo:         pageNo,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
		Extents:        compressAllocationChunkMappings(pageStartChunk, physicalChunkIDs),
	}
}

func expandAllocationChunkMappings(page AllocationPageRecord) ([]uint64, error) {
	ids, _, err := expandAllocationChunkMappingsWithHeaders(page)
	return ids, err
}

func expandAllocationChunkMappingsWithHeaders(page AllocationPageRecord) ([]uint64, []*PayloadEncryptionHeader, error) {
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return nil, nil, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
	pageStartChunk := page.PageNo * chunksPerPage
	pageEndChunk := pageStartChunk + chunksPerPage
	out := make([]uint64, int(chunksPerPage))
	headers := make([]*PayloadEncryptionHeader, int(chunksPerPage))
	for _, extent := range page.Extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if extent.ChunkCount == 0 {
			return nil, nil, fmt.Errorf("allocation extent has zero chunks: page_no=%d logical_chunk_start=%d", page.PageNo, start)
		}
		if start < pageStartChunk || end > pageEndChunk {
			return nil, nil, fmt.Errorf("allocation extent out of page bounds: page_no=%d start=%d end=%d", page.PageNo, start, end)
		}
		for logicalChunk := start; logicalChunk < end; logicalChunk++ {
			index := logicalChunk - pageStartChunk
			if extent.Kind == AllocationKindData {
				out[index] = extent.PhysicalChunkStart + (logicalChunk - start)
				headers[index] = clonePayloadEncryptionHeader(extent.Encryption)
				continue
			}
			out[index] = 0
			headers[index] = nil
		}
	}
	return out, headers, nil
}

func compressAllocationChunkMappings(pageStartChunk uint64, physicalChunkIDs []uint64) []AllocationExtentRecord {
	return compressAllocationChunkMappingsWithHeaders(pageStartChunk, physicalChunkIDs, nil)
}

func compressAllocationChunkMappingsWithHeaders(pageStartChunk uint64, physicalChunkIDs []uint64, encryptionHeaders []*PayloadEncryptionHeader) []AllocationExtentRecord {
	if len(physicalChunkIDs) == 0 {
		return nil
	}

	out := make([]AllocationExtentRecord, 0, len(physicalChunkIDs))
	for i := 0; i < len(physicalChunkIDs); {
		logicalStart := pageStartChunk + uint64(i)
		physicalStart := physicalChunkIDs[i]
		if physicalStart == 0 {
			j := i + 1
			for j < len(physicalChunkIDs) && physicalChunkIDs[j] == 0 {
				j++
			}
			out = append(out, AllocationExtentRecord{
				LogicalChunkStart: logicalStart,
				ChunkCount:        uint32(j - i),
				Kind:              AllocationKindZero,
			})
			i = j
			continue
		}

		j := i + 1
		header := allocationChunkEncryptionHeaderAt(encryptionHeaders, i)
		for j < len(physicalChunkIDs) &&
			physicalChunkIDs[j] == physicalStart+uint64(j-i) &&
			sameAllocationChunkEncryptionHeader(header, allocationChunkEncryptionHeaderAt(encryptionHeaders, j)) {
			j++
		}
		out = append(out, AllocationExtentRecord{
			LogicalChunkStart:  logicalStart,
			ChunkCount:         uint32(j - i),
			Kind:               AllocationKindData,
			PhysicalChunkStart: physicalStart,
			Encryption:         clonePayloadEncryptionHeader(header),
		})
		i = j
	}
	return out
}

func allocationChunkEncryptionHeaderAt(headers []*PayloadEncryptionHeader, index int) *PayloadEncryptionHeader {
	if index < 0 || index >= len(headers) {
		return nil
	}
	return headers[index]
}

func sameAllocationChunkEncryptionHeader(a, b *PayloadEncryptionHeader) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func mustCanonicalVolumeID(volumeID string) string {
	canonical, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return volumeID
	}
	return canonical
}
