package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	VolumeCatalogSchemaVersion = 1
	VolumeCatalogPageDefault   = 128
	VolumeCatalogPageMaximum   = 512
	VolumeCatalogBatchMaximum  = 128
)

var (
	ErrVolumeCatalogInvalid         = errors.New("invalid volume catalog request")
	ErrVolumeCatalogRebuildRequired = errors.New("volume catalog rebuild required")
	ErrVolumeCatalogRevision        = errors.New("volume catalog revision mismatch")
)

type VolumeCatalogState struct {
	SchemaVersion    int    `json:"schema_version"`
	Revision         uint64 `json:"revision"`
	Ready            bool   `json:"ready"`
	RebuildCursor    string `json:"rebuild_cursor,omitempty"`
	RebuildPageCount uint64 `json:"rebuild_page_count,omitempty"`
	ScannedSpecCount uint64 `json:"scanned_spec_count,omitempty"`
	UpdatedAtUnix    int64  `json:"updated_at_unix"`
	StateDigest      string `json:"state_digest"`
}

type VolumeCatalogFilter struct {
	Status            VolumeStatus
	RedundancyBackend string
	TopologyMode      string
}

type VolumeCatalogEntry struct {
	State VolumeState
	Spec  VolumeSpecRecord
}

type VolumeCatalogPage struct {
	State        VolumeCatalogState
	Entries      []VolumeCatalogEntry
	NextCursor   string
	ScannedCount int
}

type VolumeCatalogRebuildPageResult struct {
	Revision         uint64
	InputCount       int
	RangePageCount   int
	ScannedSpecCount uint64
	NextCursor       string
	Ready            bool
}

func (r *Repository) GetVolumeCatalogState(ctx context.Context) (VolumeCatalogState, error) {
	if r == nil {
		return VolumeCatalogState{}, ErrVolumeCatalogInvalid
	}
	state, found, err := readVolumeCatalogState(ctx, r.kv, r.root)
	if err != nil {
		return VolumeCatalogState{}, err
	}
	if !found {
		return VolumeCatalogState{}, ErrVolumeCatalogRebuildRequired
	}
	return state, nil
}

// RunVolumeCatalogRebuildPage advances only one bounded page. The authoritative
// spec keys already form the ordered catalog; the checkpoint establishes the
// revision boundary required by resumable API tokens after an upgrade.
func (r *Repository) RunVolumeCatalogRebuildPage(ctx context.Context, limit int) (VolumeCatalogRebuildPageResult, error) {
	if r == nil {
		return VolumeCatalogRebuildPageResult{}, ErrVolumeCatalogInvalid
	}
	if limit == 0 {
		limit = VolumeCatalogPageDefault
	}
	if limit < 1 || limit > VolumeCatalogPageMaximum {
		return VolumeCatalogRebuildPageResult{}, fmt.Errorf("%w: page limit %d", ErrVolumeCatalogInvalid, limit)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return VolumeCatalogRebuildPageResult{}, ErrSummaryTransactionRequired
	}
	before, found, err := readVolumeCatalogState(ctx, r.kv, r.root)
	if err != nil {
		return VolumeCatalogRebuildPageResult{}, err
	}
	if found && before.Ready {
		return VolumeCatalogRebuildPageResult{Revision: before.Revision, ScannedSpecCount: before.ScannedSpecCount, Ready: true}, nil
	}
	keys, next, err := r.kv.List(ctx, volumeCatalogSourcePrefix(r.root), before.RebuildCursor, limit)
	if err != nil {
		return VolumeCatalogRebuildPageResult{}, err
	}
	if len(keys) > limit || (next != "" && (next == before.RebuildCursor || len(keys) == 0)) {
		return VolumeCatalogRebuildPageResult{}, fmt.Errorf("%w: non-progressing source page", ErrVolumeCatalogInvalid)
	}
	now := r.now().UTC()
	var updated VolumeCatalogState
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		current, currentFound, err := readVolumeCatalogState(ctx, tx, r.root)
		if err != nil {
			return err
		}
		if currentFound != found || (found && current.StateDigest != before.StateDigest) {
			return ErrCASConflict
		}
		if !currentFound {
			current = VolumeCatalogState{SchemaVersion: VolumeCatalogSchemaVersion}
		}
		current.RebuildPageCount++
		current.ScannedSpecCount += uint64(len(keys))
		current.RebuildCursor = next
		current.Ready = next == ""
		current.Revision++
		if current.Revision == 0 {
			current.Revision = 1
		}
		current.UpdatedAtUnix = now.Unix()
		current.StateDigest = digestVolumeCatalogState(current)
		updated = current
		return putJSONStore(ctx, tx, volumeCatalogStateKey(r.root), current)
	})
	if err != nil {
		return VolumeCatalogRebuildPageResult{}, err
	}
	return VolumeCatalogRebuildPageResult{
		Revision: updated.Revision, InputCount: len(keys), RangePageCount: 1,
		ScannedSpecCount: updated.ScannedSpecCount, NextCursor: updated.RebuildCursor, Ready: updated.Ready,
	}, nil
}

func (r *Repository) ListVolumeCatalogPage(ctx context.Context, cursor string, limit int, expectedRevision uint64, filter VolumeCatalogFilter) (VolumeCatalogPage, error) {
	if r == nil {
		return VolumeCatalogPage{}, ErrVolumeCatalogInvalid
	}
	if limit == 0 {
		limit = VolumeCatalogPageDefault
	}
	if limit < 1 || limit > VolumeCatalogPageMaximum {
		return VolumeCatalogPage{}, fmt.Errorf("%w: page limit %d", ErrVolumeCatalogInvalid, limit)
	}
	filter = normalizeVolumeCatalogFilter(filter)
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		var page VolumeCatalogPage
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
			var err error
			page, err = listVolumeCatalogPageFromSnapshot(ctx, snapshot, r.root, cursor, limit, expectedRevision, filter)
			return err
		})
		return page, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		before, found, err := readVolumeCatalogState(ctx, r.kv, r.root)
		if err != nil {
			return VolumeCatalogPage{}, err
		}
		if !found || !before.Ready {
			return VolumeCatalogPage{}, ErrVolumeCatalogRebuildRequired
		}
		if expectedRevision != 0 && before.Revision != expectedRevision {
			return VolumeCatalogPage{}, ErrVolumeCatalogRevision
		}
		keys, next, err := r.kv.List(ctx, volumeCatalogSourcePrefix(r.root), strings.TrimSpace(cursor), limit)
		if err != nil {
			return VolumeCatalogPage{}, err
		}
		entries, err := readVolumeCatalogEntries(ctx, r.kv, r.root, keys, filter)
		if err != nil {
			return VolumeCatalogPage{}, err
		}
		after, afterFound, err := readVolumeCatalogState(ctx, r.kv, r.root)
		if err != nil {
			return VolumeCatalogPage{}, err
		}
		if afterFound && after.Ready && after.Revision == before.Revision {
			return VolumeCatalogPage{State: after, Entries: entries, NextCursor: next, ScannedCount: len(keys)}, nil
		}
	}
	return VolumeCatalogPage{}, ErrVolumeCatalogRevision
}

func listVolumeCatalogPageFromSnapshot(ctx context.Context, snapshot kvReadSnapshot, root, cursor string, limit int, expectedRevision uint64, filter VolumeCatalogFilter) (VolumeCatalogPage, error) {
	state, found, err := readVolumeCatalogState(ctx, snapshot, root)
	if err != nil {
		return VolumeCatalogPage{}, err
	}
	if !found || !state.Ready {
		return VolumeCatalogPage{}, ErrVolumeCatalogRebuildRequired
	}
	if expectedRevision != 0 && state.Revision != expectedRevision {
		return VolumeCatalogPage{}, ErrVolumeCatalogRevision
	}
	keys, next, err := snapshot.List(ctx, volumeCatalogSourcePrefix(root), strings.TrimSpace(cursor), limit)
	if err != nil {
		return VolumeCatalogPage{}, err
	}
	entries, err := readVolumeCatalogEntries(ctx, snapshot, root, keys, filter)
	if err != nil {
		return VolumeCatalogPage{}, err
	}
	return VolumeCatalogPage{State: state, Entries: entries, NextCursor: next, ScannedCount: len(keys)}, nil
}

type volumeCatalogReader interface {
	Get(context.Context, string) ([]byte, bool, error)
}

func readVolumeCatalogEntries(ctx context.Context, reader volumeCatalogReader, root string, keys []string, filter VolumeCatalogFilter) ([]VolumeCatalogEntry, error) {
	batchKeys := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		volumeID, err := volumeIDFromCatalogSpecKey(root, key)
		if err != nil {
			return nil, err
		}
		batchKeys = append(batchKeys, key, volumeStateKey(root, volumeID))
	}
	values := make(map[string][]byte, len(batchKeys))
	if batcher, ok := reader.(interface {
		BatchGet(context.Context, []string) (map[string][]byte, error)
	}); ok {
		for start := 0; start < len(batchKeys); start += VolumeCatalogBatchMaximum {
			end := min(start+VolumeCatalogBatchMaximum, len(batchKeys))
			batch, err := batcher.BatchGet(ctx, batchKeys[start:end])
			if err != nil {
				return nil, err
			}
			for key, raw := range batch {
				values[key] = raw
			}
		}
	} else {
		for _, key := range batchKeys {
			raw, found, err := reader.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				values[key] = raw
			}
		}
	}
	entries := make([]VolumeCatalogEntry, 0, len(keys))
	for _, key := range keys {
		volumeID, _ := volumeIDFromCatalogSpecKey(root, key)
		specRaw, specFound := values[key]
		stateRaw, stateFound := values[volumeStateKey(root, volumeID)]
		if !specFound || !stateFound {
			return nil, fmt.Errorf("%w: incomplete volume %s", ErrVolumeCatalogInvalid, volumeID)
		}
		var spec VolumeSpecRecord
		var state VolumeState
		if err := json.Unmarshal(specRaw, &spec); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(stateRaw, &state); err != nil {
			return nil, err
		}
		if volumeCatalogEntryMatches(state, spec, filter) {
			entries = append(entries, VolumeCatalogEntry{State: state, Spec: spec})
		}
	}
	return entries, nil
}

func advanceVolumeCatalogRevision(ctx context.Context, store kvReadWriter, root string, now time.Time) error {
	state, found, err := readVolumeCatalogState(ctx, store, root)
	if err != nil {
		return err
	}
	if !found {
		state = VolumeCatalogState{SchemaVersion: VolumeCatalogSchemaVersion}
	}
	state.Revision++
	if state.Revision == 0 {
		state.Revision = 1
	}
	state.UpdatedAtUnix = now.UTC().Unix()
	state.StateDigest = digestVolumeCatalogState(state)
	return putJSONStore(ctx, store, volumeCatalogStateKey(root), state)
}

func readVolumeCatalogState(ctx context.Context, reader interface {
	Get(context.Context, string) ([]byte, bool, error)
}, root string) (VolumeCatalogState, bool, error) {
	raw, found, err := reader.Get(ctx, volumeCatalogStateKey(root))
	if err != nil || !found {
		return VolumeCatalogState{}, found, err
	}
	var state VolumeCatalogState
	if err := json.Unmarshal(raw, &state); err != nil {
		return VolumeCatalogState{}, false, err
	}
	if state.SchemaVersion != VolumeCatalogSchemaVersion || state.Revision == 0 || state.StateDigest != digestVolumeCatalogState(state) {
		return VolumeCatalogState{}, false, ErrVolumeCatalogInvalid
	}
	return state, true, nil
}

func normalizeVolumeCatalogFilter(filter VolumeCatalogFilter) VolumeCatalogFilter {
	filter.RedundancyBackend = strings.ToLower(strings.TrimSpace(filter.RedundancyBackend))
	filter.TopologyMode = strings.ToLower(strings.TrimSpace(filter.TopologyMode))
	return filter
}

func volumeCatalogEntryMatches(state VolumeState, spec VolumeSpecRecord, filter VolumeCatalogFilter) bool {
	if filter.Status != "" && state.Status != filter.Status {
		return false
	}
	backend := strings.ToLower(strings.TrimSpace(state.RedundancyBackend))
	if backend == "" {
		backend = strings.ToLower(strings.TrimSpace(spec.RedundancyBackend))
	}
	if backend == "" {
		backend = RedundancyBackendReplicated
	}
	if filter.RedundancyBackend != "" && backend != filter.RedundancyBackend {
		return false
	}
	topology := strings.ToLower(strings.TrimSpace(state.TopologyMode))
	if topology == "" {
		topology = strings.ToLower(strings.TrimSpace(spec.TopologyMode))
	}
	return filter.TopologyMode == "" || topology == filter.TopologyMode
}

func volumeCatalogSourcePrefix(root string) string {
	return fmt.Sprintf("%s/admin/volumes/", root)
}

func volumeCatalogStateKey(root string) string {
	return fmt.Sprintf("%s/derived/ad/v1/volume-catalog/state", root)
}

func volumeIDFromCatalogSpecKey(root, key string) (string, error) {
	prefix := volumeCatalogSourcePrefix(root)
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/spec") {
		return "", fmt.Errorf("%w: invalid spec key %q", ErrVolumeCatalogInvalid, key)
	}
	volumeID := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/spec")
	if volumeID == "" || strings.Contains(volumeID, "/") {
		return "", fmt.Errorf("%w: invalid spec key %q", ErrVolumeCatalogInvalid, key)
	}
	canonical, err := CanonicalVolumeID(volumeID)
	if err != nil || canonical != volumeID {
		return "", fmt.Errorf("%w: invalid volume id in spec key %q", ErrVolumeCatalogInvalid, key)
	}
	return volumeID, nil
}

func digestVolumeCatalogState(state VolumeCatalogState) string {
	state.StateDigest = ""
	raw, _ := json.Marshal(state)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
