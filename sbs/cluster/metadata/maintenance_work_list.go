package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMaintenanceWorkListRebuildRequired = errors.New("maintenance work list projection rebuild required")
	ErrMaintenanceWorkListChanged         = errors.New("maintenance work list projection changed")
)

const maintenanceWorkListRevisionSchemaVersion = 2

type MaintenanceWorkListProjection struct {
	MaintenanceEpoch string
	RevisionDigest   string
	UpdatedAtUnix    int64
}

type MaintenanceWorkDetailPage struct {
	Work                  []MaintenanceWorkRecord
	NextCursor            string
	ScannedCount          int
	RangePageCount        int
	BatchGetCount         int
	BatchGetKeyCount      int
	BackendFullScanCount  int
	FullCompletionCount   int
	NestedCompletionCount int
}

type maintenanceWorkListRevisionState struct {
	SchemaVersion int    `json:"schema_version"`
	Reason        string `json:"reason"`
	State         string `json:"state"`
	Shard         int    `json:"shard"`
	Revision      uint64 `json:"revision"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	StateDigest   string `json:"state_digest"`
}

type maintenanceWorkListRevisionSnapshot struct {
	SchemaVersion    int                                    `json:"schema_version"`
	MaintenanceEpoch string                                 `json:"maintenance_epoch"`
	Reason           string                                 `json:"reason"`
	Ready            [MaintenanceWorkIndexShardCount]uint64 `json:"ready"`
	Leased           [MaintenanceWorkIndexShardCount]uint64 `json:"leased"`
}

func advanceMaintenanceWorkListRevision(ctx context.Context, store kvReadWriter, root string, index MaintenanceWorkIndexRecord, now time.Time) error {
	if store == nil || !validMaintenanceWorkReason(index.Reason) || !indexedMaintenanceWorkState(index.State) || index.WorkID == "" {
		return fmt.Errorf("%w: maintenance work revision identity", ErrMaintenanceIndexInvalid)
	}
	shard := MaintenanceWorkIndexVirtualShard(index.WorkID)
	return advanceMaintenanceWorkListRevisionShard(ctx, store, root, index.Reason, index.State, shard, now)
}

func advanceMaintenanceWorkListRevisionShard(ctx context.Context, store kvReadWriter, root, reason, workState string, shard int, now time.Time) error {
	if store == nil || !validMaintenanceWorkReason(reason) || !indexedMaintenanceWorkState(workState) || shard < 0 || shard >= MaintenanceWorkIndexShardCount {
		return fmt.Errorf("%w: maintenance work revision shard", ErrMaintenanceIndexInvalid)
	}
	key := maintenanceWorkListRevisionKey(root, reason, workState, shard)
	var state maintenanceWorkListRevisionState
	found, err := getOptionalJSONStore(ctx, store, key, &state)
	if err != nil {
		return err
	}
	if found {
		if state.SchemaVersion == 1 {
			state = maintenanceWorkListRevisionState{SchemaVersion: maintenanceWorkListRevisionSchemaVersion, Reason: reason, State: workState, Shard: shard}
		} else {
			if err := validateMaintenanceWorkListRevisionState(state, reason, workState, shard); err != nil {
				return err
			}
		}
	} else {
		state = maintenanceWorkListRevisionState{SchemaVersion: maintenanceWorkListRevisionSchemaVersion, Reason: reason, State: workState, Shard: shard}
	}
	state.Revision++
	if state.Revision == 0 {
		state.Revision = 1
	}
	state.UpdatedAtUnix = now.UTC().Unix()
	state.StateDigest = digestMaintenanceWorkListRevisionState(state)
	return putJSONStore(ctx, store, key, state)
}

func advanceMaintenanceWorkListRevisionForIndexKey(ctx context.Context, store kvReadWriter, root, key string, now time.Time) error {
	relative := strings.TrimPrefix(key, maintenanceWorkIndexRootPrefix(root))
	if relative == key {
		return nil
	}
	parts := strings.Split(relative, "/")
	if len(parts) != 5 {
		return fmt.Errorf("%w: maintenance work index key", ErrMaintenanceIndexInvalid)
	}
	shard, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("%w: maintenance work index shard", ErrMaintenanceIndexInvalid)
	}
	if shard >= MaintenanceWorkIndexShardCount && shard < maintenanceWorkLegacyShardCount {
		return nil
	}
	return advanceMaintenanceWorkListRevisionShard(ctx, store, root, parts[0], parts[1], shard, now)
}

func putMaintenanceWorkIndexStore(ctx context.Context, store kvReadWriter, root string, index MaintenanceWorkIndexRecord, now time.Time) error {
	if err := putJSONStore(ctx, store, maintenanceWorkIndexKey(root, index), index); err != nil {
		return err
	}
	return advanceMaintenanceWorkListRevision(ctx, store, root, index, now)
}

func deleteMaintenanceWorkIndexStore(ctx context.Context, store kvReadWriter, root string, index MaintenanceWorkIndexRecord, now time.Time) error {
	if err := store.Delete(ctx, maintenanceWorkIndexKey(root, index)); err != nil {
		return err
	}
	return advanceMaintenanceWorkListRevision(ctx, store, root, index, now)
}

func (r *Repository) GetMaintenanceWorkListProjection(ctx context.Context, reason string) (MaintenanceWorkListProjection, error) {
	reason = strings.TrimSpace(reason)
	if r == nil || !validMaintenanceWorkReason(reason) {
		return MaintenanceWorkListProjection{}, fmt.Errorf("%w: maintenance work reason", ErrMaintenanceIndexInvalid)
	}
	read := func(store kvReadSnapshot) (MaintenanceWorkListProjection, error) {
		var maintenance MaintenanceIndexState
		found, err := getOptionalJSONStore(ctx, store, maintenanceIndexStateKey(r.root), &maintenance)
		if err != nil {
			return MaintenanceWorkListProjection{}, err
		}
		if !found || validateMaintenanceIndexState(maintenance) != nil || !maintenance.WorkProjectionReady {
			return MaintenanceWorkListProjection{}, ErrMaintenanceWorkListRebuildRequired
		}
		keys := make([]string, 0, MaintenanceWorkIndexShardCount*2)
		for _, state := range []string{MaintenanceWorkStateReady, MaintenanceWorkStateLeased} {
			for shard := 0; shard < MaintenanceWorkIndexShardCount; shard++ {
				keys = append(keys, maintenanceWorkListRevisionKey(r.root, reason, state, shard))
			}
		}
		values, err := store.BatchGet(ctx, keys)
		if err != nil {
			return MaintenanceWorkListProjection{}, err
		}
		snapshot := maintenanceWorkListRevisionSnapshot{SchemaVersion: maintenanceWorkListRevisionSchemaVersion, MaintenanceEpoch: maintenance.ActiveEpoch, Reason: reason}
		updatedAt := maintenance.UpdatedAtUnix
		for _, stateName := range []string{MaintenanceWorkStateReady, MaintenanceWorkStateLeased} {
			for shard := 0; shard < MaintenanceWorkIndexShardCount; shard++ {
				raw, found := values[maintenanceWorkListRevisionKey(r.root, reason, stateName, shard)]
				if !found {
					continue
				}
				var state maintenanceWorkListRevisionState
				if err := json.Unmarshal(raw, &state); err != nil {
					return MaintenanceWorkListProjection{}, err
				}
				if err := validateMaintenanceWorkListRevisionState(state, reason, stateName, shard); err != nil {
					return MaintenanceWorkListProjection{}, err
				}
				if stateName == MaintenanceWorkStateReady {
					snapshot.Ready[shard] = state.Revision
				} else {
					snapshot.Leased[shard] = state.Revision
				}
				if state.UpdatedAtUnix > updatedAt {
					updatedAt = state.UpdatedAtUnix
				}
			}
		}
		return MaintenanceWorkListProjection{MaintenanceEpoch: maintenance.ActiveEpoch, RevisionDigest: digestSummaryValue(snapshot), UpdatedAtUnix: updatedAt}, nil
	}
	snapshotter, ok := r.kv.(consistentSnapshotKV)
	if !ok {
		return MaintenanceWorkListProjection{}, ErrMaintenanceWorkListRebuildRequired
	}
	var result MaintenanceWorkListProjection
	err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
		var err error
		result, err = read(snapshot)
		return err
	})
	return result, err
}

func (r *Repository) ListMaintenanceWorkDetailPage(ctx context.Context, reason, state, cursor string, limit int) (MaintenanceWorkDetailPage, error) {
	result := MaintenanceWorkDetailPage{}
	reason, state, cursor = strings.TrimSpace(reason), strings.TrimSpace(state), strings.TrimSpace(cursor)
	if r == nil || !validMaintenanceWorkReason(reason) || !indexedMaintenanceWorkState(state) || limit < 1 || limit > MaintenanceWorkPageMaximum {
		return result, fmt.Errorf("%w: maintenance work detail page", ErrMaintenanceIndexInvalid)
	}
	prefix := maintenanceWorkIndexPrefix(r.root, reason, state)
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return result, fmt.Errorf("%w: maintenance work cursor", ErrMaintenanceIndexInvalid)
	}
	read := func(store kvReadSnapshot) error {
		var maintenance MaintenanceIndexState
		found, err := getOptionalJSONStore(ctx, store, maintenanceIndexStateKey(r.root), &maintenance)
		if err != nil {
			return err
		}
		if !found || validateMaintenanceIndexState(maintenance) != nil || !maintenance.WorkProjectionReady {
			return ErrMaintenanceWorkListRebuildRequired
		}
		keys, next, err := store.List(ctx, prefix, cursor, limit)
		result.RangePageCount = 1
		if err != nil {
			return err
		}
		result.NextCursor, result.ScannedCount = next, len(keys)
		indexValues, err := batchGetOperationListValues(ctx, store, keys)
		if err != nil {
			return err
		}
		result.BatchGetCount += operationListBatchCount(len(keys))
		result.BatchGetKeyCount += len(keys)
		indexes := make([]MaintenanceWorkIndexRecord, 0, len(keys))
		authorityKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			raw, found := indexValues[key]
			if !found {
				return ErrMaintenanceWorkListChanged
			}
			var index MaintenanceWorkIndexRecord
			if err := decodeMaintenanceIndexJSON(raw, &index); err != nil {
				return err
			}
			if validateMaintenanceWorkIndexRecord(index) != nil || index.Reason != reason || index.State != state || maintenanceWorkIndexKey(r.root, index) != key {
				return ErrMaintenanceIndexInvalid
			}
			indexes = append(indexes, index)
			authorityKeys = append(authorityKeys, maintenanceWorkKey(r.root, index.WorkID))
		}
		authorities, err := batchGetOperationListValues(ctx, store, authorityKeys)
		if err != nil {
			return err
		}
		result.BatchGetCount += operationListBatchCount(len(authorityKeys))
		result.BatchGetKeyCount += len(authorityKeys)
		for i, index := range indexes {
			raw, found := authorities[authorityKeys[i]]
			if !found {
				return ErrMaintenanceWorkListChanged
			}
			var work MaintenanceWorkRecord
			if err := decodeMaintenanceIndexJSON(raw, &work); err != nil {
				return err
			}
			if validateMaintenanceWorkRecord(work) != nil || work.WorkID != index.WorkID || work.WorkRevision != index.WorkRevision || work.WorkDigest != index.WorkDigest || maintenanceWorkIndexRecord(work).IndexDigest != index.IndexDigest {
				return ErrMaintenanceWorkListChanged
			}
			result.Work = append(result.Work, work)
		}
		return nil
	}
	snapshotter, ok := r.kv.(consistentSnapshotKV)
	if !ok {
		return result, ErrMaintenanceWorkListRebuildRequired
	}
	err := snapshotter.RunInReadSnapshot(ctx, read)
	return result, err
}

func maintenanceWorkListRevisionKey(root, reason, state string, shard int) string {
	return fmt.Sprintf("%s/derived/ad/v1/work-list-revision/%s/%s/%02d", root, reason, state, shard)
}

func validateMaintenanceWorkListRevisionState(value maintenanceWorkListRevisionState, reason, state string, shard int) error {
	if value.SchemaVersion != maintenanceWorkListRevisionSchemaVersion || value.Reason != reason || value.State != state || value.Shard != shard || value.Revision == 0 || value.UpdatedAtUnix <= 0 || value.StateDigest != digestMaintenanceWorkListRevisionState(value) {
		return ErrMaintenanceIndexInvalid
	}
	return nil
}

func digestMaintenanceWorkListRevisionState(value maintenanceWorkListRevisionState) string {
	value.StateDigest = ""
	return digestSummaryValue(value)
}
