package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	OperationListSourceAdmin    = "admin"
	OperationListSourceMutation = "mutation"
)

var (
	ErrOperationListInvalid         = errors.New("invalid operation list request")
	ErrOperationListRebuildRequired = errors.New("operation list projection rebuild required")
	ErrOperationListChanged         = errors.New("operation list projection changed")
)

type OperationListProjection struct {
	MaintenanceEpoch string
	RevisionDigest   string
	UpdatedAtUnix    int64
}

type MutationOperationPage struct {
	Operations            []MutationOperationRecord
	NextCursor            string
	ScannedCount          int
	RangePageCount        int
	BatchGetCount         int
	BatchGetKeyCount      int
	BackendFullScanCount  int
	FullCompletionCount   int
	NestedCompletionCount int
}

type operationListRevisionState struct {
	SchemaVersion int    `json:"schema_version"`
	Source        string `json:"source"`
	Shard         int    `json:"shard"`
	Revision      uint64 `json:"revision"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	StateDigest   string `json:"state_digest"`
}

type operationListRevisionSnapshot struct {
	SchemaVersion    int                                `json:"schema_version"`
	MaintenanceEpoch string                             `json:"maintenance_epoch"`
	Admin            [MaintenanceIndexShardCount]uint64 `json:"admin"`
	Mutation         [MaintenanceIndexShardCount]uint64 `json:"mutation"`
}

func PutAdminOperationListRecord(ctx context.Context, store any, root, operationID string, raw []byte, now time.Time) error {
	operationID = strings.TrimSpace(operationID)
	writer, ok := store.(ReadWriter)
	if !ok || operationID == "" || len(raw) == 0 {
		return fmt.Errorf("%w: admin operation record is required", ErrOperationListInvalid)
	}
	apply := func(tx ReadWriter) error {
		return PutAdminOperationListRecordInTransaction(ctx, tx, root, operationID, raw, now)
	}
	if runner, ok := store.(transactionalKV); ok {
		return runner.RunInTransaction(ctx, func(tx kvReadWriter) error { return apply(tx) })
	}
	return apply(writer)
}

// PutAdminOperationListRecordInTransaction updates the admin operation row and
// its paged-list revision in a caller-owned transaction. It exists so the
// service can attach the Phase AD summary delta to that same transaction.
func PutAdminOperationListRecordInTransaction(ctx context.Context, store ReadWriter, root, operationID string, raw []byte, now time.Time) error {
	operationID = strings.TrimSpace(operationID)
	if store == nil || operationID == "" || len(raw) == 0 {
		return fmt.Errorf("%w: admin operation record is required", ErrOperationListInvalid)
	}
	if err := store.Set(ctx, adminOperationListKey(root, operationID), raw); err != nil {
		return err
	}
	return AdvanceOperationListRevision(ctx, store, root, OperationListSourceAdmin, operationID, now)
}

func AdvanceOperationListRevision(ctx context.Context, store ReadWriter, root, source, operationID string, now time.Time) error {
	source = strings.ToLower(strings.TrimSpace(source))
	operationID = strings.TrimSpace(operationID)
	if store == nil || !validOperationListSource(source) || operationID == "" {
		return fmt.Errorf("%w: source and operation_id are required", ErrOperationListInvalid)
	}
	shard := SummaryVirtualShard(source + "\x00" + operationID)
	key := operationListRevisionKey(root, source, shard)
	var state operationListRevisionState
	found, err := getOptionalJSONStore(ctx, store, key, &state)
	if err != nil {
		return err
	}
	if found {
		if err := validateOperationListRevisionState(state, source, shard); err != nil {
			return err
		}
	} else {
		state = operationListRevisionState{SchemaVersion: 1, Source: source, Shard: shard}
	}
	state.Revision++
	if state.Revision == 0 {
		state.Revision = 1
	}
	state.UpdatedAtUnix = now.UTC().Unix()
	state.StateDigest = digestOperationListRevisionState(state)
	return putJSONStore(ctx, store, key, state)
}

func (r *Repository) GetOperationListProjection(ctx context.Context) (OperationListProjection, error) {
	if r == nil {
		return OperationListProjection{}, ErrOperationListInvalid
	}
	maintenance, err := r.GetMaintenanceIndexState(ctx)
	if errors.Is(err, ErrNotFound) {
		return OperationListProjection{}, ErrOperationListRebuildRequired
	}
	if err != nil {
		return OperationListProjection{}, err
	}
	keys := make([]string, 0, MaintenanceIndexShardCount*2)
	for _, source := range []string{OperationListSourceAdmin, OperationListSourceMutation} {
		for shard := 0; shard < MaintenanceIndexShardCount; shard++ {
			keys = append(keys, operationListRevisionKey(r.root, source, shard))
		}
	}
	values, err := batchGetOperationListValues(ctx, r.kv, keys)
	if err != nil {
		return OperationListProjection{}, err
	}
	snapshot := operationListRevisionSnapshot{SchemaVersion: 1, MaintenanceEpoch: maintenance.ActiveEpoch}
	updatedAt := maintenance.UpdatedAtUnix
	for _, source := range []string{OperationListSourceAdmin, OperationListSourceMutation} {
		for shard := 0; shard < MaintenanceIndexShardCount; shard++ {
			key := operationListRevisionKey(r.root, source, shard)
			raw, found := values[key]
			if !found {
				continue
			}
			var state operationListRevisionState
			if err := json.Unmarshal(raw, &state); err != nil {
				return OperationListProjection{}, err
			}
			if err := validateOperationListRevisionState(state, source, shard); err != nil {
				return OperationListProjection{}, err
			}
			if source == OperationListSourceAdmin {
				snapshot.Admin[shard] = state.Revision
			} else {
				snapshot.Mutation[shard] = state.Revision
			}
			if state.UpdatedAtUnix > updatedAt {
				updatedAt = state.UpdatedAtUnix
			}
		}
	}
	return OperationListProjection{
		MaintenanceEpoch: maintenance.ActiveEpoch,
		RevisionDigest:   digestSummaryValue(snapshot),
		UpdatedAtUnix:    updatedAt,
	}, nil
}

func (r *Repository) ListMutationOperationIndexPage(ctx context.Context, cursor string, limit int) (MutationOperationPage, error) {
	result := MutationOperationPage{}
	if r == nil || limit < 1 || limit > MaintenanceIndexPageMaximum {
		return result, fmt.Errorf("%w: page limit %d", ErrOperationListInvalid, limit)
	}
	read := func(store interface {
		Get(context.Context, string) ([]byte, bool, error)
		List(context.Context, string, string, int) ([]string, string, error)
	}) error {
		var state MaintenanceIndexState
		found, err := getOptionalJSONStore(ctx, store, maintenanceIndexStateKey(r.root), &state)
		if err != nil {
			return err
		}
		if !found {
			return ErrOperationListRebuildRequired
		}
		if err := validateMaintenanceIndexState(state); err != nil {
			return err
		}
		keys, next, err := store.List(ctx, operationByIDPrefix(r.root), strings.TrimSpace(cursor), limit)
		result.RangePageCount++
		if err != nil {
			return err
		}
		result.ScannedCount = len(keys)
		result.NextCursor = next
		indexValues, err := batchGetOperationListValues(ctx, store, keys)
		if err != nil {
			return err
		}
		result.BatchGetCount += operationListBatchCount(len(keys))
		result.BatchGetKeyCount += len(keys)
		indexes := make([]OperationByIDRecord, 0, len(keys))
		authorityKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			raw, found := indexValues[key]
			if !found {
				return ErrOperationListChanged
			}
			var index OperationByIDRecord
			if err := decodeMaintenanceIndexJSON(raw, &index); err != nil {
				return err
			}
			if err := validateOperationByIDRecord(index); err != nil || operationByIDKey(r.root, index.OperationID) != key {
				return ErrMaintenanceIndexInvalid
			}
			indexes = append(indexes, index)
			authorityKeys = append(authorityKeys, mutationOperationKey(r.root, index.VolumeID, index.OperationID))
		}
		authorityValues, err := batchGetOperationListValues(ctx, store, authorityKeys)
		if err != nil {
			return err
		}
		result.BatchGetCount += operationListBatchCount(len(authorityKeys))
		result.BatchGetKeyCount += len(authorityKeys)
		for i, index := range indexes {
			raw, found := authorityValues[authorityKeys[i]]
			if !found {
				return ErrOperationListChanged
			}
			var operation MutationOperationRecord
			if err := decodeMaintenanceIndexJSON(raw, &operation); err != nil {
				return err
			}
			if operationIndexRecord(operation).IndexDigest != index.IndexDigest {
				return ErrOperationListChanged
			}
			result.Operations = append(result.Operations, operation)
		}
		return nil
	}
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error { return read(snapshot) })
		return result, err
	}
	return result, read(r.kv)
}

func batchGetOperationListValues(ctx context.Context, reader interface {
	Get(context.Context, string) ([]byte, bool, error)
}, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	batcher, canBatch := reader.(interface {
		BatchGet(context.Context, []string) (map[string][]byte, error)
	})
	for start := 0; start < len(keys); start += MaintenanceIndexBatchMaximum {
		end := min(start+MaintenanceIndexBatchMaximum, len(keys))
		if canBatch {
			values, err := batcher.BatchGet(ctx, keys[start:end])
			if err != nil {
				return nil, err
			}
			for key, raw := range values {
				out[key] = raw
			}
			continue
		}
		for _, key := range keys[start:end] {
			raw, found, err := reader.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				out[key] = raw
			}
		}
	}
	return out, nil
}

func operationListBatchCount(keys int) int {
	if keys == 0 {
		return 0
	}
	return (keys + MaintenanceIndexBatchMaximum - 1) / MaintenanceIndexBatchMaximum
}

func validOperationListSource(source string) bool {
	return source == OperationListSourceAdmin || source == OperationListSourceMutation
}

func operationListRevisionKey(root, source string, shard int) string {
	return fmt.Sprintf("%s/derived/ad/v1/operation-list-revision/%s/%02d", root, source, shard)
}

func adminOperationListKey(root, operationID string) string {
	return fmt.Sprintf("%s/admin/operations/%s", root, operationID)
}

func operationByIDPrefix(root string) string {
	return fmt.Sprintf("%s/derived/ad/v1/operation-by-id/", root)
}

func validateOperationListRevisionState(state operationListRevisionState, source string, shard int) error {
	if state.SchemaVersion != 1 || state.Source != source || state.Shard != shard || state.Revision == 0 || state.UpdatedAtUnix <= 0 || state.StateDigest != digestOperationListRevisionState(state) {
		return ErrOperationListInvalid
	}
	return nil
}

func digestOperationListRevisionState(state operationListRevisionState) string {
	state.StateDigest = ""
	return digestSummaryValue(state)
}
