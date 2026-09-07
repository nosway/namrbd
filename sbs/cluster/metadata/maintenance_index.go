package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
)

const (
	MaintenanceIndexSchemaVersion   = 1
	MaintenanceIndexShardCount      = 64
	MaintenanceWorkIndexShardCount  = 32
	maintenanceWorkLegacyShardCount = 64
	MaintenanceIndexPageDefault     = 128
	MaintenanceIndexPageMaximum     = 512
	MaintenanceIndexBatchMaximum    = 128
)

var (
	ErrMaintenanceIndexConflict = errors.New("maintenance index identity conflict")
	ErrMaintenanceIndexInvalid  = errors.New("invalid maintenance index record")
	ErrMaintenanceIndexChanged  = errors.New("maintenance index changed during read")
)

// PlacementByNodeRecord is the node-local affected-set projection for one
// replica set placement. Multiple replica descriptors for the same node are
// kept in one value so a single node/volume/placement identity owns one key.
type PlacementByNodeRecord struct {
	SchemaVersion   int                 `json:"schema_version"`
	NodeID          string              `json:"node_id"`
	VolumeID        string              `json:"volume_id"`
	PlacementRef    string              `json:"placement_ref"`
	ReplicaSetID    string              `json:"replica_set_id"`
	ReplicaSetEpoch uint64              `json:"replica_set_epoch"`
	PrimaryReplica  string              `json:"primary_replica_id,omitempty"`
	Replicas        []ReplicaDescriptor `json:"replicas"`
	IndexDigest     string              `json:"index_digest"`
}

type PlacementByNodePage struct {
	NodeID                string                  `json:"node_id"`
	RequestedLimit        int                     `json:"requested_limit"`
	Records               []PlacementByNodeRecord `json:"records"`
	NextCursor            string                  `json:"next_cursor"`
	PointGetCount         int                     `json:"point_get_count"`
	BatchGetCount         int                     `json:"batch_get_count"`
	BatchGetKeyCount      int                     `json:"batch_get_key_count"`
	RangePageCount        int                     `json:"range_page_count"`
	BackendFullScanCount  int                     `json:"backend_full_scan_count"`
	FullCompletionCount   int                     `json:"full_completion_count"`
	NestedCompletionCount int                     `json:"nested_completion_count"`
}

// OperationByIDRecord maps the globally unique mutation operation ID to its
// authoritative per-volume record. State is duplicated for anti-entropy and
// diagnostics; callers still point-read and validate the authority record.
type OperationByIDRecord struct {
	SchemaVersion int                    `json:"schema_version"`
	OperationID   string                 `json:"operation_id"`
	VolumeID      string                 `json:"volume_id"`
	Kind          string                 `json:"kind"`
	State         MutationOperationState `json:"state"`
	UpdatedAtUnix int64                  `json:"updated_at_unix,omitempty"`
	IndexDigest   string                 `json:"index_digest"`
}

type MutationOperationPointRead struct {
	Operation             MutationOperationRecord `json:"operation"`
	Index                 OperationByIDRecord     `json:"index"`
	PointGetCount         int                     `json:"point_get_count"`
	BackendFullScanCount  int                     `json:"backend_full_scan_count"`
	FullCompletionCount   int                     `json:"full_completion_count"`
	NestedCompletionCount int                     `json:"nested_completion_count"`
}

const MutationOperationCASBatchMaximum = 128

// MutationOperationCASUpdate describes one exact mutation-operation update.
// Expected is compared with the complete persisted record inside the same
// transaction that writes Updated and its derived indexes.
type MutationOperationCASUpdate struct {
	Expected MutationOperationRecord
	Updated  MutationOperationRecord
}

type maintenanceIndexReadStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	List(context.Context, string, string, int) ([]string, string, error)
}

// MaintenanceIndexVirtualShard keeps one node's placement keys in a single
// bounded prefix while distributing different nodes across 64 virtual shards.
func MaintenanceIndexVirtualShard(nodeID string) int {
	return SummaryVirtualShard(strings.TrimSpace(nodeID))
}

func (r *Repository) applyIndexedWrite(ctx context.Context, apply func(kvReadWriter) error) error {
	if runner, ok := r.kv.(transactionalKV); ok {
		return runner.RunInTransaction(ctx, apply)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return apply(r.kv)
}

// CompareAndSetMutationOperations atomically applies a bounded, canonical
// batch only when every complete source record still equals its expectation.
// This is intentionally transaction-only: a mutex cannot fence other TiKV
// writers and would make a multi-record finalizer unsafe.
func (r *Repository) CompareAndSetMutationOperations(ctx context.Context, updates []MutationOperationCASUpdate) error {
	if len(updates) == 0 || len(updates) > MutationOperationCASBatchMaximum {
		return fmt.Errorf("%w: mutation operation CAS batch size must be 1..%d", ErrMaintenanceIndexInvalid, MutationOperationCASBatchMaximum)
	}
	previousKey := ""
	for _, update := range updates {
		expectedVolumeID := maintenanceIndexVolumeID(update.Expected.VolumeID)
		updatedVolumeID := maintenanceIndexVolumeID(update.Updated.VolumeID)
		expectedOperationID := strings.TrimSpace(update.Expected.OperationID)
		updatedOperationID := strings.TrimSpace(update.Updated.OperationID)
		if expectedVolumeID == "" || expectedOperationID == "" || expectedVolumeID != updatedVolumeID || expectedOperationID != updatedOperationID || update.Expected.Kind != update.Updated.Kind {
			return fmt.Errorf("%w: mutation operation CAS cannot change identity or kind", ErrMaintenanceIndexInvalid)
		}
		key := expectedVolumeID + "\x00" + expectedOperationID
		if key <= previousKey {
			return fmt.Errorf("%w: mutation operation CAS updates must be sorted and unique", ErrMaintenanceIndexInvalid)
		}
		previousKey = key
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return fmt.Errorf("%w: mutation operation CAS requires transactional metadata", ErrMaintenanceIndexInvalid)
	}
	return runner.RunInTransaction(ctx, func(store kvReadWriter) error {
		for _, update := range updates {
			current, err := readMutationOperation(ctx, store, r.root, update.Expected.VolumeID, update.Expected.OperationID)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					return ErrCASConflict
				}
				return err
			}
			if !reflect.DeepEqual(current, update.Expected) {
				return ErrCASConflict
			}
		}
		for _, update := range updates {
			if err := r.writeMutationOperationWithIndex(ctx, store, update.Updated); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Repository) putReplicaSetWithPlacementIndex(ctx context.Context, store kvReadWriter, rec ReplicaSetState) error {
	rec.VolumeID = maintenanceIndexVolumeID(rec.VolumeID)
	if rec.VolumeID == "" {
		return fmt.Errorf("%w: volume_id is required", ErrMaintenanceIndexInvalid)
	}
	rec.ReplicaSetID = strings.TrimSpace(rec.ReplicaSetID)
	rec.PlacementRef = strings.TrimSpace(rec.PlacementRef)
	if rec.ReplicaSetID == "" {
		return fmt.Errorf("%w: replica_set_id is required", ErrMaintenanceIndexInvalid)
	}
	var before ReplicaSetState
	found, err := getOptionalJSONStore(ctx, store, replicaSetKey(r.root, rec.VolumeID, rec.ReplicaSetID), &before)
	if err != nil {
		return err
	}
	if err := writeReplicaSet(ctx, store, r.root, rec); err != nil {
		return err
	}
	if !found {
		return syncPlacementByNodeIndex(ctx, store, r.root, nil, &rec)
	}
	return syncPlacementByNodeIndex(ctx, store, r.root, &before, &rec)
}

func (r *Repository) deleteReplicaSetWithPlacementIndex(ctx context.Context, store kvReadWriter, volumeID, replicaSetID string) error {
	volumeID = maintenanceIndexVolumeID(volumeID)
	if volumeID == "" {
		return fmt.Errorf("%w: volume_id is required", ErrMaintenanceIndexInvalid)
	}
	replicaSetID = strings.TrimSpace(replicaSetID)
	if replicaSetID == "" {
		return fmt.Errorf("%w: replica_set_id is required", ErrMaintenanceIndexInvalid)
	}
	var before ReplicaSetState
	found, err := getOptionalJSONStore(ctx, store, replicaSetKey(r.root, volumeID, replicaSetID), &before)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, replicaSetKey(r.root, volumeID, replicaSetID)); err != nil {
		return err
	}
	if !found {
		return nil
	}
	return syncPlacementByNodeIndex(ctx, store, r.root, &before, nil)
}

func syncPlacementByNodeIndex(ctx context.Context, store kvReadWriter, root string, before, after *ReplicaSetState) error {
	oldRecords, err := placementIndexRecords(before)
	if err != nil {
		return err
	}
	newRecords, err := placementIndexRecords(after)
	if err != nil {
		return err
	}
	oldNodeIDs := sortedPlacementIndexNodeIDs(oldRecords)
	for _, nodeID := range oldNodeIDs {
		oldRecord := oldRecords[nodeID]
		newRecord, retained := newRecords[nodeID]
		if retained && placementByNodeKey(root, newRecord) == placementByNodeKey(root, oldRecord) {
			continue
		}
		if err := store.Delete(ctx, placementByNodeKey(root, oldRecord)); err != nil {
			return err
		}
	}
	newNodeIDs := sortedPlacementIndexNodeIDs(newRecords)
	for _, nodeID := range newNodeIDs {
		record := newRecords[nodeID]
		var existing PlacementByNodeRecord
		found, err := getOptionalJSONStore(ctx, store, placementByNodeKey(root, record), &existing)
		if err != nil {
			return err
		}
		if found {
			if err := validatePlacementByNodeRecord(existing); err != nil {
				return err
			}
			if existing.ReplicaSetID != record.ReplicaSetID {
				return fmt.Errorf("%w: placement %q on node %q belongs to replica sets %q and %q", ErrMaintenanceIndexConflict, record.PlacementRef, nodeID, existing.ReplicaSetID, record.ReplicaSetID)
			}
		}
		if err := putJSONStore(ctx, store, placementByNodeKey(root, record), record); err != nil {
			return err
		}
	}
	return nil
}

func sortedPlacementIndexNodeIDs(records map[string]PlacementByNodeRecord) []string {
	nodeIDs := make([]string, 0, len(records))
	for nodeID := range records {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	return nodeIDs
}

func placementIndexRecords(rec *ReplicaSetState) (map[string]PlacementByNodeRecord, error) {
	records := make(map[string]PlacementByNodeRecord)
	if rec == nil {
		return records, nil
	}
	volumeID := maintenanceIndexVolumeID(rec.VolumeID)
	if volumeID == "" {
		return nil, fmt.Errorf("%w: volume_id is required", ErrMaintenanceIndexInvalid)
	}
	replicaSetID := strings.TrimSpace(rec.ReplicaSetID)
	placementRef := strings.TrimSpace(rec.PlacementRef)
	if replicaSetID == "" {
		return nil, fmt.Errorf("%w: replica_set_id is required", ErrMaintenanceIndexInvalid)
	}
	// Legacy/incomplete replica records without a placement identity remain
	// readable, but cannot claim membership in the affected-set index.
	if placementRef == "" {
		return records, nil
	}
	for _, replica := range rec.Replicas {
		nodeID := strings.TrimSpace(replica.NodeID)
		if nodeID == "" {
			continue
		}
		record := records[nodeID]
		if record.SchemaVersion == 0 {
			record = PlacementByNodeRecord{
				SchemaVersion: MaintenanceIndexSchemaVersion,
				NodeID:        nodeID, VolumeID: volumeID, PlacementRef: placementRef,
				ReplicaSetID: replicaSetID, ReplicaSetEpoch: rec.Epoch,
				PrimaryReplica: strings.TrimSpace(rec.PrimaryReplicaID),
			}
		}
		replica.NodeID = nodeID
		record.Replicas = append(record.Replicas, replica)
		records[nodeID] = record
	}
	for nodeID, record := range records {
		sort.Slice(record.Replicas, func(i, j int) bool {
			if record.Replicas[i].ReplicaID == record.Replicas[j].ReplicaID {
				return record.Replicas[i].Role < record.Replicas[j].Role
			}
			return record.Replicas[i].ReplicaID < record.Replicas[j].ReplicaID
		})
		record.IndexDigest = digestPlacementByNodeRecord(record)
		records[nodeID] = record
	}
	return records, nil
}

func (r *Repository) ListPlacementByNodePage(ctx context.Context, nodeID, cursor string, limit int) (PlacementByNodePage, error) {
	nodeID = strings.TrimSpace(nodeID)
	if limit == 0 {
		limit = MaintenanceIndexPageDefault
	}
	result := PlacementByNodePage{NodeID: nodeID, RequestedLimit: limit, RangePageCount: 1}
	if r == nil || nodeID == "" || limit < 1 || limit > MaintenanceIndexPageMaximum {
		return result, fmt.Errorf("%w: invalid node placement page request", ErrMaintenanceIndexInvalid)
	}
	prefix := placementByNodePrefix(r.root, nodeID)
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return result, fmt.Errorf("%w: cursor does not belong to node", ErrMaintenanceIndexInvalid)
	}
	read := func(store maintenanceIndexReadStore) error {
		keys, next, err := store.List(ctx, prefix, cursor, limit)
		if err != nil {
			return err
		}
		result.NextCursor = next
		values := make(map[string][]byte, len(keys))
		if batcher, ok := store.(kvBatchReader); ok {
			for start := 0; start < len(keys); start += MaintenanceIndexBatchMaximum {
				end := min(start+MaintenanceIndexBatchMaximum, len(keys))
				batch, err := batcher.BatchGet(ctx, keys[start:end])
				if err != nil {
					return err
				}
				result.BatchGetCount++
				result.BatchGetKeyCount += end - start
				for key, raw := range batch {
					values[key] = raw
				}
			}
		} else {
			for _, key := range keys {
				raw, found, err := store.Get(ctx, key)
				if err != nil {
					return err
				}
				result.PointGetCount++
				if found {
					values[key] = raw
				}
			}
		}
		for _, key := range keys {
			raw, found := values[key]
			if !found {
				return fmt.Errorf("%w: placement index key disappeared", ErrMaintenanceIndexChanged)
			}
			var record PlacementByNodeRecord
			if err := decodeMaintenanceIndexJSON(raw, &record); err != nil {
				return err
			}
			if err := validatePlacementByNodeRecord(record); err != nil {
				return err
			}
			if record.NodeID != nodeID || placementByNodeKey(r.root, record) != key {
				return fmt.Errorf("%w: placement index key/value identity differs", ErrMaintenanceIndexInvalid)
			}
			result.Records = append(result.Records, record)
		}
		return nil
	}
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error { return read(snapshot) })
		return result, err
	}
	return result, read(r.kv)
}

func (r *Repository) writeMutationOperationWithIndex(ctx context.Context, store kvReadWriter, rec MutationOperationRecord) error {
	rec.OperationID = strings.TrimSpace(rec.OperationID)
	rec.VolumeID = maintenanceIndexVolumeID(rec.VolumeID)
	if rec.VolumeID == "" {
		return fmt.Errorf("%w: volume_id is required", ErrMaintenanceIndexInvalid)
	}
	if rec.OperationID == "" {
		return fmt.Errorf("%w: operation_id is required", ErrMaintenanceIndexInvalid)
	}
	if err := validateMutationRetiredReplicaTargets(rec); err != nil {
		return err
	}
	indexKey := operationByIDKey(r.root, rec.OperationID)
	var existing OperationByIDRecord
	found, err := getOptionalJSONStore(ctx, store, indexKey, &existing)
	if err != nil {
		return err
	}
	if found {
		if err := validateOperationByIDRecord(existing); err != nil {
			return err
		}
		if existing.VolumeID != rec.VolumeID || existing.Kind != rec.Kind {
			return fmt.Errorf("%w: operation %q already belongs to volume %q kind %q", ErrMaintenanceIndexConflict, rec.OperationID, existing.VolumeID, existing.Kind)
		}
	}
	var before *MutationOperationRecord
	if operationChildParent(rec) != "" {
		if existingOperation, beforeErr := readMutationOperation(ctx, store, r.root, rec.VolumeID, rec.OperationID); beforeErr == nil {
			before = &existingOperation
		} else if !errors.Is(beforeErr, ErrNotFound) {
			return beforeErr
		}
	}
	if err := writeMutationOperation(ctx, store, r.root, rec); err != nil {
		return err
	}
	index := operationIndexRecord(rec)
	if err := putJSONStore(ctx, store, indexKey, index); err != nil {
		return err
	}
	if err := updateOperationChildrenSummary(ctx, store, r.root, before, &rec); err != nil {
		return err
	}
	return AdvanceOperationListRevision(ctx, store, r.root, OperationListSourceMutation, rec.OperationID, r.now())
}

func validateMutationRetiredReplicaTargets(rec MutationOperationRecord) error {
	if len(rec.RetiredReplicaTargets) > 0 && !rec.RetiredReplicaTargetsResolved {
		return fmt.Errorf("%w: retired replica targets require resolved authority", ErrMaintenanceIndexInvalid)
	}
	if rec.RetiredReplicaTargetsResolved && rec.Kind != "transition" {
		return fmt.Errorf("%w: retired replica targets are only valid for transition operations", ErrMaintenanceIndexInvalid)
	}
	previousNodeID := ""
	for _, target := range rec.RetiredReplicaTargets {
		if target.NodeID == "" || strings.TrimSpace(target.NodeID) != target.NodeID || target.NodeID <= previousNodeID {
			return fmt.Errorf("%w: retired replica target nodes must be nonempty, canonical, sorted, and unique", ErrMaintenanceIndexInvalid)
		}
		if len(target.SourceReplicaIDs) == 0 {
			return fmt.Errorf("%w: retired replica target %q requires source replica ids", ErrMaintenanceIndexInvalid, target.NodeID)
		}
		previousReplicaID := ""
		for _, replicaID := range target.SourceReplicaIDs {
			if replicaID == "" || strings.TrimSpace(replicaID) != replicaID || replicaID <= previousReplicaID {
				return fmt.Errorf("%w: retired replica target %q replica ids must be nonempty, canonical, sorted, and unique", ErrMaintenanceIndexInvalid, target.NodeID)
			}
			previousReplicaID = replicaID
		}
		previousNodeID = target.NodeID
	}
	return nil
}

func (r *Repository) deleteMutationOperationWithIndex(ctx context.Context, store kvReadWriter, volumeID, operationID string) error {
	canonicalVolumeID := maintenanceIndexVolumeID(volumeID)
	if canonicalVolumeID == "" {
		return fmt.Errorf("%w: volume_id is required", ErrMaintenanceIndexInvalid)
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return fmt.Errorf("%w: operation_id is required", ErrMaintenanceIndexInvalid)
	}
	indexKey := operationByIDKey(r.root, operationID)
	var index OperationByIDRecord
	found, err := getOptionalJSONStore(ctx, store, indexKey, &index)
	if err != nil {
		return err
	}
	if found {
		if err := validateOperationByIDRecord(index); err != nil {
			return err
		}
		if index.VolumeID != canonicalVolumeID {
			return fmt.Errorf("%w: operation %q belongs to volume %q", ErrMaintenanceIndexConflict, operationID, index.VolumeID)
		}
	}
	var before *MutationOperationRecord
	if existingOperation, beforeErr := readMutationOperation(ctx, store, r.root, canonicalVolumeID, operationID); beforeErr == nil {
		before = &existingOperation
	} else if !errors.Is(beforeErr, ErrNotFound) {
		return beforeErr
	}
	if err := store.Delete(ctx, mutationOperationKey(r.root, canonicalVolumeID, operationID)); err != nil {
		return err
	}
	if found {
		if err := store.Delete(ctx, indexKey); err != nil {
			return err
		}
		if err := updateOperationChildrenSummary(ctx, store, r.root, before, nil); err != nil {
			return err
		}
		return AdvanceOperationListRevision(ctx, store, r.root, OperationListSourceMutation, operationID, r.now())
	}
	return nil
}

func (r *Repository) GetMutationOperationByIDPoint(ctx context.Context, operationID string) (MutationOperationPointRead, error) {
	result := MutationOperationPointRead{}
	operationID = strings.TrimSpace(operationID)
	if r == nil || operationID == "" {
		return result, fmt.Errorf("%w: operation_id is required", ErrMaintenanceIndexInvalid)
	}
	read := func(store interface {
		Get(context.Context, string) ([]byte, bool, error)
	}) error {
		indexKey := operationByIDKey(r.root, operationID)
		var index OperationByIDRecord
		result.PointGetCount++
		found, err := getOptionalJSONStore(ctx, store, indexKey, &index)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		if err := validateOperationByIDRecord(index); err != nil {
			return err
		}
		if index.OperationID != operationID {
			return fmt.Errorf("%w: operation index identity differs", ErrMaintenanceIndexInvalid)
		}
		var operation MutationOperationRecord
		result.PointGetCount++
		found, err = getOptionalJSONStore(ctx, store, mutationOperationKey(r.root, index.VolumeID, index.OperationID), &operation)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: indexed operation authority is missing", ErrMaintenanceIndexChanged)
		}
		authorityVolumeID := maintenanceIndexVolumeID(operation.VolumeID)
		if authorityVolumeID == "" {
			return fmt.Errorf("%w: operation authority volume_id", ErrMaintenanceIndexInvalid)
		}
		if operation.OperationID != index.OperationID || authorityVolumeID != index.VolumeID || operation.Kind != index.Kind || operation.State != index.State || operation.LastUpdatedAtUnix != index.UpdatedAtUnix {
			return fmt.Errorf("%w: operation authority and index differ", ErrMaintenanceIndexChanged)
		}
		result.Index = index
		result.Operation = operation
		return nil
	}
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error { return read(snapshot) })
		return result, err
	}
	return result, read(r.kv)
}

func (r *Repository) GetMutationOperationByID(ctx context.Context, operationID string) (MutationOperationRecord, error) {
	result, err := r.GetMutationOperationByIDPoint(ctx, operationID)
	return result.Operation, err
}

func operationIndexRecord(rec MutationOperationRecord) OperationByIDRecord {
	record := OperationByIDRecord{
		SchemaVersion: MaintenanceIndexSchemaVersion,
		OperationID:   strings.TrimSpace(rec.OperationID), VolumeID: maintenanceIndexVolumeID(rec.VolumeID),
		Kind: rec.Kind, State: rec.State, UpdatedAtUnix: rec.LastUpdatedAtUnix,
	}
	record.IndexDigest = digestOperationByIDRecord(record)
	return record
}

func validatePlacementByNodeRecord(record PlacementByNodeRecord) error {
	if record.SchemaVersion != MaintenanceIndexSchemaVersion || strings.TrimSpace(record.NodeID) == "" || strings.TrimSpace(record.VolumeID) == "" || strings.TrimSpace(record.PlacementRef) == "" || strings.TrimSpace(record.ReplicaSetID) == "" || len(record.Replicas) == 0 {
		return fmt.Errorf("%w: placement index identity", ErrMaintenanceIndexInvalid)
	}
	if maintenanceIndexVolumeID(record.VolumeID) != record.VolumeID {
		return fmt.Errorf("%w: placement volume_id", ErrMaintenanceIndexInvalid)
	}
	for _, replica := range record.Replicas {
		if strings.TrimSpace(replica.NodeID) != record.NodeID || strings.TrimSpace(replica.ReplicaID) == "" {
			return fmt.Errorf("%w: placement replica identity", ErrMaintenanceIndexInvalid)
		}
	}
	if record.IndexDigest != digestPlacementByNodeRecord(record) {
		return fmt.Errorf("%w: placement index digest", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateOperationByIDRecord(record OperationByIDRecord) error {
	if record.SchemaVersion != MaintenanceIndexSchemaVersion || strings.TrimSpace(record.OperationID) == "" || strings.TrimSpace(record.VolumeID) == "" {
		return fmt.Errorf("%w: operation index identity", ErrMaintenanceIndexInvalid)
	}
	if maintenanceIndexVolumeID(record.VolumeID) != record.VolumeID {
		return fmt.Errorf("%w: operation volume_id", ErrMaintenanceIndexInvalid)
	}
	if record.IndexDigest != digestOperationByIDRecord(record) {
		return fmt.Errorf("%w: operation index digest", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func digestPlacementByNodeRecord(record PlacementByNodeRecord) string {
	record.IndexDigest = ""
	return digestSummaryValue(record)
}

func digestOperationByIDRecord(record OperationByIDRecord) string {
	record.IndexDigest = ""
	return digestSummaryValue(record)
}

func decodeMaintenanceIndexJSON(raw []byte, out any) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%w: decode: %v", ErrMaintenanceIndexInvalid, err)
	}
	return nil
}

func getOptionalJSONStore(ctx context.Context, store interface {
	Get(context.Context, string) ([]byte, bool, error)
}, key string, out any) (bool, error) {
	raw, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return found, err
	}
	if err := decodeMaintenanceIndexJSON(raw, out); err != nil {
		return false, err
	}
	return true, nil
}

func placementByNodePrefix(root, nodeID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/placement-by-node/%02d/%s/", root, MaintenanceIndexVirtualShard(nodeID), escapeMaintenanceIndexPart(nodeID))
}

func placementByNodeKey(root string, record PlacementByNodeRecord) string {
	return fmt.Sprintf("%s%s/%s", placementByNodePrefix(root, record.NodeID), escapeMaintenanceIndexPart(record.VolumeID), escapeMaintenanceIndexPart(record.PlacementRef))
}

func operationByIDKey(root, operationID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/operation-by-id/%s", root, escapeMaintenanceIndexPart(operationID))
}

func escapeMaintenanceIndexPart(value string) string {
	return url.PathEscape(strings.TrimSpace(value))
}

func maintenanceIndexVolumeID(volumeID string) string {
	volumeID = strings.TrimSpace(volumeID)
	if canonical, err := CanonicalVolumeID(volumeID); err == nil {
		return canonical
	}
	return volumeID
}
