package metadata

import (
	"context"
	"fmt"
	"math"
	"strings"
)

const DrainIndexSchemaVersion = 1

type ExtentByPlacementRecord struct {
	SchemaVersion int    `json:"schema_version"`
	VolumeID      string `json:"volume_id"`
	PlacementRef  string `json:"placement_ref"`
	ExtentID      uint64 `json:"extent_id"`
	LogicalOffset uint64 `json:"logical_offset"`
	LengthBytes   uint64 `json:"length_bytes"`
	IndexDigest   string `json:"index_digest"`
}

type ExtentByPlacementPage struct {
	VolumeID              string                    `json:"volume_id"`
	PlacementRef          string                    `json:"placement_ref"`
	Records               []ExtentByPlacementRecord `json:"records"`
	NextCursor            string                    `json:"next_cursor"`
	PointGetCount         int                       `json:"point_get_count"`
	BatchGetCount         int                       `json:"batch_get_count"`
	BatchGetKeyCount      int                       `json:"batch_get_key_count"`
	RangePageCount        int                       `json:"range_page_count"`
	BackendFullScanCount  int                       `json:"backend_full_scan_count"`
	FullCompletionCount   int                       `json:"full_completion_count"`
	NestedCompletionCount int                       `json:"nested_completion_count"`
}

type DrainProgressRecord struct {
	SchemaVersion    int    `json:"schema_version"`
	NodeID           string `json:"node_id"`
	OperationID      string `json:"operation_id"`
	SourceCursor     string `json:"source_cursor"`
	ExtentCursor     string `json:"extent_cursor"`
	EnqueueCompleted bool   `json:"enqueue_completed"`
	TotalExtents     uint64 `json:"total_extents"`
	RemainingExtents uint64 `json:"remaining_extents"`
	TotalBytes       uint64 `json:"total_bytes"`
	RemainingBytes   uint64 `json:"remaining_bytes"`
	SampleRef        string `json:"sample_ref,omitempty"`
	UpdatedAtUnix    int64  `json:"updated_at_unix"`
	ProgressDigest   string `json:"progress_digest"`
}

type DrainProgressPointRead struct {
	Progress              DrainProgressRecord `json:"progress"`
	PointGetCount         int                 `json:"point_get_count"`
	BackendFullScanCount  int                 `json:"backend_full_scan_count"`
	FullCompletionCount   int                 `json:"full_completion_count"`
	NestedCompletionCount int                 `json:"nested_completion_count"`
}

type DrainWorkRecord struct {
	SchemaVersion int    `json:"schema_version"`
	OperationID   string `json:"operation_id"`
	NodeID        string `json:"node_id"`
	VolumeID      string `json:"volume_id"`
	PlacementRef  string `json:"placement_ref"`
	ReplicaSetID  string `json:"replica_set_id"`
	ExtentCount   uint64 `json:"extent_count"`
	DataBytes     uint64 `json:"data_bytes"`
	State         string `json:"state"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	WorkDigest    string `json:"work_digest"`
}

type EnqueueDrainWorkRequest struct {
	OperationID       string
	NodeID            string
	VolumeID          string
	PlacementRef      string
	ReplicaSetID      string
	Extents           []DrainWorkExtent
	FinalizePlacement bool
	TargetReplicaSet  ReplicaSetState
	Transition        PlacementTransitionRecord
}

type DrainWorkExtent struct {
	ExtentID  uint64
	DataBytes uint64
}

type DrainWorkItemRecord struct {
	SchemaVersion int    `json:"schema_version"`
	OperationID   string `json:"operation_id"`
	NodeID        string `json:"node_id"`
	VolumeID      string `json:"volume_id"`
	PlacementRef  string `json:"placement_ref"`
	ExtentID      uint64 `json:"extent_id"`
	DataBytes     uint64 `json:"data_bytes"`
	ItemDigest    string `json:"item_digest"`
}

func (r *Repository) putExtentMappingWithPlacementIndex(ctx context.Context, store kvReadWriter, rec ExtentMappingRecord) error {
	rec.VolumeID = maintenanceIndexVolumeID(rec.VolumeID)
	rec.PlacementRef = strings.TrimSpace(rec.PlacementRef)
	if rec.VolumeID == "" || rec.PlacementRef == "" {
		return fmt.Errorf("%w: extent mapping identity", ErrMaintenanceIndexInvalid)
	}
	authorityKey := extentMappingKey(r.root, rec.VolumeID, rec.ExtentID)
	var before ExtentMappingRecord
	found, err := getOptionalJSONStore(ctx, store, authorityKey, &before)
	if err != nil {
		return err
	}
	if found && strings.TrimSpace(before.PlacementRef) != rec.PlacementRef {
		if err := store.Delete(ctx, extentByPlacementKey(r.root, before.VolumeID, before.PlacementRef, before.ExtentID)); err != nil {
			return err
		}
	}
	index := extentByPlacementRecord(rec)
	indexKey := extentByPlacementKey(r.root, rec.VolumeID, rec.PlacementRef, rec.ExtentID)
	if err := writeExtentMapping(ctx, store, r.root, rec); err != nil {
		return err
	}
	return putJSONStore(ctx, store, indexKey, index)
}

func (r *Repository) deleteExtentMappingWithPlacementIndex(ctx context.Context, store kvReadWriter, volumeID string, extentID uint64) error {
	volumeID = maintenanceIndexVolumeID(volumeID)
	authorityKey := extentMappingKey(r.root, volumeID, extentID)
	var before ExtentMappingRecord
	found, err := getOptionalJSONStore(ctx, store, authorityKey, &before)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, authorityKey); err != nil {
		return err
	}
	if !found || strings.TrimSpace(before.PlacementRef) == "" {
		return nil
	}
	return store.Delete(ctx, extentByPlacementKey(r.root, before.VolumeID, before.PlacementRef, before.ExtentID))
}

func (r *Repository) ListExtentMappingsByPlacementPage(ctx context.Context, volumeID, placementRef, cursor string, limit int) (ExtentByPlacementPage, error) {
	volumeID = maintenanceIndexVolumeID(volumeID)
	placementRef = strings.TrimSpace(placementRef)
	if limit == 0 {
		limit = MaintenanceIndexPageDefault
	}
	result := ExtentByPlacementPage{VolumeID: volumeID, PlacementRef: placementRef, RangePageCount: 1}
	if r == nil || volumeID == "" || placementRef == "" || limit < 1 || limit > MaintenanceIndexPageMaximum {
		return result, fmt.Errorf("%w: extent placement point request", ErrMaintenanceIndexInvalid)
	}
	prefix := extentByPlacementPrefix(r.root, volumeID, placementRef)
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return result, fmt.Errorf("%w: extent cursor does not belong to placement", ErrMaintenanceIndexInvalid)
	}
	read := func(store maintenanceIndexReadStore) error {
		readLimit := limit
		if readLimit < MaintenanceIndexPageMaximum {
			readLimit++
		}
		keys, next, err := store.List(ctx, prefix, cursor, readLimit)
		if err != nil {
			return err
		}
		if len(keys) > limit {
			keys = keys[:limit]
			result.NextCursor = keys[len(keys)-1]
		} else if limit == MaintenanceIndexPageMaximum {
			result.NextCursor = next
		}
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
				return fmt.Errorf("%w: extent placement key disappeared", ErrMaintenanceIndexChanged)
			}
			var index ExtentByPlacementRecord
			if err := decodeMaintenanceIndexJSON(raw, &index); err != nil {
				return err
			}
			if err := validateExtentByPlacementRecord(index); err != nil || index.VolumeID != volumeID || index.PlacementRef != placementRef || extentByPlacementKey(r.root, index.VolumeID, index.PlacementRef, index.ExtentID) != key {
				return fmt.Errorf("%w: extent placement index identity", ErrMaintenanceIndexInvalid)
			}
			var mapping ExtentMappingRecord
			mappingFound, err := getOptionalJSONStore(ctx, store, extentMappingKey(r.root, index.VolumeID, index.ExtentID), &mapping)
			result.PointGetCount++
			if err != nil {
				return err
			}
			if !mappingFound || extentByPlacementRecord(mapping).IndexDigest != index.IndexDigest {
				return fmt.Errorf("%w: extent authority and index differ", ErrMaintenanceIndexChanged)
			}
			result.Records = append(result.Records, index)
		}
		return nil
	}
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error { return read(snapshot) })
		return result, err
	}
	return result, read(r.kv)
}

func (r *Repository) BeginDrainProgress(ctx context.Context, nodeID, operationID string) (DrainProgressRecord, error) {
	nodeID = strings.TrimSpace(nodeID)
	operationID = strings.TrimSpace(operationID)
	if r == nil || nodeID == "" || operationID == "" {
		return DrainProgressRecord{}, fmt.Errorf("%w: drain progress identity", ErrMaintenanceIndexInvalid)
	}
	var result DrainProgressRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		key := drainProgressKey(r.root, nodeID)
		var existing DrainProgressRecord
		found, err := getOptionalJSONStore(ctx, store, key, &existing)
		if err != nil {
			return err
		}
		if found {
			if err := validateDrainProgressRecord(existing); err != nil {
				return err
			}
			if existing.OperationID == operationID {
				result = existing
				return nil
			}
		}
		now := r.now().UTC().Unix()
		result = DrainProgressRecord{SchemaVersion: DrainIndexSchemaVersion, NodeID: nodeID, OperationID: operationID, UpdatedAtUnix: now}
		result.ProgressDigest = digestDrainProgressRecord(result)
		return putJSONStore(ctx, store, key, result)
	})
	return result, err
}

func (r *Repository) GetDrainProgress(ctx context.Context, nodeID string) (DrainProgressRecord, error) {
	point, err := r.GetDrainProgressPoint(ctx, nodeID)
	return point.Progress, err
}

func (r *Repository) GetDrainProgressPoint(ctx context.Context, nodeID string) (DrainProgressPointRead, error) {
	nodeID = strings.TrimSpace(nodeID)
	result := DrainProgressPointRead{}
	if r == nil || nodeID == "" {
		return result, fmt.Errorf("%w: drain progress node", ErrMaintenanceIndexInvalid)
	}
	var record DrainProgressRecord
	result.PointGetCount = 1
	found, err := getOptionalJSONStore(ctx, r.kv, drainProgressKey(r.root, nodeID), &record)
	if err != nil {
		return result, err
	}
	if !found {
		return result, ErrNotFound
	}
	if err := validateDrainProgressRecord(record); err != nil {
		return result, err
	}
	result.Progress = record
	return result, nil
}

func (r *Repository) AdvanceDrainEnqueueCursor(ctx context.Context, before DrainProgressRecord, sourceCursor, extentCursor string, completed bool) (DrainProgressRecord, error) {
	if err := validateDrainProgressRecord(before); err != nil {
		return DrainProgressRecord{}, err
	}
	if !completed && strings.TrimSpace(sourceCursor) == "" && strings.TrimSpace(extentCursor) == "" {
		return DrainProgressRecord{}, fmt.Errorf("%w: non-complete drain cursors are empty", ErrMaintenanceIndexInvalid)
	}
	var result DrainProgressRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		key := drainProgressKey(r.root, before.NodeID)
		var current DrainProgressRecord
		found, err := getOptionalJSONStore(ctx, store, key, &current)
		if err != nil {
			return err
		}
		if !found || current.ProgressDigest != before.ProgressDigest {
			return ErrCASConflict
		}
		current.SourceCursor = sourceCursor
		current.ExtentCursor = extentCursor
		current.EnqueueCompleted = completed
		current.UpdatedAtUnix = r.now().UTC().Unix()
		current.ProgressDigest = digestDrainProgressRecord(current)
		result = current
		return putJSONStore(ctx, store, key, current)
	})
	return result, err
}

func (r *Repository) EnqueueDrainWork(ctx context.Context, req EnqueueDrainWorkRequest) (DrainProgressRecord, bool, error) {
	req.OperationID = strings.TrimSpace(req.OperationID)
	req.NodeID = strings.TrimSpace(req.NodeID)
	req.VolumeID = maintenanceIndexVolumeID(req.VolumeID)
	req.PlacementRef = strings.TrimSpace(req.PlacementRef)
	req.ReplicaSetID = strings.TrimSpace(req.ReplicaSetID)
	if r == nil || req.OperationID == "" || req.NodeID == "" || req.VolumeID == "" || req.PlacementRef == "" || req.ReplicaSetID == "" || len(req.Extents) == 0 {
		return DrainProgressRecord{}, false, fmt.Errorf("%w: drain work request identity", ErrMaintenanceIndexInvalid)
	}
	seenExtents := make(map[uint64]struct{}, len(req.Extents))
	for _, extent := range req.Extents {
		if extent.ExtentID == 0 {
			return DrainProgressRecord{}, false, fmt.Errorf("%w: drain work extent identity", ErrMaintenanceIndexInvalid)
		}
		if _, duplicate := seenExtents[extent.ExtentID]; duplicate {
			return DrainProgressRecord{}, false, fmt.Errorf("%w: duplicate drain work extent %d", ErrMaintenanceIndexConflict, extent.ExtentID)
		}
		seenExtents[extent.ExtentID] = struct{}{}
	}
	if req.FinalizePlacement {
		req.Transition.VolumeID = maintenanceIndexVolumeID(req.Transition.VolumeID)
		req.Transition.PlacementRef = strings.TrimSpace(req.Transition.PlacementRef)
		validTransitionState := req.Transition.State == PlacementTransitionQueued || req.Transition.State == PlacementTransitionRunning || req.Transition.State == PlacementTransitionPaused || req.Transition.State == PlacementTransitionCompleted || req.Transition.State == PlacementTransitionFailed
		if req.Transition.VolumeID != req.VolumeID || req.Transition.PlacementRef != req.PlacementRef || req.Transition.Reason != "drain" || req.Transition.CurrentReplicaSetID != req.ReplicaSetID || req.Transition.TargetReplicaSetID == "" || req.Transition.TargetReplicaSetID != strings.TrimSpace(req.TargetReplicaSet.ReplicaSetID) || maintenanceIndexVolumeID(req.TargetReplicaSet.VolumeID) != req.VolumeID || !validTransitionState {
			return DrainProgressRecord{}, false, fmt.Errorf("%w: finalized drain placement identity", ErrMaintenanceIndexInvalid)
		}
	}
	var result DrainProgressRecord
	created := false
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		progressKey := drainProgressKey(r.root, req.NodeID)
		var progress DrainProgressRecord
		found, err := getOptionalJSONStore(ctx, store, progressKey, &progress)
		if err != nil {
			return err
		}
		if !found || validateDrainProgressRecord(progress) != nil || progress.OperationID != req.OperationID {
			return fmt.Errorf("%w: drain progress does not match work", ErrMaintenanceIndexChanged)
		}
		work := DrainWorkRecord{
			SchemaVersion: DrainIndexSchemaVersion, OperationID: req.OperationID, NodeID: req.NodeID,
			VolumeID: req.VolumeID, PlacementRef: req.PlacementRef,
			ReplicaSetID: req.ReplicaSetID, State: "pending", UpdatedAtUnix: r.now().UTC().Unix(),
		}
		workKey := drainWorkKey(r.root, work)
		var existing DrainWorkRecord
		workFound, err := getOptionalJSONStore(ctx, store, workKey, &existing)
		if err != nil {
			return err
		}
		if workFound {
			if err := validateDrainWorkRecord(existing); err != nil {
				return err
			}
			if existing.NodeID != work.NodeID || existing.OperationID != work.OperationID || existing.VolumeID != work.VolumeID || existing.PlacementRef != work.PlacementRef || existing.ReplicaSetID != work.ReplicaSetID {
				return fmt.Errorf("%w: drain work identity differs", ErrMaintenanceIndexConflict)
			}
			work = existing
		}
		newExtentCount := uint64(0)
		newDataBytes := uint64(0)
		firstNewExtent := uint64(0)
		for _, extent := range req.Extents {
			item := DrainWorkItemRecord{
				SchemaVersion: DrainIndexSchemaVersion, OperationID: req.OperationID, NodeID: req.NodeID,
				VolumeID: req.VolumeID, PlacementRef: req.PlacementRef, ExtentID: extent.ExtentID, DataBytes: extent.DataBytes,
			}
			item.ItemDigest = digestDrainWorkItemRecord(item)
			itemKey := drainWorkItemKey(r.root, item)
			var existingItem DrainWorkItemRecord
			itemFound, err := getOptionalJSONStore(ctx, store, itemKey, &existingItem)
			if err != nil {
				return err
			}
			if itemFound {
				if err := validateDrainWorkItemRecord(existingItem); err != nil {
					return err
				}
				if existingItem.ItemDigest != item.ItemDigest {
					return fmt.Errorf("%w: drain work extent %d differs", ErrMaintenanceIndexConflict, extent.ExtentID)
				}
				continue
			}
			if math.MaxUint64-newDataBytes < extent.DataBytes {
				return fmt.Errorf("%w: drain work byte counter overflow", ErrMaintenanceIndexInvalid)
			}
			if err := putJSONStore(ctx, store, itemKey, item); err != nil {
				return err
			}
			if firstNewExtent == 0 {
				firstNewExtent = extent.ExtentID
			}
			newExtentCount++
			newDataBytes += extent.DataBytes
		}
		if work.State == "completed" && newExtentCount != 0 {
			return fmt.Errorf("%w: completed drain work gained extents", ErrMaintenanceIndexConflict)
		}
		if math.MaxUint64-work.ExtentCount < newExtentCount || math.MaxUint64-progress.TotalExtents < newExtentCount || math.MaxUint64-progress.RemainingExtents < newExtentCount {
			return fmt.Errorf("%w: drain work extent counter overflow", ErrMaintenanceIndexInvalid)
		}
		if math.MaxUint64-work.DataBytes < newDataBytes || math.MaxUint64-progress.TotalBytes < newDataBytes || math.MaxUint64-progress.RemainingBytes < newDataBytes {
			return fmt.Errorf("%w: drain work byte counter overflow", ErrMaintenanceIndexInvalid)
		}
		if newExtentCount != 0 {
			work.ExtentCount += newExtentCount
			work.DataBytes += newDataBytes
			work.UpdatedAtUnix = r.now().UTC().Unix()
			work.WorkDigest = digestDrainWorkRecord(work)
			if err := putJSONStore(ctx, store, workKey, work); err != nil {
				return err
			}
			progress.TotalExtents += newExtentCount
			progress.RemainingExtents += newExtentCount
			progress.TotalBytes += newDataBytes
			progress.RemainingBytes += newDataBytes
			if progress.SampleRef == "" && firstNewExtent != 0 {
				progress.SampleRef = fmt.Sprintf("replica_set=%s volume=%s extent=%d", work.ReplicaSetID, work.VolumeID, firstNewExtent)
			}
			progress.UpdatedAtUnix = r.now().UTC().Unix()
			progress.ProgressDigest = digestDrainProgressRecord(progress)
			if err := putJSONStore(ctx, store, progressKey, progress); err != nil {
				return err
			}
		} else if !workFound {
			return fmt.Errorf("%w: drain work items exist without aggregate", ErrMaintenanceIndexChanged)
		}
		if req.FinalizePlacement {
			var existingTransition PlacementTransitionRecord
			transitionFound, err := getOptionalJSONStore(ctx, store, placementTransitionKey(r.root, req.VolumeID, req.PlacementRef), &existingTransition)
			if err != nil {
				return err
			}
			if transitionFound {
				if existingTransition.VolumeID != req.Transition.VolumeID || existingTransition.PlacementRef != req.Transition.PlacementRef || existingTransition.Reason != req.Transition.Reason || existingTransition.CurrentReplicaSetID != req.Transition.CurrentReplicaSetID || existingTransition.TargetReplicaSetID != req.Transition.TargetReplicaSetID {
					return fmt.Errorf("%w: drain transition identity differs", ErrMaintenanceIndexConflict)
				}
				req.Transition = existingTransition
			}
			if err := r.putReplicaSetWithPlacementIndex(ctx, store, req.TargetReplicaSet); err != nil {
				return err
			}
			if err := putJSONStore(ctx, store, drainWorkTransitionKey(r.root, work.VolumeID, work.PlacementRef), work); err != nil {
				return err
			}
			if err := r.putPlacementTransitionWithDrainProgress(ctx, store, req.Transition); err != nil {
				return err
			}
			if req.Transition.State == PlacementTransitionCompleted {
				found, err := getOptionalJSONStore(ctx, store, progressKey, &progress)
				if err != nil {
					return err
				}
				if !found || validateDrainProgressRecord(progress) != nil {
					return fmt.Errorf("%w: completed drain progress is missing", ErrMaintenanceIndexChanged)
				}
			}
		}
		result = progress
		created = newExtentCount > 0
		return nil
	})
	return result, created, err
}

func (r *Repository) putPlacementTransitionWithDrainProgress(ctx context.Context, store kvReadWriter, rec PlacementTransitionRecord) error {
	var before PlacementTransitionRecord
	found, err := getOptionalJSONStore(ctx, store, placementTransitionKey(r.root, rec.VolumeID, rec.PlacementRef), &before)
	if err != nil {
		return err
	}
	if err := putJSONStore(ctx, store, placementTransitionKey(r.root, rec.VolumeID, rec.PlacementRef), rec); err != nil {
		return err
	}
	if err := applySummaryRecordMutation(ctx, store, r.root, summarySubject("placement-transition", rec.VolumeID, rec.PlacementRef), summaryTransitionContribution(before, found), summaryTransitionContribution(rec, true), r.now()); err != nil {
		return err
	}
	if err := r.syncMaintenanceWorkIndex(ctx, store, rec); err != nil {
		return err
	}
	if rec.Reason != "drain" || rec.State != PlacementTransitionCompleted {
		return nil
	}
	var work DrainWorkRecord
	found, err = getOptionalJSONStore(ctx, store, drainWorkTransitionKey(r.root, rec.VolumeID, rec.PlacementRef), &work)
	if err != nil || !found {
		return err
	}
	if err := validateDrainWorkRecord(work); err != nil {
		return err
	}
	if work.State == "completed" {
		return nil
	}
	var progress DrainProgressRecord
	found, err = getOptionalJSONStore(ctx, store, drainProgressKey(r.root, work.NodeID), &progress)
	if err != nil {
		return err
	}
	if !found || validateDrainProgressRecord(progress) != nil || progress.OperationID != work.OperationID || progress.RemainingExtents < work.ExtentCount || progress.RemainingBytes < work.DataBytes {
		return fmt.Errorf("%w: drain progress cannot complete work", ErrMaintenanceIndexChanged)
	}
	work.State = "completed"
	work.UpdatedAtUnix = r.now().UTC().Unix()
	work.WorkDigest = digestDrainWorkRecord(work)
	progress.RemainingExtents -= work.ExtentCount
	progress.RemainingBytes -= work.DataBytes
	progress.UpdatedAtUnix = r.now().UTC().Unix()
	progress.ProgressDigest = digestDrainProgressRecord(progress)
	if err := putJSONStore(ctx, store, drainWorkKey(r.root, work), work); err != nil {
		return err
	}
	if err := putJSONStore(ctx, store, drainWorkTransitionKey(r.root, work.VolumeID, work.PlacementRef), work); err != nil {
		return err
	}
	return putJSONStore(ctx, store, drainProgressKey(r.root, work.NodeID), progress)
}

func extentByPlacementRecord(mapping ExtentMappingRecord) ExtentByPlacementRecord {
	record := ExtentByPlacementRecord{
		SchemaVersion: DrainIndexSchemaVersion, VolumeID: maintenanceIndexVolumeID(mapping.VolumeID),
		PlacementRef: strings.TrimSpace(mapping.PlacementRef), ExtentID: mapping.ExtentID,
		LogicalOffset: mapping.LogicalOffset, LengthBytes: mapping.LengthBytes,
	}
	record.IndexDigest = digestExtentByPlacementRecord(record)
	return record
}

func validateExtentByPlacementRecord(record ExtentByPlacementRecord) error {
	if record.SchemaVersion != DrainIndexSchemaVersion || record.VolumeID == "" || record.PlacementRef == "" || record.ExtentID == 0 || record.LengthBytes == 0 || maintenanceIndexVolumeID(record.VolumeID) != record.VolumeID || record.IndexDigest != digestExtentByPlacementRecord(record) {
		return fmt.Errorf("%w: extent placement record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateDrainProgressRecord(record DrainProgressRecord) error {
	if record.SchemaVersion != DrainIndexSchemaVersion || record.NodeID == "" || record.OperationID == "" || record.RemainingExtents > record.TotalExtents || record.RemainingBytes > record.TotalBytes || record.UpdatedAtUnix <= 0 || record.ProgressDigest != digestDrainProgressRecord(record) {
		return fmt.Errorf("%w: drain progress record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateDrainWorkRecord(record DrainWorkRecord) error {
	if record.SchemaVersion != DrainIndexSchemaVersion || record.OperationID == "" || record.NodeID == "" || record.VolumeID == "" || record.PlacementRef == "" || record.ReplicaSetID == "" || record.ExtentCount == 0 || (record.State != "pending" && record.State != "completed") || record.UpdatedAtUnix <= 0 || record.WorkDigest != digestDrainWorkRecord(record) {
		return fmt.Errorf("%w: drain work record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateDrainWorkItemRecord(record DrainWorkItemRecord) error {
	if record.SchemaVersion != DrainIndexSchemaVersion || record.OperationID == "" || record.NodeID == "" || record.VolumeID == "" || record.PlacementRef == "" || record.ExtentID == 0 || record.ItemDigest != digestDrainWorkItemRecord(record) {
		return fmt.Errorf("%w: drain work item record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func digestExtentByPlacementRecord(record ExtentByPlacementRecord) string {
	record.IndexDigest = ""
	return digestSummaryValue(record)
}

func digestDrainProgressRecord(record DrainProgressRecord) string {
	record.ProgressDigest = ""
	return digestSummaryValue(record)
}

func digestDrainWorkRecord(record DrainWorkRecord) string {
	record.WorkDigest = ""
	return digestSummaryValue(record)
}

func digestDrainWorkItemRecord(record DrainWorkItemRecord) string {
	record.ItemDigest = ""
	return digestSummaryValue(record)
}

func extentByPlacementPrefix(root, volumeID, placementRef string) string {
	return fmt.Sprintf("%s/derived/ad/v1/extent-by-placement/%s/%s/", root, escapeMaintenanceIndexPart(maintenanceIndexVolumeID(volumeID)), escapeMaintenanceIndexPart(placementRef))
}

func extentByPlacementKey(root, volumeID, placementRef string, extentID uint64) string {
	return fmt.Sprintf("%s%020d", extentByPlacementPrefix(root, volumeID, placementRef), extentID)
}

func drainProgressKey(root, nodeID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/drain-progress/%s", root, escapeMaintenanceIndexPart(nodeID))
}

func drainWorkKey(root string, record DrainWorkRecord) string {
	return fmt.Sprintf("%s/derived/ad/v1/drain-work/%s/%s/%s/%s", root, escapeMaintenanceIndexPart(record.NodeID), escapeMaintenanceIndexPart(record.OperationID), escapeMaintenanceIndexPart(record.VolumeID), escapeMaintenanceIndexPart(record.PlacementRef))
}

func drainWorkItemKey(root string, record DrainWorkItemRecord) string {
	return fmt.Sprintf("%s/derived/ad/v1/drain-work-item/%s/%s/%s/%s/%020d", root, escapeMaintenanceIndexPart(record.NodeID), escapeMaintenanceIndexPart(record.OperationID), escapeMaintenanceIndexPart(record.VolumeID), escapeMaintenanceIndexPart(record.PlacementRef), record.ExtentID)
}

func drainWorkTransitionKey(root, volumeID, placementRef string) string {
	return fmt.Sprintf("%s/derived/ad/v1/drain-work-by-transition/%s/%s", root, escapeMaintenanceIndexPart(maintenanceIndexVolumeID(volumeID)), escapeMaintenanceIndexPart(placementRef))
}
