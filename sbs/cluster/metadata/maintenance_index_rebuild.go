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
	MaintenanceIndexRebuildSchemaVersion       = 1
	MaintenanceIndexRebuildPageDefault         = 128
	MaintenanceIndexRebuildPageMaximum         = 512
	MaintenanceIndexRebuildRepairBudgetDefault = 128
	MaintenanceIndexRebuildRepairBudgetMaximum = 512

	maintenanceIndexRebuildPhaseAuthority = "authority"
	maintenanceIndexRebuildPhasePlacement = "placement-index"
	maintenanceIndexRebuildPhaseOperation = "operation-index"
	maintenanceIndexRebuildPhaseWork      = "work-index"
	maintenanceIndexRebuildPhaseComplete  = "complete"
	maintenanceIndexStateReady            = "ready"
	maintenanceIndexJobRebuild            = "rebuild"
	maintenanceIndexJobAntiEntropy        = "anti-entropy"
)

var (
	ErrMaintenanceIndexRebuildBudget     = errors.New("maintenance index rebuild repair budget is too small")
	ErrMaintenanceIndexRebuildIncomplete = errors.New("maintenance index rebuild is incomplete")
)

type MaintenanceIndexRebuildCheckpoint struct {
	SchemaVersion         int    `json:"schema_version"`
	Epoch                 string `json:"epoch"`
	Phase                 string `json:"phase"`
	SourceCursor          string `json:"source_cursor"`
	PageCount             uint64 `json:"page_count"`
	ScannedKeyCount       uint64 `json:"scanned_key_count"`
	AuthorityRecordCount  uint64 `json:"authority_record_count"`
	VerifiedCount         uint64 `json:"verified_count"`
	MismatchCount         uint64 `json:"mismatch_count"`
	RepairCount           uint64 `json:"repair_count"`
	DeletedCount          uint64 `json:"deleted_count"`
	Paused                bool   `json:"paused"`
	Completed             bool   `json:"completed"`
	Promoted              bool   `json:"promoted"`
	WorkProjectionRebuilt bool   `json:"work_projection_rebuilt,omitempty"`
	StartedAtUnix         int64  `json:"started_at_unix"`
	UpdatedAtUnix         int64  `json:"updated_at_unix"`
	CheckpointDigest      string `json:"checkpoint_digest"`
}

type MaintenanceIndexRebuildPageResult struct {
	JobClass              string `json:"job_class"`
	Epoch                 string `json:"epoch"`
	Phase                 string `json:"phase"`
	RequestedLimit        int    `json:"requested_limit"`
	RepairBudget          int    `json:"repair_budget"`
	InputCount            int    `json:"input_count"`
	ProcessedKeyCount     int    `json:"processed_key_count"`
	AuthorityRecordCount  int    `json:"authority_record_count"`
	VerifiedCount         int    `json:"verified_count"`
	MismatchCount         int    `json:"mismatch_count"`
	RepairCount           int    `json:"repair_count"`
	DeletedCount          int    `json:"deleted_count"`
	PointGetCount         int    `json:"point_get_count"`
	RangePageCount        int    `json:"range_page_count"`
	BackendFullScanCount  int    `json:"backend_full_scan_count"`
	FullCompletionCount   int    `json:"full_completion_count"`
	NestedCompletionCount int    `json:"nested_completion_count"`
	NextPhase             string `json:"next_phase"`
	NextCursor            string `json:"next_cursor"`
	Paused                bool   `json:"paused"`
	Completed             bool   `json:"completed"`
	Promoted              bool   `json:"promoted"`
}

type MaintenanceIndexState struct {
	SchemaVersion         int    `json:"schema_version"`
	State                 string `json:"state"`
	ActiveEpoch           string `json:"active_epoch"`
	DrainProjectionReady  bool   `json:"drain_projection_ready,omitempty"`
	HealthProjectionReady bool   `json:"health_projection_ready,omitempty"`
	WorkProjectionReady   bool   `json:"work_projection_ready,omitempty"`
	UpdatedAtUnix         int64  `json:"updated_at_unix"`
	StateDigest           string `json:"state_digest"`
}

type maintenanceIndexRepair struct {
	key    string
	value  []byte
	delete bool
}

type maintenanceIndexReconcileResult struct {
	authorityRecords int
	verified         int
	mismatches       int
	deleted          int
	pointGets        int
	repairs          []maintenanceIndexRepair
}

// RunMaintenanceIndexRebuildPage executes one authority rebuild page and then,
// on later calls, one separately checkpointed derived anti-entropy page. The
// composite result becomes complete only after both job classes are complete.
func (r *Repository) RunMaintenanceIndexRebuildPage(ctx context.Context, epoch string, limit, repairBudget int) (MaintenanceIndexRebuildPageResult, error) {
	epoch = strings.TrimSpace(epoch)
	if r == nil || !validSummaryIdentifier(epoch) {
		return MaintenanceIndexRebuildPageResult{JobClass: maintenanceIndexJobRebuild, Epoch: epoch}, fmt.Errorf("%w: invalid rebuild page request", ErrMaintenanceIndexInvalid)
	}
	rebuild, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, maintenanceIndexRebuildCheckpointKey(r.root, epoch))
	if err != nil {
		return MaintenanceIndexRebuildPageResult{}, err
	}
	if !found || !rebuild.Completed {
		page, err := r.runMaintenanceIndexProjectionPage(ctx, epoch, limit, repairBudget, maintenanceIndexJobRebuild)
		if err != nil {
			return page, err
		}
		if page.Completed {
			page.Completed = false
			page.NextPhase = maintenanceIndexRebuildPhasePlacement
		}
		return page, nil
	}
	antiEntropy, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, maintenanceIndexAntiEntropyCheckpointKey(r.root, epoch))
	if err != nil {
		return MaintenanceIndexRebuildPageResult{}, err
	}
	if !found || !antiEntropy.Completed {
		return r.runMaintenanceIndexProjectionPage(ctx, epoch, limit, repairBudget, maintenanceIndexJobAntiEntropy)
	}
	return maintenanceIndexRebuildResult(antiEntropy, limit, repairBudget, maintenanceIndexJobAntiEntropy), nil
}

// RunMaintenanceIndexAntiEntropyPage executes exactly one low-priority page of
// derived index verification. Authority rebuild completion is a prerequisite,
// and promotion still waits for this checkpoint to complete.
func (r *Repository) RunMaintenanceIndexAntiEntropyPage(ctx context.Context, epoch string, limit, repairBudget int) (MaintenanceIndexRebuildPageResult, error) {
	epoch = strings.TrimSpace(epoch)
	if r == nil || !validSummaryIdentifier(epoch) {
		return MaintenanceIndexRebuildPageResult{JobClass: maintenanceIndexJobAntiEntropy, Epoch: epoch}, fmt.Errorf("%w: invalid anti-entropy page request", ErrMaintenanceIndexInvalid)
	}
	rebuild, err := r.GetMaintenanceIndexRebuildCheckpoint(ctx, epoch)
	if err != nil {
		return MaintenanceIndexRebuildPageResult{}, err
	}
	if !rebuild.Completed || rebuild.Paused || !rebuild.WorkProjectionRebuilt {
		return MaintenanceIndexRebuildPageResult{}, ErrMaintenanceIndexRebuildIncomplete
	}
	return r.runMaintenanceIndexProjectionPage(ctx, epoch, limit, repairBudget, maintenanceIndexJobAntiEntropy)
}

func (r *Repository) runMaintenanceIndexProjectionPage(ctx context.Context, epoch string, limit, repairBudget int, jobClass string) (MaintenanceIndexRebuildPageResult, error) {
	if limit == 0 {
		limit = MaintenanceIndexRebuildPageDefault
	}
	if repairBudget == 0 {
		repairBudget = MaintenanceIndexRebuildRepairBudgetDefault
	}
	result := MaintenanceIndexRebuildPageResult{JobClass: jobClass, Epoch: strings.TrimSpace(epoch), RequestedLimit: limit, RepairBudget: repairBudget}
	if r == nil || !validSummaryIdentifier(result.Epoch) || !validMaintenanceIndexJob(jobClass) || limit < 1 || limit > MaintenanceIndexRebuildPageMaximum || repairBudget < 1 || repairBudget > MaintenanceIndexRebuildRepairBudgetMaximum {
		return result, fmt.Errorf("%w: invalid rebuild page request", ErrMaintenanceIndexInvalid)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return result, ErrSummaryTransactionRequired
	}
	checkpointKey := maintenanceIndexProjectionCheckpointKey(r.root, result.Epoch, jobClass)
	before, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, checkpointKey)
	if err != nil {
		return result, err
	}
	if !found {
		before = MaintenanceIndexRebuildCheckpoint{SchemaVersion: MaintenanceIndexRebuildSchemaVersion, Epoch: result.Epoch, Phase: maintenanceIndexInitialPhase(jobClass)}
	}
	if before.Paused || before.Completed {
		return maintenanceIndexRebuildResult(before, limit, repairBudget, jobClass), nil
	}
	result.Phase = before.Phase
	prefix, err := maintenanceIndexRebuildPhasePrefix(r.root, before.Phase)
	if err != nil {
		return result, err
	}
	keys, sourceNext, err := r.kv.List(ctx, prefix, before.SourceCursor, limit)
	if err != nil {
		return result, err
	}
	result.InputCount = len(keys)
	result.RangePageCount = 1
	if len(keys) > limit || (len(keys) == 0 && sourceNext != "") || (sourceNext != "" && sourceNext == before.SourceCursor) {
		return result, fmt.Errorf("%w: invalid or non-progressing source page", ErrMaintenanceIndexInvalid)
	}
	now := r.now().UTC().Unix()
	delta := maintenanceIndexReconcileResult{}
	processed := 0
	processedCursor := before.SourceCursor
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		current, currentFound, err := getMaintenanceIndexRebuildCheckpointStore(ctx, tx, checkpointKey)
		if err != nil {
			return err
		}
		if currentFound != found || (found && current.CheckpointDigest != before.CheckpointDigest) {
			return ErrCASConflict
		}
		if !currentFound {
			current = before
			current.StartedAtUnix = now
		}
		for _, key := range keys {
			item, err := r.planMaintenanceIndexReconcile(ctx, tx, current.Phase, key)
			if err != nil {
				return err
			}
			if len(delta.repairs)+len(item.repairs) > repairBudget {
				if processed == 0 {
					return fmt.Errorf("%w: key %q needs %d repairs, budget %d", ErrMaintenanceIndexRebuildBudget, key, len(item.repairs), repairBudget)
				}
				break
			}
			for _, repair := range item.repairs {
				if repair.delete {
					err = tx.Delete(ctx, repair.key)
				} else {
					err = tx.Set(ctx, repair.key, repair.value)
				}
				if err != nil {
					return err
				}
				if strings.HasPrefix(repair.key, maintenanceWorkIndexRootPrefix(r.root)) {
					if err := advanceMaintenanceWorkListRevisionForIndexKey(ctx, tx, r.root, repair.key, time.Unix(now, 0).UTC()); err != nil {
						return err
					}
				}
			}
			delta.authorityRecords += item.authorityRecords
			delta.verified += item.verified
			delta.mismatches += item.mismatches
			delta.deleted += item.deleted
			delta.pointGets += item.pointGets
			delta.repairs = append(delta.repairs, item.repairs...)
			processed++
			processedCursor = key
		}
		current.PageCount++
		current.ScannedKeyCount += uint64(processed)
		current.AuthorityRecordCount += uint64(delta.authorityRecords)
		current.VerifiedCount += uint64(delta.verified)
		current.MismatchCount += uint64(delta.mismatches)
		current.RepairCount += uint64(len(delta.repairs))
		current.DeletedCount += uint64(delta.deleted)
		if processed < len(keys) {
			current.SourceCursor = processedCursor
		} else if sourceNext != "" {
			current.SourceCursor = sourceNext
		} else {
			completedPhase := current.Phase
			current.Phase = nextMaintenanceIndexProjectionPhase(jobClass, current.Phase)
			current.SourceCursor = ""
			current.Completed = current.Phase == maintenanceIndexRebuildPhaseComplete
			if current.Completed && jobClass == maintenanceIndexJobRebuild && completedPhase == maintenanceIndexRebuildPhaseAuthority {
				current.WorkProjectionRebuilt = true
			}
		}
		current.UpdatedAtUnix = now
		current.CheckpointDigest = digestMaintenanceIndexRebuildCheckpoint(current)
		return putJSONStore(ctx, tx, checkpointKey, current)
	})
	if err != nil {
		return result, err
	}
	checkpoint, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, checkpointKey)
	if err != nil || !found {
		if err == nil {
			err = ErrNotFound
		}
		return result, err
	}
	result.ProcessedKeyCount = processed
	result.AuthorityRecordCount = delta.authorityRecords
	result.VerifiedCount = delta.verified
	result.MismatchCount = delta.mismatches
	result.RepairCount = len(delta.repairs)
	result.DeletedCount = delta.deleted
	result.PointGetCount = delta.pointGets
	result.NextPhase = checkpoint.Phase
	result.NextCursor = checkpoint.SourceCursor
	result.Paused = checkpoint.Paused
	result.Completed = checkpoint.Completed
	result.Promoted = checkpoint.Promoted
	return result, nil
}

func (r *Repository) SetMaintenanceIndexRebuildPaused(ctx context.Context, epoch string, paused bool) (MaintenanceIndexRebuildCheckpoint, error) {
	return r.setMaintenanceIndexProjectionPaused(ctx, epoch, paused, maintenanceIndexJobRebuild)
}

func (r *Repository) SetMaintenanceIndexAntiEntropyPaused(ctx context.Context, epoch string, paused bool) (MaintenanceIndexRebuildCheckpoint, error) {
	return r.setMaintenanceIndexProjectionPaused(ctx, epoch, paused, maintenanceIndexJobAntiEntropy)
}

func (r *Repository) setMaintenanceIndexProjectionPaused(ctx context.Context, epoch string, paused bool, jobClass string) (MaintenanceIndexRebuildCheckpoint, error) {
	epoch = strings.TrimSpace(epoch)
	if r == nil || !validSummaryIdentifier(epoch) || !validMaintenanceIndexJob(jobClass) {
		return MaintenanceIndexRebuildCheckpoint{}, fmt.Errorf("%w: invalid rebuild pause request", ErrMaintenanceIndexInvalid)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return MaintenanceIndexRebuildCheckpoint{}, ErrSummaryTransactionRequired
	}
	checkpointKey := maintenanceIndexProjectionCheckpointKey(r.root, epoch, jobClass)
	before, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, checkpointKey)
	if err != nil || !found {
		if err == nil {
			err = ErrNotFound
		}
		return MaintenanceIndexRebuildCheckpoint{}, err
	}
	now := r.now().UTC().Unix()
	var updated MaintenanceIndexRebuildCheckpoint
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		current, found, err := getMaintenanceIndexRebuildCheckpointStore(ctx, tx, checkpointKey)
		if err != nil {
			return err
		}
		if !found || current.CheckpointDigest != before.CheckpointDigest {
			return ErrCASConflict
		}
		current.Paused = paused
		current.UpdatedAtUnix = now
		current.CheckpointDigest = digestMaintenanceIndexRebuildCheckpoint(current)
		updated = current
		return putJSONStore(ctx, tx, checkpointKey, current)
	})
	if err != nil {
		return MaintenanceIndexRebuildCheckpoint{}, err
	}
	return updated, nil
}

func (r *Repository) PromoteMaintenanceIndexRebuild(ctx context.Context, epoch string) (MaintenanceIndexState, error) {
	epoch = strings.TrimSpace(epoch)
	if r == nil || !validSummaryIdentifier(epoch) {
		return MaintenanceIndexState{}, fmt.Errorf("%w: invalid rebuild promotion request", ErrMaintenanceIndexInvalid)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return MaintenanceIndexState{}, ErrSummaryTransactionRequired
	}
	before, err := r.GetMaintenanceIndexRebuildCheckpoint(ctx, epoch)
	if err != nil {
		return MaintenanceIndexState{}, err
	}
	if !before.Completed || before.Paused {
		return MaintenanceIndexState{}, ErrMaintenanceIndexRebuildIncomplete
	}
	var antiEntropy MaintenanceIndexRebuildCheckpoint
	if before.WorkProjectionRebuilt {
		antiEntropy, err = r.GetMaintenanceIndexAntiEntropyCheckpoint(ctx, epoch)
		if err != nil || !antiEntropy.Completed || antiEntropy.Paused {
			return MaintenanceIndexState{}, ErrMaintenanceIndexRebuildIncomplete
		}
	}
	if before.Promoted {
		return r.GetMaintenanceIndexState(ctx)
	}
	now := r.now().UTC().Unix()
	state := MaintenanceIndexState{
		SchemaVersion: MaintenanceIndexRebuildSchemaVersion, State: maintenanceIndexStateReady,
		ActiveEpoch: epoch, DrainProjectionReady: true, HealthProjectionReady: true, WorkProjectionReady: before.WorkProjectionRebuilt, UpdatedAtUnix: now,
	}
	state.StateDigest = digestMaintenanceIndexState(state)
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		checkpoint, found, err := getMaintenanceIndexRebuildCheckpointStore(ctx, tx, maintenanceIndexRebuildCheckpointKey(r.root, epoch))
		if err != nil {
			return err
		}
		if !found || checkpoint.CheckpointDigest != before.CheckpointDigest || !checkpoint.Completed || checkpoint.Paused || checkpoint.Promoted {
			return ErrCASConflict
		}
		var antiEntropyCheckpoint MaintenanceIndexRebuildCheckpoint
		if before.WorkProjectionRebuilt {
			antiEntropyCheckpoint, found, err = getMaintenanceIndexRebuildCheckpointStore(ctx, tx, maintenanceIndexAntiEntropyCheckpointKey(r.root, epoch))
			if err != nil {
				return err
			}
			if !found || antiEntropyCheckpoint.CheckpointDigest != antiEntropy.CheckpointDigest || !antiEntropyCheckpoint.Completed || antiEntropyCheckpoint.Paused || antiEntropyCheckpoint.Promoted {
				return ErrCASConflict
			}
		}
		if err := putJSONStore(ctx, tx, maintenanceIndexStateKey(r.root), state); err != nil {
			return err
		}
		checkpoint.Promoted = true
		checkpoint.UpdatedAtUnix = now
		checkpoint.CheckpointDigest = digestMaintenanceIndexRebuildCheckpoint(checkpoint)
		if err := putJSONStore(ctx, tx, maintenanceIndexRebuildCheckpointKey(r.root, epoch), checkpoint); err != nil {
			return err
		}
		if before.WorkProjectionRebuilt {
			antiEntropyCheckpoint.Promoted = true
			antiEntropyCheckpoint.UpdatedAtUnix = now
			antiEntropyCheckpoint.CheckpointDigest = digestMaintenanceIndexRebuildCheckpoint(antiEntropyCheckpoint)
			return putJSONStore(ctx, tx, maintenanceIndexAntiEntropyCheckpointKey(r.root, epoch), antiEntropyCheckpoint)
		}
		return nil
	})
	if err != nil {
		return MaintenanceIndexState{}, err
	}
	return state, nil
}

func (r *Repository) GetMaintenanceIndexRebuildCheckpoint(ctx context.Context, epoch string) (MaintenanceIndexRebuildCheckpoint, error) {
	return r.getMaintenanceIndexProjectionCheckpoint(ctx, epoch, maintenanceIndexJobRebuild)
}

func (r *Repository) GetMaintenanceIndexAntiEntropyCheckpoint(ctx context.Context, epoch string) (MaintenanceIndexRebuildCheckpoint, error) {
	return r.getMaintenanceIndexProjectionCheckpoint(ctx, epoch, maintenanceIndexJobAntiEntropy)
}

func (r *Repository) getMaintenanceIndexProjectionCheckpoint(ctx context.Context, epoch, jobClass string) (MaintenanceIndexRebuildCheckpoint, error) {
	epoch = strings.TrimSpace(epoch)
	if r == nil || !validSummaryIdentifier(epoch) || !validMaintenanceIndexJob(jobClass) {
		return MaintenanceIndexRebuildCheckpoint{}, fmt.Errorf("%w: invalid rebuild checkpoint request", ErrMaintenanceIndexInvalid)
	}
	checkpoint, found, err := r.getMaintenanceIndexRebuildCheckpointOptional(ctx, maintenanceIndexProjectionCheckpointKey(r.root, epoch, jobClass))
	if err != nil {
		return MaintenanceIndexRebuildCheckpoint{}, err
	}
	if !found {
		return MaintenanceIndexRebuildCheckpoint{}, ErrNotFound
	}
	return checkpoint, nil
}

func (r *Repository) GetMaintenanceIndexState(ctx context.Context) (MaintenanceIndexState, error) {
	if r == nil {
		return MaintenanceIndexState{}, fmt.Errorf("%w: nil repository", ErrMaintenanceIndexInvalid)
	}
	var state MaintenanceIndexState
	found, err := getOptionalJSONStore(ctx, r.kv, maintenanceIndexStateKey(r.root), &state)
	if err != nil {
		return MaintenanceIndexState{}, err
	}
	if !found {
		return MaintenanceIndexState{}, ErrNotFound
	}
	if err := validateMaintenanceIndexState(state); err != nil {
		return MaintenanceIndexState{}, err
	}
	return state, nil
}

func (r *Repository) getMaintenanceIndexRebuildCheckpointOptional(ctx context.Context, key string) (MaintenanceIndexRebuildCheckpoint, bool, error) {
	checkpoint, found, err := getMaintenanceIndexRebuildCheckpointStore(ctx, r.kv, key)
	if err != nil || !found {
		return checkpoint, found, err
	}
	return checkpoint, true, validateMaintenanceIndexRebuildCheckpoint(checkpoint)
}

func getMaintenanceIndexRebuildCheckpointStore(ctx context.Context, store interface {
	Get(context.Context, string) ([]byte, bool, error)
}, key string) (MaintenanceIndexRebuildCheckpoint, bool, error) {
	var checkpoint MaintenanceIndexRebuildCheckpoint
	found, err := getOptionalJSONStore(ctx, store, key, &checkpoint)
	if err != nil || !found {
		return checkpoint, found, err
	}
	if err := validateMaintenanceIndexRebuildCheckpoint(checkpoint); err != nil {
		return MaintenanceIndexRebuildCheckpoint{}, false, err
	}
	return checkpoint, true, nil
}

func (r *Repository) planMaintenanceIndexReconcile(ctx context.Context, store kvReadWriter, phase, key string) (maintenanceIndexReconcileResult, error) {
	switch phase {
	case maintenanceIndexRebuildPhaseAuthority:
		return r.planMaintenanceAuthorityRepair(ctx, store, key)
	case maintenanceIndexRebuildPhasePlacement:
		return r.planPlacementProjectionRepair(ctx, store, key)
	case maintenanceIndexRebuildPhaseOperation:
		return r.planOperationProjectionRepair(ctx, store, key)
	case maintenanceIndexRebuildPhaseWork:
		return r.planWorkProjectionRepair(ctx, store, key)
	default:
		return maintenanceIndexReconcileResult{}, fmt.Errorf("%w: invalid rebuild phase %q", ErrMaintenanceIndexInvalid, phase)
	}
}

func (r *Repository) planMaintenanceAuthorityRepair(ctx context.Context, store kvReadWriter, key string) (maintenanceIndexReconcileResult, error) {
	kind := maintenanceAuthorityKeyKind(r.root, key)
	if kind == "" {
		return maintenanceIndexReconcileResult{}, nil
	}
	result := maintenanceIndexReconcileResult{authorityRecords: 1, pointGets: 1}
	raw, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return result, err
	}
	switch kind {
	case "replica-set":
		var authority ReplicaSetState
		if err := decodeMaintenanceIndexJSON(raw, &authority); err != nil {
			return result, err
		}
		if replicaSetKey(r.root, authority.VolumeID, authority.ReplicaSetID) != key {
			return result, fmt.Errorf("%w: replica-set authority key/value identity differs", ErrMaintenanceIndexInvalid)
		}
		records, err := placementIndexRecords(&authority)
		if err != nil {
			return result, err
		}
		for _, nodeID := range sortedPlacementIndexNodeIDs(records) {
			record := records[nodeID]
			indexKey := placementByNodeKey(r.root, record)
			indexRaw, indexFound, err := store.Get(ctx, indexKey)
			result.pointGets++
			if err != nil {
				return result, err
			}
			if indexFound {
				var existing PlacementByNodeRecord
				if decodeMaintenanceIndexJSON(indexRaw, &existing) == nil && validatePlacementByNodeRecord(existing) == nil && existing.ReplicaSetID != record.ReplicaSetID {
					return result, fmt.Errorf("%w: placement %q on node %q belongs to replica sets %q and %q", ErrMaintenanceIndexConflict, record.PlacementRef, nodeID, existing.ReplicaSetID, record.ReplicaSetID)
				}
				if placementIndexRawMatches(indexRaw, record) {
					result.verified++
					continue
				}
			}
			repair, err := newMaintenanceIndexSetRepair(indexKey, record)
			if err != nil {
				return result, err
			}
			result.mismatches++
			result.repairs = append(result.repairs, repair)
		}
	case "extent":
		var authority ExtentMappingRecord
		if err := decodeMaintenanceIndexJSON(raw, &authority); err != nil {
			return result, err
		}
		if extentMappingKey(r.root, authority.VolumeID, authority.ExtentID) != key {
			return result, fmt.Errorf("%w: extent authority key/value identity differs", ErrMaintenanceIndexInvalid)
		}
		expected := extentByPlacementRecord(authority)
		if err := validateExtentByPlacementRecord(expected); err != nil {
			return result, err
		}
		indexKey := extentByPlacementKey(r.root, expected.VolumeID, expected.PlacementRef, expected.ExtentID)
		indexRaw, indexFound, err := store.Get(ctx, indexKey)
		result.pointGets++
		if err != nil {
			return result, err
		}
		if indexFound {
			if extentIndexRawMatches(indexRaw, expected) {
				result.verified++
				return result, nil
			}
		}
		repair, err := newMaintenanceIndexSetRepair(indexKey, expected)
		if err != nil {
			return result, err
		}
		result.mismatches++
		result.repairs = append(result.repairs, repair)
	case "operation":
		var authority MutationOperationRecord
		if err := decodeMaintenanceIndexJSON(raw, &authority); err != nil {
			return result, err
		}
		if mutationOperationKey(r.root, authority.VolumeID, authority.OperationID) != key {
			return result, fmt.Errorf("%w: operation authority key/value identity differs", ErrMaintenanceIndexInvalid)
		}
		expected := operationIndexRecord(authority)
		indexKey := operationByIDKey(r.root, expected.OperationID)
		indexRaw, indexFound, err := store.Get(ctx, indexKey)
		result.pointGets++
		if err != nil {
			return result, err
		}
		if indexFound {
			var existing OperationByIDRecord
			if decodeMaintenanceIndexJSON(indexRaw, &existing) == nil && validateOperationByIDRecord(existing) == nil && existing.OperationID == expected.OperationID && existing.VolumeID != expected.VolumeID {
				return result, fmt.Errorf("%w: operation %q belongs to both %q and %q", ErrMaintenanceIndexConflict, expected.OperationID, existing.VolumeID, expected.VolumeID)
			}
		}
		if indexFound && operationIndexRawMatches(indexRaw, expected) {
			result.verified++
			return result, nil
		}
		repair, err := newMaintenanceIndexSetRepair(indexKey, expected)
		if err != nil {
			return result, err
		}
		result.mismatches++
		result.repairs = append(result.repairs, repair)
	case "transition":
		var authority PlacementTransitionRecord
		if err := decodeMaintenanceIndexJSON(raw, &authority); err != nil {
			return result, err
		}
		if placementTransitionKey(r.root, authority.VolumeID, authority.PlacementRef) != key {
			return result, fmt.Errorf("%w: transition authority key/value identity differs", ErrMaintenanceIndexInvalid)
		}
		if !maintenanceTransitionReadyEligible(authority) || (authority.State != PlacementTransitionQueued && authority.State != PlacementTransitionRunning) {
			return result, nil
		}
		workKey := maintenanceWorkKey(r.root, MaintenanceWorkID(authority.VolumeID, authority.PlacementRef))
		workRaw, workFound, err := store.Get(ctx, workKey)
		result.pointGets++
		if err != nil {
			return result, err
		}
		var before MaintenanceWorkRecord
		validBefore := workFound && decodeMaintenanceIndexJSON(workRaw, &before) == nil && validateMaintenanceWorkRecord(before) == nil
		expected, err := buildMaintenanceWorkStore(ctx, store, r.root, authority, &before, validBefore, r.now().UTC())
		result.pointGets += 3
		if errors.Is(err, ErrNotFound) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		if !validBefore || before.WorkDigest != expected.WorkDigest {
			repair, err := newMaintenanceIndexSetRepair(workKey, expected)
			if err != nil {
				return result, err
			}
			result.mismatches++
			result.repairs = append(result.repairs, repair)
		} else {
			result.verified++
		}
		expectedIndex := maintenanceWorkIndexRecord(expected)
		indexKey := maintenanceWorkIndexKey(r.root, expectedIndex)
		indexRaw, indexFound, err := store.Get(ctx, indexKey)
		result.pointGets++
		if err != nil {
			return result, err
		}
		if indexFound && workIndexRawMatches(indexRaw, expectedIndex) {
			result.verified++
			return result, nil
		}
		repair, err := newMaintenanceIndexSetRepair(indexKey, expectedIndex)
		if err != nil {
			return result, err
		}
		result.mismatches++
		result.repairs = append(result.repairs, repair)
	}
	return result, nil
}

func (r *Repository) planPlacementProjectionRepair(ctx context.Context, store kvReadWriter, key string) (maintenanceIndexReconcileResult, error) {
	result := maintenanceIndexReconcileResult{pointGets: 1}
	raw, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return result, err
	}
	var record PlacementByNodeRecord
	if decodeMaintenanceIndexJSON(raw, &record) != nil || validatePlacementByNodeRecord(record) != nil || placementByNodeKey(r.root, record) != key {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	authorityKey := replicaSetKey(r.root, record.VolumeID, record.ReplicaSetID)
	authorityRaw, authorityFound, err := store.Get(ctx, authorityKey)
	result.pointGets++
	if err != nil {
		return result, err
	}
	if !authorityFound {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	var authority ReplicaSetState
	if err := decodeMaintenanceIndexJSON(authorityRaw, &authority); err != nil {
		return result, err
	}
	if replicaSetKey(r.root, authority.VolumeID, authority.ReplicaSetID) != authorityKey {
		return result, fmt.Errorf("%w: replica-set authority key/value identity differs", ErrMaintenanceIndexInvalid)
	}
	expectedRecords, err := placementIndexRecords(&authority)
	if err != nil {
		return result, err
	}
	expected, expectedFound := expectedRecords[record.NodeID]
	if !expectedFound || placementByNodeKey(r.root, expected) != key {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	if record.IndexDigest == expected.IndexDigest {
		result.verified = 1
		return result, nil
	}
	repair, err := newMaintenanceIndexSetRepair(key, expected)
	if err != nil {
		return result, err
	}
	result.mismatches = 1
	result.repairs = append(result.repairs, repair)
	return result, nil
}

func (r *Repository) planOperationProjectionRepair(ctx context.Context, store kvReadWriter, key string) (maintenanceIndexReconcileResult, error) {
	result := maintenanceIndexReconcileResult{pointGets: 1}
	raw, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return result, err
	}
	var record OperationByIDRecord
	if decodeMaintenanceIndexJSON(raw, &record) != nil || validateOperationByIDRecord(record) != nil || operationByIDKey(r.root, record.OperationID) != key {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	authorityKey := mutationOperationKey(r.root, record.VolumeID, record.OperationID)
	authorityRaw, authorityFound, err := store.Get(ctx, authorityKey)
	result.pointGets++
	if err != nil {
		return result, err
	}
	if !authorityFound {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	var authority MutationOperationRecord
	if err := decodeMaintenanceIndexJSON(authorityRaw, &authority); err != nil {
		return result, err
	}
	if mutationOperationKey(r.root, authority.VolumeID, authority.OperationID) != authorityKey {
		return result, fmt.Errorf("%w: operation authority key/value identity differs", ErrMaintenanceIndexInvalid)
	}
	expected := operationIndexRecord(authority)
	if record.IndexDigest == expected.IndexDigest {
		result.verified = 1
		return result, nil
	}
	repair, err := newMaintenanceIndexSetRepair(key, expected)
	if err != nil {
		return result, err
	}
	result.mismatches = 1
	result.repairs = append(result.repairs, repair)
	return result, nil
}

func (r *Repository) planWorkProjectionRepair(ctx context.Context, store kvReadWriter, key string) (maintenanceIndexReconcileResult, error) {
	result := maintenanceIndexReconcileResult{pointGets: 1}
	raw, found, err := store.Get(ctx, key)
	if err != nil || !found {
		return result, err
	}
	deleteIndex := func() (maintenanceIndexReconcileResult, error) {
		result.mismatches, result.deleted = 1, 1
		result.repairs = append(result.repairs, maintenanceIndexRepair{key: key, delete: true})
		return result, nil
	}
	var index MaintenanceWorkIndexRecord
	if decodeMaintenanceIndexJSON(raw, &index) != nil || validateMaintenanceWorkIndexRecord(index) != nil || maintenanceWorkIndexKey(r.root, index) != key {
		return deleteIndex()
	}
	workRaw, workFound, err := store.Get(ctx, maintenanceWorkKey(r.root, index.WorkID))
	result.pointGets++
	if err != nil {
		return result, err
	}
	var work MaintenanceWorkRecord
	if !workFound || decodeMaintenanceIndexJSON(workRaw, &work) != nil || validateMaintenanceWorkRecord(work) != nil || !indexedMaintenanceWorkState(work.State) {
		return deleteIndex()
	}
	expected := maintenanceWorkIndexRecord(work)
	if expected.IndexDigest != index.IndexDigest || maintenanceWorkIndexKey(r.root, expected) != key {
		return deleteIndex()
	}
	if err := validateMaintenanceWorkAuthorityStore(ctx, store, r.root, work); err != nil {
		result.pointGets += 4
		if errors.Is(err, ErrMaintenanceWorkStale) {
			return deleteIndex()
		}
		return result, err
	}
	result.pointGets += 4
	result.verified = 1
	return result, nil
}

func newMaintenanceIndexSetRepair(key string, value any) (maintenanceIndexRepair, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return maintenanceIndexRepair{}, err
	}
	return maintenanceIndexRepair{key: key, value: raw}, nil
}

func placementIndexRawMatches(raw []byte, expected PlacementByNodeRecord) bool {
	var record PlacementByNodeRecord
	return decodeMaintenanceIndexJSON(raw, &record) == nil && validatePlacementByNodeRecord(record) == nil && record.IndexDigest == expected.IndexDigest
}

func operationIndexRawMatches(raw []byte, expected OperationByIDRecord) bool {
	var record OperationByIDRecord
	return decodeMaintenanceIndexJSON(raw, &record) == nil && validateOperationByIDRecord(record) == nil && record.IndexDigest == expected.IndexDigest
}

func extentIndexRawMatches(raw []byte, expected ExtentByPlacementRecord) bool {
	var record ExtentByPlacementRecord
	return decodeMaintenanceIndexJSON(raw, &record) == nil && validateExtentByPlacementRecord(record) == nil && record.IndexDigest == expected.IndexDigest
}

func workIndexRawMatches(raw []byte, expected MaintenanceWorkIndexRecord) bool {
	var record MaintenanceWorkIndexRecord
	return decodeMaintenanceIndexJSON(raw, &record) == nil && validateMaintenanceWorkIndexRecord(record) == nil && record.IndexDigest == expected.IndexDigest
}

func maintenanceAuthorityKeyKind(root, key string) string {
	relative := strings.TrimPrefix(key, fmt.Sprintf("%s/volumes/", root))
	if relative == key {
		return ""
	}
	parts := strings.Split(relative, "/")
	if len(parts) == 4 && strings.TrimSpace(parts[0]) != "" && parts[1] == "placements" && strings.TrimSpace(parts[2]) != "" && parts[3] == "transition" {
		return "transition"
	}
	if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[2]) == "" {
		return ""
	}
	switch parts[1] {
	case "replicasets":
		return "replica-set"
	case "extents":
		return "extent"
	case "operations":
		return "operation"
	default:
		return ""
	}
}

func maintenanceIndexRebuildPhasePrefix(root, phase string) (string, error) {
	switch phase {
	case maintenanceIndexRebuildPhaseAuthority:
		return fmt.Sprintf("%s/volumes/", root), nil
	case maintenanceIndexRebuildPhasePlacement:
		return fmt.Sprintf("%s/derived/ad/v1/placement-by-node/", root), nil
	case maintenanceIndexRebuildPhaseOperation:
		return fmt.Sprintf("%s/derived/ad/v1/operation-by-id/", root), nil
	case maintenanceIndexRebuildPhaseWork:
		return maintenanceWorkIndexRootPrefix(root), nil
	default:
		return "", fmt.Errorf("%w: invalid rebuild phase %q", ErrMaintenanceIndexInvalid, phase)
	}
}

func validMaintenanceIndexJob(jobClass string) bool {
	return jobClass == maintenanceIndexJobRebuild || jobClass == maintenanceIndexJobAntiEntropy
}

func maintenanceIndexInitialPhase(jobClass string) string {
	if jobClass == maintenanceIndexJobRebuild {
		return maintenanceIndexRebuildPhaseAuthority
	}
	return maintenanceIndexRebuildPhasePlacement
}

func nextMaintenanceIndexProjectionPhase(jobClass, phase string) string {
	if jobClass == maintenanceIndexJobRebuild {
		return maintenanceIndexRebuildPhaseComplete
	}
	switch phase {
	case maintenanceIndexRebuildPhasePlacement:
		return maintenanceIndexRebuildPhaseOperation
	case maintenanceIndexRebuildPhaseOperation:
		return maintenanceIndexRebuildPhaseWork
	default:
		return maintenanceIndexRebuildPhaseComplete
	}
}

func maintenanceIndexRebuildResult(checkpoint MaintenanceIndexRebuildCheckpoint, limit, repairBudget int, jobClass string) MaintenanceIndexRebuildPageResult {
	return MaintenanceIndexRebuildPageResult{
		JobClass: jobClass, Epoch: checkpoint.Epoch, Phase: checkpoint.Phase, RequestedLimit: limit,
		RepairBudget: repairBudget, NextPhase: checkpoint.Phase, NextCursor: checkpoint.SourceCursor,
		Paused: checkpoint.Paused, Completed: checkpoint.Completed, Promoted: checkpoint.Promoted,
	}
}

func validateMaintenanceIndexRebuildCheckpoint(checkpoint MaintenanceIndexRebuildCheckpoint) error {
	validPhase := checkpoint.Phase == maintenanceIndexRebuildPhaseAuthority || checkpoint.Phase == maintenanceIndexRebuildPhasePlacement || checkpoint.Phase == maintenanceIndexRebuildPhaseOperation || checkpoint.Phase == maintenanceIndexRebuildPhaseWork || checkpoint.Phase == maintenanceIndexRebuildPhaseComplete
	if checkpoint.SchemaVersion != MaintenanceIndexRebuildSchemaVersion || !validSummaryIdentifier(checkpoint.Epoch) || !validPhase || checkpoint.PageCount == 0 || checkpoint.StartedAtUnix <= 0 || checkpoint.UpdatedAtUnix < checkpoint.StartedAtUnix {
		return fmt.Errorf("%w: invalid rebuild checkpoint", ErrMaintenanceIndexInvalid)
	}
	if checkpoint.Completed != (checkpoint.Phase == maintenanceIndexRebuildPhaseComplete) || checkpoint.Promoted && !checkpoint.Completed {
		return fmt.Errorf("%w: invalid rebuild completion state", ErrMaintenanceIndexInvalid)
	}
	if checkpoint.WorkProjectionRebuilt && !checkpoint.Completed {
		return fmt.Errorf("%w: invalid work projection rebuild state", ErrMaintenanceIndexInvalid)
	}
	if checkpoint.RepairCount != checkpoint.MismatchCount {
		return fmt.Errorf("%w: mismatch and repair counts differ", ErrMaintenanceIndexInvalid)
	}
	if checkpoint.CheckpointDigest != digestMaintenanceIndexRebuildCheckpoint(checkpoint) {
		return fmt.Errorf("%w: rebuild checkpoint digest", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateMaintenanceIndexState(state MaintenanceIndexState) error {
	if state.SchemaVersion != MaintenanceIndexRebuildSchemaVersion || state.State != maintenanceIndexStateReady || !validSummaryIdentifier(state.ActiveEpoch) || state.UpdatedAtUnix <= 0 || state.StateDigest != digestMaintenanceIndexState(state) {
		return fmt.Errorf("%w: invalid maintenance index state", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func digestMaintenanceIndexRebuildCheckpoint(checkpoint MaintenanceIndexRebuildCheckpoint) string {
	checkpoint.CheckpointDigest = ""
	return digestSummaryValue(checkpoint)
}

func digestMaintenanceIndexState(state MaintenanceIndexState) string {
	state.StateDigest = ""
	return digestSummaryValue(state)
}

func maintenanceIndexRebuildCheckpointKey(root, epoch string) string {
	return fmt.Sprintf("%s/derived/ad/v1/maintenance-index-rebuild/%s/checkpoint", root, escapeMaintenanceIndexPart(epoch))
}

func maintenanceIndexAntiEntropyCheckpointKey(root, epoch string) string {
	return fmt.Sprintf("%s/derived/ad/v1/maintenance-index-anti-entropy/%s/checkpoint", root, escapeMaintenanceIndexPart(epoch))
}

func maintenanceIndexProjectionCheckpointKey(root, epoch, jobClass string) string {
	if jobClass == maintenanceIndexJobAntiEntropy {
		return maintenanceIndexAntiEntropyCheckpointKey(root, epoch)
	}
	return maintenanceIndexRebuildCheckpointKey(root, epoch)
}

func maintenanceIndexStateKey(root string) string {
	return fmt.Sprintf("%s/derived/ad/v1/maintenance-index-state", root)
}
