package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	MaintenanceWorkSchemaVersion = 2
	MaintenanceWorkPageDefault   = 128
	MaintenanceWorkPageMaximum   = 512
	MaintenanceWorkLeaseMaximum  = time.Hour

	MaintenanceWorkStateReady     = "ready"
	MaintenanceWorkStateLeased    = "leased"
	MaintenanceWorkStatePaused    = "paused"
	MaintenanceWorkStateFailed    = "failed"
	MaintenanceWorkStateCompleted = "completed"
)

var (
	ErrMaintenanceWorkLeaseHeld = errors.New("maintenance work lease is held")
	ErrMaintenanceWorkLeaseLost = errors.New("maintenance work lease is lost")
	ErrMaintenanceWorkStale     = errors.New("maintenance work authority is stale")
)

type MaintenanceWorkRecord struct {
	SchemaVersion               int    `json:"schema_version"`
	WorkID                      string `json:"work_id"`
	Reason                      string `json:"reason"`
	State                       string `json:"state"`
	Priority                    int    `json:"priority"`
	VirtualShard                int    `json:"virtual_shard"`
	VolumeID                    string `json:"volume_id"`
	PlacementRef                string `json:"placement_ref"`
	CurrentReplicaSetID         string `json:"current_replica_set_id"`
	TargetReplicaSetID          string `json:"target_replica_set_id"`
	ExpectedVolumeEpoch         uint64 `json:"expected_volume_epoch"`
	ExpectedCurrentReplicaEpoch uint64 `json:"expected_current_replica_epoch"`
	ExpectedTargetReplicaEpoch  uint64 `json:"expected_target_replica_epoch"`
	TransitionDigest            string `json:"transition_digest"`
	WorkRevision                uint64 `json:"work_revision"`
	LeaseOwner                  string `json:"lease_owner,omitempty"`
	LeaseGeneration             uint64 `json:"lease_generation,omitempty"`
	LeaseExpiresAtUnix          int64  `json:"lease_expires_at_unix,omitempty"`
	StartedAtUnix               int64  `json:"started_at_unix"`
	UpdatedAtUnix               int64  `json:"updated_at_unix"`
	WorkDigest                  string `json:"work_digest"`
}

type MaintenanceWorkIndexRecord struct {
	SchemaVersion      int    `json:"schema_version"`
	WorkID             string `json:"work_id"`
	Reason             string `json:"reason"`
	State              string `json:"state"`
	Priority           int    `json:"priority"`
	VirtualShard       int    `json:"virtual_shard"`
	VolumeID           string `json:"volume_id"`
	PlacementRef       string `json:"placement_ref"`
	WorkRevision       uint64 `json:"work_revision"`
	LeaseExpiresAtUnix int64  `json:"lease_expires_at_unix,omitempty"`
	WorkDigest         string `json:"work_digest"`
	IndexDigest        string `json:"index_digest"`
}

type MaintenanceWorkPage struct {
	Reason                string                       `json:"reason"`
	State                 string                       `json:"state"`
	RequestedLimit        int                          `json:"requested_limit"`
	Records               []MaintenanceWorkIndexRecord `json:"records"`
	NextCursor            string                       `json:"next_cursor"`
	PointGetCount         int                          `json:"point_get_count"`
	BatchGetCount         int                          `json:"batch_get_count"`
	BatchGetKeyCount      int                          `json:"batch_get_key_count"`
	RangePageCount        int                          `json:"range_page_count"`
	BackendFullScanCount  int                          `json:"backend_full_scan_count"`
	FullCompletionCount   int                          `json:"full_completion_count"`
	NestedCompletionCount int                          `json:"nested_completion_count"`
}

func MaintenanceWorkID(volumeID, placementRef string) string {
	return maintenanceIndexVolumeID(volumeID) + ":" + strings.TrimSpace(placementRef)
}

func MaintenanceWorkIndexVirtualShard(workID string) int {
	return metadataVirtualShard(strings.TrimSpace(workID), MaintenanceWorkIndexShardCount)
}

func (r *Repository) GetMaintenanceWork(ctx context.Context, workID string) (MaintenanceWorkRecord, error) {
	workID = strings.TrimSpace(workID)
	if r == nil || workID == "" {
		return MaintenanceWorkRecord{}, fmt.Errorf("%w: maintenance work identity", ErrMaintenanceIndexInvalid)
	}
	var work MaintenanceWorkRecord
	found, err := getOptionalJSONStore(ctx, r.kv, maintenanceWorkKey(r.root, workID), &work)
	if err != nil {
		return MaintenanceWorkRecord{}, err
	}
	if !found {
		return MaintenanceWorkRecord{}, ErrNotFound
	}
	if err := validateMaintenanceWorkRecord(work); err != nil {
		return MaintenanceWorkRecord{}, err
	}
	return work, nil
}

func (r *Repository) ListMaintenanceWorkPage(ctx context.Context, reason, state, cursor string, limit int) (MaintenanceWorkPage, error) {
	reason = strings.TrimSpace(reason)
	state = strings.TrimSpace(state)
	if limit == 0 {
		limit = MaintenanceWorkPageDefault
	}
	result := MaintenanceWorkPage{Reason: reason, State: state, RequestedLimit: limit, RangePageCount: 1}
	if r == nil || !validMaintenanceWorkReason(reason) || !indexedMaintenanceWorkState(state) || limit < 1 || limit > MaintenanceWorkPageMaximum {
		return result, fmt.Errorf("%w: maintenance work page request", ErrMaintenanceIndexInvalid)
	}
	prefix := maintenanceWorkIndexPrefix(r.root, reason, state)
	if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return result, fmt.Errorf("%w: maintenance work cursor", ErrMaintenanceIndexInvalid)
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
				return fmt.Errorf("%w: maintenance work index disappeared", ErrMaintenanceIndexChanged)
			}
			var index MaintenanceWorkIndexRecord
			if err := decodeMaintenanceIndexJSON(raw, &index); err != nil {
				return err
			}
			if err := validateMaintenanceWorkIndexRecord(index); err != nil {
				return err
			}
			if index.Reason != reason || index.State != state || maintenanceWorkIndexKey(r.root, index) != key {
				return fmt.Errorf("%w: maintenance work index identity", ErrMaintenanceIndexInvalid)
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

func (r *Repository) ClaimMaintenanceWork(ctx context.Context, index MaintenanceWorkIndexRecord, leaseOwner string, leaseDuration time.Duration) (MaintenanceWorkRecord, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if r == nil || validateMaintenanceWorkIndexRecord(index) != nil || leaseOwner == "" || leaseDuration < time.Second || leaseDuration > MaintenanceWorkLeaseMaximum {
		return MaintenanceWorkRecord{}, fmt.Errorf("%w: maintenance work claim", ErrMaintenanceIndexInvalid)
	}
	var claimed MaintenanceWorkRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		indexKey := maintenanceWorkIndexKey(r.root, index)
		var currentIndex MaintenanceWorkIndexRecord
		found, err := getOptionalJSONStore(ctx, store, indexKey, &currentIndex)
		if err != nil {
			return err
		}
		if !found || validateMaintenanceWorkIndexRecord(currentIndex) != nil || currentIndex.IndexDigest != index.IndexDigest {
			return ErrCASConflict
		}
		var work MaintenanceWorkRecord
		found, err = getOptionalJSONStore(ctx, store, maintenanceWorkKey(r.root, index.WorkID), &work)
		if err != nil {
			return err
		}
		if !found || validateMaintenanceWorkRecord(work) != nil || work.WorkDigest != index.WorkDigest || work.WorkRevision != index.WorkRevision || work.State != index.State {
			return ErrMaintenanceIndexChanged
		}
		if err := validateMaintenanceWorkAuthorityStore(ctx, store, r.root, work); err != nil {
			return err
		}
		now := r.now().UTC()
		if work.State == MaintenanceWorkStateLeased && work.LeaseExpiresAtUnix > now.Unix() {
			if work.LeaseOwner == leaseOwner {
				claimed = work
				return nil
			}
			return ErrMaintenanceWorkLeaseHeld
		}
		if err := deleteMaintenanceWorkIndexStore(ctx, store, r.root, currentIndex, now); err != nil {
			return err
		}
		work.State = MaintenanceWorkStateLeased
		work.LeaseOwner = leaseOwner
		work.LeaseGeneration++
		if work.LeaseGeneration == 0 {
			work.LeaseGeneration = 1
		}
		work.LeaseExpiresAtUnix = now.Add(leaseDuration).Unix()
		work.WorkRevision++
		work.UpdatedAtUnix = now.Unix()
		work.WorkDigest = digestMaintenanceWorkRecord(work)
		if err := putJSONStore(ctx, store, maintenanceWorkKey(r.root, work.WorkID), work); err != nil {
			return err
		}
		leasedIndex := maintenanceWorkIndexRecord(work)
		if err := putMaintenanceWorkIndexStore(ctx, store, r.root, leasedIndex, now); err != nil {
			return err
		}
		claimed = work
		return nil
	})
	return claimed, err
}

func (r *Repository) ValidateMaintenanceWorkLease(ctx context.Context, claim MaintenanceWorkRecord, leaseOwner string) (MaintenanceWorkRecord, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if err := validateMaintenanceWorkRecord(claim); err != nil || leaseOwner == "" {
		return MaintenanceWorkRecord{}, fmt.Errorf("%w: maintenance work lease validation", ErrMaintenanceIndexInvalid)
	}
	current, err := r.GetMaintenanceWork(ctx, claim.WorkID)
	if err != nil {
		return MaintenanceWorkRecord{}, err
	}
	if current.WorkDigest != claim.WorkDigest || current.State != MaintenanceWorkStateLeased || current.LeaseOwner != leaseOwner || current.LeaseGeneration != claim.LeaseGeneration || current.LeaseExpiresAtUnix <= r.now().UTC().Unix() {
		return MaintenanceWorkRecord{}, ErrMaintenanceWorkLeaseLost
	}
	if err := validateMaintenanceWorkAuthority(ctx, r, current); err != nil {
		return MaintenanceWorkRecord{}, err
	}
	return current, nil
}

func (r *Repository) RenewMaintenanceWorkLease(ctx context.Context, claim MaintenanceWorkRecord, leaseOwner string, leaseDuration time.Duration) (MaintenanceWorkRecord, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if r == nil || validateMaintenanceWorkRecord(claim) != nil || leaseOwner == "" || leaseDuration < time.Second || leaseDuration > MaintenanceWorkLeaseMaximum {
		return MaintenanceWorkRecord{}, fmt.Errorf("%w: maintenance work lease renewal", ErrMaintenanceIndexInvalid)
	}
	var renewed MaintenanceWorkRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		var current MaintenanceWorkRecord
		found, err := getOptionalJSONStore(ctx, store, maintenanceWorkKey(r.root, claim.WorkID), &current)
		if err != nil {
			return err
		}
		now := r.now().UTC()
		if !found || validateMaintenanceWorkRecord(current) != nil || current.WorkDigest != claim.WorkDigest || current.State != MaintenanceWorkStateLeased || current.LeaseOwner != leaseOwner || current.LeaseGeneration != claim.LeaseGeneration || current.LeaseExpiresAtUnix <= now.Unix() {
			return ErrMaintenanceWorkLeaseLost
		}
		if err := validateMaintenanceWorkAuthorityStore(ctx, store, r.root, current); err != nil {
			return err
		}
		oldIndex := maintenanceWorkIndexRecord(current)
		if err := deleteMaintenanceWorkIndexStore(ctx, store, r.root, oldIndex, now); err != nil {
			return err
		}
		current.LeaseExpiresAtUnix = now.Add(leaseDuration).Unix()
		current.WorkRevision++
		current.UpdatedAtUnix = now.Unix()
		current.WorkDigest = digestMaintenanceWorkRecord(current)
		if err := putJSONStore(ctx, store, maintenanceWorkKey(r.root, current.WorkID), current); err != nil {
			return err
		}
		newIndex := maintenanceWorkIndexRecord(current)
		if err := putMaintenanceWorkIndexStore(ctx, store, r.root, newIndex, now); err != nil {
			return err
		}
		renewed = current
		return nil
	})
	return renewed, err
}

func (r *Repository) syncMaintenanceWorkIndex(ctx context.Context, store kvReadWriter, transition PlacementTransitionRecord) error {
	if maintenanceIndexVolumeID(transition.VolumeID) == "" || strings.TrimSpace(transition.PlacementRef) == "" {
		return nil
	}
	workID := MaintenanceWorkID(transition.VolumeID, transition.PlacementRef)
	now := r.now().UTC()
	key := maintenanceWorkKey(r.root, workID)
	var before MaintenanceWorkRecord
	found, err := getOptionalJSONStore(ctx, store, key, &before)
	if err != nil {
		return err
	}
	if found {
		if err := validateMaintenanceWorkRecord(before); err != nil {
			return err
		}
		if err := deleteMaintenanceWorkIndexStore(ctx, store, r.root, maintenanceWorkIndexRecord(before), now); err != nil {
			return err
		}
	}
	if transition.State != PlacementTransitionQueued && transition.State != PlacementTransitionRunning {
		if !found {
			return nil
		}
		terminalState, ok := maintenanceWorkStateFromTransition(transition.State)
		if !ok {
			return store.Delete(ctx, key)
		}
		before.State = terminalState
		before.LeaseOwner = ""
		before.LeaseExpiresAtUnix = 0
		before.WorkRevision++
		before.UpdatedAtUnix = now.Unix()
		before.WorkDigest = digestMaintenanceWorkRecord(before)
		return putJSONStore(ctx, store, key, before)
	}
	if !maintenanceTransitionReadyEligible(transition) {
		if found {
			return store.Delete(ctx, key)
		}
		return nil
	}
	if found && transition.State == PlacementTransitionQueued && before.State == MaintenanceWorkStateLeased && before.TransitionDigest == digestMaintenanceWorkTransition(transition) {
		before.State = MaintenanceWorkStateReady
		before.LeaseOwner = ""
		before.LeaseExpiresAtUnix = 0
		before.WorkRevision++
		before.UpdatedAtUnix = now.Unix()
		before.WorkDigest = digestMaintenanceWorkRecord(before)
		if err := putJSONStore(ctx, store, key, before); err != nil {
			return err
		}
		index := maintenanceWorkIndexRecord(before)
		return putMaintenanceWorkIndexStore(ctx, store, r.root, index, now)
	}
	if found && transition.State == PlacementTransitionRunning && before.State == MaintenanceWorkStateLeased && before.TransitionDigest == digestMaintenanceWorkTransition(transition) {
		return putMaintenanceWorkIndexStore(ctx, store, r.root, maintenanceWorkIndexRecord(before), now)
	}
	next, err := buildMaintenanceWorkStore(ctx, store, r.root, transition, &before, found, now)
	if err != nil {
		// Legacy callers may persist a transition before its complete placement
		// authority is available. Keep that write compatible; the rebuild pass
		// will materialize work after the authority exists.
		if !found && errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if err := putJSONStore(ctx, store, key, next); err != nil {
		return err
	}
	index := maintenanceWorkIndexRecord(next)
	return putMaintenanceWorkIndexStore(ctx, store, r.root, index, now)
}

func buildMaintenanceWorkStore(ctx context.Context, store kvReadWriter, root string, transition PlacementTransitionRecord, before *MaintenanceWorkRecord, found bool, now time.Time) (MaintenanceWorkRecord, error) {
	var volume VolumeState
	if err := getJSONStore(ctx, store, volumeStateKey(root, transition.VolumeID), &volume); err != nil {
		return MaintenanceWorkRecord{}, err
	}
	var current ReplicaSetState
	if err := getJSONStore(ctx, store, replicaSetKey(root, transition.VolumeID, transition.CurrentReplicaSetID), &current); err != nil {
		return MaintenanceWorkRecord{}, err
	}
	var target ReplicaSetState
	if err := getJSONStore(ctx, store, replicaSetKey(root, transition.VolumeID, transition.TargetReplicaSetID), &target); err != nil {
		return MaintenanceWorkRecord{}, err
	}
	startedAt := transition.StartedAtUnix
	if startedAt <= 0 {
		startedAt = now.Unix()
	}
	work := MaintenanceWorkRecord{
		SchemaVersion: MaintenanceWorkSchemaVersion,
		WorkID:        MaintenanceWorkID(transition.VolumeID, transition.PlacementRef), Reason: transition.Reason,
		State: MaintenanceWorkStateReady, Priority: maintenanceWorkPriority(transition.Reason),
		VolumeID: transition.VolumeID, PlacementRef: transition.PlacementRef,
		CurrentReplicaSetID: transition.CurrentReplicaSetID, TargetReplicaSetID: transition.TargetReplicaSetID,
		ExpectedVolumeEpoch: volume.Epoch, ExpectedCurrentReplicaEpoch: current.Epoch, ExpectedTargetReplicaEpoch: target.Epoch,
		TransitionDigest: digestMaintenanceWorkTransition(transition), WorkRevision: 1,
		StartedAtUnix: startedAt, UpdatedAtUnix: now.Unix(),
	}
	work.VirtualShard = MaintenanceWorkIndexVirtualShard(work.WorkID)
	if found {
		work.WorkRevision = before.WorkRevision + 1
		work.LeaseGeneration = before.LeaseGeneration
		if (before.State == MaintenanceWorkStateReady || before.State == MaintenanceWorkStateLeased) && before.TransitionDigest == work.TransitionDigest && before.ExpectedVolumeEpoch == work.ExpectedVolumeEpoch && before.ExpectedCurrentReplicaEpoch == work.ExpectedCurrentReplicaEpoch && before.ExpectedTargetReplicaEpoch == work.ExpectedTargetReplicaEpoch {
			return *before, nil
		}
	}
	work.WorkDigest = digestMaintenanceWorkRecord(work)
	return work, validateMaintenanceWorkRecord(work)
}

func validateMaintenanceWorkAuthority(ctx context.Context, repo *Repository, work MaintenanceWorkRecord) error {
	return validateMaintenanceWorkAuthorityStore(ctx, repo.kv, repo.root, work)
}

func validateMaintenanceWorkAuthorityStore(ctx context.Context, store kvReadWriter, root string, work MaintenanceWorkRecord) error {
	var transition PlacementTransitionRecord
	if err := getJSONStore(ctx, store, placementTransitionKey(root, work.VolumeID, work.PlacementRef), &transition); err != nil {
		return maintenanceWorkAuthorityError(err)
	}
	if (transition.State != PlacementTransitionQueued && transition.State != PlacementTransitionRunning) || digestMaintenanceWorkTransition(transition) != work.TransitionDigest {
		return ErrMaintenanceWorkStale
	}
	var volume VolumeState
	if err := getJSONStore(ctx, store, volumeStateKey(root, work.VolumeID), &volume); err != nil {
		return maintenanceWorkAuthorityError(err)
	}
	var current ReplicaSetState
	if err := getJSONStore(ctx, store, replicaSetKey(root, work.VolumeID, work.CurrentReplicaSetID), &current); err != nil {
		return maintenanceWorkAuthorityError(err)
	}
	var target ReplicaSetState
	if err := getJSONStore(ctx, store, replicaSetKey(root, work.VolumeID, work.TargetReplicaSetID), &target); err != nil {
		return maintenanceWorkAuthorityError(err)
	}
	volumeEpochValid := volume.Epoch == work.ExpectedVolumeEpoch || (transition.State == PlacementTransitionRunning && volume.Epoch >= work.ExpectedVolumeEpoch)
	if !volumeEpochValid || current.Epoch != work.ExpectedCurrentReplicaEpoch || target.Epoch != work.ExpectedTargetReplicaEpoch || current.PlacementRef != work.PlacementRef {
		return ErrMaintenanceWorkStale
	}
	return nil
}

func maintenanceWorkAuthorityError(err error) error {
	if errors.Is(err, ErrNotFound) {
		return ErrMaintenanceWorkStale
	}
	return err
}

func maintenanceWorkIndexRecord(work MaintenanceWorkRecord) MaintenanceWorkIndexRecord {
	index := MaintenanceWorkIndexRecord{
		SchemaVersion: MaintenanceWorkSchemaVersion,
		WorkID:        work.WorkID, Reason: work.Reason, State: work.State, Priority: work.Priority,
		VirtualShard: work.VirtualShard, VolumeID: work.VolumeID, PlacementRef: work.PlacementRef,
		WorkRevision: work.WorkRevision, LeaseExpiresAtUnix: work.LeaseExpiresAtUnix, WorkDigest: work.WorkDigest,
	}
	index.IndexDigest = digestMaintenanceWorkIndexRecord(index)
	return index
}

func validateMaintenanceWorkRecord(work MaintenanceWorkRecord) error {
	validState := work.State == MaintenanceWorkStateReady || work.State == MaintenanceWorkStateLeased || work.State == MaintenanceWorkStatePaused || work.State == MaintenanceWorkStateFailed || work.State == MaintenanceWorkStateCompleted
	leaseValid := (work.State != MaintenanceWorkStateLeased && work.LeaseOwner == "" && work.LeaseExpiresAtUnix == 0) ||
		(work.State == MaintenanceWorkStateLeased && work.LeaseOwner != "" && work.LeaseGeneration > 0 && work.LeaseExpiresAtUnix > 0)
	if work.SchemaVersion != MaintenanceWorkSchemaVersion || work.WorkID == "" || !validMaintenanceWorkReason(work.Reason) || !validState || !leaseValid || work.Priority != maintenanceWorkPriority(work.Reason) || work.VirtualShard != MaintenanceWorkIndexVirtualShard(work.WorkID) || work.VolumeID == "" || work.PlacementRef == "" || work.CurrentReplicaSetID == "" || work.TargetReplicaSetID == "" || work.ExpectedVolumeEpoch == 0 || work.ExpectedCurrentReplicaEpoch == 0 || work.ExpectedTargetReplicaEpoch == 0 || work.TransitionDigest == "" || work.WorkRevision == 0 || work.StartedAtUnix <= 0 || work.UpdatedAtUnix <= 0 || work.WorkDigest != digestMaintenanceWorkRecord(work) {
		return fmt.Errorf("%w: maintenance work record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func validateMaintenanceWorkIndexRecord(index MaintenanceWorkIndexRecord) error {
	if index.SchemaVersion != MaintenanceWorkSchemaVersion || index.WorkID == "" || !validMaintenanceWorkReason(index.Reason) || !indexedMaintenanceWorkState(index.State) || index.Priority != maintenanceWorkPriority(index.Reason) || index.VirtualShard != MaintenanceWorkIndexVirtualShard(index.WorkID) || index.VolumeID == "" || index.PlacementRef == "" || index.WorkRevision == 0 || index.WorkDigest == "" || index.State == MaintenanceWorkStateReady && index.LeaseExpiresAtUnix != 0 || index.State == MaintenanceWorkStateLeased && index.LeaseExpiresAtUnix <= 0 || index.IndexDigest != digestMaintenanceWorkIndexRecord(index) {
		return fmt.Errorf("%w: maintenance work index record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func maintenanceTransitionReadyEligible(transition PlacementTransitionRecord) bool {
	return maintenanceIndexVolumeID(transition.VolumeID) != "" && strings.TrimSpace(transition.PlacementRef) != "" && validMaintenanceWorkReason(transition.Reason) && strings.TrimSpace(transition.CurrentReplicaSetID) != "" && strings.TrimSpace(transition.TargetReplicaSetID) != ""
}

func validMaintenanceWorkReason(reason string) bool {
	return reason == "repair" || reason == "rebalance" || reason == "drain"
}

func indexedMaintenanceWorkState(state string) bool {
	return state == MaintenanceWorkStateReady || state == MaintenanceWorkStateLeased
}

func maintenanceWorkStateFromTransition(state PlacementTransitionState) (string, bool) {
	switch state {
	case PlacementTransitionCompleted:
		return MaintenanceWorkStateCompleted, true
	case PlacementTransitionPaused:
		return MaintenanceWorkStatePaused, true
	case PlacementTransitionFailed:
		return MaintenanceWorkStateFailed, true
	default:
		return "", false
	}
}

func maintenanceWorkPriority(reason string) int {
	switch reason {
	case "drain":
		return 0
	case "repair":
		return 1
	default:
		return 2
	}
}

func digestMaintenanceWorkTransition(transition PlacementTransitionRecord) string {
	identity := struct {
		VolumeID            string `json:"volume_id"`
		PlacementRef        string `json:"placement_ref"`
		Reason              string `json:"reason"`
		CurrentReplicaSetID string `json:"current_replica_set_id"`
		TargetReplicaSetID  string `json:"target_replica_set_id"`
		StartedAtUnix       int64  `json:"started_at_unix"`
	}{transition.VolumeID, transition.PlacementRef, transition.Reason, transition.CurrentReplicaSetID, transition.TargetReplicaSetID, transition.StartedAtUnix}
	return digestSummaryValue(identity)
}

func digestMaintenanceWorkRecord(work MaintenanceWorkRecord) string {
	work.WorkDigest = ""
	return digestSummaryValue(work)
}

func digestMaintenanceWorkIndexRecord(index MaintenanceWorkIndexRecord) string {
	index.IndexDigest = ""
	return digestSummaryValue(index)
}

func maintenanceWorkKey(root, workID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/maintenance-work/%s", root, escapeMaintenanceIndexPart(workID))
}

func maintenanceWorkIndexPrefix(root, reason, state string) string {
	return fmt.Sprintf("%s/derived/ad/v1/work-ready/%s/%s/", root, reason, state)
}

func maintenanceWorkIndexRootPrefix(root string) string {
	return fmt.Sprintf("%s/derived/ad/v1/work-ready/", root)
}

func maintenanceWorkIndexKey(root string, index MaintenanceWorkIndexRecord) string {
	return fmt.Sprintf("%s%02d/%03d/%s", maintenanceWorkIndexPrefix(root, index.Reason, index.State), index.VirtualShard, index.Priority, escapeMaintenanceIndexPart(index.WorkID))
}
