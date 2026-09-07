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

// ApplySummarySubjectMutationInTransaction couples one authoritative subject
// change to its shard-local aggregate delta. A missing aggregate is the
// disabled/pre-bootstrap state and deliberately remains a no-op. Once an
// aggregate is promoted, an invalid or incomplete baseline fails the authority
// mutation instead of silently letting the projection drift.
func (r *Repository) ApplySummarySubjectMutationInTransaction(ctx context.Context, writer ReadWriter, subjectID string, before, after SummaryCounters, now time.Time) (bool, error) {
	if r == nil || writer == nil {
		return false, fmt.Errorf("summary subject mutation requires repository and transaction writer")
	}
	if before == after {
		return false, nil
	}
	var state SummaryAggregateState
	found, err := getOptionalSummaryJSON(ctx, writer, summaryAggregateStateKey(r.root, SummaryKindCluster), &state)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if err := validateSummaryAggregateState(state); err != nil {
		return false, err
	}
	subjectID = strings.TrimSpace(subjectID)
	if !validSummaryIdentifier(subjectID) {
		return false, fmt.Errorf("invalid summary subject identity")
	}
	revision := summaryMutationRevision(now)
	delta, err := NewSummaryDelta(summaryMutationEventID(subjectID, revision, before, after), subjectID, revision, before, after)
	if err != nil {
		return false, err
	}
	return applySummaryAuthorityDelta(ctx, writer, r.root, delta, now.UTC())
}

// applySummaryAuthorityDelta updates exactly one virtual shard. Unlike the
// explicit/outbox delta API, it does not create a permanent applied-event row:
// the authoritative row and this shard are already in one transaction, and a
// retry re-reads the authoritative before-image before deciding its delta.
func applySummaryAuthorityDelta(ctx context.Context, writer ReadWriter, root string, delta SummaryDelta, now time.Time) (bool, error) {
	if err := ValidateSummaryDelta(delta); err != nil {
		return false, err
	}
	key := summaryAggregateKey(root, delta.Kind, delta.VirtualShard)
	shard := newSummaryAggregateShard(delta.Kind, delta.VirtualShard, "")
	found, err := getOptionalSummaryJSON(ctx, writer, key, &shard)
	if err != nil {
		return false, err
	}
	if found {
		if err := validateSummaryAggregateShard(shard); err != nil {
			return false, err
		}
	}
	counters, err := applySummaryCounterDelta(shard.Counters, delta.Before, delta.After)
	if err != nil {
		return false, err
	}
	shard.Counters = counters
	if delta.SourceRevision > shard.SourceRevision {
		shard.SourceRevision = delta.SourceRevision
	}
	shard.UpdatedAtUnix = now.Unix()
	shard.Checksum = digestSummaryAggregateShard(shard)
	if err := putSummaryJSON(ctx, writer, key, shard); err != nil {
		return false, err
	}
	return true, nil
}

// RefreshClusterSummaryFreshness is a leader-only bounded verification
// heartbeat. Transactional source writers keep the counters current without a
// global per-mutation key; this method advances only the aggregate-state
// timestamp after a complete 64-shard bounded read has succeeded.
func (r *Repository) RefreshClusterSummaryFreshness(ctx context.Context, kind string, now time.Time) (SummaryAggregateState, error) {
	if r == nil || kind != SummaryKindCluster || now.IsZero() {
		return SummaryAggregateState{}, fmt.Errorf("invalid cluster summary freshness refresh")
	}
	read, err := r.GetClusterSummary(ctx, kind)
	if err != nil {
		return SummaryAggregateState{}, err
	}
	before := read.State
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return SummaryAggregateState{}, ErrSummaryTransactionRequired
	}
	after := before
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var current SummaryAggregateState
		if err := getSummaryJSON(ctx, tx, summaryAggregateStateKey(r.root, kind), &current); err != nil {
			return err
		}
		if current.StateDigest != before.StateDigest {
			return ErrCASConflict
		}
		after = current
		after.UpdatedAtUnix = now.UTC().Unix()
		after.StateDigest = digestSummaryAggregateState(after)
		return putSummaryJSON(ctx, tx, summaryAggregateStateKey(r.root, kind), after)
	})
	return after, err
}

func summaryMutationRevision(now time.Time) uint64 {
	nanos := now.UTC().UnixNano()
	if nanos <= 0 {
		return 1
	}
	return uint64(nanos)
}

func summaryMutationEventID(subjectID string, revision uint64, before, after SummaryCounters) string {
	raw, _ := json.Marshal(struct {
		SubjectID string          `json:"subject_id"`
		Revision  uint64          `json:"revision"`
		Before    SummaryCounters `json:"before"`
		After     SummaryCounters `json:"after"`
	}{subjectID, revision, before, after})
	sum := sha256.Sum256(raw)
	return "record-" + hex.EncodeToString(sum[:])
}

func summaryHasActiveAggregate(ctx context.Context, store ReadWriter, root string) (bool, error) {
	var state SummaryAggregateState
	found, err := getOptionalSummaryJSON(ctx, store, summaryAggregateStateKey(root, SummaryKindCluster), &state)
	if err != nil || !found {
		return found, err
	}
	return true, validateSummaryAggregateState(state)
}

func summarySubject(kind string, identity ...string) string {
	h := sha256.New()
	for _, part := range identity {
		_, _ = h.Write([]byte(strings.TrimSpace(part)))
		_, _ = h.Write([]byte{0})
	}
	return kind + ":" + hex.EncodeToString(h.Sum(nil))
}

func summaryMembershipContribution(rec NodeMembershipRecord, found bool) SummaryCounters {
	if !found {
		return SummaryCounters{}
	}
	counters := SummaryCounters{KnownNodes: 1}
	switch rec.LifecycleState {
	case NodeLifecycleActive:
		counters.ActiveNodes = 1
	case NodeLifecycleDraining:
		counters.DrainingNodes = 1
	case NodeLifecycleRemoved:
		counters.RemovedNodes = 1
	}
	switch rec.HealthState {
	case NodeHealthHealthy:
		counters.HealthyNodes = 1
	case NodeHealthSuspect:
		counters.SuspectNodes = 1
	case NodeHealthDown:
		counters.DownNodes = 1
	}
	return counters
}

func summaryVolumeContribution(state VolumeState, stateFound bool, spec VolumeSpecRecord, specFound bool) SummaryCounters {
	if !stateFound {
		return SummaryCounters{}
	}
	counters := SummaryCounters{VolumeCount: 1}
	switch state.Status {
	case VolumeStatusHealthy:
		counters.HealthyVolumes = 1
	case VolumeStatusDegraded:
		counters.DegradedVolumes = 1
	case VolumeStatusRepairing:
		counters.RepairingVolumes = 1
	case VolumeStatusRebalancing:
		counters.RebalancingVolumes = 1
	case VolumeStatusBlocked:
		counters.BlockedVolumes = 1
	}
	if specFound {
		counters.TotalBytes = spec.SizeBytes
	}
	if specFound && spec.ChunkSizeBytes > 0 {
		counters.AllocatedChunks = spec.SizeBytes / uint64(spec.ChunkSizeBytes)
		if spec.SizeBytes%uint64(spec.ChunkSizeBytes) != 0 {
			counters.AllocatedChunks++
		}
	}
	return counters
}

func summaryTransitionContribution(rec PlacementTransitionRecord, found bool) SummaryCounters {
	if !found {
		return SummaryCounters{}
	}
	counters := SummaryCounters{}
	active := rec.State == PlacementTransitionQueued || rec.State == PlacementTransitionRunning || rec.State == PlacementTransitionPaused
	if active {
		switch strings.TrimSpace(rec.Reason) {
		case "repair":
			counters.RepairBacklog = 1
		case "rebalance":
			counters.RebalanceBacklog = 1
		case "drain":
			counters.DrainBacklog = 1
		}
	}
	return counters
}

// AdminOperationSummaryContribution is shared with the service-owned admin
// operation store so its authority row and aggregate counters can commit in
// the same transaction without importing protobuf types into metadata.
func AdminOperationSummaryContribution(operationID, state string, found bool) (string, SummaryCounters) {
	subjectID := summarySubject("admin-operation", operationID)
	if !found {
		return subjectID, SummaryCounters{}
	}
	counters := SummaryCounters{OperationCount: 1}
	switch strings.TrimSpace(state) {
	case "OPERATION_STATE_PENDING", "pending":
		counters.PendingOperations = 1
	case "OPERATION_STATE_RUNNING", "running":
		counters.RunningOperations = 1
	case "OPERATION_STATE_COMPLETED", "completed":
		counters.CompletedOperations = 1
	case "OPERATION_STATE_FAILED", "failed":
		counters.FailedOperations = 1
	case "OPERATION_STATE_CANCELED", "canceled":
		counters.CanceledOperations = 1
	}
	return subjectID, counters
}

func applySummaryRecordMutation(ctx context.Context, store kvReadWriter, root, subjectID string, before, after SummaryCounters, now time.Time) error {
	if before == after {
		return nil
	}
	active, err := summaryHasActiveAggregate(ctx, store, root)
	if err != nil || !active {
		return err
	}
	repo := NewRepository(storeAsKV{store}, root)
	_, err = repo.ApplySummarySubjectMutationInTransaction(ctx, store, subjectID, before, after, now)
	return err
}

// storeAsKV is used only while an existing transaction is in progress. List is
// intentionally unavailable because summary mutation coupling performs point
// reads and writes only.
type storeAsKV struct{ ReadWriter }

func (storeAsKV) List(context.Context, string, string, int) ([]string, string, error) {
	return nil, "", errors.New("transaction summary adapter does not support list")
}
