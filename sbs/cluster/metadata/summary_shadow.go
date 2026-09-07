package metadata

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

const SummaryShadowMaxPageSize = 512

var (
	ErrSummaryShadowSourceChanged    = errors.New("summary shadow source revision changed")
	ErrSummaryShadowAggregateChanged = errors.New("summary aggregate changed during shadow comparison")
	ErrSummaryShadowInvalidPage      = errors.New("invalid summary shadow source page")
)

// SummaryShadowComparison is diagnostic evidence only. LegacyFullCompletionCount
// is intentionally one because the old side must be completed to establish
// parity before the aggregate is enforced. ProductionPeriodicInvocationCount
// must remain zero; normal status and metrics paths must never call this API.
type SummaryShadowComparison struct {
	SchemaVersion                     int             `json:"schema_version"`
	Kind                              string          `json:"kind"`
	AggregateEpoch                    string          `json:"aggregate_epoch"`
	SourceRevision                    uint64          `json:"source_revision"`
	LegacyCounters                    SummaryCounters `json:"legacy_counters"`
	AggregateCounters                 SummaryCounters `json:"aggregate_counters"`
	Match                             bool            `json:"match"`
	MismatchFields                    []string        `json:"mismatch_fields,omitempty"`
	ComparedSubjectCount              uint64          `json:"compared_subject_count"`
	LegacyRangePageCount              int             `json:"legacy_range_page_count"`
	LegacyFullCompletionCount         int             `json:"legacy_full_completion_count"`
	AggregatePointReadCount           int             `json:"aggregate_point_read_count"`
	BackendFullScanCount              int             `json:"backend_full_scan_count"`
	NestedCompletionCount             int             `json:"nested_completion_count"`
	ProductionPeriodicInvocationCount int             `json:"production_periodic_invocation_count"`
	ComparisonDigest                  string          `json:"comparison_digest"`
}

type summaryAggregateObservation struct {
	state     SummaryAggregateState
	counters  SummaryCounters
	checksums []string
}

// CompareSummaryShadowDiagnostic completes the supplied legacy source for an
// explicit parity check. It is deliberately separate from all default status,
// metrics, and polling paths. Both the legacy source revision and every live
// shard checksum are fenced across the comparison interval.
func (r *Repository) CompareSummaryShadowDiagnostic(ctx context.Context, source SummaryRebuildSource, kind string, pageLimit int) (SummaryShadowComparison, error) {
	comparison := SummaryShadowComparison{
		SchemaVersion: SummarySchemaVersion, Kind: kind,
		LegacyFullCompletionCount: 1, ProductionPeriodicInvocationCount: 0,
	}
	if r == nil || source == nil || kind != SummaryKindCluster || pageLimit <= 0 || pageLimit > SummaryShadowMaxPageSize {
		return comparison, fmt.Errorf("%w: kind or page limit is invalid", ErrSummaryShadowInvalidPage)
	}
	before, err := r.readSummaryAggregateObservation(ctx, kind)
	if err != nil {
		return comparison, err
	}
	comparison.AggregatePointReadCount += SummaryVirtualShardCount + 1
	comparison.AggregateEpoch = before.state.ActiveEpoch
	comparison.SourceRevision = before.state.SourceRevision
	comparison.AggregateCounters = before.counters

	cursor := ""
	lastSubject := ""
	seen := make(map[string]struct{})
	for {
		page, err := source.ListSummaryContributions(ctx, kind, cursor, pageLimit)
		if err != nil {
			return comparison, err
		}
		comparison.LegacyRangePageCount++
		if page.SourceRevision != before.state.SourceRevision {
			return comparison, fmt.Errorf("%w: aggregate=%d legacy=%d", ErrSummaryShadowSourceChanged, before.state.SourceRevision, page.SourceRevision)
		}
		if len(page.Contributions) > pageLimit || (len(page.Contributions) == 0 && page.NextCursor != "") || (page.NextCursor != "" && page.NextCursor == cursor) {
			return comparison, ErrSummaryShadowInvalidPage
		}
		for _, contribution := range page.Contributions {
			if err := ValidateSummaryContribution(contribution); err != nil {
				return comparison, err
			}
			if lastSubject != "" && contribution.SubjectID <= lastSubject {
				return comparison, fmt.Errorf("%w: subjects are not globally ordered", ErrSummaryShadowInvalidPage)
			}
			if _, duplicate := seen[contribution.SubjectID]; duplicate {
				return comparison, fmt.Errorf("%w: duplicate subject %s", ErrSummaryShadowInvalidPage, contribution.SubjectID)
			}
			seen[contribution.SubjectID] = struct{}{}
			lastSubject = contribution.SubjectID
			comparison.LegacyCounters, err = addSummaryCounters(comparison.LegacyCounters, contribution.Counters)
			if err != nil {
				return comparison, err
			}
			comparison.ComparedSubjectCount++
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	after, err := r.readSummaryAggregateObservation(ctx, kind)
	if err != nil {
		return comparison, err
	}
	comparison.AggregatePointReadCount += SummaryVirtualShardCount + 1
	if before.state.StateDigest != after.state.StateDigest || !slices.Equal(before.checksums, after.checksums) {
		return comparison, ErrSummaryShadowAggregateChanged
	}
	comparison.MismatchFields = summaryCounterMismatchFields(comparison.LegacyCounters, comparison.AggregateCounters)
	comparison.Match = len(comparison.MismatchFields) == 0
	comparison.ComparisonDigest = digestSummaryShadowComparison(comparison)
	return comparison, ValidateSummaryShadowComparison(comparison)
}

func ValidateSummaryShadowComparison(comparison SummaryShadowComparison) error {
	if comparison.SchemaVersion != SummarySchemaVersion || comparison.Kind != SummaryKindCluster || !validSummaryIdentifier(comparison.AggregateEpoch) || comparison.SourceRevision == 0 {
		return fmt.Errorf("invalid summary shadow comparison identity")
	}
	if comparison.LegacyRangePageCount <= 0 || comparison.LegacyFullCompletionCount != 1 || comparison.AggregatePointReadCount != 2*(SummaryVirtualShardCount+1) || comparison.BackendFullScanCount != 0 || comparison.NestedCompletionCount != 0 || comparison.ProductionPeriodicInvocationCount != 0 {
		return fmt.Errorf("invalid summary shadow request-class evidence")
	}
	wantFields := summaryCounterMismatchFields(comparison.LegacyCounters, comparison.AggregateCounters)
	if comparison.Match != (len(wantFields) == 0) || !slices.Equal(comparison.MismatchFields, wantFields) {
		return fmt.Errorf("invalid summary shadow mismatch evidence")
	}
	if comparison.ComparisonDigest != digestSummaryShadowComparison(comparison) {
		return fmt.Errorf("summary shadow comparison digest mismatch")
	}
	return nil
}

func (r *Repository) readSummaryAggregateObservation(ctx context.Context, kind string) (summaryAggregateObservation, error) {
	state, err := r.GetSummaryAggregateState(ctx, kind)
	if err != nil {
		return summaryAggregateObservation{}, err
	}
	observation := summaryAggregateObservation{state: state, checksums: make([]string, SummaryVirtualShardCount)}
	for shardID := 0; shardID < SummaryVirtualShardCount; shardID++ {
		shard, err := r.GetSummaryAggregateShard(ctx, kind, shardID)
		if err != nil {
			return summaryAggregateObservation{}, err
		}
		if shard.RebuildEpoch != state.ActiveEpoch || shard.SourceRevision != state.SourceRevision {
			return summaryAggregateObservation{}, fmt.Errorf("%w: shard %d epoch/revision differs from aggregate state", ErrSummaryShadowAggregateChanged, shardID)
		}
		if shard.PendingOutboxCount != 0 {
			return summaryAggregateObservation{}, fmt.Errorf("%w: virtual shard %d has %d events", ErrSummaryOutboxPending, shardID, shard.PendingOutboxCount)
		}
		observation.counters, err = addSummaryCounters(observation.counters, shard.Counters)
		if err != nil {
			return summaryAggregateObservation{}, err
		}
		observation.checksums[shardID] = shard.Checksum
	}
	return observation, nil
}

func summaryCounterMismatchFields(left, right SummaryCounters) []string {
	fields := make([]string, 0)
	for _, candidate := range []struct {
		name  string
		left  uint64
		right uint64
	}{
		{"known_nodes", left.KnownNodes, right.KnownNodes},
		{"active_nodes", left.ActiveNodes, right.ActiveNodes},
		{"draining_nodes", left.DrainingNodes, right.DrainingNodes},
		{"removed_nodes", left.RemovedNodes, right.RemovedNodes},
		{"healthy_nodes", left.HealthyNodes, right.HealthyNodes},
		{"suspect_nodes", left.SuspectNodes, right.SuspectNodes},
		{"down_nodes", left.DownNodes, right.DownNodes},
		{"volume_count", left.VolumeCount, right.VolumeCount},
		{"total_bytes", left.TotalBytes, right.TotalBytes},
		{"allocated_chunks", left.AllocatedChunks, right.AllocatedChunks},
		{"healthy_volumes", left.HealthyVolumes, right.HealthyVolumes},
		{"degraded_volumes", left.DegradedVolumes, right.DegradedVolumes},
		{"repairing_volumes", left.RepairingVolumes, right.RepairingVolumes},
		{"rebalancing_volumes", left.RebalancingVolumes, right.RebalancingVolumes},
		{"blocked_volumes", left.BlockedVolumes, right.BlockedVolumes},
		{"degraded_extents", left.DegradedExtents, right.DegradedExtents},
		{"repair_backlog", left.RepairBacklog, right.RepairBacklog},
		{"repair_backlog_bytes", left.RepairBacklogBytes, right.RepairBacklogBytes},
		{"repair_backlog_chunks", left.RepairBacklogChunks, right.RepairBacklogChunks},
		{"rebalance_backlog", left.RebalanceBacklog, right.RebalanceBacklog},
		{"rebalance_backlog_bytes", left.RebalanceBacklogBytes, right.RebalanceBacklogBytes},
		{"rebalance_backlog_chunks", left.RebalanceBacklogChunks, right.RebalanceBacklogChunks},
		{"drain_backlog", left.DrainBacklog, right.DrainBacklog},
		{"drain_backlog_bytes", left.DrainBacklogBytes, right.DrainBacklogBytes},
		{"drain_backlog_chunks", left.DrainBacklogChunks, right.DrainBacklogChunks},
		{"retired_payload_backlog_bytes", left.RetiredPayloadBacklogBytes, right.RetiredPayloadBacklogBytes},
		{"retired_payload_backlog_chunks", left.RetiredPayloadBacklogChunks, right.RetiredPayloadBacklogChunks},
		{"retired_payload_failed_batches", left.RetiredPayloadFailedBatches, right.RetiredPayloadFailedBatches},
		{"transition_failed_batches", left.TransitionFailedBatches, right.TransitionFailedBatches},
		{"transition_recent_batches", left.TransitionRecentBatches, right.TransitionRecentBatches},
		{"transition_small_batches", left.TransitionSmallBatches, right.TransitionSmallBatches},
		{"transition_requeued", left.TransitionRequeued, right.TransitionRequeued},
		{"transition_retry_pages", left.TransitionRetryPages, right.TransitionRetryPages},
		{"transition_retry_windows", left.TransitionRetryWindows, right.TransitionRetryWindows},
		{"transition_retry_window_bytes", left.TransitionRetryWindowBytes, right.TransitionRetryWindowBytes},
		{"transition_retry_window_chunks", left.TransitionRetryWindowChunks, right.TransitionRetryWindowChunks},
		{"maintenance_cooldown_volumes", left.MaintenanceCooldownVolumes, right.MaintenanceCooldownVolumes},
		{"operation_count", left.OperationCount, right.OperationCount},
		{"pending_operations", left.PendingOperations, right.PendingOperations},
		{"running_operations", left.RunningOperations, right.RunningOperations},
		{"completed_operations", left.CompletedOperations, right.CompletedOperations},
		{"failed_operations", left.FailedOperations, right.FailedOperations},
		{"canceled_operations", left.CanceledOperations, right.CanceledOperations},
	} {
		if candidate.left != candidate.right {
			fields = append(fields, candidate.name)
		}
	}
	return fields
}

func digestSummaryShadowComparison(comparison SummaryShadowComparison) string {
	comparison.ComparisonDigest = ""
	return digestSummaryValue(comparison)
}
