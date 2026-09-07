package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const SummaryMaximumFutureSkew = 5 * time.Second

type SummaryHealth string

const (
	SummaryHealthReady           SummaryHealth = "ready"
	SummaryHealthDegraded        SummaryHealth = "degraded"
	SummaryHealthRebuildRequired SummaryHealth = "rebuild_required"
)

type ClusterSummaryPolicy struct {
	DegradedAfter        time.Duration
	RebuildRequiredAfter time.Duration
}

type ClusterSummaryAssessment struct {
	Read                 ClusterSummaryRead `json:"read"`
	Health               SummaryHealth      `json:"health"`
	Reason               string             `json:"reason"`
	Partial              bool               `json:"partial"`
	Stale                bool               `json:"stale"`
	RebuildRequired      bool               `json:"rebuild_required"`
	FreshnessAgeMillis   uint64             `json:"freshness_age_millis"`
	FreshnessUpdatedUnix int64              `json:"freshness_updated_unix"`
	AssessedAtUnix       int64              `json:"assessed_at_unix"`
}

type ClusterSummaryRead struct {
	SchemaVersion          int                   `json:"schema_version"`
	Kind                   string                `json:"kind"`
	State                  SummaryAggregateState `json:"state"`
	Counters               SummaryCounters       `json:"counters"`
	BaselineSourceRevision uint64                `json:"baseline_source_revision"`
	MaximumSourceRevision  uint64                `json:"maximum_source_revision"`
	OldestUpdatedAtUnix    int64                 `json:"oldest_updated_at_unix"`
	PointGetCount          int                   `json:"point_get_count"`
	BatchGetCount          int                   `json:"batch_get_count"`
	BatchGetKeyCount       int                   `json:"batch_get_key_count"`
	BackendFullScanCount   int                   `json:"backend_full_scan_count"`
	FullCompletionCount    int                   `json:"full_completion_count"`
	NestedCompletionCount  int                   `json:"nested_completion_count"`
}

type summaryReadStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	BatchGet(context.Context, []string) (map[string][]byte, error)
}

// AssessClusterSummary turns a bounded aggregate read into the serving health
// contract. Read failures are data, not a signal to complete a legacy scan.
// Only an invalid caller policy is returned as an error.
func (r *Repository) AssessClusterSummary(ctx context.Context, kind string, now time.Time, policy ClusterSummaryPolicy) (ClusterSummaryAssessment, error) {
	assessment := ClusterSummaryAssessment{
		Read:   ClusterSummaryRead{SchemaVersion: SummarySchemaVersion, Kind: kind},
		Health: SummaryHealthRebuildRequired, Reason: "aggregate_missing",
		Partial: true, RebuildRequired: true, AssessedAtUnix: now.UTC().Unix(),
	}
	if r == nil || kind != SummaryKindCluster || now.IsZero() || policy.DegradedAfter <= 0 || policy.RebuildRequiredAfter <= policy.DegradedAfter {
		return assessment, fmt.Errorf("invalid cluster summary assessment policy")
	}
	read, err := r.GetClusterSummary(ctx, kind)
	assessment.Read = read
	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			assessment.Reason = "aggregate_missing"
		case errors.Is(err, ErrSummaryInvalidAggregate):
			assessment.Reason = "aggregate_invalid"
		case errors.Is(err, ErrSummaryOutboxPending):
			assessment.Health = SummaryHealthDegraded
			assessment.Reason = "outbox_pending"
			assessment.RebuildRequired = false
		case errors.Is(err, ErrSummaryShadowAggregateChanged):
			assessment.Health = SummaryHealthDegraded
			assessment.Reason = "aggregate_changed"
			assessment.RebuildRequired = false
		default:
			assessment.Health = SummaryHealthDegraded
			assessment.Reason = "aggregate_unavailable"
			assessment.RebuildRequired = false
		}
		// A failed read may have accumulated a prefix of the 64 shards. Never
		// expose that prefix as if it were a cluster total.
		assessment.Read.Counters = SummaryCounters{}
		return assessment, nil
	}

	// Source mutations are transactionally coupled to one shard and therefore
	// do not make untouched shards stale. Freshness is the leader's last
	// successful bounded verification timestamp, not the age of the oldest
	// unchanged shard.
	updatedAt := read.State.UpdatedAtUnix
	assessment.FreshnessUpdatedUnix = updatedAt
	if updatedAt <= 0 {
		assessment.Reason = "freshness_missing"
		return assessment, nil
	}
	updated := time.Unix(updatedAt, 0).UTC()
	if updated.After(now.UTC().Add(SummaryMaximumFutureSkew)) {
		assessment.Reason = "aggregate_timestamp_in_future"
		return assessment, nil
	}
	age := now.UTC().Sub(updated)
	if age < 0 {
		age = 0
	}
	assessment.FreshnessAgeMillis = uint64(age / time.Millisecond)
	switch {
	case age >= policy.RebuildRequiredAfter:
		assessment.Health = SummaryHealthRebuildRequired
		assessment.Reason = "freshness_expired"
		assessment.Partial = false
		assessment.Stale = true
		assessment.RebuildRequired = true
	case age >= policy.DegradedAfter:
		assessment.Health = SummaryHealthDegraded
		assessment.Reason = "freshness_stale"
		assessment.Partial = false
		assessment.Stale = true
		assessment.RebuildRequired = false
	default:
		assessment.Health = SummaryHealthReady
		assessment.Reason = "none"
		assessment.Partial = false
		assessment.Stale = false
		assessment.RebuildRequired = false
	}
	return assessment, nil
}

// GetClusterSummary reads aggregate-state plus exactly 64 virtual shards. TiKV
// and other snapshot-capable stores execute one point read and one bounded
// BatchGet in the same read snapshot. There is no range scan or completion.
func (r *Repository) GetClusterSummary(ctx context.Context, kind string) (ClusterSummaryRead, error) {
	result := ClusterSummaryRead{SchemaVersion: SummarySchemaVersion, Kind: kind}
	if r == nil || kind != SummaryKindCluster {
		return result, fmt.Errorf("invalid cluster summary read request")
	}
	if snapshotter, ok := r.kv.(consistentSnapshotKV); ok {
		err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
			var err error
			result, _, err = r.readClusterSummary(ctx, snapshot, kind)
			return err
		})
		return result, err
	}
	store := summaryReadStoreAdapter{base: r.kv}
	before, beforeChecksums, err := r.readClusterSummary(ctx, store, kind)
	if err != nil {
		return result, err
	}
	after, afterChecksums, err := r.readClusterSummary(ctx, store, kind)
	if err != nil {
		return result, err
	}
	if before.State.StateDigest != after.State.StateDigest || before.Counters != after.Counters || before.MaximumSourceRevision != after.MaximumSourceRevision || before.OldestUpdatedAtUnix != after.OldestUpdatedAtUnix || !equalSummaryChecksums(beforeChecksums, afterChecksums) {
		return result, ErrSummaryShadowAggregateChanged
	}
	before.PointGetCount += after.PointGetCount
	before.BatchGetCount += after.BatchGetCount
	before.BatchGetKeyCount += after.BatchGetKeyCount
	return before, nil
}

func (r *Repository) readClusterSummary(ctx context.Context, store summaryReadStore, kind string) (ClusterSummaryRead, []string, error) {
	result := ClusterSummaryRead{
		SchemaVersion: SummarySchemaVersion, Kind: kind,
		PointGetCount: 1, BatchGetCount: 1, BatchGetKeyCount: SummaryVirtualShardCount,
	}
	checksums := make([]string, SummaryVirtualShardCount)
	var state SummaryAggregateState
	if err := getSummaryJSON(ctx, store, summaryAggregateStateKey(r.root, kind), &state); err != nil {
		return result, nil, err
	}
	if err := validateSummaryAggregateState(state); err != nil {
		return result, nil, err
	}
	keys := make([]string, SummaryVirtualShardCount)
	for shardID := 0; shardID < SummaryVirtualShardCount; shardID++ {
		keys[shardID] = summaryAggregateKey(r.root, kind, shardID)
	}
	values, err := store.BatchGet(ctx, keys)
	if err != nil {
		return result, nil, err
	}
	result.State = state
	result.BaselineSourceRevision = state.SourceRevision
	result.MaximumSourceRevision = state.SourceRevision
	for shardID, key := range keys {
		raw, found := values[key]
		if !found {
			return result, nil, fmt.Errorf("%w: aggregate virtual shard %d is missing", ErrNotFound, shardID)
		}
		var shard SummaryAggregateShard
		if err := json.Unmarshal(raw, &shard); err != nil {
			return result, nil, fmt.Errorf("decode summary key %s: %w", key, err)
		}
		if err := validateSummaryAggregateShard(shard); err != nil {
			return result, nil, err
		}
		if shard.RebuildEpoch != state.ActiveEpoch || shard.SourceRevision < state.SourceRevision {
			return result, nil, fmt.Errorf("%w: shard %d epoch/revision differs from aggregate state", ErrSummaryShadowAggregateChanged, shardID)
		}
		if shard.PendingOutboxCount != 0 {
			return result, nil, fmt.Errorf("%w: virtual shard %d has %d events", ErrSummaryOutboxPending, shardID, shard.PendingOutboxCount)
		}
		checksums[shardID] = shard.Checksum
		result.Counters, err = addSummaryCounters(result.Counters, shard.Counters)
		if err != nil {
			return result, nil, err
		}
		if shard.SourceRevision > result.MaximumSourceRevision {
			result.MaximumSourceRevision = shard.SourceRevision
		}
		if result.OldestUpdatedAtUnix == 0 || shard.UpdatedAtUnix < result.OldestUpdatedAtUnix {
			result.OldestUpdatedAtUnix = shard.UpdatedAtUnix
		}
	}
	return result, checksums, nil
}

func equalSummaryChecksums(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type summaryReadStoreAdapter struct {
	base kvStore
}

func (a summaryReadStoreAdapter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return a.base.Get(ctx, key)
}

func (a summaryReadStoreAdapter) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	if batcher, ok := a.base.(kvBatchReader); ok {
		return batcher.BatchGet(ctx, keys)
	}
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, found, err := a.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			values[key] = value
		}
	}
	return values, nil
}
