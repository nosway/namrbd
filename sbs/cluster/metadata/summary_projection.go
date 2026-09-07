package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	SummarySchemaVersion       = 1
	SummaryKindCluster         = "cluster"
	SummaryVirtualShardCount   = 64
	SummaryOutboxMaxPageSize   = 512
	SummaryAggregateStateReady = "ready"
)

var (
	ErrSummaryTransactionRequired = errors.New("summary projection requires transactional metadata")
	ErrSummaryInvalidDelta        = errors.New("invalid summary delta")
	ErrSummaryCounterUnderflow    = errors.New("summary counter underflow")
	ErrSummaryCounterOverflow     = errors.New("summary counter overflow")
	ErrSummaryOutboxPending       = errors.New("summary outbox has pending events")
	ErrSummaryInvalidAggregate    = errors.New("invalid summary aggregate")
)

type SummaryCounters struct {
	KnownNodes                  uint64 `json:"known_nodes"`
	ActiveNodes                 uint64 `json:"active_nodes"`
	DrainingNodes               uint64 `json:"draining_nodes"`
	RemovedNodes                uint64 `json:"removed_nodes"`
	HealthyNodes                uint64 `json:"healthy_nodes"`
	SuspectNodes                uint64 `json:"suspect_nodes"`
	DownNodes                   uint64 `json:"down_nodes"`
	VolumeCount                 uint64 `json:"volume_count"`
	TotalBytes                  uint64 `json:"total_bytes"`
	AllocatedChunks             uint64 `json:"allocated_chunks"`
	HealthyVolumes              uint64 `json:"healthy_volumes"`
	DegradedVolumes             uint64 `json:"degraded_volumes"`
	RepairingVolumes            uint64 `json:"repairing_volumes"`
	RebalancingVolumes          uint64 `json:"rebalancing_volumes"`
	BlockedVolumes              uint64 `json:"blocked_volumes"`
	DegradedExtents             uint64 `json:"degraded_extents"`
	RepairBacklog               uint64 `json:"repair_backlog"`
	RepairBacklogBytes          uint64 `json:"repair_backlog_bytes"`
	RepairBacklogChunks         uint64 `json:"repair_backlog_chunks"`
	RebalanceBacklog            uint64 `json:"rebalance_backlog"`
	RebalanceBacklogBytes       uint64 `json:"rebalance_backlog_bytes"`
	RebalanceBacklogChunks      uint64 `json:"rebalance_backlog_chunks"`
	DrainBacklog                uint64 `json:"drain_backlog"`
	DrainBacklogBytes           uint64 `json:"drain_backlog_bytes"`
	DrainBacklogChunks          uint64 `json:"drain_backlog_chunks"`
	RetiredPayloadBacklogBytes  uint64 `json:"retired_payload_backlog_bytes"`
	RetiredPayloadBacklogChunks uint64 `json:"retired_payload_backlog_chunks"`
	RetiredPayloadFailedBatches uint64 `json:"retired_payload_failed_batches"`
	TransitionFailedBatches     uint64 `json:"transition_failed_batches"`
	TransitionRecentBatches     uint64 `json:"transition_recent_batches"`
	TransitionSmallBatches      uint64 `json:"transition_small_batches"`
	TransitionRequeued          uint64 `json:"transition_requeued"`
	TransitionRetryPages        uint64 `json:"transition_retry_pages"`
	TransitionRetryWindows      uint64 `json:"transition_retry_windows"`
	TransitionRetryWindowBytes  uint64 `json:"transition_retry_window_bytes"`
	TransitionRetryWindowChunks uint64 `json:"transition_retry_window_chunks"`
	MaintenanceCooldownVolumes  uint64 `json:"maintenance_cooldown_volumes"`
	OperationCount              uint64 `json:"operation_count"`
	PendingOperations           uint64 `json:"pending_operations"`
	RunningOperations           uint64 `json:"running_operations"`
	CompletedOperations         uint64 `json:"completed_operations"`
	FailedOperations            uint64 `json:"failed_operations"`
	CanceledOperations          uint64 `json:"canceled_operations"`
}

type SummaryDelta struct {
	SchemaVersion  int             `json:"schema_version"`
	EventID        string          `json:"event_id"`
	Kind           string          `json:"kind"`
	SubjectID      string          `json:"subject_id"`
	VirtualShard   int             `json:"virtual_shard"`
	SourceRevision uint64          `json:"source_revision"`
	Before         SummaryCounters `json:"before"`
	After          SummaryCounters `json:"after"`
	DeltaDigest    string          `json:"delta_digest"`
}

type SummaryAggregateShard struct {
	SchemaVersion      int             `json:"schema_version"`
	Kind               string          `json:"kind"`
	VirtualShard       int             `json:"virtual_shard"`
	Counters           SummaryCounters `json:"counters"`
	SourceRevision     uint64          `json:"source_revision"`
	PendingOutboxCount uint64          `json:"pending_outbox_count"`
	UpdatedAtUnix      int64           `json:"updated_at_unix"`
	RebuildEpoch       string          `json:"rebuild_epoch,omitempty"`
	Checksum           string          `json:"checksum"`
}

type SummaryAppliedEvent struct {
	SchemaVersion  int    `json:"schema_version"`
	EventID        string `json:"event_id"`
	Kind           string `json:"kind"`
	VirtualShard   int    `json:"virtual_shard"`
	SourceRevision uint64 `json:"source_revision"`
	DeltaDigest    string `json:"delta_digest"`
	AppliedAtUnix  int64  `json:"applied_at_unix"`
}

type SummaryOutboxRecord struct {
	SchemaVersion  int          `json:"schema_version"`
	Delta          SummaryDelta `json:"delta"`
	EnqueuedAtUnix int64        `json:"enqueued_at_unix"`
	RecordDigest   string       `json:"record_digest"`
}

type SummaryOutboxPageResult struct {
	Kind                string `json:"kind"`
	RequestedLimit      int    `json:"requested_limit"`
	ListedCount         int    `json:"listed_count"`
	ProcessedCount      int    `json:"processed_count"`
	IdempotentCount     int    `json:"idempotent_count"`
	NextCursor          string `json:"next_cursor"`
	PointReadCount      int    `json:"point_read_count"`
	RangePageCount      int    `json:"range_page_count"`
	FullCompletionCount int    `json:"full_completion_count"`
}

type SummaryAggregateState struct {
	SchemaVersion  int    `json:"schema_version"`
	Kind           string `json:"kind"`
	State          string `json:"state"`
	ActiveEpoch    string `json:"active_epoch"`
	SourceRevision uint64 `json:"source_revision"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
	StateDigest    string `json:"state_digest"`
}

func NewSummaryDelta(eventID, subjectID string, sourceRevision uint64, before, after SummaryCounters) (SummaryDelta, error) {
	delta := SummaryDelta{
		SchemaVersion: SummarySchemaVersion, EventID: strings.TrimSpace(eventID), Kind: SummaryKindCluster,
		SubjectID: strings.TrimSpace(subjectID), SourceRevision: sourceRevision, Before: before, After: after,
	}
	delta.VirtualShard = SummaryVirtualShard(delta.SubjectID)
	delta.DeltaDigest = digestSummaryDelta(delta)
	if err := ValidateSummaryDelta(delta); err != nil {
		return SummaryDelta{}, err
	}
	return delta, nil
}

func ValidateSummaryDelta(delta SummaryDelta) error {
	if delta.SchemaVersion != SummarySchemaVersion || delta.Kind != SummaryKindCluster || !validSummaryIdentifier(delta.EventID) || !validSummaryIdentifier(delta.SubjectID) || delta.SourceRevision == 0 {
		return fmt.Errorf("%w: identity/schema/source revision is invalid", ErrSummaryInvalidDelta)
	}
	if delta.VirtualShard != SummaryVirtualShard(delta.SubjectID) {
		return fmt.Errorf("%w: virtual shard %d does not match subject hash", ErrSummaryInvalidDelta, delta.VirtualShard)
	}
	if err := validateSummaryContribution(delta.Before); err != nil {
		return fmt.Errorf("%w: before contribution: %v", ErrSummaryInvalidDelta, err)
	}
	if err := validateSummaryContribution(delta.After); err != nil {
		return fmt.Errorf("%w: after contribution: %v", ErrSummaryInvalidDelta, err)
	}
	if delta.DeltaDigest != digestSummaryDelta(delta) {
		return fmt.Errorf("%w: digest does not match canonical content", ErrSummaryInvalidDelta)
	}
	return nil
}

func SummaryVirtualShard(subjectID string) int {
	return metadataVirtualShard(subjectID, SummaryVirtualShardCount)
}

func metadataVirtualShard(subjectID string, shardCount int) int {
	sum := sha256.Sum256([]byte(strings.TrimSpace(subjectID)))
	return int(sum[0]) % shardCount
}

// ApplySummaryDeltaInTransaction is the product hook for source writers. The
// caller supplies the same transaction writer that persists the authoritative
// source mutation, so the source and its shard-local summary delta commit or
// abort together. This method never writes aggregate-state or another global
// per-mutation key.
func (r *Repository) ApplySummaryDeltaInTransaction(ctx context.Context, writer ReadWriter, delta SummaryDelta) (bool, error) {
	if r == nil || writer == nil {
		return false, fmt.Errorf("%w: repository and writer are required", ErrSummaryInvalidDelta)
	}
	return applySummaryDelta(ctx, writer, r.root, delta, r.now().UTC())
}

func (r *Repository) ApplySummaryDelta(ctx context.Context, delta SummaryDelta) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("%w: repository is nil", ErrSummaryInvalidDelta)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return false, ErrSummaryTransactionRequired
	}
	applied := false
	err := runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var err error
		applied, err = applySummaryDelta(ctx, tx, r.root, delta, r.now().UTC())
		return err
	})
	return applied, err
}

func (r *Repository) EnqueueSummaryDeltaInTransaction(ctx context.Context, writer ReadWriter, delta SummaryDelta) (bool, error) {
	if r == nil || writer == nil {
		return false, fmt.Errorf("%w: repository and writer are required", ErrSummaryInvalidDelta)
	}
	return enqueueSummaryDelta(ctx, writer, r.root, delta, r.now().UTC())
}

func (r *Repository) EnqueueSummaryDelta(ctx context.Context, delta SummaryDelta) (bool, error) {
	if r == nil {
		return false, fmt.Errorf("%w: repository is nil", ErrSummaryInvalidDelta)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return false, ErrSummaryTransactionRequired
	}
	enqueued := false
	err := runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var err error
		enqueued, err = enqueueSummaryDelta(ctx, tx, r.root, delta, r.now().UTC())
		return err
	})
	return enqueued, err
}

func (r *Repository) ProcessSummaryOutboxPage(ctx context.Context, kind, cursor string, limit int) (SummaryOutboxPageResult, error) {
	result := SummaryOutboxPageResult{Kind: kind, RequestedLimit: limit, RangePageCount: 1}
	if r == nil || kind != SummaryKindCluster || limit <= 0 || limit > SummaryOutboxMaxPageSize {
		return result, fmt.Errorf("%w: kind or page limit is invalid", ErrSummaryInvalidDelta)
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return result, ErrSummaryTransactionRequired
	}
	keys, next, err := r.kv.List(ctx, summaryOutboxPrefix(r.root, kind), cursor, limit)
	if err != nil {
		return result, err
	}
	result.ListedCount = len(keys)
	result.NextCursor = next
	for _, key := range keys {
		idempotent := false
		processed := false
		err := runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
			var record SummaryOutboxRecord
			found, err := getOptionalSummaryJSON(ctx, tx, key, &record)
			result.PointReadCount++
			if err != nil || !found {
				return err
			}
			if err := validateSummaryOutboxRecord(record); err != nil {
				return err
			}
			applied, err := applySummaryDelta(ctx, tx, r.root, record.Delta, r.now().UTC())
			if err != nil {
				return err
			}
			idempotent = !applied
			if err := tx.Delete(ctx, key); err != nil {
				return err
			}
			if err := adjustSummaryPendingOutbox(ctx, tx, r.root, record.Delta.Kind, record.Delta.VirtualShard, -1, r.now().UTC()); err != nil {
				return err
			}
			processed = true
			return nil
		})
		if err != nil {
			return result, err
		}
		if processed {
			result.ProcessedCount++
			if idempotent {
				result.IdempotentCount++
			}
		}
	}
	return result, nil
}

func (r *Repository) GetSummaryAggregateShard(ctx context.Context, kind string, virtualShard int) (SummaryAggregateShard, error) {
	if r == nil || kind != SummaryKindCluster || virtualShard < 0 || virtualShard >= SummaryVirtualShardCount {
		return SummaryAggregateShard{}, fmt.Errorf("invalid summary kind or virtual shard")
	}
	var shard SummaryAggregateShard
	found, err := getOptionalSummaryJSON(ctx, r.kv, summaryAggregateKey(r.root, kind, virtualShard), &shard)
	if err != nil {
		return SummaryAggregateShard{}, err
	}
	if !found {
		return newSummaryAggregateShard(kind, virtualShard, ""), nil
	}
	if err := validateSummaryAggregateShard(shard); err != nil {
		return SummaryAggregateShard{}, err
	}
	return shard, nil
}

func (r *Repository) GetSummaryAggregateState(ctx context.Context, kind string) (SummaryAggregateState, error) {
	if r == nil || kind != SummaryKindCluster {
		return SummaryAggregateState{}, fmt.Errorf("invalid summary kind")
	}
	var state SummaryAggregateState
	if err := getSummaryJSON(ctx, r.kv, summaryAggregateStateKey(r.root, kind), &state); err != nil {
		return SummaryAggregateState{}, err
	}
	if err := validateSummaryAggregateState(state); err != nil {
		return SummaryAggregateState{}, err
	}
	return state, nil
}

func applySummaryDelta(ctx context.Context, writer ReadWriter, root string, delta SummaryDelta, now time.Time) (bool, error) {
	if err := ValidateSummaryDelta(delta); err != nil {
		return false, err
	}
	markerKey := summaryAppliedEventKey(root, delta.Kind, delta.VirtualShard, delta.EventID)
	var marker SummaryAppliedEvent
	found, err := getOptionalSummaryJSON(ctx, writer, markerKey, &marker)
	if err != nil {
		return false, err
	}
	if found {
		if marker.DeltaDigest == delta.DeltaDigest && marker.SourceRevision == delta.SourceRevision {
			return false, nil
		}
		return false, fmt.Errorf("%w: event %s was already applied with different content", ErrCASConflict, delta.EventID)
	}
	key := summaryAggregateKey(root, delta.Kind, delta.VirtualShard)
	shard := newSummaryAggregateShard(delta.Kind, delta.VirtualShard, "")
	found, err = getOptionalSummaryJSON(ctx, writer, key, &shard)
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
	marker = SummaryAppliedEvent{
		SchemaVersion: SummarySchemaVersion, EventID: delta.EventID, Kind: delta.Kind,
		VirtualShard: delta.VirtualShard, SourceRevision: delta.SourceRevision,
		DeltaDigest: delta.DeltaDigest, AppliedAtUnix: now.Unix(),
	}
	if err := putSummaryJSON(ctx, writer, markerKey, marker); err != nil {
		return false, err
	}
	return true, nil
}

func enqueueSummaryDelta(ctx context.Context, writer ReadWriter, root string, delta SummaryDelta, now time.Time) (bool, error) {
	if err := ValidateSummaryDelta(delta); err != nil {
		return false, err
	}
	key := summaryOutboxKey(root, delta.Kind, delta.VirtualShard, delta.EventID)
	var existing SummaryOutboxRecord
	found, err := getOptionalSummaryJSON(ctx, writer, key, &existing)
	if err != nil {
		return false, err
	}
	record := SummaryOutboxRecord{SchemaVersion: SummarySchemaVersion, Delta: delta, EnqueuedAtUnix: now.Unix()}
	record.RecordDigest = digestSummaryOutboxRecord(record)
	if found {
		if err := validateSummaryOutboxRecord(existing); err != nil {
			return false, err
		}
		if existing.Delta.DeltaDigest == delta.DeltaDigest {
			return false, nil
		}
		return false, fmt.Errorf("%w: outbox event %s exists with different content", ErrCASConflict, delta.EventID)
	}
	if err := putSummaryJSON(ctx, writer, key, record); err != nil {
		return false, err
	}
	if err := adjustSummaryPendingOutbox(ctx, writer, root, delta.Kind, delta.VirtualShard, 1, now); err != nil {
		return false, err
	}
	return true, nil
}

func adjustSummaryPendingOutbox(ctx context.Context, writer ReadWriter, root, kind string, virtualShard, direction int, now time.Time) error {
	key := summaryAggregateKey(root, kind, virtualShard)
	shard := newSummaryAggregateShard(kind, virtualShard, "")
	found, err := getOptionalSummaryJSON(ctx, writer, key, &shard)
	if err != nil {
		return err
	}
	if found {
		if err := validateSummaryAggregateShard(shard); err != nil {
			return err
		}
	}
	switch direction {
	case 1:
		if shard.PendingOutboxCount == math.MaxUint64 {
			return ErrSummaryCounterOverflow
		}
		shard.PendingOutboxCount++
	case -1:
		if shard.PendingOutboxCount == 0 {
			return ErrSummaryCounterUnderflow
		}
		shard.PendingOutboxCount--
	default:
		return fmt.Errorf("%w: invalid pending outbox adjustment", ErrSummaryInvalidDelta)
	}
	shard.UpdatedAtUnix = now.Unix()
	shard.Checksum = digestSummaryAggregateShard(shard)
	return putSummaryJSON(ctx, writer, key, shard)
}

func applySummaryCounterDelta(current, before, after SummaryCounters) (SummaryCounters, error) {
	currentValues := summaryCounterValues(current)
	beforeValues := summaryCounterValues(before)
	afterValues := summaryCounterValues(after)
	for index := range currentValues {
		if currentValues[index] < beforeValues[index] {
			return SummaryCounters{}, fmt.Errorf("%w at field %d", ErrSummaryCounterUnderflow, index)
		}
		base := currentValues[index] - beforeValues[index]
		if math.MaxUint64-base < afterValues[index] {
			return SummaryCounters{}, fmt.Errorf("%w at field %d", ErrSummaryCounterOverflow, index)
		}
		currentValues[index] = base + afterValues[index]
	}
	return summaryCountersFromValues(currentValues), nil
}

func validateSummaryContribution(counters SummaryCounters) error {
	for _, value := range []uint64{
		counters.ActiveNodes, counters.DrainingNodes, counters.RemovedNodes,
		counters.HealthyNodes, counters.SuspectNodes, counters.DownNodes,
		counters.HealthyVolumes, counters.DegradedVolumes, counters.RepairingVolumes,
		counters.RebalancingVolumes, counters.BlockedVolumes, counters.PendingOperations,
		counters.RunningOperations, counters.CompletedOperations, counters.FailedOperations,
		counters.CanceledOperations,
	} {
		if value > 1 {
			return fmt.Errorf("one subject cannot contribute more than one state count")
		}
	}
	nodeLifecycleStates := counters.ActiveNodes + counters.DrainingNodes + counters.RemovedNodes
	nodeHealthStates := counters.HealthyNodes + counters.SuspectNodes + counters.DownNodes
	if nodeLifecycleStates != counters.KnownNodes || nodeHealthStates != counters.KnownNodes || counters.KnownNodes > 1 {
		return fmt.Errorf("node contribution count/status is inconsistent")
	}
	volumeStates := counters.HealthyVolumes + counters.DegradedVolumes + counters.RepairingVolumes + counters.RebalancingVolumes + counters.BlockedVolumes
	if volumeStates != counters.VolumeCount || counters.VolumeCount > 1 {
		return fmt.Errorf("volume contribution count/status is inconsistent")
	}
	operationStates := counters.PendingOperations + counters.RunningOperations + counters.CompletedOperations + counters.FailedOperations + counters.CanceledOperations
	if operationStates != counters.OperationCount || counters.OperationCount > 1 {
		return fmt.Errorf("operation contribution count/status is inconsistent")
	}
	if counters.KnownNodes+counters.VolumeCount+counters.OperationCount > 1 {
		return fmt.Errorf("one subject cannot contribute multiple node, volume, or operation counts")
	}
	if counters.VolumeCount == 0 && (counters.TotalBytes != 0 || counters.AllocatedChunks != 0) {
		return fmt.Errorf("bytes/chunks require a volume contribution")
	}
	return nil
}

func validateSummaryAggregateShard(shard SummaryAggregateShard) error {
	if shard.SchemaVersion != SummarySchemaVersion || shard.Kind != SummaryKindCluster || shard.VirtualShard < 0 || shard.VirtualShard >= SummaryVirtualShardCount || shard.UpdatedAtUnix < 0 {
		return fmt.Errorf("%w: shard identity", ErrSummaryInvalidAggregate)
	}
	if shard.Checksum != digestSummaryAggregateShard(shard) {
		return fmt.Errorf("%w: shard checksum mismatch", ErrSummaryInvalidAggregate)
	}
	return nil
}

func validateSummaryOutboxRecord(record SummaryOutboxRecord) error {
	if record.SchemaVersion != SummarySchemaVersion || record.EnqueuedAtUnix <= 0 || record.RecordDigest != digestSummaryOutboxRecord(record) {
		return fmt.Errorf("invalid summary outbox record")
	}
	return ValidateSummaryDelta(record.Delta)
}

func validateSummaryAggregateState(state SummaryAggregateState) error {
	if state.SchemaVersion != SummarySchemaVersion || state.Kind != SummaryKindCluster || state.State != SummaryAggregateStateReady || !validSummaryIdentifier(state.ActiveEpoch) || state.SourceRevision == 0 || state.UpdatedAtUnix <= 0 || state.StateDigest != digestSummaryAggregateState(state) {
		return fmt.Errorf("%w: aggregate state", ErrSummaryInvalidAggregate)
	}
	return nil
}

func newSummaryAggregateShard(kind string, virtualShard int, rebuildEpoch string) SummaryAggregateShard {
	shard := SummaryAggregateShard{SchemaVersion: SummarySchemaVersion, Kind: kind, VirtualShard: virtualShard, RebuildEpoch: rebuildEpoch}
	shard.Checksum = digestSummaryAggregateShard(shard)
	return shard
}

func digestSummaryDelta(delta SummaryDelta) string {
	delta.DeltaDigest = ""
	return digestSummaryValue(delta)
}

func digestSummaryAggregateShard(shard SummaryAggregateShard) string {
	shard.Checksum = ""
	return digestSummaryValue(shard)
}

func digestSummaryOutboxRecord(record SummaryOutboxRecord) string {
	record.RecordDigest = ""
	return digestSummaryValue(record)
}

func digestSummaryAggregateState(state SummaryAggregateState) string {
	state.StateDigest = ""
	return digestSummaryValue(state)
}

func digestSummaryValue(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func summaryCounterValues(counters SummaryCounters) []uint64 {
	return []uint64{
		counters.KnownNodes, counters.ActiveNodes, counters.DrainingNodes,
		counters.RemovedNodes, counters.HealthyNodes, counters.SuspectNodes, counters.DownNodes,
		counters.VolumeCount, counters.TotalBytes, counters.AllocatedChunks,
		counters.HealthyVolumes, counters.DegradedVolumes, counters.RepairingVolumes,
		counters.RebalancingVolumes, counters.BlockedVolumes, counters.DegradedExtents,
		counters.RepairBacklog, counters.RepairBacklogBytes, counters.RepairBacklogChunks,
		counters.RebalanceBacklog, counters.RebalanceBacklogBytes, counters.RebalanceBacklogChunks,
		counters.DrainBacklog, counters.DrainBacklogBytes, counters.DrainBacklogChunks,
		counters.RetiredPayloadBacklogBytes, counters.RetiredPayloadBacklogChunks,
		counters.RetiredPayloadFailedBatches, counters.TransitionFailedBatches,
		counters.TransitionRecentBatches, counters.TransitionSmallBatches,
		counters.TransitionRequeued, counters.TransitionRetryPages, counters.TransitionRetryWindows,
		counters.TransitionRetryWindowBytes, counters.TransitionRetryWindowChunks,
		counters.MaintenanceCooldownVolumes,
		counters.OperationCount, counters.PendingOperations, counters.RunningOperations,
		counters.CompletedOperations, counters.FailedOperations, counters.CanceledOperations,
	}
}

func summaryCountersFromValues(values []uint64) SummaryCounters {
	return SummaryCounters{
		KnownNodes: values[0], ActiveNodes: values[1], DrainingNodes: values[2],
		RemovedNodes: values[3], HealthyNodes: values[4], SuspectNodes: values[5], DownNodes: values[6],
		VolumeCount: values[7], TotalBytes: values[8], AllocatedChunks: values[9],
		HealthyVolumes: values[10], DegradedVolumes: values[11], RepairingVolumes: values[12],
		RebalancingVolumes: values[13], BlockedVolumes: values[14], DegradedExtents: values[15],
		RepairBacklog: values[16], RepairBacklogBytes: values[17], RepairBacklogChunks: values[18],
		RebalanceBacklog: values[19], RebalanceBacklogBytes: values[20], RebalanceBacklogChunks: values[21],
		DrainBacklog: values[22], DrainBacklogBytes: values[23], DrainBacklogChunks: values[24],
		RetiredPayloadBacklogBytes: values[25], RetiredPayloadBacklogChunks: values[26],
		RetiredPayloadFailedBatches: values[27], TransitionFailedBatches: values[28],
		TransitionRecentBatches: values[29], TransitionSmallBatches: values[30],
		TransitionRequeued: values[31], TransitionRetryPages: values[32], TransitionRetryWindows: values[33],
		TransitionRetryWindowBytes: values[34], TransitionRetryWindowChunks: values[35],
		MaintenanceCooldownVolumes: values[36],
		OperationCount:             values[37], PendingOperations: values[38], RunningOperations: values[39],
		CompletedOperations: values[40], FailedOperations: values[41], CanceledOperations: values[42],
	}
}

func validSummaryIdentifier(value string) bool {
	if value == "" || len(value) > 160 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' || char == ':' {
			continue
		}
		return false
	}
	return true
}

func summaryAggregateKey(root, kind string, shard int) string {
	return fmt.Sprintf("%s/derived/ad/v1/aggregate/%s/%02d", root, kind, shard)
}

func summaryAppliedEventKey(root, kind string, shard int, eventID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/applied/%s/%02d/%s", root, kind, shard, eventID)
}

func summaryOutboxPrefix(root, kind string) string {
	return fmt.Sprintf("%s/derived/ad/v1/outbox/%s/", root, kind)
}

func summaryOutboxKey(root, kind string, shard int, eventID string) string {
	return fmt.Sprintf("%s%02d/%s", summaryOutboxPrefix(root, kind), shard, eventID)
}

func summaryAggregateStateKey(root, kind string) string {
	return fmt.Sprintf("%s/derived/ad/v1/aggregate-state/%s", root, kind)
}

func putSummaryJSON(ctx context.Context, writer ReadWriter, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writer.Set(ctx, key, raw)
}

func getSummaryJSON(ctx context.Context, reader interface {
	Get(context.Context, string) ([]byte, bool, error)
}, key string, value any) error {
	raw, found, err := reader.Get(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return fmt.Errorf("decode summary key %s: %w", key, err)
	}
	return nil
}

func getOptionalSummaryJSON(ctx context.Context, reader interface {
	Get(context.Context, string) ([]byte, bool, error)
}, key string, value any) (bool, error) {
	raw, found, err := reader.Get(ctx, key)
	if err != nil || !found {
		return found, err
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return false, fmt.Errorf("decode summary key %s: %w", key, err)
	}
	return true, nil
}

func sortedSummaryKeys(values map[int]SummaryCounters) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}
