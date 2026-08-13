package replication

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type ReplicaWriteRequest struct {
	RequestID      string
	VolumeID       string
	AttachmentID   string
	Generation     uint64
	IdempotencyKey string
	OffsetBytes    uint64
	LengthBytes    uint64
	Data           []byte
	ZeroSemantic   bool
}

type ReplicaWriteResult struct {
	AckedReplicaIDs        []string
	FailureMessages        []string
	ChunkEncryptionHeaders []ReplicaChunkEncryptionHeader
	Stats                  ReplicaWriteStats
	AllowNonPrimaryQuorum  bool
}

type ReplicaChunkEncryptionHeader struct {
	LogicalChunk    uint64
	PhysicalChunkID uint64
	Header          *metadata.PayloadEncryptionHeader
}

type ReplicaWriteStats struct {
	FirstAckDuration   time.Duration
	QuorumAckDuration  time.Duration
	AllAckDuration     time.Duration
	SlowestReplicaID   string
	SlowestDuration    time.Duration
	PerReplicaDuration map[string]time.Duration
	QuorumEarlyReturn  bool
	PendingReplicas    int
	PrimaryAckRequired bool
	PrimaryAcked       bool
}

type replicaWriter interface {
	WriteExtent(ctx context.Context, plan ExtentWritePlan, req ReplicaWriteRequest) (*ReplicaWriteResult, error)
}

type WriteService struct {
	executor    *Executor
	writer      replicaWriter
	commitLocks *metadataCommitLockSet
}

const writeMetadataCommitMaxAttempts = 64

var defaultMetadataCommitLocks = newMetadataCommitLockSet()

func NewWriteService(executor *Executor, writer replicaWriter) *WriteService {
	return &WriteService{
		executor:    executor,
		writer:      writer,
		commitLocks: defaultMetadataCommitLocks,
	}
}

type WriteRequest struct {
	VolumeID                      string
	CloneID                       string
	RequestID                     string
	AttachmentID                  string
	Generation                    uint64
	IdempotencyKey                string
	OffsetBytes                   uint64
	LengthBytes                   uint64
	Data                          []byte
	PageBytes                     uint32
	ChunkSizeBytes                uint32
	ZeroSemantic                  bool
	AllowZeroNoop                 bool
	UnsafeZeroNoopSkipIdempotency bool
}

type WriteResponse struct {
	State          WritePipelineState
	Replay         bool
	Committed      bool
	VolumeID       string
	CloneID        string
	IdempotencyKey string
	Revision       uint64
}

func (s *WriteService) WriteClone(ctx context.Context, cloneID string, req WriteRequest) (*WriteResponse, error) {
	req.CloneID = cloneID
	return s.Write(ctx, req)
}

func (s *WriteService) Write(ctx context.Context, req WriteRequest) (*WriteResponse, error) {
	start := time.Now()
	beginStart := time.Now()
	beginReq := BeginWriteRequest{
		VolumeID:                      req.VolumeID,
		CloneID:                       req.CloneID,
		RequestID:                     req.RequestID,
		AttachmentID:                  req.AttachmentID,
		Generation:                    req.Generation,
		IdempotencyKey:                req.IdempotencyKey,
		OffsetBytes:                   req.OffsetBytes,
		LengthBytes:                   req.LengthBytes,
		PageBytes:                     req.PageBytes,
		ChunkSizeBytes:                req.ChunkSizeBytes,
		ZeroSemantic:                  req.ZeroSemantic,
		AllowZeroNoop:                 req.AllowZeroNoop,
		UnsafeZeroNoopSkipIdempotency: req.UnsafeZeroNoopSkipIdempotency,
	}
	var begin *BeginWriteResult
	var err error
	if req.CloneID != "" {
		begin, err = s.executor.BeginCloneWrite(ctx, beginReq)
	} else {
		begin, err = s.executor.BeginWrite(ctx, beginReq)
	}
	beginDuration := time.Since(beginStart)
	if err != nil {
		structuredlog.Error("sbs.replication", "write_begin_failed", err,
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", req.CloneID),
			structuredlog.F("attachment_id", req.AttachmentID),
			structuredlog.F("generation", req.Generation),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
		)
		return nil, err
	}
	if begin.Replay != nil {
		structuredlog.Info("sbs.replication", "write_replayed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.AttachmentID),
			structuredlog.F("generation", req.Generation),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("revision", begin.Replay.Revision),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
		)
		return &WriteResponse{
			State:          WriteStateAcked,
			Replay:         true,
			Committed:      begin.Replay.ResultState == metadata.IdempotencyCommitted,
			VolumeID:       begin.Replay.VolumeID,
			CloneID:        req.CloneID,
			IdempotencyKey: begin.Replay.IdempotencyKey,
			Revision:       begin.Replay.Revision,
		}, nil
	}
	if begin.Noop != nil {
		structuredlog.Info("sbs.replication", "write_committed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", req.CloneID),
			structuredlog.F("attachment_id", req.AttachmentID),
			structuredlog.F("generation", req.Generation),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("revision", begin.Noop.Revision),
			structuredlog.F("extent_count", begin.Noop.ExtentCount),
			structuredlog.F("placement_extent_count", begin.Noop.ExtentCount),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
			structuredlog.F("begin_get_volume_state_duration_ms", begin.Stats.GetVolumeStateDuration.Milliseconds()),
			structuredlog.F("begin_idempotency_lookup_duration_ms", begin.Stats.GetIdempotencyDuration.Milliseconds()),
			structuredlog.F("begin_mutation_lookup_duration_ms", begin.Stats.GetMutationDuration.Milliseconds()),
			structuredlog.F("begin_plan_duration_ms", begin.Stats.PlanDuration.Milliseconds()),
			structuredlog.F("begin_plan_resolve_placements_duration_ms", begin.Stats.PlanResolvePlacementsDuration.Milliseconds()),
			structuredlog.F("begin_plan_resolve_allocations_duration_ms", begin.Stats.PlanResolveAllocationsDuration.Milliseconds()),
			structuredlog.F("begin_plan_source_cow_duration_ms", begin.Stats.PlanSourceCOWDuration.Milliseconds()),
			structuredlog.F("begin_plan_build_targets_duration_ms", begin.Stats.PlanBuildTargetsDuration.Milliseconds()),
			structuredlog.F("begin_plan_resolved_placement_count", begin.Stats.PlanResolvedPlacementCount),
			structuredlog.F("begin_plan_resolved_allocation_page_count", begin.Stats.PlanResolvedAllocationPageCount),
			structuredlog.F("begin_plan_copy_on_write", begin.Stats.PlanCopyOnWrite),
			structuredlog.F("begin_allocation_prepare_duration_ms", begin.Stats.AllocationPrepareDuration.Milliseconds()),
			structuredlog.F("begin_put_intent_duration_ms", begin.Stats.PutIntentDuration.Milliseconds()),
			structuredlog.F("begin_put_idempotency_duration_ms", begin.Stats.PutIdempotencyDuration.Milliseconds()),
			structuredlog.F("begin_put_mutation_duration_ms", begin.Stats.PutMutationDuration.Milliseconds()),
			structuredlog.F("extent_write_duration_ms", int64(0)),
			structuredlog.F("replica_first_ack_duration_ms", int64(0)),
			structuredlog.F("replica_quorum_ack_duration_ms", int64(0)),
			structuredlog.F("replica_all_ack_duration_ms", int64(0)),
			structuredlog.F("replica_slowest_duration_ms", int64(0)),
			structuredlog.F("replica_slowest_replica_id", ""),
			structuredlog.F("replica_pending_after_quorum", 0),
			structuredlog.F("replica_quorum_early_return", false),
			structuredlog.F("replica_primary_ack_required", false),
			structuredlog.F("replica_primary_acked", false),
			structuredlog.F("prepare_duration_ms", int64(0)),
			structuredlog.F("commit_duration_ms", int64(0)),
			structuredlog.F("metadata_commit_mode", "zero_noop"),
			structuredlog.F("zero_noop", true),
			structuredlog.F("zero_noop_idempotency_skipped", begin.UnsafeZeroNoopIdempotencySkipped),
		)
		return &WriteResponse{
			State:          WriteStateAcked,
			Replay:         false,
			Committed:      true,
			VolumeID:       req.VolumeID,
			CloneID:        req.CloneID,
			IdempotencyKey: req.IdempotencyKey,
			Revision:       begin.Noop.Revision,
		}, nil
	}

	exec := begin.Execution
	markFailed := func(err error) {
		if req.CloneID != "" {
			exec.MarkFailed(err)
			return
		}
		_ = s.executor.MarkFailed(ctx, exec, err)
	}
	var extentWriteDuration time.Duration
	var replicaFirstAckDuration time.Duration
	var replicaQuorumAckDuration time.Duration
	var replicaAllAckDuration time.Duration
	var replicaSlowestDuration time.Duration
	var replicaSlowestID string
	var replicaPendingAfterQuorum int
	var replicaQuorumEarlyReturn bool
	var replicaPrimaryAckRequired bool
	replicaData := req.Data
	if req.ZeroSemantic && len(replicaData) == 0 && !writeExecutionCanOmitZeroSemanticPayload(exec, req) {
		replicaData = make([]byte, req.LengthBytes)
	}
	for extentIndex, extent := range exec.Extents {
		extentStart := time.Now()
		result, err := s.writer.WriteExtent(ctx, extent.Plan, ReplicaWriteRequest{
			RequestID:      req.RequestID,
			VolumeID:       req.VolumeID,
			AttachmentID:   req.AttachmentID,
			Generation:     req.Generation,
			IdempotencyKey: req.IdempotencyKey,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			Data:           append([]byte(nil), replicaData...),
			ZeroSemantic:   req.ZeroSemantic,
		})
		extentWriteDuration += time.Since(extentStart)
		if err != nil {
			markFailed(err)
			structuredlog.Error("sbs.replication", "write_extent_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", req.CloneID),
				structuredlog.F("placement_ref", extent.Plan.PlacementRef),
				structuredlog.F("extent_id", extent.Plan.Extent.ExtentID),
				structuredlog.F("idempotency_key", req.IdempotencyKey),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
				structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
				structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
			)
			return nil, err
		}
		if result == nil {
			err := fmt.Errorf("replica writer returned nil result")
			markFailed(err)
			return nil, err
		}
		if err := exec.RecordChunkEncryptionHeaders(extentIndex, result.ChunkEncryptionHeaders); err != nil {
			markFailed(err)
			return nil, err
		}
		accumulateReplicaWriteStats(result.Stats, &replicaFirstAckDuration, &replicaQuorumAckDuration, &replicaAllAckDuration, &replicaSlowestDuration, &replicaSlowestID, &replicaPendingAfterQuorum, &replicaQuorumEarlyReturn)
		requirePrimaryAck := !result.AllowNonPrimaryQuorum
		if requirePrimaryAck {
			replicaPrimaryAckRequired = true
		}
		for _, replicaID := range result.AckedReplicaIDs {
			if err := exec.MarkReplicaAckWithPolicy(extentIndex, replicaID, requirePrimaryAck); err != nil {
				markFailed(err)
				return nil, err
			}
		}
		structuredlog.Info("sbs.replication", "write_extent_payload_acked",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("placement_ref", extent.Plan.PlacementRef),
			structuredlog.F("extent_id", extent.Plan.Extent.ExtentID),
			structuredlog.F("write_targets", len(extent.Plan.WriteTargets)),
			structuredlog.F("required_acks", extent.Plan.RequiredAcks),
			structuredlog.F("acked_replicas", len(result.AckedReplicaIDs)),
			structuredlog.F("acked_replica_ids", result.AckedReplicaIDs),
			structuredlog.F("replica_failures", result.FailureMessages),
			structuredlog.F("primary_replica_id", extent.Plan.Primary.ReplicaID),
			structuredlog.F("primary_ack_required", requirePrimaryAck),
			structuredlog.F("primary_acked", exec.Extents[extentIndex].PrimaryAcked),
			structuredlog.F("quorum_reached", exec.Extents[extentIndex].QuorumReached),
			structuredlog.F("extent_write_duration_ms", time.Since(extentStart).Milliseconds()),
			structuredlog.F("replica_first_ack_duration_ms", result.Stats.FirstAckDuration.Milliseconds()),
			structuredlog.F("replica_quorum_ack_duration_ms", result.Stats.QuorumAckDuration.Milliseconds()),
			structuredlog.F("replica_all_ack_duration_ms", result.Stats.AllAckDuration.Milliseconds()),
			structuredlog.F("replica_slowest_duration_ms", result.Stats.SlowestDuration.Milliseconds()),
			structuredlog.F("replica_slowest_replica_id", result.Stats.SlowestReplicaID),
			structuredlog.F("replica_pending_after_quorum", result.Stats.PendingReplicas),
			structuredlog.F("replica_quorum_early_return", result.Stats.QuorumEarlyReturn),
			structuredlog.F("replica_primary_ack_required", requirePrimaryAck),
			structuredlog.F("replica_primary_acked", exec.Extents[extentIndex].PrimaryAcked),
			structuredlog.F("replica_per_replica_duration_ms", replicaDurationsMillis(result.Stats.PerReplicaDuration)),
		)
		if !exec.Extents[extentIndex].QuorumReached {
			status := exec.Extents[extentIndex]
			err := writeQuorumError(extent.Plan, result, status)
			markFailed(err)
			structuredlog.Error("sbs.replication", "write_quorum_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("placement_ref", extent.Plan.PlacementRef),
				structuredlog.F("extent_id", extent.Plan.Extent.ExtentID),
				structuredlog.F("idempotency_key", req.IdempotencyKey),
				structuredlog.F("required_acks", extent.Plan.RequiredAcks),
				structuredlog.F("acked_replicas", len(result.AckedReplicaIDs)),
				structuredlog.F("acked_replica_ids", result.AckedReplicaIDs),
				structuredlog.F("replica_failures", result.FailureMessages),
				structuredlog.F("primary_replica_id", extent.Plan.Primary.ReplicaID),
				structuredlog.F("primary_ack_required", requirePrimaryAck),
				structuredlog.F("primary_acked", status.PrimaryAcked),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
				structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
				structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
			)
			return nil, err
		}
	}

	prepareStart := time.Now()
	allocationPages, retiredPhysicalChunkIDs, affectedPageChunkRanges, err := s.executor.PrepareAllocationCommit(exec, req)
	prepareDuration := time.Since(prepareStart)
	metadataCommitMode := s.executor.metadataCommitMode(allocationPages)
	if req.CloneID != "" {
		metadataCommitMode = "clone_delta"
		retiredPhysicalChunkIDs = nil
	}
	if err != nil {
		markFailed(err)
		structuredlog.Error("sbs.replication", "write_allocation_prepare_failed", err,
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
			structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
			structuredlog.F("prepare_duration_ms", prepareDuration.Milliseconds()),
			structuredlog.F("metadata_commit_mode", metadataCommitMode),
		)
		return nil, err
	}

	commitStart := time.Now()
	var committedRevision uint64
	if req.CloneID != "" {
		err = s.executor.CommitCloneDeltaMetadata(ctx, exec, req.CloneID, allocationPages)
	} else {
		committedRevision, err = s.commitMetadataWithRetry(ctx, exec, req, allocationPages, retiredPhysicalChunkIDs, affectedPageChunkRanges)
	}
	commitDuration := time.Since(commitStart)
	if err != nil {
		markFailed(err)
		structuredlog.Error("sbs.replication", "write_metadata_commit_failed", err,
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", req.CloneID),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
			structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
			structuredlog.F("prepare_duration_ms", prepareDuration.Milliseconds()),
			structuredlog.F("commit_duration_ms", commitDuration.Milliseconds()),
			structuredlog.F("metadata_commit_mode", metadataCommitMode),
		)
		return nil, err
	}
	if err := exec.MarkAcked(); err != nil {
		markFailed(err)
		structuredlog.Error("sbs.replication", "write_ack_failed", err,
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("revision", committedRevision),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
			structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
			structuredlog.F("prepare_duration_ms", prepareDuration.Milliseconds()),
			structuredlog.F("commit_duration_ms", commitDuration.Milliseconds()),
			structuredlog.F("metadata_commit_mode", metadataCommitMode),
		)
		return nil, err
	}
	structuredlog.Info("sbs.replication", "write_committed",
		structuredlog.F("request_id", req.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("clone_id", req.CloneID),
		structuredlog.F("attachment_id", req.AttachmentID),
		structuredlog.F("generation", req.Generation),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("revision", committedRevision),
		structuredlog.F("extent_count", len(exec.Extents)),
		structuredlog.F("placement_extent_count", len(exec.Extents)),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("begin_duration_ms", beginDuration.Milliseconds()),
		structuredlog.F("begin_get_volume_state_duration_ms", begin.Stats.GetVolumeStateDuration.Milliseconds()),
		structuredlog.F("begin_idempotency_lookup_duration_ms", begin.Stats.GetIdempotencyDuration.Milliseconds()),
		structuredlog.F("begin_mutation_lookup_duration_ms", begin.Stats.GetMutationDuration.Milliseconds()),
		structuredlog.F("begin_plan_duration_ms", begin.Stats.PlanDuration.Milliseconds()),
		structuredlog.F("begin_plan_resolve_placements_duration_ms", begin.Stats.PlanResolvePlacementsDuration.Milliseconds()),
		structuredlog.F("begin_plan_resolve_allocations_duration_ms", begin.Stats.PlanResolveAllocationsDuration.Milliseconds()),
		structuredlog.F("begin_plan_source_cow_duration_ms", begin.Stats.PlanSourceCOWDuration.Milliseconds()),
		structuredlog.F("begin_plan_build_targets_duration_ms", begin.Stats.PlanBuildTargetsDuration.Milliseconds()),
		structuredlog.F("begin_plan_resolved_placement_count", begin.Stats.PlanResolvedPlacementCount),
		structuredlog.F("begin_plan_resolved_allocation_page_count", begin.Stats.PlanResolvedAllocationPageCount),
		structuredlog.F("begin_plan_copy_on_write", begin.Stats.PlanCopyOnWrite),
		structuredlog.F("begin_allocation_prepare_duration_ms", begin.Stats.AllocationPrepareDuration.Milliseconds()),
		structuredlog.F("begin_put_intent_duration_ms", begin.Stats.PutIntentDuration.Milliseconds()),
		structuredlog.F("begin_put_idempotency_duration_ms", begin.Stats.PutIdempotencyDuration.Milliseconds()),
		structuredlog.F("begin_put_mutation_duration_ms", begin.Stats.PutMutationDuration.Milliseconds()),
		structuredlog.F("extent_write_duration_ms", extentWriteDuration.Milliseconds()),
		structuredlog.F("replica_first_ack_duration_ms", replicaFirstAckDuration.Milliseconds()),
		structuredlog.F("replica_quorum_ack_duration_ms", replicaQuorumAckDuration.Milliseconds()),
		structuredlog.F("replica_all_ack_duration_ms", replicaAllAckDuration.Milliseconds()),
		structuredlog.F("replica_slowest_duration_ms", replicaSlowestDuration.Milliseconds()),
		structuredlog.F("replica_slowest_replica_id", replicaSlowestID),
		structuredlog.F("replica_pending_after_quorum", replicaPendingAfterQuorum),
		structuredlog.F("replica_quorum_early_return", replicaQuorumEarlyReturn),
		structuredlog.F("replica_primary_ack_required", replicaPrimaryAckRequired),
		structuredlog.F("prepare_duration_ms", prepareDuration.Milliseconds()),
		structuredlog.F("commit_duration_ms", commitDuration.Milliseconds()),
		structuredlog.F("metadata_commit_mode", metadataCommitMode),
	)

	return &WriteResponse{
		State:          exec.State,
		Replay:         false,
		Committed:      true,
		VolumeID:       exec.VolumeID,
		CloneID:        req.CloneID,
		IdempotencyKey: exec.IdempotencyKey,
		Revision:       committedRevision,
	}, nil
}

func writeExecutionCanOmitZeroSemanticPayload(exec *WriteExecution, req WriteRequest) bool {
	if exec == nil || !req.ZeroSemantic || req.LengthBytes == 0 {
		return false
	}
	for _, extent := range exec.Extents {
		replicaReq := ReplicaWriteRequest{
			OffsetBytes:  req.OffsetBytes,
			LengthBytes:  req.LengthBytes,
			ZeroSemantic: true,
		}
		if !canOmitZeroSemanticPayload(extent.Plan, replicaReq) {
			return false
		}
	}
	return len(exec.Extents) > 0
}

func accumulateReplicaWriteStats(stats ReplicaWriteStats, firstAck, quorumAck, allAck, slowest *time.Duration, slowestID *string, pendingAfterQuorum *int, quorumEarlyReturn *bool) {
	if stats.FirstAckDuration > 0 && (*firstAck == 0 || stats.FirstAckDuration < *firstAck) {
		*firstAck = stats.FirstAckDuration
	}
	if stats.QuorumAckDuration > *quorumAck {
		*quorumAck = stats.QuorumAckDuration
	}
	if stats.AllAckDuration > *allAck {
		*allAck = stats.AllAckDuration
	}
	if stats.SlowestDuration > *slowest {
		*slowest = stats.SlowestDuration
		*slowestID = stats.SlowestReplicaID
	}
	*pendingAfterQuorum += stats.PendingReplicas
	if stats.QuorumEarlyReturn {
		*quorumEarlyReturn = true
	}
}

func replicaDurationsMillis(durations map[string]time.Duration) map[string]int64 {
	if len(durations) == 0 {
		return nil
	}
	out := make(map[string]int64, len(durations))
	for replicaID, duration := range durations {
		out[replicaID] = duration.Milliseconds()
	}
	return out
}

func writeQuorumError(plan ExtentWritePlan, result *ReplicaWriteResult, status ExtentWriteStatus) error {
	primaryReplicaID := plan.Primary.ReplicaID
	if primaryReplicaID == "" {
		primaryReplicaID = primaryReplicaIDForPlan(plan)
	}
	parts := []string{
		fmt.Sprintf("extent %d did not reach quorum", plan.Extent.ExtentID),
		fmt.Sprintf("acked=%d", len(result.AckedReplicaIDs)),
		fmt.Sprintf("required=%d", plan.RequiredAcks),
		fmt.Sprintf("primary=%s", primaryReplicaID),
		fmt.Sprintf("primary_acked=%t", status.PrimaryAcked),
	}
	if len(result.AckedReplicaIDs) > 0 {
		parts = append(parts, fmt.Sprintf("acked_replica_ids=%s", strings.Join(result.AckedReplicaIDs, ",")))
	}
	if len(result.FailureMessages) > 0 {
		parts = append(parts, fmt.Sprintf("failures=%s", strings.Join(result.FailureMessages, "; ")))
	}
	return errors.New(strings.Join(parts, " "))
}

type metadataCommitLockSet struct {
	mu    sync.Mutex
	locks map[string]*metadataCommitLock
}

type metadataCommitLock struct {
	token chan struct{}
	refs  int
}

type metadataCommitGuard struct {
	set *metadataCommitLockSet
	key string
	ent *metadataCommitLock
}

func newMetadataCommitLockSet() *metadataCommitLockSet {
	return &metadataCommitLockSet{
		locks: make(map[string]*metadataCommitLock),
	}
}

func (s *metadataCommitLockSet) acquire(ctx context.Context, key string) (*metadataCommitGuard, error) {
	if key == "" {
		key = "_unknown_volume"
	}
	s.mu.Lock()
	ent := s.locks[key]
	if ent == nil {
		ent = &metadataCommitLock{token: make(chan struct{}, 1)}
		ent.token <- struct{}{}
		s.locks[key] = ent
	}
	ent.refs++
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.releaseRef(key, ent)
		return nil, ctx.Err()
	case <-ent.token:
		return &metadataCommitGuard{set: s, key: key, ent: ent}, nil
	}
}

func (g *metadataCommitGuard) release() {
	if g == nil || g.set == nil || g.ent == nil {
		return
	}
	g.ent.token <- struct{}{}
	g.set.releaseRef(g.key, g.ent)
}

func (s *metadataCommitLockSet) releaseRef(key string, ent *metadataCommitLock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ent.refs--
	if ent.refs == 0 && s.locks[key] == ent {
		delete(s.locks, key)
	}
}

func (s *WriteService) commitMetadataWithRetry(ctx context.Context, exec *WriteExecution, req WriteRequest, allocationPages []metadata.AllocationPageRecord, retiredPhysicalChunkIDs []uint64, affectedPageChunkRanges []metadata.AllocationPageChunkRangeRecord) (uint64, error) {
	metadataCommitMode := s.executor.metadataCommitMode(allocationPages)
	if shouldSerializeMetadataCommitMode(metadataCommitMode) {
		if s.commitLocks == nil {
			s.commitLocks = newMetadataCommitLockSet()
		}
		guard, err := s.commitLocks.acquire(ctx, req.VolumeID)
		if err != nil {
			return 0, err
		}
		defer guard.release()
	}

	var lastErr error
	for attempt := 1; attempt <= writeMetadataCommitMaxAttempts; attempt++ {
		revision, err := s.executor.CommitMetadata(ctx, exec, allocationPages, retiredPhysicalChunkIDs, affectedPageChunkRanges)
		if err == nil {
			return revision, nil
		}
		lastErr = err
		if !isRetryableMetadataCommitError(err) || attempt == writeMetadataCommitMaxAttempts {
			return 0, err
		}
		structuredlog.Info("sbs.replication", "write_metadata_commit_retry",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("attempt", attempt),
			structuredlog.F("metadata_commit_mode", metadataCommitMode),
			structuredlog.F("error", err.Error()),
		)
		if err := s.executor.rebaseWritePlanForRetry(ctx, exec, req); err != nil {
			return 0, err
		}
		allocationPages, retiredPhysicalChunkIDs, affectedPageChunkRanges, err = s.executor.PrepareAllocationCommit(exec, req)
		if err != nil {
			return 0, err
		}
		metadataCommitMode = s.executor.metadataCommitMode(allocationPages)
		timer := time.NewTimer(writeMetadataCommitBackoff(attempt, req.IdempotencyKey))
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, lastErr
}

func shouldSerializeMetadataCommitMode(mode string) bool {
	return mode == "volume_scoped" || mode == "volume_scoped_async_effects"
}

func isRetryableMetadataCommitError(err error) bool {
	return errors.Is(err, metadata.ErrCASConflict)
}

func writeMetadataCommitBackoff(attempt int, key string) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt == 1 {
		return 0
	}
	backoff := time.Duration(attempt*attempt) * 5 * time.Millisecond
	if backoff > 50*time.Millisecond {
		return 50*time.Millisecond + writeMetadataCommitJitter(key, attempt)
	}
	return backoff + writeMetadataCommitJitter(key, attempt)
}

func writeMetadataCommitJitter(key string, attempt int) time.Duration {
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	jitterMs := (uint32(attempt)*17 + h.Sum32()) % 13
	return time.Duration(jitterMs) * time.Millisecond
}
