package control

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const writeSessionServiceOutcomeOK = "ok"

// WriteSessionOutcomeRecorder records classified internal gRPC service
// outcomes. The caller keeps owning metrics storage.
type WriteSessionOutcomeRecorder func(class string, duration time.Duration)

// WriteSessionCommitVolumeLocker serializes write-state commits for a volume.
// The caller owns the lock table and can return a no-op unlock when locking is
// not needed.
type WriteSessionCommitVolumeLocker func(volumeID string) func()

// WriteSessionEffectsQueue applies deferred write metadata effects after the
// append-only state commit. The service runtime owns queueing; this interface
// keeps that runtime detail out of the gRPC façade.
type WriteSessionEffectsQueue interface {
	EnqueueAndWait(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) (WriteSessionEffectsQueueStats, error)
}

type WriteSessionAppendOnlyCommitQueue interface {
	EnqueueAppendOnlyCommitAndWait(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, WriteSessionEffectsQueueStats, error)
}

// WriteSessionEffectsQueueStats records the visible wait shape for a
// service-owned write effects request. EnqueueWaitDuration is measured by the
// gRPC facade and remains the historical end-to-end EnqueueAndWait bucket;
// QueueWaitDuration and ApplyDuration split the queue-owned portion.
type WriteSessionEffectsQueueStats struct {
	Depth             int
	LaneKey           string
	QueueWaitDuration time.Duration
	ApplyDuration     time.Duration
	BatchSize         int
}

// ServeGetVolumeState handles the server side of the internal write-session
// GetVolumeState gRPC façade.
func ServeGetVolumeState(ctx context.Context, req *internalv1.GetVolumeStateRequest, service WriteSessionInternalService) (*internalv1.GetVolumeStateResponse, error) {
	state, err := service.GetVolumeState(ctx, req.GetVolumeId())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.GetVolumeStateResponse{VolumeState: VolumeStateToProto(state)}, nil
}

// ServePutVolumeState handles the server side of the internal write-session
// PutVolumeState gRPC façade.
func ServePutVolumeState(ctx context.Context, req *internalv1.PutVolumeStateRequest, service WriteSessionInternalService) (*internalv1.PutVolumeStateResponse, error) {
	state := VolumeStateFromProto(req.GetVolumeState())
	if err := service.PutVolumeState(ctx, state); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutVolumeStateResponse{}, nil
}

// ServeGetIdempotencyRecord handles the server side of the internal
// write-session GetIdempotencyRecord gRPC façade.
func ServeGetIdempotencyRecord(ctx context.Context, req *internalv1.GetIdempotencyRecordRequest, service WriteSessionInternalService) (*internalv1.GetIdempotencyRecordResponse, error) {
	record, err := service.GetIdempotencyRecord(ctx, req.GetVolumeId(), req.GetIdempotencyKey())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.GetIdempotencyRecordResponse{IdempotencyRecord: IdempotencyRecordToProto(record)}, nil
}

// ServePutIdempotencyRecord handles the server side of the internal
// write-session PutIdempotencyRecord gRPC façade.
func ServePutIdempotencyRecord(ctx context.Context, req *internalv1.PutIdempotencyRecordRequest, service WriteSessionInternalService) (*internalv1.PutIdempotencyRecordResponse, error) {
	record, err := IdempotencyRecordFromProto(req.GetIdempotencyRecord())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.PutIdempotencyRecord(ctx, record); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutIdempotencyRecordResponse{}, nil
}

// ServeGetMutationOperation handles the server side of the internal
// write-session GetMutationOperation gRPC façade.
func ServeGetMutationOperation(ctx context.Context, req *internalv1.GetMutationOperationRequest, service WriteSessionInternalService) (*internalv1.GetMutationOperationResponse, error) {
	record, err := service.GetMutationOperation(ctx, req.GetVolumeId(), req.GetOperationId())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.GetMutationOperationResponse{MutationOperation: MutationOperationRecordToProto(record)}, nil
}

// ServePutMutationOperation handles the server side of the internal
// write-session PutMutationOperation gRPC façade.
func ServePutMutationOperation(ctx context.Context, req *internalv1.PutMutationOperationRequest, service WriteSessionInternalService) (*internalv1.PutMutationOperationResponse, error) {
	record, err := MutationOperationRecordFromProto(req.GetMutationOperation())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.PutMutationOperation(ctx, record); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutMutationOperationResponse{}, nil
}

// ServePutWriteIntent handles the server side of the internal write-session
// PutWriteIntent gRPC façade.
func ServePutWriteIntent(ctx context.Context, req *internalv1.PutWriteIntentRequest, service WriteSessionInternalService) (*internalv1.PutWriteIntentResponse, error) {
	record, err := IdempotencyRecordFromProto(req.GetIdempotencyRecord())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	operation, err := MutationOperationRecordFromProto(req.GetMutationOperation())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.PutWriteIntent(ctx, record, operation); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutWriteIntentResponse{}, nil
}

// ServeCommitWriteState handles the server side of the internal write-session
// CommitWriteState gRPC façade.
func ServeCommitWriteState(ctx context.Context, req *internalv1.CommitWriteStateRequest, service WriteSessionInternalService, lockVolume WriteSessionCommitVolumeLocker, record WriteSessionOutcomeRecorder) (*internalv1.CommitWriteStateResponse, error) {
	start := time.Now()
	commitReq, err := CommitWriteStateRequestFromProto(req)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("committed_revision", req.GetCommittedRevision()),
			structuredlog.F("idempotency_key", req.GetIdempotencyKey()),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("write session internal service is required")
		class := WriteSessionErrorInternal
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", commitReq.VolumeID),
			structuredlog.F("expected_revision", commitReq.ExpectedRevision),
			structuredlog.F("committed_revision", commitReq.CommittedRevision),
			structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	unlock := lockWriteSessionCommitVolume(lockVolume, commitReq.VolumeID)
	defer unlock()
	state, recordState, err := service.CommitWriteState(ctx, commitReq)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", commitReq.VolumeID),
			structuredlog.F("expected_revision", commitReq.ExpectedRevision),
			structuredlog.F("committed_revision", commitReq.CommittedRevision),
			structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "write_session_committed",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("expected_revision", commitReq.ExpectedRevision),
		structuredlog.F("committed_revision", commitReq.CommittedRevision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
	)
	return CommitWriteStateResponseToProto(state, recordState), nil
}

// ServeCommitPageScopedWriteMetadata handles the server side of the internal
// write-session CommitPageScopedWriteMetadata gRPC façade.
func ServeCommitPageScopedWriteMetadata(ctx context.Context, req *internalv1.CommitPageScopedWriteMetadataRequest, service WriteSessionInternalService, record WriteSessionOutcomeRecorder) (*internalv1.CommitPageScopedWriteMetadataResponse, error) {
	start := time.Now()
	commitReq, err := CommitPageScopedWriteMetadataRequestFromProto(req)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_page_scoped_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("committed_revision", req.GetCommittedRevision()),
			structuredlog.F("idempotency_key", req.GetIdempotencyKey()),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("write session internal service is required")
		class := WriteSessionErrorInternal
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logPageScopedWriteMetadataFailure(err, class, duration, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	state, recordState, err := service.CommitPageScopedWriteMetadata(ctx, commitReq)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logPageScopedWriteMetadataFailure(err, class, duration, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "write_session_page_scoped_committed",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("expected_revision", commitReq.ExpectedRevision),
		structuredlog.F("committed_revision", recordState.Revision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(commitReq.AllocationPages)),
	)
	return CommitPageScopedWriteMetadataResponseToProto(state, recordState), nil
}

func logPageScopedWriteMetadataFailure(err error, class WriteSessionErrorClass, duration time.Duration, req metadata.CommitWriteMetadataRequest) {
	structuredlog.Error("sbs.service", "write_session_page_scoped_commit_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
	)
}

// ServeCommitRangeLocalWriteState handles the server side of the internal
// write-session CommitRangeLocalWriteState gRPC façade.
func ServeCommitRangeLocalWriteState(ctx context.Context, req *internalv1.CommitRangeLocalWriteStateRequest, service WriteSessionInternalService, record WriteSessionOutcomeRecorder) (*internalv1.CommitRangeLocalWriteStateResponse, error) {
	start := time.Now()
	commitReq, err := CommitRangeLocalWriteStateRequestFromProto(req)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_range_local_state_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("committed_revision", req.GetCommittedRevision()),
			structuredlog.F("idempotency_key", req.GetIdempotencyKey()),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("write session internal service is required")
		class := WriteSessionErrorInternal
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logRangeLocalWriteStateFailure(err, class, duration, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	state, recordState, err := service.CommitRangeLocalWriteState(ctx, commitReq)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logRangeLocalWriteStateFailure(err, class, duration, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "write_session_range_local_state_committed",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("expected_revision", commitReq.ExpectedRevision),
		structuredlog.F("committed_revision", recordState.Revision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(commitReq.AllocationPages)),
	)
	return CommitRangeLocalWriteStateResponseToProto(state, recordState), nil
}

func logRangeLocalWriteStateFailure(err error, class WriteSessionErrorClass, duration time.Duration, req metadata.CommitWriteMetadataRequest) {
	structuredlog.Error("sbs.service", "write_session_range_local_state_commit_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
	)
}

// ServeCommitAppendOnlyWriteStateAndQueueEffects handles the server side of the
// internal write-session CommitAppendOnlyWriteStateAndQueueEffects gRPC façade.
func ServeCommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest, service WriteSessionInternalService, effectsQueue WriteSessionEffectsQueue, nativeAllocationFastPath bool, record WriteSessionOutcomeRecorder) (*internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse, error) {
	start := time.Now()
	commitReq, err := CommitAppendOnlyWriteStateAndQueueEffectsRequestFromProto(req)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_append_only_effects_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("committed_revision", req.GetCommittedRevision()),
			structuredlog.F("idempotency_key", req.GetIdempotencyKey()),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("write session internal service is required")
		class := WriteSessionErrorInternal
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logAppendOnlyWriteStateAndQueueEffectsFailure(err, class, duration, 0, 0, 0, WriteSessionEffectsQueueStats{}, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if effectsQueue == nil {
		err := status.Error(codes.FailedPrecondition, "service-owned write effects queue is disabled")
		duration := time.Since(start)
		recordWriteSessionOutcome(record, "failed_precondition", duration)
		structuredlog.Error("sbs.service", "write_session_append_only_effects_commit_failed", err,
			structuredlog.F("error_class", "failed_precondition"),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", commitReq.VolumeID),
			structuredlog.F("expected_revision", commitReq.ExpectedRevision),
			structuredlog.F("committed_revision", commitReq.CommittedRevision),
			structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(commitReq.AllocationPages)),
		)
		return nil, err
	}
	if commitQueue, ok := effectsQueue.(WriteSessionAppendOnlyCommitQueue); ok {
		effectsReq := commitReq.EffectsApplyRequest()
		effectsLaneKey := writeSessionEffectsLaneKey(effectsReq, nativeAllocationFastPath)
		effectsEnqueueWaitStart := time.Now()
		state, recordState, effectsQueueStats, err := commitQueue.EnqueueAppendOnlyCommitAndWait(ctx, commitReq)
		effectsEnqueueWaitDuration := time.Since(effectsEnqueueWaitStart)
		if effectsQueueStats.LaneKey == "" {
			effectsQueueStats.LaneKey = effectsLaneKey
		}
		if err != nil {
			class := ClassifyWriteSessionError(err)
			duration := time.Since(start)
			recordWriteSessionOutcome(record, string(class), duration)
			logAppendOnlyWriteStateAndQueueEffectsFailure(err, class, duration, 0, 0, effectsEnqueueWaitDuration, effectsQueueStats, commitReq)
			return nil, WriteSessionErrorToGRPCStatus(err)
		}
		duration := time.Since(start)
		committedRevision := commitReq.CommittedRevision
		if recordState.Revision != 0 {
			committedRevision = recordState.Revision
		}
		recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
		structuredlog.Info("sbs.service", "write_session_append_only_effects_committed",
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("append_only_state_commit_duration_ms", int64(0)),
			structuredlog.F("effects_apply_request_build_duration_ms", int64(0)),
			structuredlog.F("effects_enqueue_wait_duration_ms", effectsEnqueueWaitDuration.Milliseconds()),
			structuredlog.F("effects_queue_wait_duration_ms", effectsQueueStats.QueueWaitDuration.Milliseconds()),
			structuredlog.F("effects_apply_duration_ms", effectsQueueStats.ApplyDuration.Milliseconds()),
			structuredlog.F("effects_batch_size", effectsQueueStats.BatchSize),
			structuredlog.F("volume_id", commitReq.VolumeID),
			structuredlog.F("expected_revision", commitReq.ExpectedRevision),
			structuredlog.F("committed_revision", committedRevision),
			structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(commitReq.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(commitReq.NormalizeExtentMappings)),
			structuredlog.F("affected_page_count", len(commitReq.AffectedPageNos)),
			structuredlog.F("affected_extent_count", len(commitReq.AffectedExtentIDs)),
			structuredlog.F("effects_queue_lane", effectsQueueStats.LaneKey),
			structuredlog.F("effects_queue_depth", effectsQueueStats.Depth),
		)
		return CommitAppendOnlyWriteStateAndQueueEffectsResponseToProto(state, recordState), nil
	}
	appendOnlyStart := time.Now()
	state, recordState, err := service.CommitAppendOnlyWriteStateAndQueueEffects(ctx, commitReq)
	appendOnlyDuration := time.Since(appendOnlyStart)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logAppendOnlyWriteStateAndQueueEffectsFailure(err, class, duration, appendOnlyDuration, 0, 0, WriteSessionEffectsQueueStats{}, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	committedRevision := recordState.Revision
	if committedRevision == 0 {
		committedRevision = commitReq.CommittedRevision
	}
	effectsBuildStart := time.Now()
	effectsReq := commitReq.EffectsApplyRequest()
	effectsReq.CommittedRevision = committedRevision
	effectsLaneKey := writeSessionEffectsLaneKey(effectsReq, nativeAllocationFastPath)
	effectsBuildDuration := time.Since(effectsBuildStart)
	effectsEnqueueWaitStart := time.Now()
	effectsQueueStats, err := effectsQueue.EnqueueAndWait(ctx, effectsReq)
	effectsEnqueueWaitDuration := time.Since(effectsEnqueueWaitStart)
	if effectsQueueStats.LaneKey == "" {
		effectsQueueStats.LaneKey = effectsLaneKey
	}
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logAppendOnlyWriteStateAndQueueEffectsFailure(err, class, duration, appendOnlyDuration, effectsBuildDuration, effectsEnqueueWaitDuration, effectsQueueStats, commitReq)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "write_session_append_only_effects_committed",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("append_only_state_commit_duration_ms", appendOnlyDuration.Milliseconds()),
		structuredlog.F("effects_apply_request_build_duration_ms", effectsBuildDuration.Milliseconds()),
		structuredlog.F("effects_enqueue_wait_duration_ms", effectsEnqueueWaitDuration.Milliseconds()),
		structuredlog.F("effects_queue_wait_duration_ms", effectsQueueStats.QueueWaitDuration.Milliseconds()),
		structuredlog.F("effects_apply_duration_ms", effectsQueueStats.ApplyDuration.Milliseconds()),
		structuredlog.F("effects_batch_size", effectsQueueStats.BatchSize),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("expected_revision", commitReq.ExpectedRevision),
		structuredlog.F("committed_revision", committedRevision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(commitReq.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(commitReq.NormalizeExtentMappings)),
		structuredlog.F("affected_page_count", len(commitReq.AffectedPageNos)),
		structuredlog.F("affected_extent_count", len(commitReq.AffectedExtentIDs)),
		structuredlog.F("effects_queue_lane", effectsQueueStats.LaneKey),
		structuredlog.F("effects_queue_depth", effectsQueueStats.Depth),
	)
	return CommitAppendOnlyWriteStateAndQueueEffectsResponseToProto(state, recordState), nil
}

// ServeCommitCloneDeltaAllocationPages handles the server side of the internal
// write-session CommitCloneDeltaAllocationPages gRPC façade.
func ServeCommitCloneDeltaAllocationPages(ctx context.Context, req *internalv1.CommitCloneDeltaAllocationPagesRequest, service WriteSessionCloneDeltaInternalService, record WriteSessionOutcomeRecorder) (*internalv1.CommitCloneDeltaAllocationPagesResponse, error) {
	start := time.Now()
	cloneID, pages, err := CommitCloneDeltaAllocationPagesRequestFromProto(req)
	if err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "write_session_clone_delta_commit_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("clone_id", req.GetCloneId()),
			structuredlog.F("allocation_page_count", len(req.GetAllocationPages())),
		)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("write session internal service is required")
		class := WriteSessionErrorInternal
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logCloneDeltaAllocationPagesFailure(err, class, duration, cloneID, len(pages))
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.CommitCloneDeltaAllocationPages(ctx, cloneID, pages); err != nil {
		class := ClassifyWriteSessionError(err)
		duration := time.Since(start)
		recordWriteSessionOutcome(record, string(class), duration)
		logCloneDeltaAllocationPagesFailure(err, class, duration, cloneID, len(pages))
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "write_session_clone_delta_committed",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("allocation_page_count", len(pages)),
	)
	return &internalv1.CommitCloneDeltaAllocationPagesResponse{}, nil
}

func logCloneDeltaAllocationPagesFailure(err error, class WriteSessionErrorClass, duration time.Duration, cloneID string, pageCount int) {
	structuredlog.Error("sbs.service", "write_session_clone_delta_commit_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("allocation_page_count", pageCount),
	)
}

func logAppendOnlyWriteStateAndQueueEffectsFailure(err error, class WriteSessionErrorClass, duration, appendOnlyDuration, effectsBuildDuration, effectsEnqueueWaitDuration time.Duration, effectsQueueStats WriteSessionEffectsQueueStats, req metadata.CommitWriteMetadataRequest) {
	structuredlog.Error("sbs.service", "write_session_append_only_effects_commit_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("append_only_state_commit_duration_ms", appendOnlyDuration.Milliseconds()),
		structuredlog.F("effects_apply_request_build_duration_ms", effectsBuildDuration.Milliseconds()),
		structuredlog.F("effects_enqueue_wait_duration_ms", effectsEnqueueWaitDuration.Milliseconds()),
		structuredlog.F("effects_queue_wait_duration_ms", effectsQueueStats.QueueWaitDuration.Milliseconds()),
		structuredlog.F("effects_apply_duration_ms", effectsQueueStats.ApplyDuration.Milliseconds()),
		structuredlog.F("effects_batch_size", effectsQueueStats.BatchSize),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
		structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
		structuredlog.F("effects_queue_lane", effectsQueueStats.LaneKey),
		structuredlog.F("effects_queue_depth", effectsQueueStats.Depth),
	)
}

func writeSessionEffectsLaneKey(req metadata.ApplyCommittedWriteEffectsRequest, nativeAllocationFastPath bool) string {
	volumeID := req.VolumeID
	extentIDs := uniqueSortedWriteSessionUint64s(append([]uint64(nil), req.NormalizeExtentMappings...))
	if !nativeAllocationFastPath || !hasSinglePageScopedWriteSessionAllocationEffects(req) {
		if len(extentIDs) == 1 {
			return fmt.Sprintf("volume:%s:extent:%d", volumeID, extentIDs[0])
		}
		if len(extentIDs) > 1 {
			return fmt.Sprintf("volume:%s:all", volumeID)
		}
	}

	pageNos := append([]uint64(nil), req.AffectedPageNos...)
	if len(pageNos) == 0 {
		for _, page := range req.AllocationPages {
			if page.VolumeID != "" {
				volumeID = page.VolumeID
			}
			pageNos = append(pageNos, page.PageNo)
		}
	}
	pageNos = uniqueSortedWriteSessionUint64s(pageNos)
	if len(pageNos) == 1 {
		return fmt.Sprintf("volume:%s:page:%d", volumeID, pageNos[0])
	}
	return fmt.Sprintf("volume:%s:all", volumeID)
}

func hasSinglePageScopedWriteSessionAllocationEffects(req metadata.ApplyCommittedWriteEffectsRequest) bool {
	if len(req.AllocationPages) == 0 {
		return false
	}
	pageNos := make([]uint64, 0, len(req.AffectedPageNos)+len(req.AllocationPages))
	pageNos = append(pageNos, req.AffectedPageNos...)
	for _, page := range req.AllocationPages {
		pageNos = append(pageNos, page.PageNo)
	}
	return len(uniqueSortedWriteSessionUint64s(pageNos)) == 1
}

func uniqueSortedWriteSessionUint64s(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	out := values[:0]
	var last uint64
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}

func lockWriteSessionCommitVolume(lockVolume WriteSessionCommitVolumeLocker, volumeID string) func() {
	if lockVolume == nil {
		return func() {}
	}
	unlock := lockVolume(volumeID)
	if unlock == nil {
		return func() {}
	}
	return unlock
}

func recordWriteSessionOutcome(record WriteSessionOutcomeRecorder, class string, duration time.Duration) {
	if record != nil {
		record(class, duration)
	}
}
