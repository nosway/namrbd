package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

type GRPCWriteSessionAdapter struct {
	client internalv1.WriteSessionServiceClient
}

func NewGRPCWriteSessionAdapter(client internalv1.WriteSessionServiceClient) *GRPCWriteSessionAdapter {
	return &GRPCWriteSessionAdapter{client: client}
}

func (a *GRPCWriteSessionAdapter) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	if a.client == nil {
		return metadata.VolumeState{}, fmt.Errorf("write session gRPC client is required")
	}
	resp, err := a.client.GetVolumeState(ctx, &internalv1.GetVolumeStateRequest{VolumeId: volumeID})
	if err != nil {
		return metadata.VolumeState{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return VolumeStateFromProto(resp.GetVolumeState()), nil
}

func (a *GRPCWriteSessionAdapter) PutVolumeState(ctx context.Context, state metadata.VolumeState) error {
	if a.client == nil {
		return fmt.Errorf("write session gRPC client is required")
	}
	_, err := a.client.PutVolumeState(ctx, &internalv1.PutVolumeStateRequest{VolumeState: VolumeStateToProto(state)})
	return err
}

func (a *GRPCWriteSessionAdapter) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	if a.client == nil {
		return metadata.IdempotencyRecord{}, fmt.Errorf("write session gRPC client is required")
	}
	resp, err := a.client.GetIdempotencyRecord(ctx, &internalv1.GetIdempotencyRecordRequest{
		VolumeId:       volumeID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return IdempotencyRecordFromProto(resp.GetIdempotencyRecord())
}

func (a *GRPCWriteSessionAdapter) PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error {
	if a.client == nil {
		return fmt.Errorf("write session gRPC client is required")
	}
	_, err := a.client.PutIdempotencyRecord(ctx, &internalv1.PutIdempotencyRecordRequest{IdempotencyRecord: IdempotencyRecordToProto(rec)})
	return err
}

func (a *GRPCWriteSessionAdapter) GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error) {
	if a.client == nil {
		return metadata.MutationOperationRecord{}, fmt.Errorf("write session gRPC client is required")
	}
	resp, err := a.client.GetMutationOperation(ctx, &internalv1.GetMutationOperationRequest{
		VolumeId:    volumeID,
		OperationId: operationID,
	})
	if err != nil {
		return metadata.MutationOperationRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return MutationOperationRecordFromProto(resp.GetMutationOperation())
}

func (a *GRPCWriteSessionAdapter) PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error {
	if a.client == nil {
		return fmt.Errorf("write session gRPC client is required")
	}
	_, err := a.client.PutMutationOperation(ctx, &internalv1.PutMutationOperationRequest{MutationOperation: MutationOperationRecordToProto(rec)})
	return err
}

func (a *GRPCWriteSessionAdapter) PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	if a.client == nil {
		return fmt.Errorf("write session gRPC client is required")
	}
	_, err := a.client.PutWriteIntent(ctx, &internalv1.PutWriteIntentRequest{
		IdempotencyRecord: IdempotencyRecordToProto(record),
		MutationOperation: MutationOperationRecordToProto(operation),
	})
	return err
}

func (a *GRPCWriteSessionAdapter) CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("write session gRPC client is required")
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	resp, err := a.client.CommitWriteState(ctx, CommitWriteStateRequestToProto(req))
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionTransportError(err), time.Since(start), req)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	state, record, err := CommitWriteStateResponseFromProto(resp)
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (a *GRPCWriteSessionAdapter) CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("write session gRPC client is required")
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	resp, err := a.client.CommitPageScopedWriteMetadata(ctx, CommitPageScopedWriteMetadataRequestToProto(req))
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionTransportError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	state, record, err := CommitPageScopedWriteMetadataResponseFromProto(resp)
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (a *GRPCWriteSessionAdapter) CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("write session gRPC client is required")
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	resp, err := a.client.CommitRangeLocalWriteState(ctx, CommitRangeLocalWriteStateRequestToProto(req))
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionTransportError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	state, record, err := CommitRangeLocalWriteStateResponseFromProto(resp)
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (a *GRPCWriteSessionAdapter) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("write session gRPC client is required")
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	resp, err := a.client.CommitAppendOnlyWriteStateAndQueueEffects(ctx, CommitAppendOnlyWriteStateAndQueueEffectsRequestToProto(req))
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionTransportError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	state, record, err := CommitAppendOnlyWriteStateAndQueueEffectsResponseFromProto(resp)
	if err != nil {
		logWriteSessionGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), req.StateCommitRequest())
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (a *GRPCWriteSessionAdapter) CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("write session gRPC client is required")
		logWriteSessionCloneDeltaGRPCFailure(err, ClassifyWriteSessionError(err), time.Since(start), cloneID, len(pages))
		return err
	}
	_, err := a.client.CommitCloneDeltaAllocationPages(ctx, CommitCloneDeltaAllocationPagesRequestToProto(cloneID, pages))
	if err != nil {
		logWriteSessionCloneDeltaGRPCFailure(err, ClassifyWriteSessionTransportError(err), time.Since(start), cloneID, len(pages))
		return WriteSessionTransportErrorToMetadataError(err)
	}
	return nil
}

func logWriteSessionGRPCFailure(err error, class WriteSessionErrorClass, duration time.Duration, req metadata.CommitWriteStateRequest) {
	structuredlog.Error("sbs.cluster.control", "write_session_grpc_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
	)
}

func logWriteSessionCloneDeltaGRPCFailure(err error, class WriteSessionErrorClass, duration time.Duration, cloneID string, pageCount int) {
	structuredlog.Error("sbs.cluster.control", "write_session_grpc_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("allocation_page_count", pageCount),
	)
}
