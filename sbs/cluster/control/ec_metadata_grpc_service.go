package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func ServeGetPhysicalObject(ctx context.Context, req *internalv1.GetPhysicalObjectRequest, service ECMetadataInternalService) (*internalv1.GetPhysicalObjectResponse, error) {
	if service == nil {
		return nil, WriteSessionErrorToGRPCStatus(fmt.Errorf("ec metadata internal service is required"))
	}
	rec, err := service.GetPhysicalObject(ctx, req.GetVolumeId(), req.GetObjectId())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.GetPhysicalObjectResponse{PhysicalObject: PhysicalObjectRecordToProto(rec)}, nil
}

func ServePutPhysicalObject(ctx context.Context, req *internalv1.PutPhysicalObjectRequest, service ECMetadataInternalService) (*internalv1.PutPhysicalObjectResponse, error) {
	if service == nil {
		return nil, WriteSessionErrorToGRPCStatus(fmt.Errorf("ec metadata internal service is required"))
	}
	rec, err := PhysicalObjectRecordFromProto(req.GetPhysicalObject())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.PutPhysicalObject(ctx, rec); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutPhysicalObjectResponse{}, nil
}

func ServeGetECStripe(ctx context.Context, req *internalv1.GetECStripeRequest, service ECMetadataInternalService) (*internalv1.GetECStripeResponse, error) {
	if service == nil {
		return nil, WriteSessionErrorToGRPCStatus(fmt.Errorf("ec metadata internal service is required"))
	}
	rec, err := service.GetECStripe(ctx, req.GetVolumeId(), req.GetStripeId(), req.GetStripeGeneration())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.GetECStripeResponse{EcStripe: ECStripeRecordToProto(rec)}, nil
}

func ServePutECStripe(ctx context.Context, req *internalv1.PutECStripeRequest, service ECMetadataInternalService) (*internalv1.PutECStripeResponse, error) {
	if service == nil {
		return nil, WriteSessionErrorToGRPCStatus(fmt.Errorf("ec metadata internal service is required"))
	}
	rec, err := ECStripeRecordFromProto(req.GetEcStripe())
	if err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if err := service.PutECStripe(ctx, rec); err != nil {
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	return &internalv1.PutECStripeResponse{}, nil
}

func ServeCommitECFullStripeWrite(ctx context.Context, req *internalv1.CommitECFullStripeWriteRequest, service ECMetadataInternalService, lockVolume WriteSessionCommitVolumeLocker, record WriteSessionOutcomeRecorder) (*internalv1.CommitECFullStripeWriteResponse, error) {
	start := time.Now()
	commitReq, err := CommitECFullStripeWriteRequestFromProto(req)
	if err != nil {
		recordECMetadataOutcome(record, err, time.Since(start), req.GetVolumeId(), req.GetCommittedRevision(), req.GetIdempotencyKey())
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("ec metadata internal service is required")
		recordECMetadataOutcome(record, err, time.Since(start), commitReq.VolumeID, commitReq.CommittedRevision, commitReq.IdempotencyKey)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	unlock := lockWriteSessionCommitVolume(lockVolume, commitReq.VolumeID)
	defer unlock()
	state, idempotency, err := service.CommitECFullStripeWrite(ctx, commitReq)
	if err != nil {
		recordECMetadataOutcome(record, err, time.Since(start), commitReq.VolumeID, commitReq.CommittedRevision, commitReq.IdempotencyKey)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, time.Since(start))
	structuredlog.Info("sbs.service", "ec_full_stripe_committed",
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("committed_revision", commitReq.CommittedRevision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		structuredlog.F("stripe_id", commitReq.ECStripe.StripeID),
	)
	return CommitECFullStripeWriteResponseToProto(state, idempotency), nil
}

func ServeCommitECDiscard(ctx context.Context, req *internalv1.CommitECDiscardRequest, service ECMetadataInternalService, lockVolume WriteSessionCommitVolumeLocker, record WriteSessionOutcomeRecorder) (*internalv1.CommitECDiscardResponse, error) {
	start := time.Now()
	commitReq, err := CommitECDiscardRequestFromProto(req)
	if err != nil {
		recordECMetadataOutcome(record, err, time.Since(start), req.GetVolumeId(), req.GetCommittedRevision(), req.GetIdempotencyKey())
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("ec metadata internal service is required")
		recordECMetadataOutcome(record, err, time.Since(start), commitReq.VolumeID, commitReq.CommittedRevision, commitReq.IdempotencyKey)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	unlock := lockWriteSessionCommitVolume(lockVolume, commitReq.VolumeID)
	defer unlock()
	state, idempotency, err := service.CommitECDiscard(ctx, commitReq)
	if err != nil {
		recordECMetadataOutcome(record, err, time.Since(start), commitReq.VolumeID, commitReq.CommittedRevision, commitReq.IdempotencyKey)
		return nil, WriteSessionErrorToGRPCStatus(err)
	}
	recordWriteSessionOutcome(record, writeSessionServiceOutcomeOK, time.Since(start))
	structuredlog.Info("sbs.service", "ec_discard_committed",
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("volume_id", commitReq.VolumeID),
		structuredlog.F("committed_revision", commitReq.CommittedRevision),
		structuredlog.F("idempotency_key", commitReq.IdempotencyKey),
		structuredlog.F("retired_ec_object_count", len(commitReq.RetiredECObjects)),
	)
	return CommitECDiscardResponseToProto(state, idempotency), nil
}

func recordECMetadataOutcome(record WriteSessionOutcomeRecorder, err error, duration time.Duration, volumeID string, committedRevision uint64, idempotencyKey string) {
	class := ClassifyWriteSessionError(err)
	recordWriteSessionOutcome(record, string(class), duration)
	structuredlog.Error("sbs.service", "ec_metadata_commit_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("committed_revision", committedRevision),
		structuredlog.F("idempotency_key", idempotencyKey),
	)
}

var _ ECMetadataInternalService = (*RepositoryBackedECMetadataInternalService)(nil)
var _ ECMetadataAdapter = (*GRPCECMetadataAdapter)(nil)
var _ ECMetadataAdapter = (*ServiceBackedECMetadataAdapter)(nil)
