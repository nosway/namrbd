package control

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

type GRPCECMetadataAdapter struct {
	client internalv1.ECMetadataServiceClient
}

func NewGRPCECMetadataAdapter(client internalv1.ECMetadataServiceClient) *GRPCECMetadataAdapter {
	return &GRPCECMetadataAdapter{client: client}
}

func (a *GRPCECMetadataAdapter) GetPhysicalObject(ctx context.Context, volumeID, objectID string) (metadata.PhysicalObjectRecord, error) {
	if a.client == nil {
		return metadata.PhysicalObjectRecord{}, fmt.Errorf("ec metadata gRPC client is required")
	}
	resp, err := a.client.GetPhysicalObject(ctx, &internalv1.GetPhysicalObjectRequest{
		VolumeId: volumeID,
		ObjectId: objectID,
	})
	if err != nil {
		return metadata.PhysicalObjectRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return PhysicalObjectRecordFromProto(resp.GetPhysicalObject())
}

func (a *GRPCECMetadataAdapter) PutPhysicalObject(ctx context.Context, rec metadata.PhysicalObjectRecord) error {
	if a.client == nil {
		return fmt.Errorf("ec metadata gRPC client is required")
	}
	_, err := a.client.PutPhysicalObject(ctx, &internalv1.PutPhysicalObjectRequest{
		PhysicalObject: PhysicalObjectRecordToProto(rec),
	})
	return WriteSessionTransportErrorToMetadataError(err)
}

func (a *GRPCECMetadataAdapter) GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (metadata.ECStripeRecord, error) {
	if a.client == nil {
		return metadata.ECStripeRecord{}, fmt.Errorf("ec metadata gRPC client is required")
	}
	resp, err := a.client.GetECStripe(ctx, &internalv1.GetECStripeRequest{
		VolumeId:         volumeID,
		StripeId:         stripeID,
		StripeGeneration: stripeGeneration,
	})
	if err != nil {
		return metadata.ECStripeRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return ECStripeRecordFromProto(resp.GetEcStripe())
}

func (a *GRPCECMetadataAdapter) PutECStripe(ctx context.Context, rec metadata.ECStripeRecord) error {
	if a.client == nil {
		return fmt.Errorf("ec metadata gRPC client is required")
	}
	_, err := a.client.PutECStripe(ctx, &internalv1.PutECStripeRequest{
		EcStripe: ECStripeRecordToProto(rec),
	})
	return WriteSessionTransportErrorToMetadataError(err)
}

func (a *GRPCECMetadataAdapter) CommitECFullStripeWrite(ctx context.Context, req metadata.CommitECFullStripeWriteRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.client == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata gRPC client is required")
	}
	resp, err := a.client.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequestToProto(req))
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return CommitECFullStripeWriteResponseFromProto(resp)
}

func (a *GRPCECMetadataAdapter) CommitECDiscard(ctx context.Context, req metadata.CommitECDiscardRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.client == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata gRPC client is required")
	}
	resp, err := a.client.CommitECDiscard(ctx, CommitECDiscardRequestToProto(req))
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, WriteSessionTransportErrorToMetadataError(err)
	}
	return CommitECDiscardResponseFromProto(resp)
}
