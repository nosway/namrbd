package control

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type ECMetadataAdapter interface {
	GetPhysicalObject(ctx context.Context, volumeID, objectID string) (metadata.PhysicalObjectRecord, error)
	PutPhysicalObject(ctx context.Context, rec metadata.PhysicalObjectRecord) error
	GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (metadata.ECStripeRecord, error)
	PutECStripe(ctx context.Context, rec metadata.ECStripeRecord) error
	CommitECFullStripeWrite(ctx context.Context, req metadata.CommitECFullStripeWriteRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitECDiscard(ctx context.Context, req metadata.CommitECDiscardRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type ECMetadataInternalService interface {
	ECMetadataAdapter
}

type ecMetadataRepository interface {
	ECMetadataAdapter
}

type RepositoryBackedECMetadataInternalService struct {
	store ecMetadataRepository
}

func NewRepositoryBackedECMetadataInternalService(store ecMetadataRepository) *RepositoryBackedECMetadataInternalService {
	return &RepositoryBackedECMetadataInternalService{store: store}
}

func (s *RepositoryBackedECMetadataInternalService) GetPhysicalObject(ctx context.Context, volumeID, objectID string) (metadata.PhysicalObjectRecord, error) {
	if s.store == nil {
		return metadata.PhysicalObjectRecord{}, fmt.Errorf("ec metadata store is required")
	}
	return s.store.GetPhysicalObject(ctx, volumeID, objectID)
}

func (s *RepositoryBackedECMetadataInternalService) PutPhysicalObject(ctx context.Context, rec metadata.PhysicalObjectRecord) error {
	if s.store == nil {
		return fmt.Errorf("ec metadata store is required")
	}
	return s.store.PutPhysicalObject(ctx, rec)
}

func (s *RepositoryBackedECMetadataInternalService) GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (metadata.ECStripeRecord, error) {
	if s.store == nil {
		return metadata.ECStripeRecord{}, fmt.Errorf("ec metadata store is required")
	}
	return s.store.GetECStripe(ctx, volumeID, stripeID, stripeGeneration)
}

func (s *RepositoryBackedECMetadataInternalService) PutECStripe(ctx context.Context, rec metadata.ECStripeRecord) error {
	if s.store == nil {
		return fmt.Errorf("ec metadata store is required")
	}
	return s.store.PutECStripe(ctx, rec)
}

func (s *RepositoryBackedECMetadataInternalService) CommitECFullStripeWrite(ctx context.Context, req metadata.CommitECFullStripeWriteRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata store is required")
	}
	return s.store.CommitECFullStripeWrite(ctx, req)
}

func (s *RepositoryBackedECMetadataInternalService) CommitECDiscard(ctx context.Context, req metadata.CommitECDiscardRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata store is required")
	}
	return s.store.CommitECDiscard(ctx, req)
}

type ServiceBackedECMetadataAdapter struct {
	service ECMetadataInternalService
}

func NewServiceBackedECMetadataAdapter(service ECMetadataInternalService) *ServiceBackedECMetadataAdapter {
	return &ServiceBackedECMetadataAdapter{service: service}
}

func (a *ServiceBackedECMetadataAdapter) GetPhysicalObject(ctx context.Context, volumeID, objectID string) (metadata.PhysicalObjectRecord, error) {
	if a.service == nil {
		return metadata.PhysicalObjectRecord{}, fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.GetPhysicalObject(ctx, volumeID, objectID)
}

func (a *ServiceBackedECMetadataAdapter) PutPhysicalObject(ctx context.Context, rec metadata.PhysicalObjectRecord) error {
	if a.service == nil {
		return fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.PutPhysicalObject(ctx, rec)
}

func (a *ServiceBackedECMetadataAdapter) GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (metadata.ECStripeRecord, error) {
	if a.service == nil {
		return metadata.ECStripeRecord{}, fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.GetECStripe(ctx, volumeID, stripeID, stripeGeneration)
}

func (a *ServiceBackedECMetadataAdapter) PutECStripe(ctx context.Context, rec metadata.ECStripeRecord) error {
	if a.service == nil {
		return fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.PutECStripe(ctx, rec)
}

func (a *ServiceBackedECMetadataAdapter) CommitECFullStripeWrite(ctx context.Context, req metadata.CommitECFullStripeWriteRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.CommitECFullStripeWrite(ctx, req)
}

func (a *ServiceBackedECMetadataAdapter) CommitECDiscard(ctx context.Context, req metadata.CommitECDiscardRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("ec metadata internal service is required")
	}
	return a.service.CommitECDiscard(ctx, req)
}
