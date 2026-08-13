package control

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

// WriteSessionAdapter is the draft caller-side boundary for gateway write
// session/control records that are still repository-backed in Phase I.
type WriteSessionAdapter interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	PutVolumeState(ctx context.Context, state metadata.VolumeState) error
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
	PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error
	CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

// WriteSessionInternalService is the minimal service-owned authority shape that
// can later move gateway write session/control records behind transport.
type WriteSessionInternalService interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	PutVolumeState(ctx context.Context, state metadata.VolumeState) error
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
	PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error
	CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type WriteSessionCloneDeltaInternalService interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error
}

type writeSessionRepository interface {
	WriteSessionAdapter
	CommitAppendOnlyWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error
}

type writeSessionInlineAppendOnlyEffectsRepository interface {
	CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type writeSessionIntentBatchRepository interface {
	PutWriteIntentBatch(ctx context.Context, intents []metadata.WriteIntentRecord) error
}

type RepositoryBackedWriteSessionInternalService struct {
	store                   writeSessionRepository
	inlineAppendOnlyEffects bool
}

func NewRepositoryBackedWriteSessionInternalService(store writeSessionRepository) *RepositoryBackedWriteSessionInternalService {
	return &RepositoryBackedWriteSessionInternalService{store: store}
}

func NewRepositoryBackedWriteSessionInternalServiceWithInlineEffects(store writeSessionRepository) *RepositoryBackedWriteSessionInternalService {
	return &RepositoryBackedWriteSessionInternalService{store: store, inlineAppendOnlyEffects: true}
}

func (s *RepositoryBackedWriteSessionInternalService) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	if s.store == nil {
		return metadata.VolumeState{}, fmt.Errorf("write session store is required")
	}
	return s.store.GetVolumeState(ctx, volumeID)
}

func (s *RepositoryBackedWriteSessionInternalService) PutVolumeState(ctx context.Context, state metadata.VolumeState) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	return s.store.PutVolumeState(ctx, state)
}

func (s *RepositoryBackedWriteSessionInternalService) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.IdempotencyRecord{}, fmt.Errorf("write session store is required")
	}
	return s.store.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

func (s *RepositoryBackedWriteSessionInternalService) PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	return s.store.PutIdempotencyRecord(ctx, rec)
}

func (s *RepositoryBackedWriteSessionInternalService) GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error) {
	if s.store == nil {
		return metadata.MutationOperationRecord{}, fmt.Errorf("write session store is required")
	}
	return s.store.GetMutationOperation(ctx, volumeID, operationID)
}

func (s *RepositoryBackedWriteSessionInternalService) PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	return s.store.PutMutationOperation(ctx, rec)
}

func (s *RepositoryBackedWriteSessionInternalService) PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	return s.store.PutWriteIntent(ctx, record, operation)
}

func (s *RepositoryBackedWriteSessionInternalService) PutWriteIntentBatch(ctx context.Context, intents []metadata.WriteIntentRecord) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	if batcher, ok := s.store.(writeSessionIntentBatchRepository); ok {
		return batcher.PutWriteIntentBatch(ctx, intents)
	}
	for _, intent := range intents {
		if err := s.store.PutWriteIntent(ctx, intent.IdempotencyRecord, intent.MutationOperation); err != nil {
			return err
		}
	}
	return nil
}

func (s *RepositoryBackedWriteSessionInternalService) CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session store is required")
	}
	return s.store.CommitWriteState(ctx, req)
}

func (s *RepositoryBackedWriteSessionInternalService) CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session store is required")
	}
	return s.store.CommitPageScopedWriteMetadata(ctx, req)
}

func (s *RepositoryBackedWriteSessionInternalService) CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session store is required")
	}
	return s.store.CommitRangeLocalWriteState(ctx, req)
}

func (s *RepositoryBackedWriteSessionInternalService) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.store == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session store is required")
	}
	if s.inlineAppendOnlyEffects {
		committer, ok := s.store.(writeSessionInlineAppendOnlyEffectsRepository)
		if !ok {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session store does not support inline append-only write effects")
		}
		return committer.CommitAppendOnlyWriteStateAndQueueEffects(ctx, req)
	}
	return s.store.CommitAppendOnlyWriteState(ctx, req.StateCommitRequest())
}

func (s *RepositoryBackedWriteSessionInternalService) CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	if s.store == nil {
		return fmt.Errorf("write session store is required")
	}
	return s.store.CommitCloneDeltaAllocationPages(ctx, cloneID, pages)
}

type ServiceBackedWriteSessionAdapter struct {
	service WriteSessionInternalService
}

func NewServiceBackedWriteSessionAdapter(service WriteSessionInternalService) *ServiceBackedWriteSessionAdapter {
	return &ServiceBackedWriteSessionAdapter{service: service}
}

func (a *ServiceBackedWriteSessionAdapter) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	if a.service == nil {
		return metadata.VolumeState{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.GetVolumeState(ctx, volumeID)
}

func (a *ServiceBackedWriteSessionAdapter) PutVolumeState(ctx context.Context, state metadata.VolumeState) error {
	if a.service == nil {
		return fmt.Errorf("write session internal service is required")
	}
	return a.service.PutVolumeState(ctx, state)
}

func (a *ServiceBackedWriteSessionAdapter) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.IdempotencyRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

func (a *ServiceBackedWriteSessionAdapter) PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error {
	if a.service == nil {
		return fmt.Errorf("write session internal service is required")
	}
	return a.service.PutIdempotencyRecord(ctx, rec)
}

func (a *ServiceBackedWriteSessionAdapter) GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error) {
	if a.service == nil {
		return metadata.MutationOperationRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.GetMutationOperation(ctx, volumeID, operationID)
}

func (a *ServiceBackedWriteSessionAdapter) PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error {
	if a.service == nil {
		return fmt.Errorf("write session internal service is required")
	}
	return a.service.PutMutationOperation(ctx, rec)
}

func (a *ServiceBackedWriteSessionAdapter) PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	if a.service == nil {
		return fmt.Errorf("write session internal service is required")
	}
	return a.service.PutWriteIntent(ctx, record, operation)
}

func (a *ServiceBackedWriteSessionAdapter) CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.CommitWriteState(ctx, req)
}

func (a *ServiceBackedWriteSessionAdapter) CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.CommitPageScopedWriteMetadata(ctx, req)
}

func (a *ServiceBackedWriteSessionAdapter) CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.CommitRangeLocalWriteState(ctx, req)
}

func (a *ServiceBackedWriteSessionAdapter) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if a.service == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("write session internal service is required")
	}
	return a.service.CommitAppendOnlyWriteStateAndQueueEffects(ctx, req)
}

func (a *ServiceBackedWriteSessionAdapter) CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	if a.service == nil {
		return fmt.Errorf("write session internal service is required")
	}
	cloneService, ok := a.service.(WriteSessionCloneDeltaInternalService)
	if !ok || cloneService == nil {
		return fmt.Errorf("write session internal service does not support clone delta commits")
	}
	return cloneService.CommitCloneDeltaAllocationPages(ctx, cloneID, pages)
}
