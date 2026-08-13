package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type cachedBeginWriteVolumeStateIntentStore struct {
	next metadataIntentStore
	ttl  time.Duration
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]cachedBeginWriteVolumeState
}

type cachedBeginWriteVolumeState struct {
	state     metadata.VolumeState
	expiresAt time.Time
}

type beginWriteIntentStore interface {
	PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error
}

func newCachedBeginWriteVolumeStateIntentStore(next metadataIntentStore, ttl time.Duration) *cachedBeginWriteVolumeStateIntentStore {
	return &cachedBeginWriteVolumeStateIntentStore{
		next:    next,
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[string]cachedBeginWriteVolumeState),
	}
}

func (s *cachedBeginWriteVolumeStateIntentStore) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	if s == nil || s.next == nil {
		return metadata.VolumeState{}, metadata.ErrNotFound
	}
	if s.ttl <= 0 {
		return s.next.GetVolumeState(ctx, volumeID)
	}
	now := s.now()
	s.mu.Lock()
	entry, ok := s.entries[volumeID]
	if ok && now.Before(entry.expiresAt) {
		state := entry.state
		s.mu.Unlock()
		return state, nil
	}
	s.mu.Unlock()

	state, err := s.next.GetVolumeState(ctx, volumeID)
	if err != nil {
		return metadata.VolumeState{}, err
	}
	s.mu.Lock()
	s.entries[volumeID] = cachedBeginWriteVolumeState{state: state, expiresAt: now.Add(s.ttl)}
	s.mu.Unlock()
	return state, nil
}

func (s *cachedBeginWriteVolumeStateIntentStore) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	return s.next.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

func (s *cachedBeginWriteVolumeStateIntentStore) PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error {
	return s.next.PutIdempotencyRecord(ctx, rec)
}

func (s *cachedBeginWriteVolumeStateIntentStore) GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error) {
	return s.next.GetMutationOperation(ctx, volumeID, operationID)
}

func (s *cachedBeginWriteVolumeStateIntentStore) PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error {
	return s.next.PutMutationOperation(ctx, rec)
}

func (s *cachedBeginWriteVolumeStateIntentStore) PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	if writer, ok := s.next.(beginWriteIntentStore); ok {
		return writer.PutWriteIntent(ctx, record, operation)
	}
	if err := s.next.PutIdempotencyRecord(ctx, record); err != nil {
		return err
	}
	return s.next.PutMutationOperation(ctx, operation)
}
