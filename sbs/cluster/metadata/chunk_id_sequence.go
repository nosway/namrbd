package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type ChunkIDSequenceStore interface {
	GetNextChunkID(ctx context.Context, volumeID string) (uint64, error)
	PutNextChunkID(ctx context.Context, volumeID string, nextID uint64) error
}

type ChunkIDAllocator interface {
	AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error)
}

type ChunkIDAllocationRequest struct {
	VolumeID string
	Count    uint32
}

func (r ChunkIDAllocationRequest) Validate() error {
	if _, err := CanonicalVolumeID(r.VolumeID); err != nil {
		return fmt.Errorf("invalid chunk id allocation volume_id %q: %w", r.VolumeID, err)
	}
	return nil
}

type ChunkIDAllocationService struct {
	mu    sync.Mutex
	store ChunkIDSequenceStore
}

func NewChunkIDAllocationService(store ChunkIDSequenceStore) *ChunkIDAllocationService {
	return &ChunkIDAllocationService{store: store}
}

func (s *ChunkIDAllocationService) AllocateChunkIDs(ctx context.Context, req ChunkIDAllocationRequest) (uint64, error) {
	if s.store == nil {
		return 0, fmt.Errorf("chunk id sequence store is required")
	}
	if err := req.Validate(); err != nil {
		return 0, err
	}
	if allocator, ok := s.store.(ChunkIDAllocator); ok {
		return allocator.AllocateChunkIDs(ctx, req.VolumeID, req.Count)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return AllocateChunkIDsFromSequence(ctx, s.store, req.VolumeID, req.Count)
}

func AllocateChunkIDsFromSequence(ctx context.Context, store ChunkIDSequenceStore, volumeID string, count uint32) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	nextID, err := store.GetNextChunkID(ctx, volumeID)
	if errors.Is(err, ErrNotFound) {
		nextID = 1
	} else if err != nil {
		return 0, err
	}
	if nextID == 0 {
		nextID = 1
	}
	startID := nextID
	nextID += uint64(count)
	if err := store.PutNextChunkID(ctx, volumeID, nextID); err != nil {
		return 0, err
	}
	return startID, nil
}
