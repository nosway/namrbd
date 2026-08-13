package control

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

// ChunkIDAllocatorAdapter is the caller-side boundary for a future
// sbs-service-owned chunk ID allocator authority.
type ChunkIDAllocatorAdapter interface {
	AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error)
}

type PhysicalChunkIDAllocatorAdapter = ChunkIDAllocatorAdapter

// ChunkIDAllocatorInternalService is the minimal service shape behind a
// transport-backed allocator adapter.
type ChunkIDAllocatorInternalService interface {
	AllocateChunkIDs(ctx context.Context, req metadata.ChunkIDAllocationRequest) (uint64, error)
}

type PhysicalChunkIDAllocatorInternalService = ChunkIDAllocatorInternalService

type RepositoryBackedChunkIDAllocatorInternalService struct {
	service *metadata.ChunkIDAllocationService
}

func NewRepositoryBackedChunkIDAllocatorInternalService(store metadata.ChunkIDSequenceStore) *RepositoryBackedChunkIDAllocatorInternalService {
	return &RepositoryBackedChunkIDAllocatorInternalService{
		service: metadata.NewChunkIDAllocationService(store),
	}
}

func (s *RepositoryBackedChunkIDAllocatorInternalService) AllocateChunkIDs(ctx context.Context, req metadata.ChunkIDAllocationRequest) (uint64, error) {
	return s.service.AllocateChunkIDs(ctx, req)
}

type RepositoryBackedChunkIDAllocatorAdapter struct {
	service ChunkIDAllocatorInternalService
}

func NewRepositoryBackedChunkIDAllocatorAdapter(store metadata.ChunkIDSequenceStore) *RepositoryBackedChunkIDAllocatorAdapter {
	return &RepositoryBackedChunkIDAllocatorAdapter{
		service: NewRepositoryBackedChunkIDAllocatorInternalService(store),
	}
}

func (a *RepositoryBackedChunkIDAllocatorAdapter) AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error) {
	return a.service.AllocateChunkIDs(ctx, metadata.ChunkIDAllocationRequest{
		VolumeID: volumeID,
		Count:    count,
	})
}

type ServiceBackedChunkIDAllocatorAdapter struct {
	service ChunkIDAllocatorInternalService
}

func NewServiceBackedChunkIDAllocatorAdapter(service ChunkIDAllocatorInternalService) *ServiceBackedChunkIDAllocatorAdapter {
	return &ServiceBackedChunkIDAllocatorAdapter{service: service}
}

func (a *ServiceBackedChunkIDAllocatorAdapter) AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error) {
	if a.service == nil {
		return 0, fmt.Errorf("chunk id allocator internal service is required")
	}
	req := metadata.ChunkIDAllocationRequest{
		VolumeID: volumeID,
		Count:    count,
	}
	if err := req.Validate(); err != nil {
		return 0, err
	}
	return a.service.AllocateChunkIDs(ctx, req)
}
