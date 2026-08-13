package control

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakeChunkIDAllocatorInternalService struct {
	req   metadata.ChunkIDAllocationRequest
	calls int
	start uint64
	err   error
}

func (s *fakeChunkIDAllocatorInternalService) AllocateChunkIDs(_ context.Context, req metadata.ChunkIDAllocationRequest) (uint64, error) {
	s.req = req
	s.calls++
	return s.start, s.err
}

func TestRepositoryBackedChunkIDAllocatorAdapterAllocatesChunkIDs(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	adapter := NewRepositoryBackedChunkIDAllocatorAdapter(repo)
	startID, err := adapter.AllocateChunkIDs(ctx, "00a1b2c3", 4)
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	if startID != 1 {
		t.Fatalf("start_id=%d want=1", startID)
	}
	nextID, err := repo.GetNextChunkID(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetNextChunkID: %v", err)
	}
	if nextID != 5 {
		t.Fatalf("next_id=%d want=5", nextID)
	}
}

func TestServiceBackedChunkIDAllocatorAdapterDelegatesToInternalService(t *testing.T) {
	service := &fakeChunkIDAllocatorInternalService{start: 7}
	adapter := NewServiceBackedChunkIDAllocatorAdapter(service)
	startID, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	if startID != 7 {
		t.Fatalf("start_id=%d want=7", startID)
	}
	if service.calls != 1 {
		t.Fatalf("calls=%d want=1", service.calls)
	}
	if service.req.VolumeID != "00a1b2c3" || service.req.Count != 2 {
		t.Fatalf("unexpected request: %+v", service.req)
	}
}

func TestServiceBackedChunkIDAllocatorAdapterPropagatesInternalServiceError(t *testing.T) {
	expected := errors.New("allocator unavailable")
	adapter := NewServiceBackedChunkIDAllocatorAdapter(&fakeChunkIDAllocatorInternalService{err: expected})
	_, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 1)
	if !errors.Is(err, expected) {
		t.Fatalf("AllocateChunkIDs error=%v want %v", err, expected)
	}
}

func TestServiceBackedChunkIDAllocatorAdapterRequiresInternalService(t *testing.T) {
	adapter := NewServiceBackedChunkIDAllocatorAdapter(nil)
	_, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 1)
	if err == nil || !strings.Contains(err.Error(), "chunk id allocator internal service is required") {
		t.Fatalf("AllocateChunkIDs error=%v want required service", err)
	}
}

func TestServiceBackedChunkIDAllocatorAdapterValidatesRequestBeforeCallingInternalService(t *testing.T) {
	service := &fakeChunkIDAllocatorInternalService{}
	adapter := NewServiceBackedChunkIDAllocatorAdapter(service)
	_, err := adapter.AllocateChunkIDs(context.Background(), "not-a-volume", 1)
	if err == nil || !strings.Contains(err.Error(), "invalid chunk id allocation volume_id") {
		t.Fatalf("AllocateChunkIDs error=%v want invalid volume_id", err)
	}
	if service.calls != 0 {
		t.Fatalf("calls=%d want=0", service.calls)
	}
}
