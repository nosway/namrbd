package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakePlacementApplyInternalService struct {
	called bool
	req    metadata.PlacementApplyRequest
	err    error
}

func (s *fakePlacementApplyInternalService) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	s.called = true
	s.req = req
	return s.err
}

type blockingPlacementApplyAdapter struct{}

func (blockingPlacementApplyAdapter) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRepositoryBackedPlacementApplyAdapterAppliesPlacementChanges(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      3,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	adapter := NewRepositoryBackedPlacementApplyAdapter(repo)
	err := adapter.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 11,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []metadata.AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 4, Kind: metadata.AllocationKindZero},
				},
			},
		},
		NormalizeExtentIDs: []uint64{1},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}

	pages, err := repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 || pages[0].Revision != 11 {
		t.Fatalf("unexpected pages: %+v", pages)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 11 {
		t.Fatalf("unexpected mapping: %+v", mapping)
	}
}

func TestRepositoryBackedPlacementApplyInternalServiceAppliesPlacementChanges(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4096,
		LengthBytes:   4096,
		ChunkID:       202,
		PlacementRef:  "pl-1",
		Revision:      3,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	service := NewRepositoryBackedPlacementApplyInternalService(repo)
	err := service.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 12,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				PageNo:         1,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []metadata.AllocationExtentRecord{
					{LogicalChunkStart: 4, ChunkCount: 4, Kind: metadata.AllocationKindZero},
				},
			},
		},
		NormalizeExtentIDs: []uint64{2},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}

	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 2)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 12 {
		t.Fatalf("unexpected mapping: %+v", mapping)
	}
}

func TestServiceBackedPlacementApplyAdapterDelegatesToInternalService(t *testing.T) {
	ctx := context.Background()
	service := &fakePlacementApplyInternalService{}
	adapter := NewServiceBackedPlacementApplyAdapter(service)

	err := adapter.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 13,
		AllocationPages: []metadata.AllocationPageRecord{
			{PageNo: 3, PageBytes: 4096, ChunkSizeBytes: 1024},
		},
		NormalizeExtentIDs: []uint64{7, 8},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}
	if !service.called {
		t.Fatalf("expected internal service to be called")
	}
	if service.req.VolumeID != "00a1b2c3" || service.req.CommittedRevision != 13 {
		t.Fatalf("unexpected request: %+v", service.req)
	}
	if len(service.req.AllocationPages) != 1 || service.req.AllocationPages[0].PageNo != 3 {
		t.Fatalf("unexpected allocation pages: %+v", service.req.AllocationPages)
	}
	if len(service.req.NormalizeExtentIDs) != 2 || service.req.NormalizeExtentIDs[0] != 7 || service.req.NormalizeExtentIDs[1] != 8 {
		t.Fatalf("unexpected normalize extent IDs: %+v", service.req.NormalizeExtentIDs)
	}
}

func TestServiceBackedPlacementApplyAdapterPropagatesInternalServiceError(t *testing.T) {
	expected := errors.New("apply failed")
	adapter := NewServiceBackedPlacementApplyAdapter(&fakePlacementApplyInternalService{err: expected})

	err := adapter.ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected %v, got %v", expected, err)
	}
}

func TestServiceBackedPlacementApplyAdapterRequiresInternalService(t *testing.T) {
	adapter := NewServiceBackedPlacementApplyAdapter(nil)

	err := adapter.ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{VolumeID: "00a1b2c3"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestServiceBackedPlacementApplyAdapterValidatesRequestBeforeCallingInternalService(t *testing.T) {
	service := &fakePlacementApplyInternalService{}
	adapter := NewServiceBackedPlacementApplyAdapter(service)

	err := adapter.ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{
		VolumeID: "00a1b2c3",
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if service.called {
		t.Fatalf("internal service should not be called")
	}
}

func TestTimeoutPlacementApplyAdapterAppliesDeadline(t *testing.T) {
	adapter := NewTimeoutPlacementApplyAdapter(blockingPlacementApplyAdapter{}, time.Millisecond)

	err := adapter.ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v want context deadline exceeded", err)
	}
}

func TestTimeoutPlacementApplyAdapterRequiresNextAdapter(t *testing.T) {
	adapter := NewTimeoutPlacementApplyAdapter(nil, time.Millisecond)

	err := adapter.ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestTimeoutPlacementApplyAdapterNonPositiveTimeoutPassesThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := NewTimeoutPlacementApplyAdapter(blockingPlacementApplyAdapter{}, -time.Second)
	done := make(chan error, 1)
	go func() {
		done <- adapter.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 1,
		})
	}()

	select {
	case err := <-done:
		t.Fatalf("adapter returned before caller cancellation: %v", err)
	case <-time.After(5 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context canceled", err)
	}
}
