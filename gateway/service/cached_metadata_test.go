package service

import (
	"context"
	"testing"
	"time"
)

func TestCachedMetadataRepositoryCachesExtentPages(t *testing.T) {
	next := NewInMemoryMetadataRepository(nil)
	cached := NewCachedMetadataRepository(next, time.Minute)
	ctx := context.Background()

	created, err := next.CreateVolume(ctx, VolumeCreateRequest{
		Name:      "vol-cache",
		SizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	stored, err := next.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       created.ID,
		PageNo:         0,
		PageBytes:      created.ExtentPageBytes,
		ChunkSizeBytes: created.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  0,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: 11,
		}},
	}, 0)
	if err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}

	page, err := cached.GetExtentPage(ctx, uint64(created.ID), 0)
	if err != nil {
		t.Fatalf("cached GetExtentPage failed: %v", err)
	}
	if page.Revision != stored.Revision {
		t.Fatalf("unexpected cached revision: got=%d want=%d", page.Revision, stored.Revision)
	}

	updated, err := next.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       created.ID,
		PageNo:         0,
		PageBytes:      created.ExtentPageBytes,
		ChunkSizeBytes: created.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  0,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: 22,
		}},
	}, stored.Revision)
	if err != nil {
		t.Fatalf("direct PutExtentPage failed: %v", err)
	}

	page, err = cached.GetExtentPage(ctx, uint64(created.ID), 0)
	if err != nil {
		t.Fatalf("cached GetExtentPage after direct update failed: %v", err)
	}
	if page.Extents[0].PhysicalChunkStart != 11 {
		t.Fatalf("expected cached extent page before invalidation, got %+v", page.Extents[0])
	}

	if _, err := cached.Attach(ctx, AttachRequest{VolumeID: uint64(created.ID), HostID: "host-a", DeviceID: 1}); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	page, err = cached.GetExtentPage(ctx, uint64(created.ID), 0)
	if err != nil {
		t.Fatalf("cached GetExtentPage after invalidation failed: %v", err)
	}
	if page.Revision != updated.Revision {
		t.Fatalf("expected refreshed revision after invalidation: got=%d want=%d", page.Revision, updated.Revision)
	}
	if page.Extents[0].PhysicalChunkStart != 22 {
		t.Fatalf("expected refreshed extent page after invalidation, got %+v", page.Extents[0])
	}
}
