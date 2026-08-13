package service

import (
	"context"
	"testing"
)

func TestMemoryMetadataCreateUpdateDeleteVolume(t *testing.T) {
	repo := NewInMemoryMetadataRepository(nil)
	ctx := context.Background()

	created, err := repo.CreateVolume(ctx, VolumeCreateRequest{
		Name:      "vol-a",
		SizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if created.Name != "vol-a" {
		t.Fatalf("unexpected volume name: %+v", created)
	}
	if created.Prefix != BuildVolumePrefix("vol-a", uint64(created.ID)) {
		t.Fatalf("unexpected prefix: %s", created.Prefix)
	}
	if created.ChunkSizeBytes != DefaultAllocationChunkSize {
		t.Fatalf("unexpected default chunk size: %d", created.ChunkSizeBytes)
	}
	if created.ExtentPageBytes != DefaultAllocationPageSize {
		t.Fatalf("unexpected default extent page size: %d", created.ExtentPageBytes)
	}
	if CanonicalVolumeID(uint64(created.ID)) == "" {
		t.Fatalf("expected canonical volume id")
	}

	if _, err := repo.CreateVolume(ctx, VolumeCreateRequest{Name: "vol-a", SizeBytes: 1 << 20}); err != ErrVolumeNameConflict {
		t.Fatalf("expected name conflict, got %v", err)
	}

	newName := "vol-b"
	updated, err := repo.UpdateVolume(ctx, uint64(created.ID), VolumeUpdateRequest{Name: &newName})
	if err != nil {
		t.Fatalf("UpdateVolume failed: %v", err)
	}
	if updated.Name != newName || updated.Prefix != BuildVolumePrefix(newName, uint64(created.ID)) {
		t.Fatalf("unexpected updated volume: %+v", updated)
	}

	if err := repo.DeleteVolume(ctx, uint64(created.ID)); err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}
	if _, err := repo.GetVolume(ctx, uint64(created.ID)); err != ErrVolumeNotFound {
		t.Fatalf("expected volume to be deleted, got %v", err)
	}
}

func TestMemoryMetadataRejectsVolumeGeometryUpdate(t *testing.T) {
	repo := NewInMemoryMetadataRepository(nil)
	ctx := context.Background()

	created, err := repo.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-geometry",
		SizeBytes:       1 << 20,
		ChunkSizeBytes:  64 << 10,
		ExtentPageBytes: 256 << 10,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	samePageSize := uint32(256 << 10)
	if _, err := repo.UpdateVolume(ctx, uint64(created.ID), VolumeUpdateRequest{ExtentPageBytes: &samePageSize}); err != nil {
		t.Fatalf("same geometry update failed: %v", err)
	}

	changedBlockSize := uint32(8192)
	if _, err := repo.UpdateVolume(ctx, uint64(created.ID), VolumeUpdateRequest{BlockSize: &changedBlockSize}); err != ErrVolumeGeometryChange {
		t.Fatalf("expected ErrVolumeGeometryChange for block size, got %v", err)
	}

	changedPageSize := uint32(512 << 10)
	if _, err := repo.UpdateVolume(ctx, uint64(created.ID), VolumeUpdateRequest{ExtentPageBytes: &changedPageSize}); err != ErrVolumeGeometryChange {
		t.Fatalf("expected ErrVolumeGeometryChange for page size, got %v", err)
	}

	changedChunkSize := uint32(128 << 10)
	if _, err := repo.UpdateVolume(ctx, uint64(created.ID), VolumeUpdateRequest{ChunkSizeBytes: &changedChunkSize}); err != ErrVolumeGeometryChange {
		t.Fatalf("expected ErrVolumeGeometryChange for chunk size, got %v", err)
	}
}

func TestMemoryMetadataExtentPageCASAndChunkAllocation(t *testing.T) {
	repo := NewInMemoryMetadataRepository(nil)
	ctx := context.Background()

	created, err := repo.CreateVolume(ctx, VolumeCreateRequest{
		Name:      "vol-c6",
		SizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	startChunkID, err := repo.AllocateChunkIDs(ctx, uint64(created.ID), 3)
	if err != nil {
		t.Fatalf("AllocateChunkIDs failed: %v", err)
	}
	if startChunkID != 1 {
		t.Fatalf("unexpected start chunk id: %d", startChunkID)
	}

	page, err := repo.GetExtentPage(ctx, uint64(created.ID), 0)
	if err != nil {
		t.Fatalf("GetExtentPage failed: %v", err)
	}
	if page.Revision != 0 {
		t.Fatalf("expected empty page revision 0, got %d", page.Revision)
	}

	stored, err := repo.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       created.ID,
		PageNo:         0,
		PageBytes:      created.ExtentPageBytes,
		ChunkSizeBytes: created.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  0,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: startChunkID,
		}},
	}, 0)
	if err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}
	if stored.Revision != 1 {
		t.Fatalf("expected revision 1, got %d", stored.Revision)
	}

	if _, err := repo.PutExtentPage(ctx, stored, 0); err != ErrMetadataCASConflict {
		t.Fatalf("expected CAS conflict, got %v", err)
	}

	pages, err := repo.ListExtentPages(ctx, uint64(created.ID))
	if err != nil {
		t.Fatalf("ListExtentPages failed: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("expected 1 extent page, got %d", len(pages))
	}
	if pages[0].Extents[0].PhysicalChunkStart != startChunkID {
		t.Fatalf("unexpected physical chunk start: %+v", pages[0].Extents[0])
	}
}
