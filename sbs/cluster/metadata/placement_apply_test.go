package metadata

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestApplyPlacementChangesPersistsPagesAndNormalizesExtents(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
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
	err := ApplyPlacementChanges(ctx, repo, PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 9,
		AllocationPages: []AllocationPageRecord{
			{
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindZero},
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
	if len(pages) != 1 || pages[0].Revision != 9 {
		t.Fatalf("unexpected pages: %+v", pages)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 9 {
		t.Fatalf("unexpected mapping after apply: %+v", mapping)
	}
}

func TestPlacementApplyServiceAppliesPlacementChanges(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4096,
		LengthBytes:   4096,
		ChunkID:       102,
		PlacementRef:  "pl-1",
		Revision:      3,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	svc := NewPlacementApplyService(repo)
	err := svc.ApplyPlacementChanges(ctx, PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 10,
		AllocationPages: []AllocationPageRecord{
			{
				PageNo:         1,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 4, ChunkCount: 4, Kind: AllocationKindZero},
				},
			},
		},
		NormalizeExtentIDs: []uint64{2},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges(service): %v", err)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 2)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 10 {
		t.Fatalf("unexpected mapping after service apply: %+v", mapping)
	}
}

func TestApplyPlacementChangesMergesStalePageByTouchedExtent(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       5,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 1001},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4,
		LengthBytes:   4,
		ChunkID:       2001,
		PlacementRef:  "pl-1",
		Revision:      5,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	err := ApplyPlacementChanges(ctx, repo, PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 6,
		AllocationPages: []AllocationPageRecord{
			{
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 2001},
				},
			},
		},
		NormalizeExtentIDs: []uint64{2},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if len(chunks) != 2 || chunks[0] != 1001 || chunks[1] != 2001 {
		t.Fatalf("allocation chunks=%v want [1001 2001], page=%+v", chunks, page)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 2)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 6 {
		t.Fatalf("unexpected mapping after apply: %+v", mapping)
	}
}

func TestPlacementApplyRequestValidateRejectsInvalidVolumeID(t *testing.T) {
	err := (PlacementApplyRequest{
		VolumeID:          "not-a-volume",
		CommittedRevision: 1,
	}).Validate()
	if err == nil || !strings.Contains(err.Error(), "invalid placement apply volume_id") {
		t.Fatalf("Validate error=%v", err)
	}
	if !errors.Is(err, ErrInvalidPlacementApplyRequest) {
		t.Fatalf("Validate error=%v want ErrInvalidPlacementApplyRequest", err)
	}
}

func TestPlacementApplyServiceRequiresStore(t *testing.T) {
	err := NewPlacementApplyService(nil).ApplyPlacementChanges(context.Background(), PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "placement apply store is required") {
		t.Fatalf("ApplyPlacementChanges error=%v", err)
	}
}

func TestPlacementApplyRequestValidateRejectsMissingCommittedRevision(t *testing.T) {
	err := (PlacementApplyRequest{VolumeID: "00a1b2c3"}).Validate()
	if err == nil || !strings.Contains(err.Error(), "committed_revision") {
		t.Fatalf("Validate error=%v", err)
	}
}

func TestPlacementApplyRequestValidateRejectsInvalidAllocationPageGeometry(t *testing.T) {
	err := (PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
		AllocationPages: []AllocationPageRecord{
			{PageNo: 7, PageBytes: 4096, ChunkSizeBytes: 3000},
		},
	}).Validate()
	if err == nil || !strings.Contains(err.Error(), "allocation page geometry") {
		t.Fatalf("Validate error=%v", err)
	}
}

func TestPlacementApplyRequestValidateRejectsInvalidAllocationExtent(t *testing.T) {
	err := (PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
		AllocationPages: []AllocationPageRecord{
			{
				PageNo:         7,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 28, ChunkCount: 0, Kind: AllocationKindZero},
				},
			},
		},
	}).Validate()
	if err == nil || !strings.Contains(err.Error(), "chunk_count=0") {
		t.Fatalf("Validate error=%v", err)
	}
}

func TestPlacementApplyRequestValidateRejectsInvalidAllocationExtentKind(t *testing.T) {
	err := (PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 1,
		AllocationPages: []AllocationPageRecord{
			{
				PageNo:         7,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 28, ChunkCount: 1, Kind: AllocationKind("mystery")},
				},
			},
		},
	}).Validate()
	if err == nil || !strings.Contains(err.Error(), "allocation extent kind") {
		t.Fatalf("Validate error=%v", err)
	}
}
