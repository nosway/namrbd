package metadata

import (
	"context"
	"testing"
)

func TestPersistZeroAllocationPagesWritesExpectedPages(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	spec := ZeroAllocationPersistSpec{
		VolumeID:        "00a1b2c3",
		SizeBytes:       1 << 20,
		ChunkSizeBytes:  64 << 10,
		ExtentPageBytes: 1 << 20,
	}
	if err := PersistZeroAllocationPages(context.Background(), repo, spec); err != nil {
		t.Fatalf("PersistZeroAllocationPages: %v", err)
	}
	pages, err := repo.ListAllocationPages(context.Background(), spec.VolumeID)
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("allocation pages=%d want=1", len(pages))
	}
	if pages[0].Revision != 1 {
		t.Fatalf("page revision=%d want=1", pages[0].Revision)
	}
	if len(pages[0].Extents) != 1 {
		t.Fatalf("page extents=%d want=1", len(pages[0].Extents))
	}
	if pages[0].Extents[0].Kind != AllocationKindZero {
		t.Fatalf("page kind=%q want=%q", pages[0].Extents[0].Kind, AllocationKindZero)
	}
}

func TestPersistZeroAllocationPagesRejectsInvalidGeometry(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	err := PersistZeroAllocationPages(context.Background(), repo, ZeroAllocationPersistSpec{
		VolumeID:        "00a1b2c3",
		SizeBytes:       1 << 20,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 0,
	})
	if err == nil {
		t.Fatal("PersistZeroAllocationPages succeeded with invalid geometry")
	}
}

func TestPersistAllocationPagesAppliesRevisionAndSortsByPageNo(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	pages := []AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 8, ChunkCount: 4, Kind: AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindZero},
			},
		},
	}
	if err := PersistAllocationPages(context.Background(), repo, 7, pages); err != nil {
		t.Fatalf("PersistAllocationPages: %v", err)
	}
	got, err := repo.ListAllocationPages(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("allocation pages=%d want=2", len(got))
	}
	if got[0].PageNo != 0 || got[1].PageNo != 2 {
		t.Fatalf("page order=(%d,%d) want=(0,2)", got[0].PageNo, got[1].PageNo)
	}
	for _, page := range got {
		if page.Revision != 7 {
			t.Fatalf("page revision=%d want=7", page.Revision)
		}
	}
}
