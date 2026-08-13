package metadata

import (
	"context"
	"testing"
)

func TestNormalizeExtentMappingsClearsChunkIDAndAppliesRevision(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
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
	if err := NormalizeExtentMappings(ctx, repo, "00a1b2c3", []uint64{1}, 9); err != nil {
		t.Fatalf("NormalizeExtentMappings: %v", err)
	}
	got, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if got.ChunkID != 0 {
		t.Fatalf("chunk_id=%d want=0", got.ChunkID)
	}
	if got.Revision != 9 {
		t.Fatalf("revision=%d want=9", got.Revision)
	}
}

func TestNormalizeExtentMappingsDoesNotMoveRevisionBackward(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := NormalizeExtentMappings(ctx, repo, "00a1b2c3", []uint64{1}, 9); err != nil {
		t.Fatalf("NormalizeExtentMappings: %v", err)
	}
	got, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if got.ChunkID != 0 {
		t.Fatalf("chunk_id=%d want=0", got.ChunkID)
	}
	if got.Revision != 11 {
		t.Fatalf("revision=%d want=11", got.Revision)
	}
}

func TestNormalizeExtentMappingsReturnsNotFoundForMissingExtent(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	err := NormalizeExtentMappings(context.Background(), repo, "00a1b2c3", []uint64{42}, 9)
	if err != ErrNotFound {
		t.Fatalf("err=%v want=%v", err, ErrNotFound)
	}
}
