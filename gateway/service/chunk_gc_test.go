package service

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
)

func TestChunkGarbageCollectorDeletesUnreferencedChunks(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{{
		ID:              101,
		Name:            "devA",
		Prefix:          "devA",
		SizeBytes:       4 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  DefaultAllocationChunkSize,
		ExtentPageBytes: DefaultAllocationPageSize,
	}})
	objects := store.NewMemoryStore()
	ctx := context.Background()

	if err := objects.Put(ctx, store.BuildChunkKey("devA", 1), make([]byte, DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("put chunk 1 failed: %v", err)
	}
	if err := objects.Put(ctx, buildPhysicalChunkKey("devA", PhysicalChunkRef{StoreID: "bulk", ShardID: 1, ChunkID: 2}), make([]byte, DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("put chunk 2 failed: %v", err)
	}
	if _, err := meta.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       101,
		PageNo:         0,
		PageBytes:      DefaultAllocationPageSize,
		ChunkSizeBytes: DefaultAllocationChunkSize,
		Extents: []AllocationChunkRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationChunkKindData, PhysicalChunkStart: 1},
			{LogicalChunkStart: 1, ChunkCount: 63, Kind: AllocationChunkKindZero},
		},
	}, 0); err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}
	if err := meta.PutChunkGarbage(ctx, AllocationChunkGarbageRecord{VolumeID: 101, ChunkID: 1}); err != nil {
		t.Fatalf("PutChunkGarbage chunk 1 failed: %v", err)
	}
	if err := meta.PutChunkGarbage(ctx, AllocationChunkGarbageRecord{VolumeID: 101, StoreID: "bulk", ShardID: 1, ChunkID: 2}); err != nil {
		t.Fatalf("PutChunkGarbage chunk 2 failed: %v", err)
	}

	collector := NewChunkGarbageCollector(meta, objects)
	result, err := collector.SweepVolume(ctx, 101, 16)
	if err != nil {
		t.Fatalf("SweepVolume failed: %v", err)
	}
	if result.DeletedCount != 1 || result.RetainedCount != 1 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}

	if _, found, err := objects.Get(ctx, store.BuildChunkKey("devA", 1)); err != nil || !found {
		t.Fatalf("expected referenced chunk to remain: found=%t err=%v", found, err)
	}
	if _, found, err := objects.Get(ctx, buildPhysicalChunkKey("devA", PhysicalChunkRef{StoreID: "bulk", ShardID: 1, ChunkID: 2})); err != nil || found {
		t.Fatalf("expected unreferenced chunk to be deleted: found=%t err=%v", found, err)
	}
}

func TestChunkGarbageCollectorRetainsExternallyProtectedChunk(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{{
		ID:              101,
		Name:            "devA",
		Prefix:          "devA",
		SizeBytes:       4 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  DefaultAllocationChunkSize,
		ExtentPageBytes: DefaultAllocationPageSize,
	}})
	objects := store.NewMemoryStore()
	ctx := context.Background()

	protected := PhysicalChunkRef{StoreID: "bulk", ShardID: 1, ChunkID: 2}
	if err := objects.Put(ctx, buildPhysicalChunkKey("devA", protected), make([]byte, DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("put protected chunk failed: %v", err)
	}
	if err := meta.PutChunkGarbage(ctx, AllocationChunkGarbageRecord{
		VolumeID: 101,
		StoreID:  protected.StoreID,
		ShardID:  protected.ShardID,
		ChunkID:  protected.ChunkID,
	}); err != nil {
		t.Fatalf("PutChunkGarbage protected chunk failed: %v", err)
	}

	collector := NewChunkGarbageCollector(meta, objects)
	result, err := collector.SweepVolumeWithProtectedRefs(ctx, 101, 16, []PhysicalChunkRef{protected})
	if err != nil {
		t.Fatalf("SweepVolumeWithProtectedRefs failed: %v", err)
	}
	if result.DeletedCount != 0 || result.RetainedCount != 1 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	if _, found, err := objects.Get(ctx, buildPhysicalChunkKey("devA", protected)); err != nil || !found {
		t.Fatalf("expected externally protected chunk to remain: found=%t err=%v", found, err)
	}
}

func TestChunkGarbageCollectorRetainsChunkIDWildcardProtectedChunk(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{{
		ID:              101,
		Name:            "devA",
		Prefix:          "devA",
		SizeBytes:       4 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  DefaultAllocationChunkSize,
		ExtentPageBytes: DefaultAllocationPageSize,
	}})
	objects := store.NewMemoryStore()
	ctx := context.Background()

	ref := PhysicalChunkRef{StoreID: "bulk", ShardID: 1, ChunkID: 2}
	if err := objects.Put(ctx, buildPhysicalChunkKey("devA", ref), make([]byte, DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("put protected chunk failed: %v", err)
	}
	if err := meta.PutChunkGarbage(ctx, AllocationChunkGarbageRecord{
		VolumeID: 101,
		StoreID:  ref.StoreID,
		ShardID:  ref.ShardID,
		ChunkID:  ref.ChunkID,
	}); err != nil {
		t.Fatalf("PutChunkGarbage protected chunk failed: %v", err)
	}

	collector := NewChunkGarbageCollector(meta, objects)
	result, err := collector.SweepVolumeWithProtectedRefs(ctx, 101, 16, []PhysicalChunkRef{{ChunkID: ref.ChunkID}})
	if err != nil {
		t.Fatalf("SweepVolumeWithProtectedRefs failed: %v", err)
	}
	if result.DeletedCount != 0 || result.RetainedCount != 1 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	if _, found, err := objects.Get(ctx, buildPhysicalChunkKey("devA", ref)); err != nil || !found {
		t.Fatalf("expected chunk-id wildcard protected chunk to remain: found=%t err=%v", found, err)
	}
}
