package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
)

func TestChunkExtentAlignedDiscardRecordsTrueReclaimAndRetiresChunk(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{
		ID:        101,
		Prefix:    "devA",
		SizeBytes: uint64(DefaultAllocationChunkSize * 2),
	}})

	payload := bytes.Repeat([]byte{0xAB}, DefaultAllocationChunkSize)
	if err := svc.Write(ctx, 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if _, ok, err := mem.Get(ctx, store.BuildChunkKey("devA", 1)); err != nil || !ok {
		t.Fatalf("expected seed chunk before discard: ok=%t err=%v", ok, err)
	}

	if err := svc.Discard(ctx, 101, 0, uint64(DefaultAllocationChunkSize)); err != nil {
		t.Fatalf("Discard failed: %v", err)
	}
	readBack, err := svc.Read(ctx, 101, 0, uint64(DefaultAllocationChunkSize))
	if err != nil {
		t.Fatalf("Read after discard failed: %v", err)
	}
	if !bytes.Equal(readBack, make([]byte, DefaultAllocationChunkSize)) {
		t.Fatalf("discarded range did not read as zero")
	}

	metrics := svc.MetricsSnapshot()
	if metrics.IOIdentity == nil {
		t.Fatalf("io_identity metrics missing: %+v", metrics)
	}
	identity := metrics.IOIdentity
	if identity.DiscardBytes != uint64(DefaultAllocationChunkSize) || identity.LogicalZeroBytes != uint64(DefaultAllocationChunkSize) {
		t.Fatalf("unexpected discard identity bytes: %+v", identity)
	}
	if identity.DiscardTrueReclaimBytes != uint64(DefaultAllocationChunkSize) || identity.DiscardZeroFallbackBytes != 0 {
		t.Fatalf("unexpected reclaim counters: %+v", identity)
	}
	if identity.DiscardAlignedCount != 1 || identity.DiscardUnalignedCount != 0 || identity.DiscardAlignmentFallbacks != 0 {
		t.Fatalf("unexpected alignment counters: %+v", identity)
	}
	if identity.ByDiscardPolicy[DiscardPolicyTrueReclaim] != 1 {
		t.Fatalf("unexpected policy counters: %+v", identity.ByDiscardPolicy)
	}
	if identity.LastObservation == nil || identity.LastObservation.Policy != DiscardPolicyTrueReclaim || !identity.LastObservation.AlignedToReclaimGeometry {
		t.Fatalf("unexpected last observation: %+v", identity.LastObservation)
	}

	meta := svc.metadata.(*inMemoryMetadataRepository)
	page, err := meta.GetExtentPage(ctx, 101, 0)
	if err != nil {
		t.Fatalf("GetExtentPage failed: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != AllocationChunkKindZero || page.Extents[0].ChunkCount != 2 {
		t.Fatalf("expected live allocation page to be zero-only after discard: %+v", page.Extents)
	}
	garbage, err := meta.ListChunkGarbage(ctx, 101, 16)
	if err != nil {
		t.Fatalf("ListChunkGarbage failed: %v", err)
	}
	if len(garbage) != 1 || garbage[0].ChunkID != 1 {
		t.Fatalf("unexpected retired chunk garbage: %+v", garbage)
	}

	result, err := NewChunkGarbageCollector(meta, mem).SweepVolume(ctx, 101, 16)
	if err != nil {
		t.Fatalf("SweepVolume failed: %v", err)
	}
	if result.DeletedCount != 1 || result.RetainedCount != 0 {
		t.Fatalf("unexpected sweep result: %+v", result)
	}
	if _, ok, err := mem.Get(ctx, store.BuildChunkKey("devA", 1)); err != nil || ok {
		t.Fatalf("expected retired chunk to be deleted after sweep: ok=%t err=%v", ok, err)
	}
}
