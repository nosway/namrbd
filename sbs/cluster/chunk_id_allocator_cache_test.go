package cluster

import (
	"context"
	"sync"
	"testing"
)

type recordingChunkIDAllocator struct {
	mu    sync.Mutex
	next  uint64
	calls []uint32
}

func (a *recordingChunkIDAllocator) AllocateChunkIDs(_ context.Context, _ string, count uint32) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.next == 0 {
		a.next = 1
	}
	start := a.next
	a.next += uint64(count)
	a.calls = append(a.calls, count)
	return start, nil
}

func TestCachedChunkIDAllocatorServesFromRefilledRange(t *testing.T) {
	base := &recordingChunkIDAllocator{}
	allocator := newCachedChunkIDAllocator(base, 8)

	got1, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs #1: %v", err)
	}
	got2, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs #2: %v", err)
	}
	got3, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 4)
	if err != nil {
		t.Fatalf("AllocateChunkIDs #3: %v", err)
	}
	if got1 != 1 || got2 != 3 || got3 != 5 {
		t.Fatalf("starts=(%d,%d,%d), want (1,3,5)", got1, got2, got3)
	}
	if len(base.calls) != 1 || base.calls[0] != 8 {
		t.Fatalf("base calls=%v, want [8]", base.calls)
	}

	got4, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs #4: %v", err)
	}
	if got4 != 9 {
		t.Fatalf("got4=%d, want 9", got4)
	}
	if len(base.calls) != 2 || base.calls[1] != 8 {
		t.Fatalf("base calls=%v, want [8 8]", base.calls)
	}
}

func TestCachedChunkIDAllocatorKeepsVolumeRangesSeparate(t *testing.T) {
	base := &recordingChunkIDAllocator{}
	allocator := newCachedChunkIDAllocator(base, 4)

	gotA, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs volume A: %v", err)
	}
	gotB, err := allocator.AllocateChunkIDs(context.Background(), "0000007d", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs volume B: %v", err)
	}
	gotA2, err := allocator.AllocateChunkIDs(context.Background(), "0000007c", 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs volume A #2: %v", err)
	}
	if gotA != 1 || gotB != 5 || gotA2 != 3 {
		t.Fatalf("starts=(%d,%d,%d), want (1,5,3)", gotA, gotB, gotA2)
	}
	if len(base.calls) != 2 {
		t.Fatalf("base calls=%v, want two refills", base.calls)
	}
}
