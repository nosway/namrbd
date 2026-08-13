package cluster

import (
	"context"
	"fmt"
	"sync"
)

type cachedChunkIDAllocator struct {
	next      metadataChunkIDAllocator
	cacheSize uint32

	mu      sync.Mutex
	volumes map[string]chunkIDCacheRange
}

type chunkIDCacheRange struct {
	next uint64
	end  uint64
}

func newCachedChunkIDAllocator(next metadataChunkIDAllocator, cacheSize uint32) *cachedChunkIDAllocator {
	return &cachedChunkIDAllocator{
		next:      next,
		cacheSize: cacheSize,
		volumes:   make(map[string]chunkIDCacheRange),
	}
}

func (a *cachedChunkIDAllocator) AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	if a == nil || a.next == nil {
		return 0, fmt.Errorf("chunk id allocator is required")
	}
	if a.cacheSize == 0 || count > a.cacheSize {
		return a.next.AllocateChunkIDs(ctx, volumeID, count)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	cached := a.volumes[volumeID]
	if cached.end-cached.next < uint64(count) {
		refillCount := a.cacheSize
		if refillCount < count {
			refillCount = count
		}
		start, err := a.next.AllocateChunkIDs(ctx, volumeID, refillCount)
		if err != nil {
			return 0, err
		}
		cached = chunkIDCacheRange{
			next: start,
			end:  start + uint64(refillCount),
		}
	}

	start := cached.next
	cached.next += uint64(count)
	a.volumes[volumeID] = cached
	return start, nil
}
