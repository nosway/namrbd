package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nosway/namrbd/gateway/store"
)

type chunkPayloadRepository struct {
	objects store.ObjectStore
}

func newChunkPayloadRepository(objects store.ObjectStore) *chunkPayloadRepository {
	return &chunkPayloadRepository{objects: objects}
}

func (r *chunkPayloadRepository) ReadChunk(ctx context.Context, volume VolumeSpec, chunkID uint64) ([]byte, error) {
	return r.ReadChunkRef(ctx, volume, PhysicalChunkRef{ChunkID: chunkID})
}

func (r *chunkPayloadRepository) ReadChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef) ([]byte, error) {
	chunkSize := int(volume.ChunkSizeBytes)
	if chunkSize <= 0 {
		chunkSize = DefaultAllocationChunkSize
	}
	key := buildPhysicalChunkKey(volume.Prefix, ref)
	value, found, err := r.objects.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return make([]byte, chunkSize), nil
	}
	if len(value) != chunkSize {
		return nil, fmt.Errorf("invalid chunk size in store key=%s got=%d want=%d", key, len(value), chunkSize)
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out, nil
}

func (r *chunkPayloadRepository) WriteChunk(ctx context.Context, volume VolumeSpec, chunkID uint64, data []byte) error {
	return r.WriteChunkRef(ctx, volume, PhysicalChunkRef{ChunkID: chunkID}, data)
}

func (r *chunkPayloadRepository) WriteChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef, data []byte) error {
	chunkSize := int(volume.ChunkSizeBytes)
	if chunkSize <= 0 {
		chunkSize = DefaultAllocationChunkSize
	}
	if len(data) != chunkSize {
		return fmt.Errorf("invalid chunk payload size got=%d want=%d", len(data), chunkSize)
	}
	key := buildPhysicalChunkKey(volume.Prefix, ref)
	return r.objects.Put(ctx, key, data)
}

func (r *chunkPayloadRepository) DeleteChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef) error {
	return r.objects.Delete(ctx, buildPhysicalChunkKey(volume.Prefix, ref))
}

func buildPhysicalChunkKey(prefix string, ref PhysicalChunkRef) string {
	if ref.StoreID == "" && ref.ShardID == 0 {
		return store.BuildChunkKey(prefix, ref.ChunkID)
	}
	return prefix + ":store:" + ref.StoreID + ":shard:" + strconv.FormatUint(uint64(ref.ShardID), 10) + ":chk:" + strconv.FormatUint(ref.ChunkID, 10)
}
