package service

import (
	"context"

	"github.com/nosway/namrbd/gateway/store"
)

type ChunkGarbageSweepResult struct {
	VolumeID       HexVolumeID `json:"volume_id"`
	CandidateCount int         `json:"candidate_count"`
	DeletedCount   int         `json:"deleted_count"`
	RetainedCount  int         `json:"retained_count"`
}

type ChunkGarbageCollector struct {
	meta    MetadataRepository
	objects store.ObjectStore
}

func NewChunkGarbageCollector(meta MetadataRepository, objects store.ObjectStore) *ChunkGarbageCollector {
	return &ChunkGarbageCollector{meta: meta, objects: objects}
}

func (c *ChunkGarbageCollector) SweepAll(ctx context.Context, limit int) ([]ChunkGarbageSweepResult, error) {
	volumes, err := c.meta.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]ChunkGarbageSweepResult, 0, len(volumes))
	for _, volume := range volumes {
		result, err := c.SweepVolume(ctx, uint64(volume.ID), limit)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (c *ChunkGarbageCollector) SweepVolume(ctx context.Context, volumeID uint64, limit int) (ChunkGarbageSweepResult, error) {
	return c.SweepVolumeWithProtectedRefs(ctx, volumeID, limit, nil)
}

func (c *ChunkGarbageCollector) SweepVolumeWithProtectedRefs(ctx context.Context, volumeID uint64, limit int, protectedRefs []PhysicalChunkRef) (ChunkGarbageSweepResult, error) {
	volume, err := c.meta.GetVolume(ctx, volumeID)
	if err != nil {
		return ChunkGarbageSweepResult{}, err
	}
	candidates, err := c.meta.ListChunkGarbage(ctx, volumeID, limit)
	if err != nil {
		return ChunkGarbageSweepResult{}, err
	}
	result := ChunkGarbageSweepResult{
		VolumeID:       HexVolumeID(volumeID),
		CandidateCount: len(candidates),
	}
	if len(candidates) == 0 {
		return result, nil
	}

	referenced, err := c.referencedChunkSet(ctx, volumeID)
	if err != nil {
		return ChunkGarbageSweepResult{}, err
	}
	protectedChunkIDs := make(map[uint64]struct{})
	for _, ref := range protectedRefs {
		if ref.ChunkID == 0 {
			continue
		}
		if ref.StoreID == "" && ref.ShardID == 0 {
			protectedChunkIDs[ref.ChunkID] = struct{}{}
			continue
		}
		referenced[ref] = struct{}{}
	}
	for _, candidate := range candidates {
		ref := PhysicalChunkRef{StoreID: candidate.StoreID, ShardID: candidate.ShardID, ChunkID: candidate.ChunkID}
		if _, ok := protectedChunkIDs[candidate.ChunkID]; ok {
			result.RetainedCount++
			continue
		}
		if _, ok := referenced[ref]; ok {
			result.RetainedCount++
			continue
		}
		if err := c.objects.Delete(ctx, buildPhysicalChunkKey(volume.Prefix, ref)); err != nil {
			return ChunkGarbageSweepResult{}, err
		}
		if err := c.meta.DeleteChunkGarbage(ctx, volumeID, candidate.ChunkID); err != nil {
			return ChunkGarbageSweepResult{}, err
		}
		result.DeletedCount++
	}
	return result, nil
}

func (c *ChunkGarbageCollector) referencedChunkSet(ctx context.Context, volumeID uint64) (map[PhysicalChunkRef]struct{}, error) {
	pages, err := c.meta.ListExtentPages(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	referenced := make(map[PhysicalChunkRef]struct{})
	for _, page := range pages {
		for _, extent := range page.Extents {
			if extent.Kind != AllocationChunkKindData {
				continue
			}
			for i := uint32(0); i < extent.ChunkCount; i++ {
				referenced[PhysicalChunkRef{
					StoreID: extent.StoreID,
					ShardID: extent.ShardID,
					ChunkID: extent.PhysicalChunkStart + uint64(i),
				}] = struct{}{}
			}
		}
	}
	return referenced, nil
}
