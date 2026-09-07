package service

import (
	"context"

	"github.com/nosway/namrbd/gateway/store"
)

type ChunkGarbageSweepResult struct {
	VolumeID       HexVolumeID `json:"volume_id"`
	ScannedCount   int         `json:"scanned_count"`
	CandidateCount int         `json:"candidate_count"`
	DeletableCount int         `json:"deletable_count"`
	DeletedCount   int         `json:"deleted_count"`
	RetainedCount  int         `json:"retained_count"`
	InspectionOnly bool        `json:"inspection_only"`
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
	return c.SweepVolumeCandidatesWithProtectedRefs(ctx, volumeID, limit, nil, protectedRefs)
}

func (c *ChunkGarbageCollector) SweepVolumeCandidatesWithProtectedRefs(ctx context.Context, volumeID uint64, limit int, candidateRefs, protectedRefs []PhysicalChunkRef) (ChunkGarbageSweepResult, error) {
	volume, deletable, result, err := c.classifyVolumeWithProtectedRefs(ctx, volumeID, limit, candidateRefs, protectedRefs)
	if err != nil {
		return ChunkGarbageSweepResult{}, err
	}
	for _, candidate := range deletable {
		ref := PhysicalChunkRef{StoreID: candidate.StoreID, ShardID: candidate.ShardID, ChunkID: candidate.ChunkID}
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

// InspectVolumeWithProtectedRefs runs the exact sweep classification without
// deleting payload or garbage records. Apply paths classify again, so an
// inspection result is evidence for a plan rather than an authorization token.
func (c *ChunkGarbageCollector) InspectVolumeWithProtectedRefs(ctx context.Context, volumeID uint64, limit int, protectedRefs []PhysicalChunkRef) (ChunkGarbageSweepResult, error) {
	return c.InspectVolumeCandidatesWithProtectedRefs(ctx, volumeID, limit, nil, protectedRefs)
}

func (c *ChunkGarbageCollector) InspectVolumeCandidatesWithProtectedRefs(ctx context.Context, volumeID uint64, limit int, candidateRefs, protectedRefs []PhysicalChunkRef) (ChunkGarbageSweepResult, error) {
	_, _, result, err := c.classifyVolumeWithProtectedRefs(ctx, volumeID, limit, candidateRefs, protectedRefs)
	result.InspectionOnly = true
	return result, err
}

func (c *ChunkGarbageCollector) classifyVolumeWithProtectedRefs(ctx context.Context, volumeID uint64, limit int, candidateRefs, protectedRefs []PhysicalChunkRef) (VolumeSpec, []AllocationChunkGarbageRecord, ChunkGarbageSweepResult, error) {
	volume, err := c.meta.GetVolume(ctx, volumeID)
	if err != nil {
		return VolumeSpec{}, nil, ChunkGarbageSweepResult{}, err
	}
	candidates, err := c.meta.ListChunkGarbage(ctx, volumeID, limit)
	if err != nil {
		return VolumeSpec{}, nil, ChunkGarbageSweepResult{}, err
	}
	result := ChunkGarbageSweepResult{VolumeID: HexVolumeID(volumeID), ScannedCount: len(candidates)}
	candidates = filterChunkGarbageCandidates(candidates, candidateRefs)
	result.CandidateCount = len(candidates)
	if len(candidates) == 0 {
		return volume, nil, result, nil
	}
	referenced, err := c.referencedChunkSet(ctx, volumeID)
	if err != nil {
		return VolumeSpec{}, nil, ChunkGarbageSweepResult{}, err
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
	deletable := make([]AllocationChunkGarbageRecord, 0, len(candidates))
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
		deletable = append(deletable, candidate)
	}
	result.DeletableCount = len(deletable)
	return volume, deletable, result, nil
}

func filterChunkGarbageCandidates(candidates []AllocationChunkGarbageRecord, refs []PhysicalChunkRef) []AllocationChunkGarbageRecord {
	if len(refs) == 0 {
		return candidates
	}
	exact := make(map[PhysicalChunkRef]struct{}, len(refs))
	wildcardChunkIDs := make(map[uint64]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ChunkID == 0 {
			continue
		}
		if ref.StoreID == "" && ref.ShardID == 0 {
			wildcardChunkIDs[ref.ChunkID] = struct{}{}
			continue
		}
		exact[ref] = struct{}{}
	}
	out := make([]AllocationChunkGarbageRecord, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := wildcardChunkIDs[candidate.ChunkID]; ok {
			out = append(out, candidate)
			continue
		}
		ref := PhysicalChunkRef{StoreID: candidate.StoreID, ShardID: candidate.ShardID, ChunkID: candidate.ChunkID}
		if _, ok := exact[ref]; ok {
			out = append(out, candidate)
		}
	}
	return out
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
