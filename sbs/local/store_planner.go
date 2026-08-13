package local

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/nosway/namrbd/gateway/service"
)

type storePlanner struct {
	currentStores func() []StoreSpec
	currentStates func() map[string]string
}

func newStorePlanner(currentStores func() []StoreSpec, currentStates func() map[string]string) *storePlanner {
	return &storePlanner{
		currentStores: currentStores,
		currentStates: currentStates,
	}
}

func (p *storePlanner) ChoosePhysicalChunkRef(volume service.VolumeSpec, logicalChunk, physicalChunkID uint64) (service.PhysicalChunkRef, error) {
	stores := p.currentStores()
	states := p.currentStates()
	eligible := make([]StoreSpec, 0, len(stores))
	totalWeight := 0
	for _, spec := range stores {
		if !storeEligibleForAllocation(states[spec.ID]) {
			continue
		}
		if spec.Weight <= 0 {
			continue
		}
		eligible = append(eligible, spec)
		totalWeight += spec.Weight
	}
	if len(eligible) == 0 {
		return service.PhysicalChunkRef{}, fmt.Errorf("no writable store available for volume %s", service.CanonicalVolumeID(uint64(volume.ID)))
	}

	storeHash := plannerHash(uint64(volume.ID), logicalChunk, physicalChunkID, 0)
	choice := int(storeHash % uint64(totalWeight))
	for _, spec := range eligible {
		if choice < spec.Weight {
			shardHash := plannerHash(uint64(volume.ID), logicalChunk, physicalChunkID, 1)
			return service.PhysicalChunkRef{
				StoreID: spec.ID,
				ShardID: uint32(shardHash % uint64(spec.Shards)),
				ChunkID: physicalChunkID,
			}, nil
		}
		choice -= spec.Weight
	}

	last := eligible[len(eligible)-1]
	return service.PhysicalChunkRef{
		StoreID: last.ID,
		ShardID: uint32(plannerHash(uint64(volume.ID), logicalChunk, physicalChunkID, 1) % uint64(last.Shards)),
		ChunkID: physicalChunkID,
	}, nil
}

func storeEligibleForAllocation(state string) bool {
	switch state {
	case StoreStateFailed, StoreStateReadOnly, StoreStateDraining:
		return false
	default:
		return true
	}
}

func plannerHash(volumeID, logicalChunk, physicalChunkID uint64, salt byte) uint64 {
	var buf [25]byte
	binary.BigEndian.PutUint64(buf[0:8], volumeID)
	binary.BigEndian.PutUint64(buf[8:16], logicalChunk)
	binary.BigEndian.PutUint64(buf[16:24], physicalChunkID)
	buf[24] = salt
	sum := sha256.Sum256(buf[:])
	return binary.BigEndian.Uint64(sum[:8])
}
