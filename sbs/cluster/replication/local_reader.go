package replication

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/gateway/store"
)

type ReplicaReadRequest struct {
	RequestID   string
	VolumeID    string
	CloneID     string
	SnapshotID  string
	OffsetBytes uint64
	LengthBytes uint64
	Attribution bool
}

type replicaReader interface {
	ReadExtent(ctx context.Context, plan ExtentReadPlan, req ReplicaReadRequest) ([]byte, string, error)
}

type LocalReplicaReader struct {
	stores     map[string]store.ObjectStore
	encryption *localReplicaPayloadEncryptor
}

func NewLocalReplicaReader(stores map[string]store.ObjectStore) *LocalReplicaReader {
	cloned := make(map[string]store.ObjectStore, len(stores))
	for replicaID, payloadStore := range stores {
		cloned[replicaID] = payloadStore
	}
	return &LocalReplicaReader{stores: cloned}
}

func NewEncryptedLocalReplicaReaderForPhaseP(stores map[string]store.ObjectStore, cfg PhasePReplicaEncryptionConfig) *LocalReplicaReader {
	reader := NewLocalReplicaReader(stores)
	reader.encryption = newLocalReplicaPayloadEncryptor(cfg)
	return reader
}

func (r *LocalReplicaReader) ReadExtent(ctx context.Context, plan ExtentReadPlan, req ReplicaReadRequest) ([]byte, string, error) {
	readStart, readLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, "", err
	}
	if readRangeSatisfiedByZeroAllocation(plan, req) {
		return make([]byte, readLength), "", nil
	}
	targets := append([]ReplicaTarget{plan.Preferred}, plan.Fallbacks...)
	var lastErr error
	sawMissingPayload := false
	for _, target := range targets {
		payloadStore, ok := r.stores[target.ReplicaID]
		if !ok {
			lastErr = fmt.Errorf("replica %q has no local payload store", target.ReplicaID)
			continue
		}
		if data, ok, sawMissing, err := r.readViaAllocationChunks(ctx, payloadStore, target.ReplicaID, plan, req, readStart, readLength); ok {
			if err != nil {
				lastErr = err
				continue
			}
			return data, target.ReplicaID, nil
		} else if sawMissing {
			sawMissingPayload = true
			if err != nil {
				lastErr = err
			}
			continue
		}
		if r.encryption != nil {
			lastErr = fmt.Errorf("phase p encrypted replica reader requires allocation chunk mappings")
			continue
		}
		keys := []string{localReplicaPayloadKey(target.ReplicaID, req.VolumeID, plan.Extent.ExtentID, plan.Extent.ChunkID)}
		if physicalChunkID, ok := resolvedReadPhysicalChunkID(plan, req); ok && physicalChunkID != plan.Extent.ChunkID {
			keys = append([]string{localReplicaPayloadKey(target.ReplicaID, req.VolumeID, plan.Extent.ExtentID, physicalChunkID)}, keys...)
		}
		for _, key := range keys {
			value, found, err := payloadStore.Get(ctx, key)
			if err != nil {
				lastErr = err
				continue
			}
			if !found {
				sawMissingPayload = true
				lastErr = fmt.Errorf("replica %q payload not found", target.ReplicaID)
				continue
			}
			return append([]byte(nil), value...), target.ReplicaID, nil
		}
	}
	if sawMissingPayload {
		return make([]byte, readLength), "", nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no readable replica found")
	}
	return nil, "", lastErr
}

func (r *LocalReplicaReader) readViaAllocationChunks(ctx context.Context, payloadStore store.ObjectStore, replicaID string, plan ExtentReadPlan, req ReplicaReadRequest, readStart, readLength uint64) ([]byte, bool, bool, error) {
	chunks, ok := resolvedReadChunkMappings(plan, req)
	if !ok || len(chunks) == 0 {
		return nil, false, false, nil
	}
	out := make([]byte, readLength)
	chunkSize := uint64(plan.ChunkSizeBytes)
	for _, chunk := range chunks {
		chunkStart := chunk.LogicalChunk * chunkSize
		copyStart := maxUint64(readStart, chunkStart)
		copyEnd := minUint64(readStart+readLength, chunkStart+chunkSize)
		if copyStart >= copyEnd {
			continue
		}
		outOffset := copyStart - readStart
		chunkOffset := copyStart - chunkStart
		copyLen := copyEnd - copyStart
		if chunk.Zero {
			continue
		}
		key := localReplicaPayloadKey(replicaID, req.VolumeID, plan.Extent.ExtentID, chunk.PhysicalChunkID)
		value, found, err := payloadStore.Get(ctx, key)
		if err != nil {
			return nil, true, false, err
		}
		if !found {
			if chunk.Encryption != nil {
				return nil, true, false, fmt.Errorf("encrypted replica %q payload not found", replicaID)
			}
			return nil, true, true, fmt.Errorf("replica %q payload not found", replicaID)
		}
		chunkValue := value
		if chunk.Encryption != nil {
			var err error
			chunkValue, err = r.encryption.decryptChunk(ctx, req.VolumeID, chunk.LogicalChunk, chunk.PhysicalChunkID, chunkSize, chunk.Encryption, value)
			if err != nil {
				return nil, true, false, err
			}
		}
		if uint64(len(chunkValue)) < chunkOffset+copyLen {
			return nil, true, false, fmt.Errorf("replica %q payload shorter than requested chunk window", replicaID)
		}
		copy(out[outOffset:outOffset+copyLen], chunkValue[chunkOffset:chunkOffset+copyLen])
	}
	return out, true, false, nil
}
