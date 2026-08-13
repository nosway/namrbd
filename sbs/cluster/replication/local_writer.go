package replication

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type LocalReplicaWriter struct {
	stores     map[string]store.ObjectStore
	encryption *localReplicaPayloadEncryptor
}

func NewLocalReplicaWriter(stores map[string]store.ObjectStore) *LocalReplicaWriter {
	cloned := make(map[string]store.ObjectStore, len(stores))
	for replicaID, payloadStore := range stores {
		cloned[replicaID] = payloadStore
	}
	return &LocalReplicaWriter{stores: cloned}
}

func NewEncryptedLocalReplicaWriterForPhaseP(stores map[string]store.ObjectStore, cfg PhasePReplicaEncryptionConfig) *LocalReplicaWriter {
	writer := NewLocalReplicaWriter(stores)
	writer.encryption = newLocalReplicaPayloadEncryptor(cfg)
	return writer
}

func (w *LocalReplicaWriter) WriteExtent(ctx context.Context, plan ExtentWritePlan, req ReplicaWriteRequest) (*ReplicaWriteResult, error) {
	start := time.Now()
	payload, err := payloadForExtent(plan, req)
	if err != nil {
		return nil, err
	}
	writeStart, writeLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, err
	}
	acked := make([]string, 0, len(plan.WriteTargets))
	perReplicaDurations := make(map[string]time.Duration, len(plan.WriteTargets))
	var firstAckDuration time.Duration
	var slowestDuration time.Duration
	var slowestReplicaID string
	chunkEncryptionHeaders := make(map[uint64]ReplicaChunkEncryptionHeader)
	for _, target := range plan.WriteTargets {
		targetStart := time.Now()
		payloadStore, ok := w.stores[target.ReplicaID]
		if !ok {
			return nil, fmt.Errorf("replica %q has no local payload store", target.ReplicaID)
		}
		if ok, err := w.writeViaAllocationChunks(ctx, payloadStore, target.ReplicaID, plan, req, writeStart, writeLength, payload, chunkEncryptionHeaders); ok {
			if err != nil {
				return nil, err
			}
			acked = append(acked, target.ReplicaID)
			targetDuration := time.Since(targetStart)
			perReplicaDurations[target.ReplicaID] = targetDuration
			if firstAckDuration == 0 {
				firstAckDuration = time.Since(start)
			}
			if targetDuration > slowestDuration {
				slowestDuration = targetDuration
				slowestReplicaID = target.ReplicaID
			}
			continue
		}
		if w.encryption != nil {
			return nil, fmt.Errorf("phase p encrypted replica writer requires allocation chunk mappings")
		}
		chunkID := plan.Extent.ChunkID
		if physicalChunkID, ok := resolvedWritePhysicalChunkID(plan, req); ok {
			chunkID = physicalChunkID
		}
		key := localReplicaPayloadKey(target.ReplicaID, req.VolumeID, plan.Extent.ExtentID, chunkID)
		if err := payloadStore.Put(ctx, key, payload); err != nil {
			return nil, err
		}
		acked = append(acked, target.ReplicaID)
		targetDuration := time.Since(targetStart)
		perReplicaDurations[target.ReplicaID] = targetDuration
		if firstAckDuration == 0 {
			firstAckDuration = time.Since(start)
		}
		if targetDuration > slowestDuration {
			slowestDuration = targetDuration
			slowestReplicaID = target.ReplicaID
		}
	}
	allDuration := time.Since(start)
	return &ReplicaWriteResult{
		AckedReplicaIDs:        acked,
		ChunkEncryptionHeaders: sortedReplicaChunkEncryptionHeaders(chunkEncryptionHeaders),
		Stats: ReplicaWriteStats{
			FirstAckDuration:   firstAckDuration,
			QuorumAckDuration:  allDuration,
			AllAckDuration:     allDuration,
			SlowestReplicaID:   slowestReplicaID,
			SlowestDuration:    slowestDuration,
			PerReplicaDuration: perReplicaDurations,
		},
	}, nil
}

func (w *LocalReplicaWriter) writeViaAllocationChunks(ctx context.Context, payloadStore store.ObjectStore, replicaID string, plan ExtentWritePlan, req ReplicaWriteRequest, writeStart, writeLength uint64, payload []byte, headers map[uint64]ReplicaChunkEncryptionHeader) (bool, error) {
	chunks, ok := resolvedWriteChunkMappings(plan, req)
	if !ok {
		return false, nil
	}
	if len(chunks) == 0 {
		return true, nil
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	if uint64(len(payload)) != writeLength {
		return false, fmt.Errorf("payload length does not match write length")
	}
	for _, chunk := range chunks {
		logicalChunk := chunk.LogicalChunk
		chunkStart := logicalChunk * chunkSize
		copyStart := maxUint64(writeStart, chunkStart)
		copyEnd := minUint64(writeStart+writeLength, chunkStart+chunkSize)
		if copyStart >= copyEnd {
			continue
		}
		if shouldSkipChunkPayloadWrite(req, chunkStart, chunkStart+chunkSize, copyStart, copyEnd) {
			continue
		}
		basePhysicalChunkID := chunk.PhysicalChunkID
		baseEncryption := chunk.Encryption
		if chunk.BasePhysicalChunkID != 0 && chunk.BasePhysicalChunkID != chunk.PhysicalChunkID {
			basePhysicalChunkID = chunk.BasePhysicalChunkID
			baseEncryption = chunk.BaseEncryption
		}
		chunkBuf, err := w.loadChunkForMerge(ctx, payloadStore, replicaID, req.VolumeID, plan.Extent.ExtentID, logicalChunk, basePhysicalChunkID, chunkSize, baseEncryption, copyStart == chunkStart && copyEnd == chunkStart+chunkSize)
		if err != nil {
			return false, err
		}
		payloadOffset := copyStart - writeStart
		chunkOffset := copyStart - chunkStart
		copyLen := copyEnd - copyStart
		if payloadOffset+copyLen > uint64(len(payload)) || chunkOffset+copyLen > uint64(len(chunkBuf)) {
			return false, fmt.Errorf("chunk merge window exceeds payload bounds")
		}
		copy(chunkBuf[chunkOffset:chunkOffset+copyLen], payload[payloadOffset:payloadOffset+copyLen])
		stored := chunkBuf
		encryptionHeader := (*metadata.PayloadEncryptionHeader)(nil)
		if w.encryption != nil {
			var err error
			stored, encryptionHeader, err = w.encryption.encryptChunk(ctx, req.VolumeID, logicalChunk, chunk.PhysicalChunkID, chunkSize, chunkBuf)
			if err != nil {
				return false, err
			}
			if err := recordReplicaChunkEncryptionHeader(headers, logicalChunk, chunk.PhysicalChunkID, encryptionHeader); err != nil {
				return false, err
			}
		}
		key := localReplicaPayloadKey(replicaID, req.VolumeID, plan.Extent.ExtentID, chunk.PhysicalChunkID)
		if err := payloadStore.Put(ctx, key, stored); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *LocalReplicaWriter) loadChunkForMerge(ctx context.Context, payloadStore store.ObjectStore, replicaID, volumeID string, extentID, logicalChunk, physicalChunkID, chunkSize uint64, encryptionHeader *metadata.PayloadEncryptionHeader, fullChunkWrite bool) ([]byte, error) {
	if fullChunkWrite {
		return make([]byte, chunkSize), nil
	}
	key := localReplicaPayloadKey(replicaID, volumeID, extentID, physicalChunkID)
	value, found, err := payloadStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, chunkSize)
	if !found {
		if encryptionHeader != nil {
			return nil, fmt.Errorf("encrypted replica %q payload not found", replicaID)
		}
		return buf, nil
	}
	plaintext, err := w.encryption.decryptChunk(ctx, volumeID, logicalChunk, physicalChunkID, chunkSize, encryptionHeader, value)
	if err != nil {
		return nil, err
	}
	copy(buf, plaintext)
	return buf, nil
}

func recordReplicaChunkEncryptionHeader(headers map[uint64]ReplicaChunkEncryptionHeader, logicalChunk, physicalChunkID uint64, header *metadata.PayloadEncryptionHeader) error {
	if header == nil {
		return nil
	}
	if headers == nil {
		return nil
	}
	cloned := *header
	if existing, ok := headers[logicalChunk]; ok {
		if existing.PhysicalChunkID != physicalChunkID || !samePayloadEncryptionHeader(existing.Header, &cloned) {
			return fmt.Errorf("conflicting encrypted replica metadata for logical chunk %d", logicalChunk)
		}
		return nil
	}
	headers[logicalChunk] = ReplicaChunkEncryptionHeader{
		LogicalChunk:    logicalChunk,
		PhysicalChunkID: physicalChunkID,
		Header:          &cloned,
	}
	return nil
}

func sortedReplicaChunkEncryptionHeaders(headers map[uint64]ReplicaChunkEncryptionHeader) []ReplicaChunkEncryptionHeader {
	if len(headers) == 0 {
		return nil
	}
	keys := make([]uint64, 0, len(headers))
	for logicalChunk := range headers {
		keys = append(keys, logicalChunk)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]ReplicaChunkEncryptionHeader, 0, len(keys))
	for _, logicalChunk := range keys {
		header := headers[logicalChunk]
		if header.Header != nil {
			cloned := *header.Header
			header.Header = &cloned
		}
		out = append(out, header)
	}
	return out
}

func payloadForExtent(plan ExtentWritePlan, req ReplicaWriteRequest) ([]byte, error) {
	extent := plan.Extent
	extentStart := extent.GetLogicalOffset()
	extentEnd := extent.GetLogicalOffset() + extent.GetLengthBytes()
	reqEnd := req.OffsetBytes + req.LengthBytes
	if extentEnd <= req.OffsetBytes || extentStart >= reqEnd {
		return nil, fmt.Errorf("request does not overlap extent")
	}
	if canOmitZeroSemanticPayload(plan, req) {
		return nil, nil
	}
	copyStart := maxUint64(req.OffsetBytes, extentStart)
	copyEnd := minUint64(reqEnd, extentEnd)
	start := copyStart - req.OffsetBytes
	end := copyEnd - req.OffsetBytes
	if end > uint64(len(req.Data)) {
		return nil, fmt.Errorf("request data shorter than declared length")
	}
	return append([]byte(nil), req.Data[start:end]...), nil
}

func localReplicaPayloadKey(replicaID, volumeID string, extentID, chunkID uint64) string {
	return fmt.Sprintf("replicas/%s/volumes/%s/extents/%020d/chunks/%020d", replicaID, volumeID, extentID, chunkID)
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func shouldSkipChunkPayloadWrite(req ReplicaWriteRequest, chunkStart, chunkEnd, copyStart, copyEnd uint64) bool {
	return req.ZeroSemantic && copyStart == chunkStart && copyEnd == chunkEnd
}

func canOmitZeroSemanticPayload(plan ExtentWritePlan, req ReplicaWriteRequest) bool {
	if !req.ZeroSemantic {
		return false
	}
	chunks, ok := resolvedWriteChunkMappings(plan, req)
	return ok && len(chunks) == 0
}
