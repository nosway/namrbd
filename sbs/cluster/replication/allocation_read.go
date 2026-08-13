package replication

import "github.com/nosway/namrbd/sbs/cluster/metadata"

type resolvedChunkRead struct {
	LogicalChunk    uint64
	PhysicalChunkID uint64
	PayloadVolumeID string
	Zero            bool
	Encryption      *metadata.PayloadEncryptionHeader
}

type resolvedChunkWrite struct {
	LogicalChunk        uint64
	PhysicalChunkID     uint64
	BasePhysicalChunkID uint64
	Encryption          *metadata.PayloadEncryptionHeader
	BaseEncryption      *metadata.PayloadEncryptionHeader
}

func readRangeSatisfiedByZeroAllocation(plan ExtentReadPlan, req ReplicaReadRequest) bool {
	if plan.ChunkSizeBytes == 0 || len(plan.AllocationPages) == 0 {
		return false
	}

	readStart, readLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil || readLength == 0 {
		return false
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	startChunk := readStart / chunkSize
	endChunk := (readStart + readLength - 1) / chunkSize

	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		covered, zero, _, _ := logicalChunkAllocationState(plan.AllocationPages, logicalChunk)
		if !covered || !zero {
			return false
		}
	}
	return true
}

func resolvedReadChunkMappings(plan ExtentReadPlan, req ReplicaReadRequest) ([]resolvedChunkRead, bool) {
	if plan.ChunkSizeBytes == 0 || len(plan.AllocationPages) == 0 {
		return nil, false
	}
	readStart, readLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil || readLength == 0 {
		return nil, false
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	startChunk := readStart / chunkSize
	endChunk := (readStart + readLength - 1) / chunkSize
	out := make([]resolvedChunkRead, 0, endChunk-startChunk+1)
	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		covered, zero, physicalChunkID, payloadVolumeID, encryption := logicalChunkAllocationRecord(plan.AllocationPages, logicalChunk)
		if !covered {
			return nil, false
		}
		out = append(out, resolvedChunkRead{
			LogicalChunk:    logicalChunk,
			PhysicalChunkID: physicalChunkID,
			PayloadVolumeID: payloadVolumeID,
			Zero:            zero,
			Encryption:      encryption,
		})
	}
	return out, true
}

func resolvedReadPhysicalChunkID(plan ExtentReadPlan, req ReplicaReadRequest) (uint64, bool) {
	chunks, ok := resolvedReadChunkMappings(plan, req)
	if !ok || len(chunks) != 1 {
		return 0, false
	}
	if chunks[0].Zero || chunks[0].PhysicalChunkID == 0 {
		return 0, false
	}
	return chunks[0].PhysicalChunkID, true
}

func resolvedWritePhysicalChunkID(plan ExtentWritePlan, req ReplicaWriteRequest) (uint64, bool) {
	if plan.ChunkSizeBytes == 0 || len(plan.AllocationPages) == 0 {
		return 0, false
	}
	writeStart, writeLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil || writeLength == 0 {
		return 0, false
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	startChunk := writeStart / chunkSize
	endChunk := (writeStart + writeLength - 1) / chunkSize
	if startChunk != endChunk {
		return 0, false
	}

	covered, zero, physicalChunkID, _ := logicalChunkAllocationState(plan.AllocationPages, startChunk)
	if !covered || zero || physicalChunkID == 0 {
		return 0, false
	}
	return physicalChunkID, true
}

func resolvedWritePhysicalChunkIDs(plan ExtentWritePlan, req ReplicaWriteRequest) ([]uint64, bool) {
	chunks, ok := resolvedWriteChunkMappings(plan, req)
	if !ok {
		return nil, false
	}
	out := make([]uint64, 0, len(chunks))
	for _, chunk := range chunks {
		out = append(out, chunk.PhysicalChunkID)
	}
	return out, true
}

func resolvedWriteChunkMappings(plan ExtentWritePlan, req ReplicaWriteRequest) ([]resolvedChunkWrite, bool) {
	if plan.ChunkSizeBytes == 0 || len(plan.AllocationPages) == 0 {
		return nil, false
	}
	writeStart, writeLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil || writeLength == 0 {
		return nil, false
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	startChunk := writeStart / chunkSize
	endChunk := (writeStart + writeLength - 1) / chunkSize
	out := make([]resolvedChunkWrite, 0, endChunk-startChunk+1)
	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		chunkStart := logicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		copyStart := maxUint64(writeStart, chunkStart)
		copyEnd := minUint64(writeStart+writeLength, chunkEnd)
		covered, zero, physicalChunkID, _, encryption := logicalChunkAllocationRecord(plan.AllocationPages, logicalChunk)
		if !covered {
			return nil, false
		}
		if shouldSkipChunkPayloadWrite(req, chunkStart, chunkEnd, copyStart, copyEnd) {
			continue
		}
		if zero || physicalChunkID == 0 {
			return nil, false
		}
		var basePhysicalChunkID uint64
		var baseEncryption *metadata.PayloadEncryptionHeader
		if plan.CopyOnWrite && len(plan.BaseAllocations) > 0 {
			_, baseZero, basePhysical, _, header := logicalChunkAllocationRecord(plan.BaseAllocations, logicalChunk)
			if !baseZero {
				basePhysicalChunkID = basePhysical
				baseEncryption = header
			}
		}
		out = append(out, resolvedChunkWrite{
			LogicalChunk:        logicalChunk,
			PhysicalChunkID:     physicalChunkID,
			BasePhysicalChunkID: basePhysicalChunkID,
			Encryption:          encryption,
			BaseEncryption:      baseEncryption,
		})
	}
	return out, true
}

func logicalChunkAllocationState(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (covered bool, zero bool, physicalChunkID uint64, payloadVolumeID string) {
	covered, zero, physicalChunkID, payloadVolumeID, _ = logicalChunkAllocationRecord(pages, logicalChunk)
	return covered, zero, physicalChunkID, payloadVolumeID
}

func logicalChunkAllocationRecord(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (covered bool, zero bool, physicalChunkID uint64, payloadVolumeID string, encryption *metadata.PayloadEncryptionHeader) {
	for _, page := range pages {
		if logicalChunk < page.RangeStartChunk || logicalChunk >= page.RangeEndChunk {
			continue
		}
		for _, extent := range page.Page.Extents {
			start := extent.LogicalChunkStart
			end := start + uint64(extent.ChunkCount)
			if logicalChunk < start || logicalChunk >= end {
				continue
			}
			if extent.Kind == metadata.AllocationKindZero {
				return true, true, 0, page.Page.VolumeID, nil
			}
			if extent.Kind == metadata.AllocationKindData {
				return true, false, extent.PhysicalChunkStart + (logicalChunk - start), page.Page.VolumeID, cloneCommitPayloadEncryptionHeader(extent.Encryption)
			}
			return true, false, 0, page.Page.VolumeID, nil
		}
		return false, false, 0, "", nil
	}
	return false, false, 0, "", nil
}
