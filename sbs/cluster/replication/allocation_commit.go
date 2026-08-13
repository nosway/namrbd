package replication

import (
	"fmt"
	"slices"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type allocationCommitPageState struct {
	volumeID          string
	pageNo            uint64
	pageBytes         uint32
	chunkSizeBytes    uint32
	revision          uint64
	originalChunkIDs  []uint64
	physicalChunkIDs  []uint64
	encryptionHeaders []*metadata.PayloadEncryptionHeader
}

func newAllocationCommitPageState(page metadata.AllocationPageRecord) (*allocationCommitPageState, error) {
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	chunkIDs, headers, err := expandAllocationPageChunkMappings(page)
	if err != nil {
		return nil, err
	}
	return &allocationCommitPageState{
		volumeID:          page.VolumeID,
		pageNo:            page.PageNo,
		pageBytes:         page.PageBytes,
		chunkSizeBytes:    page.ChunkSizeBytes,
		revision:          page.Revision,
		originalChunkIDs:  append([]uint64(nil), chunkIDs...),
		physicalChunkIDs:  chunkIDs,
		encryptionHeaders: headers,
	}, nil
}

func (s *allocationCommitPageState) applyWrite(plan ExtentWritePlan, req WriteRequest, writeStart, writeLength uint64, encryptionHeaders map[uint64]*metadata.PayloadEncryptionHeader) error {
	if writeLength == 0 {
		return nil
	}
	chunkSize := uint64(s.chunkSizeBytes)
	writeEnd := writeStart + writeLength
	startChunk := writeStart / chunkSize
	endChunk := (writeEnd - 1) / chunkSize
	pageStartChunk := s.pageNo * uint64(s.pageBytes/s.chunkSizeBytes)
	pageEndChunk := pageStartChunk + uint64(len(s.physicalChunkIDs))
	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		if logicalChunk < pageStartChunk || logicalChunk >= pageEndChunk {
			continue
		}
		chunkStart := logicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		if req.ZeroSemantic && writeStart <= chunkStart && writeEnd >= chunkEnd {
			s.physicalChunkIDs[logicalChunk-pageStartChunk] = 0
			s.encryptionHeaders[logicalChunk-pageStartChunk] = nil
			continue
		}
		physicalChunkID, ok := physicalChunkIDForCommittedWrite(plan, logicalChunk)
		if !ok {
			return fmt.Errorf("PE %d logical AC %d has no resolvable physical AC id for allocation commit", plan.Extent.ExtentID, logicalChunk)
		}
		pageIndex := logicalChunk - pageStartChunk
		s.physicalChunkIDs[pageIndex] = physicalChunkID
		s.encryptionHeaders[pageIndex] = cloneCommitPayloadEncryptionHeader(encryptionHeaders[logicalChunk])
	}
	return nil
}

func (s *allocationCommitPageState) toRecord() metadata.AllocationPageRecord {
	pageStartChunk := s.pageNo * uint64(s.pageBytes/s.chunkSizeBytes)
	return metadata.AllocationPageRecord{
		VolumeID:       s.volumeID,
		PageNo:         s.pageNo,
		PageBytes:      s.pageBytes,
		ChunkSizeBytes: s.chunkSizeBytes,
		Revision:       s.revision,
		Extents:        compressAllocationCommitChunkMappings(pageStartChunk, s.physicalChunkIDs, s.encryptionHeaders),
	}
}

func (s *allocationCommitPageState) retiredPhysicalChunkIDs() []uint64 {
	if len(s.originalChunkIDs) == 0 || len(s.originalChunkIDs) != len(s.physicalChunkIDs) {
		return nil
	}
	seen := make(map[uint64]struct{})
	out := make([]uint64, 0)
	for i, original := range s.originalChunkIDs {
		if original == 0 || original == s.physicalChunkIDs[i] {
			continue
		}
		if _, ok := seen[original]; ok {
			continue
		}
		seen[original] = struct{}{}
		out = append(out, original)
	}
	return out
}

func expandAllocationPageChunkMappings(page metadata.AllocationPageRecord) ([]uint64, []*metadata.PayloadEncryptionHeader, error) {
	chunksPerPage := int(page.PageBytes / page.ChunkSizeBytes)
	out := make([]uint64, chunksPerPage)
	headers := make([]*metadata.PayloadEncryptionHeader, chunksPerPage)
	pageStartChunk := page.PageNo * uint64(chunksPerPage)
	pageEndChunk := pageStartChunk + uint64(chunksPerPage)
	for _, extent := range page.Extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if start < pageStartChunk || end > pageEndChunk {
			return nil, nil, fmt.Errorf("allocation extent out of page bounds: page_no=%d start=%d end=%d", page.PageNo, start, end)
		}
		for logicalChunk := start; logicalChunk < end; logicalChunk++ {
			index := logicalChunk - pageStartChunk
			if extent.Kind == metadata.AllocationKindData {
				out[index] = extent.PhysicalChunkStart + (logicalChunk - start)
				headers[index] = cloneCommitPayloadEncryptionHeader(extent.Encryption)
				continue
			}
			out[index] = 0
			headers[index] = nil
		}
	}
	return out, headers, nil
}

func compressAllocationCommitChunkMappings(pageStartChunk uint64, physicalChunkIDs []uint64, encryptionHeaders []*metadata.PayloadEncryptionHeader) []metadata.AllocationExtentRecord {
	if len(physicalChunkIDs) == 0 {
		return nil
	}
	out := make([]metadata.AllocationExtentRecord, 0, len(physicalChunkIDs))
	for i := 0; i < len(physicalChunkIDs); {
		logicalStart := pageStartChunk + uint64(i)
		physicalStart := physicalChunkIDs[i]
		if physicalStart == 0 {
			j := i + 1
			for j < len(physicalChunkIDs) && physicalChunkIDs[j] == 0 {
				j++
			}
			out = append(out, metadata.AllocationExtentRecord{
				LogicalChunkStart: logicalStart,
				ChunkCount:        uint32(j - i),
				Kind:              metadata.AllocationKindZero,
			})
			i = j
			continue
		}
		j := i + 1
		header := commitPayloadEncryptionHeaderAt(encryptionHeaders, i)
		for j < len(physicalChunkIDs) && physicalChunkIDs[j] == physicalStart+uint64(j-i) && header == nil && commitPayloadEncryptionHeaderAt(encryptionHeaders, j) == nil {
			j++
		}
		extent := metadata.AllocationExtentRecord{
			LogicalChunkStart:  logicalStart,
			ChunkCount:         uint32(j - i),
			Kind:               metadata.AllocationKindData,
			PhysicalChunkStart: physicalStart,
			Encryption:         cloneCommitPayloadEncryptionHeader(header),
		}
		out = append(out, extent)
		i = j
	}
	return out
}

func commitPayloadEncryptionHeaderAt(headers []*metadata.PayloadEncryptionHeader, index int) *metadata.PayloadEncryptionHeader {
	if index < 0 || index >= len(headers) {
		return nil
	}
	return headers[index]
}

func cloneCommitPayloadEncryptionHeader(header *metadata.PayloadEncryptionHeader) *metadata.PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	cloned := *header
	return &cloned
}

func allocationCommitAffectedPageChunkRange(page metadata.AllocationPageRecord, writeStart, writeLength uint64) (metadata.AllocationPageChunkRangeRecord, bool, error) {
	if writeLength == 0 {
		return metadata.AllocationPageChunkRangeRecord{}, false, nil
	}
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return metadata.AllocationPageChunkRangeRecord{}, false, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	pageStart := page.PageNo * uint64(page.PageBytes)
	pageEnd := pageStart + uint64(page.PageBytes)
	writeEnd := writeStart + writeLength
	if writeEnd <= pageStart || writeStart >= pageEnd {
		return metadata.AllocationPageChunkRangeRecord{}, false, nil
	}
	overlapStart := maxUint64(writeStart, pageStart)
	overlapEnd := minUint64(writeEnd, pageEnd)
	chunkSize := uint64(page.ChunkSizeBytes)
	startChunk := (overlapStart - pageStart) / chunkSize
	endChunk := (overlapEnd - pageStart + chunkSize - 1) / chunkSize
	return metadata.AllocationPageChunkRangeRecord{
		PageNo:     page.PageNo,
		StartChunk: startChunk,
		EndChunk:   endChunk,
	}, true, nil
}

func mergeAllocationCommitPageChunkRanges(ranges []metadata.AllocationPageChunkRangeRecord) []metadata.AllocationPageChunkRangeRecord {
	if len(ranges) == 0 {
		return nil
	}
	out := append([]metadata.AllocationPageChunkRangeRecord(nil), ranges...)
	slices.SortFunc(out, func(a, b metadata.AllocationPageChunkRangeRecord) int {
		switch {
		case a.PageNo < b.PageNo:
			return -1
		case a.PageNo > b.PageNo:
			return 1
		case a.StartChunk < b.StartChunk:
			return -1
		case a.StartChunk > b.StartChunk:
			return 1
		case a.EndChunk < b.EndChunk:
			return -1
		case a.EndChunk > b.EndChunk:
			return 1
		default:
			return 0
		}
	})
	merged := out[:0]
	for _, rng := range out {
		if len(merged) == 0 || rng.PageNo != merged[len(merged)-1].PageNo || rng.StartChunk > merged[len(merged)-1].EndChunk {
			merged = append(merged, rng)
			continue
		}
		if rng.EndChunk > merged[len(merged)-1].EndChunk {
			merged[len(merged)-1].EndChunk = rng.EndChunk
		}
	}
	return merged
}

func physicalChunkIDForCommittedWrite(plan ExtentWritePlan, logicalChunk uint64) (uint64, bool) {
	if plan.ChunkSizeBytes > 0 && len(plan.AllocationPages) > 0 {
		covered, _, physicalChunkID, _ := logicalChunkAllocationState(plan.AllocationPages, logicalChunk)
		if covered && physicalChunkID != 0 {
			return physicalChunkID, true
		}
	}
	if plan.Extent.ChunkID == 0 || plan.ChunkSizeBytes == 0 {
		return 0, false
	}
	extentStartChunk := plan.Extent.LogicalOffset / uint64(plan.ChunkSizeBytes)
	if logicalChunk < extentStartChunk {
		return 0, false
	}
	return plan.Extent.ChunkID + (logicalChunk - extentStartChunk), true
}
