package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/store"
)

const chunkExtentWriteRetryLimit = 8
const chunkExtentWritePageLockStripes = 128

type PhysicalChunkPlanner interface {
	ChoosePhysicalChunkRef(volume VolumeSpec, logicalChunk, physicalChunkID uint64) (PhysicalChunkRef, error)
}

type chunkExtentDataRepository struct {
	meta    MetadataRepository
	chunks  chunkPayloadStore
	planner PhysicalChunkPlanner
	locks   [chunkExtentWritePageLockStripes]sync.Mutex
}

type chunkPayloadStore interface {
	ReadChunk(ctx context.Context, volume VolumeSpec, chunkID uint64) ([]byte, error)
	ReadChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef) ([]byte, error)
	WriteChunk(ctx context.Context, volume VolumeSpec, chunkID uint64, data []byte) error
	WriteChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef, data []byte) error
	DeleteChunkRef(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef) error
}

type chunkPayloadEncryptionHeaderStore interface {
	ReadChunkRefWithEncryptionHeader(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef, expected *ChunkPayloadEncryptionHeader) ([]byte, error)
	WriteChunkRefWithEncryptionHeader(ctx context.Context, volume VolumeSpec, ref PhysicalChunkRef, data []byte) (*ChunkPayloadEncryptionHeader, error)
}

type allocationChunkMapping struct {
	Ref               PhysicalChunkRef
	PayloadEncryption *ChunkPayloadEncryptionHeader
}

func NewChunkExtentDataRepository(meta MetadataRepository, objects store.ObjectStore) DataRepository {
	return NewChunkExtentDataRepositoryWithPlanner(meta, objects, nil)
}

func NewChunkExtentDataRepositoryWithPlanner(meta MetadataRepository, objects store.ObjectStore, planner PhysicalChunkPlanner) DataRepository {
	return &chunkExtentDataRepository{
		meta:    meta,
		chunks:  newChunkPayloadRepository(objects),
		planner: planner,
	}
}

func (r *chunkExtentDataRepository) CloseAttachment(context.Context, uint64, AttachmentRecord) error {
	return nil
}

func (r *chunkExtentDataRepository) ReadAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) ([]byte, error) {
	volume = NormalizeVolumeSpec(volume)
	if lengthBytes == 0 {
		return nil, nil
	}

	out := make([]byte, lengthBytes)
	chunkSize := uint64(volume.ChunkSizeBytes)
	pageBytes := uint64(volume.ExtentPageBytes)
	chunksPerPage := pageBytes / chunkSize

	startChunk := offsetBytes / chunkSize
	endChunk := (offsetBytes + lengthBytes - 1) / chunkSize

	pageCache := make(map[uint64]AllocationPageRecord)
	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		pageNo := logicalChunk / chunksPerPage
		page, ok := pageCache[pageNo]
		if !ok {
			var err error
			page, err = r.meta.GetExtentPage(ctx, uint64(volume.ID), pageNo)
			if err != nil {
				return nil, err
			}
			pageCache[pageNo] = page
		}

		chunkData, err := r.readLogicalChunk(ctx, volume, page, logicalChunk)
		if err != nil {
			return nil, err
		}

		chunkStartOffset := logicalChunk * chunkSize
		copyStart := maxUint64(offsetBytes, chunkStartOffset)
		copyEnd := minUint64(offsetBytes+lengthBytes, chunkStartOffset+chunkSize)
		outStart := copyStart - offsetBytes
		chunkOffset := copyStart - chunkStartOffset
		copy(out[outStart:outStart+(copyEnd-copyStart)], chunkData[chunkOffset:chunkOffset+(copyEnd-copyStart)])
	}

	return out, nil
}

func (r *chunkExtentDataRepository) WriteAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) error {
	_, err := r.WriteAtWithStats(ctx, volume, offsetBytes, lengthBytes, data)
	return err
}

func (r *chunkExtentDataRepository) WriteAtWithStats(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) (DataWriteStats, error) {
	var stats DataWriteStats
	volume = NormalizeVolumeSpec(volume)
	if lengthBytes == 0 {
		return stats, nil
	}

	chunkSize := uint64(volume.ChunkSizeBytes)
	pageBytes := uint64(volume.ExtentPageBytes)
	chunksPerPage := pageBytes / chunkSize

	startChunk := offsetBytes / chunkSize
	endChunk := (offsetBytes + lengthBytes - 1) / chunkSize
	startPage := startChunk / chunksPerPage
	endPage := endChunk / chunksPerPage

	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		stats.Pages++
		if err := r.writePage(ctx, volume, pageNo, offsetBytes, lengthBytes, data, &stats); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (r *chunkExtentDataRepository) ReadPhysicalChunkAt(ctx context.Context, volume VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64) ([]byte, error) {
	volume = NormalizeVolumeSpec(volume)
	chunkSize := uint64(volume.ChunkSizeBytes)
	if physicalChunkID == 0 || chunkOffsetBytes >= chunkSize || chunkOffsetBytes+lengthBytes > chunkSize {
		return nil, ErrOutOfRange
	}
	if lengthBytes == 0 {
		return nil, ErrBadAlignment
	}
	chunk, err := r.chunks.ReadChunk(ctx, volume, physicalChunkID)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), chunk[chunkOffsetBytes:chunkOffsetBytes+lengthBytes]...), nil
}

func (r *chunkExtentDataRepository) WritePhysicalChunkAt(ctx context.Context, volume VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64, data []byte) error {
	_, err := r.WritePhysicalChunkAtWithStats(ctx, volume, physicalChunkID, chunkOffsetBytes, lengthBytes, data)
	return err
}

func (r *chunkExtentDataRepository) WritePhysicalChunkAtWithStats(ctx context.Context, volume VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64, data []byte) (PhysicalChunkWriteStats, error) {
	var stats PhysicalChunkWriteStats
	volume = NormalizeVolumeSpec(volume)
	chunkSize := uint64(volume.ChunkSizeBytes)
	if physicalChunkID == 0 || chunkOffsetBytes >= chunkSize || chunkOffsetBytes+lengthBytes > chunkSize {
		return stats, ErrOutOfRange
	}
	if lengthBytes == 0 {
		return stats, ErrBadAlignment
	}
	if uint64(len(data)) != lengthBytes {
		return stats, ErrBadDataLength
	}
	fullChunkOverwrite := chunkOffsetBytes == 0 && lengthBytes == chunkSize
	phaseStart := time.Now()
	chunk := make([]byte, chunkSize)
	if fullChunkOverwrite {
		copy(chunk, data)
	} else {
		var err error
		chunk, err = r.chunks.ReadChunk(ctx, volume, physicalChunkID)
		stats.ChunkReadDuration += time.Since(phaseStart)
		stats.ChunksRead++
		if err != nil {
			return stats, err
		}
		copy(chunk[chunkOffsetBytes:chunkOffsetBytes+lengthBytes], data)
	}
	phaseStart = time.Now()
	err := r.chunks.WriteChunk(ctx, volume, physicalChunkID, chunk)
	stats.ChunkPayloadDuration += time.Since(phaseStart)
	if err != nil {
		return stats, err
	}
	stats.ChunksWritten++
	if fullChunkOverwrite {
		stats.FullChunkOverwrites++
	}
	return stats, nil
}

func (r *chunkExtentDataRepository) FlushVolume(_ context.Context, _ VolumeSpec) error {
	return nil
}

func (r *chunkExtentDataRepository) DiscardAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error {
	if !DiscardRangeAligned(volume, offsetBytes, lengthBytes) {
		return NewDiscardAlignmentError(volume, offsetBytes, lengthBytes)
	}
	zeroes := make([]byte, lengthBytes)
	return r.WriteAt(ctx, volume, offsetBytes, lengthBytes, zeroes)
}

func (r *chunkExtentDataRepository) DiscardObservationFor(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	volume = NormalizeVolumeSpec(volume)
	if !reclaimAligned(volume, offsetBytes, lengthBytes) {
		return NewDiscardAlignmentZeroFallbackObservation(volume, offsetBytes, lengthBytes)
	}
	if volume.RedundancyBackend == RedundancyBackendReplicated {
		return NewDiscardTrueReclaimObservation(volume, offsetBytes, lengthBytes)
	}
	return NewDiscardZeroFallbackObservation(volume, offsetBytes, lengthBytes)
}

func (r *chunkExtentDataRepository) ZeroAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error {
	zeroes := make([]byte, lengthBytes)
	return r.WriteAt(ctx, volume, offsetBytes, lengthBytes, zeroes)
}

func (r *chunkExtentDataRepository) readLogicalChunk(ctx context.Context, volume VolumeSpec, page AllocationPageRecord, logicalChunk uint64) ([]byte, error) {
	chunkSize := int(volume.ChunkSizeBytes)
	if chunkSize <= 0 {
		chunkSize = DefaultAllocationChunkSize
	}
	extent, ok := findAllocationChunk(page.Extents, logicalChunk)
	if !ok || extent.Kind == AllocationChunkKindZero {
		return make([]byte, chunkSize), nil
	}
	ref := extent.chunkRef(logicalChunk)
	if extent.PayloadEncryption != nil {
		headerStore, ok := r.chunks.(chunkPayloadEncryptionHeaderStore)
		if !ok {
			return nil, fmt.Errorf("encrypted chunk metadata present but payload store does not support encryption headers")
		}
		if err := extent.PayloadEncryption.ValidateForChunk(volume, ref); err != nil {
			return nil, fmt.Errorf("validate encrypted chunk metadata: %w", err)
		}
		return headerStore.ReadChunkRefWithEncryptionHeader(ctx, volume, ref, extent.PayloadEncryption)
	}
	return r.chunks.ReadChunkRef(ctx, volume, ref)
}

func (r *chunkExtentDataRepository) writePage(ctx context.Context, volume VolumeSpec, pageNo, offsetBytes, lengthBytes uint64, data []byte, stats *DataWriteStats) error {
	lock := r.writePageLock(uint64(volume.ID), pageNo)
	chunkSize := uint64(volume.ChunkSizeBytes)
	pageBytes := uint64(volume.ExtentPageBytes)
	chunksPerPage := pageBytes / chunkSize
	pageStartChunk := pageNo * chunksPerPage
	pageChunkCount := logicalChunkCountInPage(volume, pageNo)
	if pageChunkCount == 0 {
		return nil
	}

	pageStartOffset := pageStartChunk * chunkSize
	pageEndOffset := pageStartOffset + uint64(pageChunkCount)*chunkSize
	writeStart := maxUint64(offsetBytes, pageStartOffset)
	writeEnd := minUint64(offsetBytes+lengthBytes, pageEndOffset)
	if writeStart >= writeEnd {
		return nil
	}

	type pendingChunk struct {
		pageIndex int
		payload   []byte
		ref       PhysicalChunkRef
	}

	recordGarbage := func(refs map[PhysicalChunkRef]struct{}) error {
		for ref := range refs {
			phaseStart := time.Now()
			if err := r.meta.PutChunkGarbage(ctx, AllocationChunkGarbageRecord{
				VolumeID: volume.ID,
				StoreID:  ref.StoreID,
				ShardID:  ref.ShardID,
				ChunkID:  ref.ChunkID,
			}); err != nil {
				return err
			}
			stats.ChunkGarbageDuration += time.Since(phaseStart)
			stats.ChunkGarbageRecordsPut++
		}
		return nil
	}

	startIndex := int((writeStart - pageStartOffset) / chunkSize)
	endIndex := int((writeEnd - pageStartOffset - 1) / chunkSize)

	for attempt := 0; attempt < chunkExtentWriteRetryLimit; attempt++ {
		stats.Attempts++

		lockWaitStart := time.Now()
		lock.Lock()
		stats.PageLockWaitDuration += time.Since(lockWaitStart)
		phaseStart := time.Now()
		page, err := r.meta.GetExtentPage(ctx, uint64(volume.ID), pageNo)
		stats.ExtentPageGetDuration += time.Since(phaseStart)
		if err != nil {
			lock.Unlock()
			return err
		}

		currentMappings := buildAllocationChunkMappings(page, pageStartChunk, pageChunkCount)
		nextMappings := append([]allocationChunkMapping(nil), currentMappings...)
		staleChunkRefs := make(map[PhysicalChunkRef]struct{})
		pending := make([]pendingChunk, 0)
		fullChunkMergeable := true

		for pageIndex := startIndex; pageIndex <= endIndex; pageIndex++ {
			logicalChunk := pageStartChunk + uint64(pageIndex)
			chunkStartOffset := logicalChunk * chunkSize
			copyStart := maxUint64(offsetBytes, chunkStartOffset)
			copyEnd := minUint64(offsetBytes+lengthBytes, chunkStartOffset+chunkSize)
			if copyStart >= copyEnd {
				continue
			}

			srcStart := copyStart - offsetBytes
			copyLength := copyEnd - copyStart
			var chunkData []byte
			fullOverwrite := copyStart == chunkStartOffset && copyEnd == chunkStartOffset+chunkSize
			if fullOverwrite {
				chunkData = append([]byte(nil), data[srcStart:srcStart+copyLength]...)
				stats.FullChunkOverwrites++
			} else {
				fullChunkMergeable = false
				phaseStart = time.Now()
				var err error
				chunkData, err = r.readChunkByMapping(ctx, volume, currentMappings[pageIndex])
				stats.ChunkReadDuration += time.Since(phaseStart)
				stats.ChunksRead++
				if err != nil {
					lock.Unlock()
					return err
				}
				chunkOffset := copyStart - chunkStartOffset
				copy(chunkData[chunkOffset:chunkOffset+copyLength], data[srcStart:srcStart+copyLength])
			}

			if isAllZero(chunkData) {
				fullChunkMergeable = false
				if currentMappings[pageIndex].Ref.ChunkID != 0 {
					staleChunkRefs[currentMappings[pageIndex].Ref] = struct{}{}
				}
				nextMappings[pageIndex] = allocationChunkMapping{}
				continue
			}
			pending = append(pending, pendingChunk{pageIndex: pageIndex, payload: chunkData})
		}
		lock.Unlock()

		newChunkRefs := make(map[PhysicalChunkRef]struct{})

		if len(pending) > 0 {
			phaseStart = time.Now()
			startChunkID, err := r.meta.AllocateChunkIDs(ctx, uint64(volume.ID), uint32(len(pending)))
			stats.ChunkAllocateDuration += time.Since(phaseStart)
			if err != nil {
				return err
			}
			for i := range pending {
				chunk := &pending[i]
				chunkID := startChunkID + uint64(i)
				ref, err := r.choosePhysicalChunkRef(volume, pageStartChunk+uint64(chunk.pageIndex), chunkID)
				if err != nil {
					if garbageErr := recordGarbage(newChunkRefs); garbageErr != nil {
						return garbageErr
					}
					return err
				}
				chunk.ref = ref
				phaseStart = time.Now()
				var payloadEncryption *ChunkPayloadEncryptionHeader
				if headerStore, ok := r.chunks.(chunkPayloadEncryptionHeaderStore); ok {
					payloadEncryption, err = headerStore.WriteChunkRefWithEncryptionHeader(ctx, volume, ref, chunk.payload)
				} else {
					err = r.chunks.WriteChunkRef(ctx, volume, ref, chunk.payload)
				}
				if err != nil {
					if garbageErr := recordGarbage(newChunkRefs); garbageErr != nil {
						return garbageErr
					}
					return err
				}
				stats.ChunkPayloadDuration += time.Since(phaseStart)
				stats.ChunksWritten++
				newChunkRefs[ref] = struct{}{}
				if currentMappings[chunk.pageIndex].Ref.ChunkID != 0 && currentMappings[chunk.pageIndex].Ref != ref {
					staleChunkRefs[currentMappings[chunk.pageIndex].Ref] = struct{}{}
				}
				nextMappings[chunk.pageIndex] = allocationChunkMapping{Ref: ref, PayloadEncryption: payloadEncryption}
			}
		}

		lockWaitStart = time.Now()
		lock.Lock()
		stats.PageLockWaitDuration += time.Since(lockWaitStart)
		phaseStart = time.Now()
		commitPage, err := r.meta.GetExtentPage(ctx, uint64(volume.ID), pageNo)
		stats.ExtentPageGetDuration += time.Since(phaseStart)
		if err != nil {
			lock.Unlock()
			return err
		}
		commitMappings := nextMappings
		commitRevision := page.Revision
		commitStaleChunkRefs := staleChunkRefs
		if commitPage.Revision != page.Revision {
			if !fullChunkMergeable || len(pending) == 0 {
				lock.Unlock()
				if err := recordGarbage(newChunkRefs); err != nil {
					return err
				}
				continue
			}
			latestMappings := buildAllocationChunkMappings(commitPage, pageStartChunk, pageChunkCount)
			mergedStaleChunkRefs := make(map[PhysicalChunkRef]struct{})
			for _, chunk := range pending {
				currentRef := latestMappings[chunk.pageIndex].Ref
				if currentRef.ChunkID != 0 && currentRef != chunk.ref {
					mergedStaleChunkRefs[currentRef] = struct{}{}
				}
				latestMappings[chunk.pageIndex] = nextMappings[chunk.pageIndex]
			}
			commitMappings = latestMappings
			commitRevision = commitPage.Revision
			commitStaleChunkRefs = mergedStaleChunkRefs
		}

		updatedPage := AllocationPageRecord{
			VolumeID:       volume.ID,
			PageNo:         pageNo,
			PageBytes:      volume.ExtentPageBytes,
			ChunkSizeBytes: volume.ChunkSizeBytes,
			Extents:        compressAllocationChunkMappings(pageStartChunk, commitMappings),
		}
		phaseStart = time.Now()
		if _, err := r.meta.PutExtentPage(ctx, updatedPage, commitRevision); err != nil {
			stats.ExtentPagePutDuration += time.Since(phaseStart)
			lock.Unlock()
			if err == ErrMetadataCASConflict {
				if err := recordGarbage(newChunkRefs); err != nil {
					return err
				}
				continue
			}
			return err
		}
		stats.ExtentPagePutDuration += time.Since(phaseStart)
		lock.Unlock()

		if err := recordGarbage(commitStaleChunkRefs); err != nil {
			return err
		}
		return nil
	}

	return ErrMetadataCASConflict
}

func (r *chunkExtentDataRepository) writePageLock(volumeID, pageNo uint64) *sync.Mutex {
	x := volumeID
	x ^= pageNo + 0x9e3779b97f4a7c15 + (x << 6) + (x >> 2)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return &r.locks[x%chunkExtentWritePageLockStripes]
}

func (r *chunkExtentDataRepository) readChunkByMapping(ctx context.Context, volume VolumeSpec, mapping allocationChunkMapping) ([]byte, error) {
	if mapping.Ref.ChunkID == 0 {
		return make([]byte, int(volume.ChunkSizeBytes)), nil
	}
	if mapping.PayloadEncryption != nil {
		headerStore, ok := r.chunks.(chunkPayloadEncryptionHeaderStore)
		if !ok {
			return nil, fmt.Errorf("encrypted chunk metadata present but payload store does not support encryption headers")
		}
		if err := mapping.PayloadEncryption.ValidateForChunk(volume, mapping.Ref); err != nil {
			return nil, fmt.Errorf("validate encrypted chunk metadata: %w", err)
		}
		return headerStore.ReadChunkRefWithEncryptionHeader(ctx, volume, mapping.Ref, mapping.PayloadEncryption)
	}
	return r.chunks.ReadChunkRef(ctx, volume, mapping.Ref)
}

func (r *chunkExtentDataRepository) choosePhysicalChunkRef(volume VolumeSpec, logicalChunk, physicalChunkID uint64) (PhysicalChunkRef, error) {
	if physicalChunkID == 0 {
		return PhysicalChunkRef{}, nil
	}
	if r.planner == nil {
		return PhysicalChunkRef{ChunkID: physicalChunkID}, nil
	}
	ref, err := r.planner.ChoosePhysicalChunkRef(volume, logicalChunk, physicalChunkID)
	if err != nil {
		return PhysicalChunkRef{}, err
	}
	if ref.ChunkID == 0 {
		ref.ChunkID = physicalChunkID
	}
	return ref, nil
}

func findAllocationChunk(extents []AllocationChunkRecord, logicalChunk uint64) (AllocationChunkRecord, bool) {
	for _, extent := range extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if logicalChunk >= start && logicalChunk < end {
			return extent, true
		}
	}
	return AllocationChunkRecord{}, false
}

func buildAllocationChunkMappings(page AllocationPageRecord, pageStartChunk uint64, pageChunkCount int) []allocationChunkMapping {
	mappings := make([]allocationChunkMapping, pageChunkCount)
	for _, extent := range page.Extents {
		if extent.Kind != AllocationChunkKindData {
			continue
		}
		for i := uint32(0); i < extent.ChunkCount; i++ {
			logicalChunk := extent.LogicalChunkStart + uint64(i)
			if logicalChunk < pageStartChunk {
				continue
			}
			pageIndex := logicalChunk - pageStartChunk
			if pageIndex >= uint64(pageChunkCount) {
				break
			}
			mappings[pageIndex] = allocationChunkMapping{
				Ref:               extent.chunkRef(logicalChunk),
				PayloadEncryption: extent.PayloadEncryption,
			}
		}
	}
	return mappings
}

func compressAllocationChunkMappings(pageStartChunk uint64, mappings []allocationChunkMapping) []AllocationChunkRecord {
	if len(mappings) == 0 {
		return nil
	}
	extents := make([]AllocationChunkRecord, 0)
	currentStart := 0
	currentKind := kindForChunkMapping(mappings[0])
	currentRef := mappings[0]

	flush := func(end int) {
		extent := AllocationChunkRecord{
			LogicalChunkStart: pageStartChunk + uint64(currentStart),
			ChunkCount:        uint32(end - currentStart),
			Kind:              currentKind,
		}
		if currentKind == AllocationChunkKindData {
			extent.StoreID = currentRef.Ref.StoreID
			extent.ShardID = currentRef.Ref.ShardID
			extent.PhysicalChunkStart = currentRef.Ref.ChunkID
			extent.PayloadEncryption = cloneChunkPayloadEncryptionHeader(currentRef.PayloadEncryption)
		}
		extents = append(extents, extent)
	}

	for i := 1; i < len(mappings); i++ {
		nextKind := kindForChunkMapping(mappings[i])
		contiguous := currentKind == AllocationChunkKindData &&
			nextKind == AllocationChunkKindData &&
			samePhysicalLocation(currentRef, mappings[i]) &&
			currentRef.PayloadEncryption == nil &&
			mappings[i].PayloadEncryption == nil &&
			mappings[i].Ref.ChunkID == currentRef.Ref.ChunkID+uint64(i-currentStart)
		if nextKind == currentKind && (currentKind == AllocationChunkKindZero || contiguous) {
			continue
		}
		flush(i)
		currentStart = i
		currentKind = nextKind
		currentRef = mappings[i]
	}
	flush(len(mappings))
	return extents
}

func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func kindForChunkMapping(mapping allocationChunkMapping) AllocationChunkKind {
	if mapping.Ref.ChunkID == 0 {
		return AllocationChunkKindZero
	}
	return AllocationChunkKindData
}

func samePhysicalLocation(a, b allocationChunkMapping) bool {
	return a.Ref.StoreID == b.Ref.StoreID && a.Ref.ShardID == b.Ref.ShardID
}

func (r AllocationChunkRecord) chunkRef(logicalChunk uint64) PhysicalChunkRef {
	if logicalChunk < r.LogicalChunkStart {
		return PhysicalChunkRef{}
	}
	offset := logicalChunk - r.LogicalChunkStart
	if offset >= uint64(r.ChunkCount) || r.PhysicalChunkStart == 0 {
		return PhysicalChunkRef{}
	}
	return PhysicalChunkRef{
		StoreID: r.StoreID,
		ShardID: r.ShardID,
		ChunkID: r.PhysicalChunkStart + offset,
	}
}

func (r PhysicalChunkRef) String() string {
	if r.ChunkID == 0 {
		return "zero"
	}
	return fmt.Sprintf("%s/%d/%d", r.StoreID, r.ShardID, r.ChunkID)
}

func logicalChunkCountInPage(volume VolumeSpec, pageNo uint64) int {
	chunkSize := uint64(volume.ChunkSizeBytes)
	pageBytes := uint64(volume.ExtentPageBytes)
	chunksPerPage := pageBytes / chunkSize
	totalChunks := (volume.SizeBytes + chunkSize - 1) / chunkSize
	pageStartChunk := pageNo * chunksPerPage
	if pageStartChunk >= totalChunks {
		return 0
	}
	remaining := totalChunks - pageStartChunk
	if remaining > chunksPerPage {
		remaining = chunksPerPage
	}
	return int(remaining)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
