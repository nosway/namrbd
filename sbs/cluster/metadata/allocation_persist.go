package metadata

import (
	"context"
	"fmt"
	"sort"
)

type AllocationPersistStore interface {
	PutAllocationPage(ctx context.Context, rec AllocationPageRecord) error
}

type ZeroAllocationPersistSpec struct {
	VolumeID        string
	SizeBytes       uint64
	ChunkSizeBytes  uint32
	ExtentPageBytes uint32
	Revision        uint64
}

func PersistAllocationPages(ctx context.Context, store AllocationPersistStore, revision uint64, pages []AllocationPageRecord) error {
	if len(pages) == 0 {
		return nil
	}
	ordered := append([]AllocationPageRecord(nil), pages...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PageNo < ordered[j].PageNo })
	for _, page := range ordered {
		page.Revision = revision
		if err := store.PutAllocationPage(ctx, page); err != nil {
			return err
		}
	}
	return nil
}

func PersistZeroAllocationPages(ctx context.Context, store AllocationPersistStore, spec ZeroAllocationPersistSpec) error {
	if spec.SizeBytes == 0 {
		return nil
	}
	if spec.ChunkSizeBytes == 0 || spec.ExtentPageBytes == 0 || spec.ExtentPageBytes%spec.ChunkSizeBytes != 0 {
		return fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", spec.ExtentPageBytes, spec.ChunkSizeBytes)
	}
	revision := spec.Revision
	if revision == 0 {
		revision = 1
	}
	pageBytes := uint64(spec.ExtentPageBytes)
	chunkSizeBytes := uint64(spec.ChunkSizeBytes)
	chunksPerPage := pageBytes / chunkSizeBytes
	pageCount := (spec.SizeBytes + pageBytes - 1) / pageBytes
	pages := make([]AllocationPageRecord, 0, pageCount)
	for pageNo := uint64(0); pageNo < pageCount; pageNo++ {
		pageStart := pageNo * pageBytes
		coveredBytes := minUint64(pageBytes, spec.SizeBytes-pageStart)
		chunkCount := uint32((coveredBytes + chunkSizeBytes - 1) / chunkSizeBytes)
		if chunkCount == 0 {
			continue
		}
		pages = append(pages, AllocationPageRecord{
			VolumeID:       spec.VolumeID,
			PageNo:         pageNo,
			PageBytes:      spec.ExtentPageBytes,
			ChunkSizeBytes: spec.ChunkSizeBytes,
			Extents: []AllocationExtentRecord{
				{
					LogicalChunkStart:  pageNo * chunksPerPage,
					ChunkCount:         chunkCount,
					Kind:               AllocationKindZero,
					PhysicalChunkStart: 0,
				},
			},
		})
	}
	return PersistAllocationPages(ctx, store, revision, pages)
}
