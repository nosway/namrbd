package replication

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type pendingAllocatedChunk struct {
	state        *allocationCommitPageState
	logicalChunk uint64
}

func (e *Executor) prepareWritePlanAllocations(ctx context.Context, plan *WritePlan, req BeginWriteRequest) error {
	if plan == nil {
		return nil
	}

	pageStates := make(map[uint64]*allocationCommitPageState)
	pending := make([]pendingAllocatedChunk, 0)

	for extentIndex := range plan.Extents {
		extent := &plan.Extents[extentIndex]
		if extent.ChunkSizeBytes == 0 || len(extent.AllocationPages) == 0 {
			continue
		}
		writeStart, writeLength, err := overlapRange(extent.Extent.LogicalOffset, extent.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
		if err != nil || writeLength == 0 {
			continue
		}
		chunkSize := uint64(extent.ChunkSizeBytes)
		writeEnd := writeStart + writeLength
		startChunk := writeStart / chunkSize
		endChunk := (writeEnd - 1) / chunkSize

		for _, resolvedPage := range extent.AllocationPages {
			if _, ok := pageStates[resolvedPage.Page.PageNo]; ok {
				continue
			}
			state, err := newAllocationCommitPageState(resolvedPage.Page)
			if err != nil {
				return err
			}
			pageStates[resolvedPage.Page.PageNo] = state
		}

		for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
			chunkStart := logicalChunk * chunkSize
			chunkEnd := chunkStart + chunkSize
			if req.ZeroSemantic && writeStart <= chunkStart && writeEnd >= chunkEnd {
				continue
			}
			state, pageStartChunk, ok := allocationPlanStateForLogicalChunk(pageStates, extent.AllocationPages, logicalChunk)
			if !ok {
				continue
			}
			pageIndex := logicalChunk - pageStartChunk
			if state.physicalChunkIDs[pageIndex] != 0 && !extent.CopyOnWrite {
				continue
			}
			pending = append(pending, pendingAllocatedChunk{
				state:        state,
				logicalChunk: logicalChunk,
			})
		}
	}

	if len(pending) == 0 {
		return nil
	}

	if e.allocator == nil {
		return fmt.Errorf("chunk id allocator is not configured")
	}
	startChunkID, err := e.allocator.AllocateChunkIDs(ctx, req.VolumeID, uint32(len(pending)))
	if err != nil {
		return err
	}
	for i, allocated := range pending {
		chunksPerPage := uint64(allocated.state.pageBytes / allocated.state.chunkSizeBytes)
		pageStartChunk := allocated.state.pageNo * chunksPerPage
		pageIndex := allocated.logicalChunk - pageStartChunk
		allocated.state.physicalChunkIDs[pageIndex] = startChunkID + uint64(i)
	}

	for extentIndex := range plan.Extents {
		extent := &plan.Extents[extentIndex]
		for pageIndex, resolvedPage := range extent.AllocationPages {
			state, ok := pageStates[resolvedPage.Page.PageNo]
			if !ok {
				continue
			}
			extent.AllocationPages[pageIndex].Page = state.toRecord()
		}
	}

	return nil
}

func allocationPlanStateForLogicalChunk(pageStates map[uint64]*allocationCommitPageState, pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (*allocationCommitPageState, uint64, bool) {
	for _, page := range pages {
		if logicalChunk < page.RangeStartChunk || logicalChunk >= page.RangeEndChunk {
			continue
		}
		state, ok := pageStates[page.Page.PageNo]
		if !ok {
			return nil, 0, false
		}
		chunksPerPage := uint64(state.pageBytes / state.chunkSizeBytes)
		pageStartChunk := state.pageNo * chunksPerPage
		if logicalChunk < pageStartChunk || logicalChunk >= pageStartChunk+uint64(len(state.physicalChunkIDs)) {
			return nil, 0, false
		}
		return state, pageStartChunk, true
	}
	return nil, 0, false
}
