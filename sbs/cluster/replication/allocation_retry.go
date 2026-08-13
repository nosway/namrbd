package replication

import (
	"context"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

// rebaseWritePlanForRetry refreshes allocation pages from the latest metadata
// view while preserving the physical chunk ids already assigned to this write.
func (e *Executor) rebaseWritePlanForRetry(ctx context.Context, exec *WriteExecution, req WriteRequest) error {
	if e == nil || exec == nil || len(exec.Extents) == 0 {
		return nil
	}

	state, err := e.intents.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return err
	}
	exec.MetadataEpoch = state.Epoch
	exec.MetadataRevision = state.Revision
	latestPlan, err := e.planner.PlanWrite(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	if err != nil {
		return err
	}

	latestByExtentID := make(map[uint64]ExtentWritePlan, len(latestPlan.Extents))
	for _, extent := range latestPlan.Extents {
		latestByExtentID[extent.Extent.ExtentID] = extent
	}

	for i := range exec.Extents {
		current := &exec.Extents[i].Plan
		latest, ok := latestByExtentID[current.Extent.ExtentID]
		if !ok {
			continue
		}
		mergedPages, err := mergedRetryAllocationPages(*current, latest, req, exec.Extents[i].ChunkEncryptionHeaders)
		if err != nil {
			return err
		}
		current.Extent = latest.Extent
		current.MetadataRevision = latest.MetadataRevision
		current.ChunkSizeBytes = latest.ChunkSizeBytes
		current.AllocationPages = mergedPages
	}

	return nil
}

func mergedRetryAllocationPages(current, latest ExtentWritePlan, req WriteRequest, encryptionHeaders map[uint64]*metadata.PayloadEncryptionHeader) ([]metadata.ResolvedAllocationPage, error) {
	if len(latest.AllocationPages) == 0 {
		return nil, nil
	}
	if len(current.AllocationPages) == 0 || current.ChunkSizeBytes == 0 {
		return cloneResolvedAllocationPages(latest.AllocationPages), nil
	}

	writeStart, writeLength, err := overlapRange(current.Extent.LogicalOffset, current.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil || writeLength == 0 {
		return cloneResolvedAllocationPages(latest.AllocationPages), nil
	}

	out := cloneResolvedAllocationPages(latest.AllocationPages)
	for i := range out {
		state, err := newAllocationCommitPageState(out[i].Page)
		if err != nil {
			return nil, err
		}
		if err := state.applyWrite(current, req, writeStart, writeLength, encryptionHeaders); err != nil {
			return nil, err
		}
		out[i].Page = state.toRecord()
	}
	return out, nil
}

func cloneResolvedAllocationPages(in []metadata.ResolvedAllocationPage) []metadata.ResolvedAllocationPage {
	if len(in) == 0 {
		return nil
	}
	out := make([]metadata.ResolvedAllocationPage, len(in))
	copy(out, in)
	for i := range out {
		page := out[i].Page
		page.Extents = append([]metadata.AllocationExtentRecord(nil), page.Extents...)
		out[i].Page = page
	}
	return out
}
