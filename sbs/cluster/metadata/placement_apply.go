package metadata

import (
	"context"
	"errors"
	"fmt"
)

var ErrInvalidPlacementApplyRequest = errors.New("invalid placement apply request")

type PlacementApplyService struct {
	store PlacementApplyAuthority
}

func NewPlacementApplyService(store PlacementApplyAuthority) *PlacementApplyService {
	return &PlacementApplyService{store: store}
}

type PlacementApplyRequest struct {
	VolumeID                string
	CommittedRevision       uint64
	AllocationPages         []AllocationPageRecord
	NormalizeExtentIDs      []uint64
	RetiredPhysicalChunkIDs []uint64
}

func (r PlacementApplyRequest) Validate() error {
	if _, err := CanonicalVolumeID(r.VolumeID); err != nil {
		return fmt.Errorf("%w: invalid placement apply volume_id %q: %v", ErrInvalidPlacementApplyRequest, r.VolumeID, err)
	}
	if r.CommittedRevision == 0 {
		return fmt.Errorf("%w: placement apply committed_revision is required", ErrInvalidPlacementApplyRequest)
	}
	for _, page := range r.AllocationPages {
		if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
			return fmt.Errorf("%w: invalid placement apply allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", ErrInvalidPlacementApplyRequest, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
		}
		for _, extent := range page.Extents {
			if extent.ChunkCount == 0 {
				return fmt.Errorf("%w: invalid placement apply allocation extent: page_no=%d logical_chunk_start=%d chunk_count=0", ErrInvalidPlacementApplyRequest, page.PageNo, extent.LogicalChunkStart)
			}
			switch extent.Kind {
			case AllocationKindZero, AllocationKindData, AllocationKindShared:
			default:
				return fmt.Errorf("%w: invalid placement apply allocation extent kind %q: page_no=%d logical_chunk_start=%d", ErrInvalidPlacementApplyRequest, extent.Kind, page.PageNo, extent.LogicalChunkStart)
			}
		}
	}
	return nil
}

func ApplyPlacementChanges(ctx context.Context, store PlacementApplyAuthority, req PlacementApplyRequest) error {
	return NewPlacementApplyService(store).ApplyPlacementChanges(ctx, req)
}

func (s *PlacementApplyService) ApplyPlacementChanges(ctx context.Context, req PlacementApplyRequest) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("placement apply store is required")
	}
	if repo, ok := s.store.(*Repository); ok {
		return repo.applyPlacementChanges(ctx, req)
	}
	if err := req.Validate(); err != nil {
		return err
	}
	pages := placementApplyAllocationPages(req)
	if err := PersistAllocationPages(ctx, s.store, req.CommittedRevision, pages); err != nil {
		return err
	}
	if err := NormalizeExtentMappings(ctx, s.store, req.VolumeID, req.NormalizeExtentIDs, req.CommittedRevision); err != nil {
		return err
	}
	return nil
}

func placementApplyAllocationPages(req PlacementApplyRequest) []AllocationPageRecord {
	pages := append([]AllocationPageRecord(nil), req.AllocationPages...)
	for i := range pages {
		pages[i].VolumeID = req.VolumeID
		pages[i].Extents = append([]AllocationExtentRecord(nil), pages[i].Extents...)
	}
	return pages
}
