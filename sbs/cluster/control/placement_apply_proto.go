package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func PlacementApplyRequestToProto(req metadata.PlacementApplyRequest) *internalv1.ApplyPlacementChangesRequest {
	pages := make([]*internalv1.AllocationPage, 0, len(req.AllocationPages))
	for _, page := range req.AllocationPages {
		extents := make([]*internalv1.AllocationExtent, 0, len(page.Extents))
		for _, extent := range page.Extents {
			extents = append(extents, &internalv1.AllocationExtent{
				LogicalChunkStart:  extent.LogicalChunkStart,
				ChunkCount:         extent.ChunkCount,
				Kind:               placementApplyKindToProto(extent.Kind),
				PhysicalChunkStart: extent.PhysicalChunkStart,
				BackingRef:         extent.BackingRef,
				Generation:         extent.Generation,
				Checksum:           extent.Checksum,
				Encryption:         payloadEncryptionHeaderToProto(extent.Encryption),
			})
		}
		pages = append(pages, &internalv1.AllocationPage{
			VolumeId:       page.VolumeID,
			PageNo:         page.PageNo,
			PageBytes:      page.PageBytes,
			ChunkSizeBytes: page.ChunkSizeBytes,
			Revision:       page.Revision,
			Extents:        extents,
		})
	}
	return &internalv1.ApplyPlacementChangesRequest{
		VolumeId:                req.VolumeID,
		CommittedRevision:       req.CommittedRevision,
		AllocationPages:         pages,
		NormalizeExtentIds:      append([]uint64(nil), req.NormalizeExtentIDs...),
		RetiredPhysicalChunkIds: append([]uint64(nil), req.RetiredPhysicalChunkIDs...),
	}
}

func PlacementApplyRequestFromProto(req *internalv1.ApplyPlacementChangesRequest) (metadata.PlacementApplyRequest, error) {
	if req == nil {
		return metadata.PlacementApplyRequest{}, fmt.Errorf("%w: placement apply proto request is required", metadata.ErrInvalidPlacementApplyRequest)
	}
	pages := make([]metadata.AllocationPageRecord, 0, len(req.GetAllocationPages()))
	for _, page := range req.GetAllocationPages() {
		if page == nil {
			return metadata.PlacementApplyRequest{}, fmt.Errorf("%w: placement apply allocation page is required", metadata.ErrInvalidPlacementApplyRequest)
		}
		extents := make([]metadata.AllocationExtentRecord, 0, len(page.GetExtents()))
		for _, extent := range page.GetExtents() {
			if extent == nil {
				return metadata.PlacementApplyRequest{}, fmt.Errorf("%w: placement apply allocation extent is required", metadata.ErrInvalidPlacementApplyRequest)
			}
			kind, err := placementApplyKindFromProto(extent.GetKind())
			if err != nil {
				return metadata.PlacementApplyRequest{}, err
			}
			extents = append(extents, metadata.AllocationExtentRecord{
				LogicalChunkStart:  extent.GetLogicalChunkStart(),
				ChunkCount:         extent.GetChunkCount(),
				Kind:               kind,
				PhysicalChunkStart: extent.GetPhysicalChunkStart(),
				BackingRef:         extent.GetBackingRef(),
				Generation:         extent.GetGeneration(),
				Checksum:           extent.GetChecksum(),
				Encryption:         payloadEncryptionHeaderFromProto(extent.GetEncryption()),
			})
		}
		pages = append(pages, metadata.AllocationPageRecord{
			VolumeID:       page.GetVolumeId(),
			PageNo:         page.GetPageNo(),
			PageBytes:      page.GetPageBytes(),
			ChunkSizeBytes: page.GetChunkSizeBytes(),
			Revision:       page.GetRevision(),
			Extents:        extents,
		})
	}
	out := metadata.PlacementApplyRequest{
		VolumeID:                req.GetVolumeId(),
		CommittedRevision:       req.GetCommittedRevision(),
		AllocationPages:         pages,
		NormalizeExtentIDs:      append([]uint64(nil), req.GetNormalizeExtentIds()...),
		RetiredPhysicalChunkIDs: append([]uint64(nil), req.GetRetiredPhysicalChunkIds()...),
	}
	if err := out.Validate(); err != nil {
		return metadata.PlacementApplyRequest{}, err
	}
	return out, nil
}

func placementApplyKindToProto(kind metadata.AllocationKind) internalv1.AllocationKind {
	switch kind {
	case metadata.AllocationKindZero:
		return internalv1.AllocationKind_ALLOCATION_KIND_ZERO
	case metadata.AllocationKindData:
		return internalv1.AllocationKind_ALLOCATION_KIND_DATA
	case metadata.AllocationKindShared:
		return internalv1.AllocationKind_ALLOCATION_KIND_SHARED
	default:
		return internalv1.AllocationKind_ALLOCATION_KIND_UNSPECIFIED
	}
}

func placementApplyKindFromProto(kind internalv1.AllocationKind) (metadata.AllocationKind, error) {
	switch kind {
	case internalv1.AllocationKind_ALLOCATION_KIND_ZERO:
		return metadata.AllocationKindZero, nil
	case internalv1.AllocationKind_ALLOCATION_KIND_DATA:
		return metadata.AllocationKindData, nil
	case internalv1.AllocationKind_ALLOCATION_KIND_SHARED:
		return metadata.AllocationKindShared, nil
	default:
		return "", fmt.Errorf("%w: invalid placement apply allocation kind %q", metadata.ErrInvalidPlacementApplyRequest, kind.String())
	}
}
