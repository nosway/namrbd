package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

type GRPCPlacementResolverAdapter struct {
	client internalv1.PlacementResolverServiceClient
}

func NewGRPCPlacementResolverAdapter(client internalv1.PlacementResolverServiceClient) *GRPCPlacementResolverAdapter {
	return &GRPCPlacementResolverAdapter{client: client}
}

func (a *GRPCPlacementResolverAdapter) ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error) {
	start := time.Now()
	if err := ValidatePlacementResolverRange(volumeID, offsetBytes, lengthBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_extent_placements", volumeID, "", offsetBytes, lengthBytes, 0, 0)
		return nil, err
	}
	if a.client == nil {
		err := fmt.Errorf("placement resolver gRPC client is required")
		logPlacementResolverGRPCFailure(err, PlacementResolverErrorInternal, time.Since(start), "resolve_extent_placements", volumeID, "", offsetBytes, lengthBytes, 0, 0)
		return nil, err
	}
	resp, err := a.client.ResolveExtentPlacements(ctx, &internalv1.ResolveExtentPlacementsRequest{
		VolumeId:    volumeID,
		OffsetBytes: offsetBytes,
		LengthBytes: lengthBytes,
	})
	if err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverTransportError(err), time.Since(start), "resolve_extent_placements", volumeID, "", offsetBytes, lengthBytes, 0, 0)
		return nil, err
	}
	out := make([]metadata.ResolvedExtentPlacement, 0, len(resp.GetPlacements()))
	for _, rec := range resp.GetPlacements() {
		placement, err := ResolvedExtentPlacementFromProto(rec)
		if err != nil {
			logPlacementResolverGRPCFailure(err, PlacementResolverErrorInvalidArgument, time.Since(start), "resolve_extent_placements", volumeID, "", offsetBytes, lengthBytes, 0, 0)
			return nil, err
		}
		out = append(out, placement)
	}
	return out, nil
}

func (a *GRPCPlacementResolverAdapter) ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	start := time.Now()
	if err := ValidatePlacementResolverRange(volumeID, offsetBytes, lengthBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_allocation_pages", volumeID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_allocation_pages", volumeID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if a.client == nil {
		err := fmt.Errorf("placement resolver gRPC client is required")
		logPlacementResolverGRPCFailure(err, PlacementResolverErrorInternal, time.Since(start), "resolve_allocation_pages", volumeID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	resp, err := a.client.ResolveAllocationPages(ctx, &internalv1.ResolveAllocationPagesRequest{
		VolumeId:       volumeID,
		OffsetBytes:    offsetBytes,
		LengthBytes:    lengthBytes,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
	})
	if err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverTransportError(err), time.Since(start), "resolve_allocation_pages", volumeID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	out := make([]metadata.ResolvedAllocationPage, 0, len(resp.GetAllocationPages()))
	for _, rec := range resp.GetAllocationPages() {
		page, err := ResolvedAllocationPageFromProto(rec)
		if err != nil {
			logPlacementResolverGRPCFailure(err, PlacementResolverErrorInvalidArgument, time.Since(start), "resolve_allocation_pages", volumeID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
			return nil, err
		}
		out = append(out, page)
	}
	return out, nil
}

func (a *GRPCPlacementResolverAdapter) ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	start := time.Now()
	if err := ValidatePlacementResolverSnapshotRange(snapshotID, offsetBytes, lengthBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_snapshot_allocation_pages", "", snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_snapshot_allocation_pages", "", snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if a.client == nil {
		err := fmt.Errorf("placement resolver gRPC client is required")
		logPlacementResolverGRPCFailure(err, PlacementResolverErrorInternal, time.Since(start), "resolve_snapshot_allocation_pages", "", snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	resp, err := a.client.ResolveSnapshotAllocationPages(ctx, &internalv1.ResolveSnapshotAllocationPagesRequest{
		SnapshotId:     snapshotID,
		OffsetBytes:    offsetBytes,
		LengthBytes:    lengthBytes,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
	})
	if err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverTransportError(err), time.Since(start), "resolve_snapshot_allocation_pages", "", snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	out := make([]metadata.ResolvedAllocationPage, 0, len(resp.GetAllocationPages()))
	for _, rec := range resp.GetAllocationPages() {
		page, err := ResolvedAllocationPageFromProto(rec)
		if err != nil {
			logPlacementResolverGRPCFailure(err, PlacementResolverErrorInvalidArgument, time.Since(start), "resolve_snapshot_allocation_pages", "", snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
			return nil, err
		}
		out = append(out, page)
	}
	return out, nil
}

func (a *GRPCPlacementResolverAdapter) ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	start := time.Now()
	if err := ValidatePlacementResolverCloneRange(cloneID, offsetBytes, lengthBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_clone_allocation_pages", "", cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverError(err), time.Since(start), "resolve_clone_allocation_pages", "", cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	if a.client == nil {
		err := fmt.Errorf("placement resolver gRPC client is required")
		logPlacementResolverGRPCFailure(err, PlacementResolverErrorInternal, time.Since(start), "resolve_clone_allocation_pages", "", cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	resp, err := a.client.ResolveCloneAllocationPages(ctx, &internalv1.ResolveCloneAllocationPagesRequest{
		CloneId:        cloneID,
		OffsetBytes:    offsetBytes,
		LengthBytes:    lengthBytes,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
	})
	if err != nil {
		logPlacementResolverGRPCFailure(err, ClassifyPlacementResolverTransportError(err), time.Since(start), "resolve_clone_allocation_pages", "", cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		return nil, err
	}
	out := make([]metadata.ResolvedAllocationPage, 0, len(resp.GetAllocationPages()))
	for _, rec := range resp.GetAllocationPages() {
		page, err := ResolvedAllocationPageFromProto(rec)
		if err != nil {
			logPlacementResolverGRPCFailure(err, PlacementResolverErrorInvalidArgument, time.Since(start), "resolve_clone_allocation_pages", "", cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
			return nil, err
		}
		out = append(out, page)
	}
	return out, nil
}

func logPlacementResolverGRPCFailure(err error, class PlacementResolverErrorClass, duration time.Duration, method, volumeID, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) {
	structuredlog.Error("sbs.cluster.control", "placement_resolver_grpc_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", method),
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("snapshot_id", snapshotID),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
		structuredlog.F("page_bytes", pageBytes),
		structuredlog.F("chunk_size_bytes", chunkSizeBytes),
	)
}
