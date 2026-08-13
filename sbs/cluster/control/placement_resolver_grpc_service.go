package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

const placementResolverServiceOutcomeOK = "ok"

// PlacementResolverOutcomeRecorder records classified internal gRPC service
// outcomes. The caller keeps owning metrics storage.
type PlacementResolverOutcomeRecorder func(class string, duration time.Duration)

// ServeResolveExtentPlacements handles the server side of the internal
// placement resolver gRPC façade.
func ServeResolveExtentPlacements(ctx context.Context, req *internalv1.ResolveExtentPlacementsRequest, resolver PlacementResolverInternalService, record PlacementResolverOutcomeRecorder) (*internalv1.ResolveExtentPlacementsResponse, error) {
	start := time.Now()
	if err := ValidatePlacementResolverRange(req.GetVolumeId(), req.GetOffsetBytes(), req.GetLengthBytes()); err != nil {
		class := ClassifyPlacementResolverError(err)
		duration := time.Since(start)
		recordPlacementResolverOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "placement_resolver_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("method", "resolve_extent_placements"),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("offset_bytes", req.GetOffsetBytes()),
			structuredlog.F("length_bytes", req.GetLengthBytes()),
		)
		return nil, PlacementResolverErrorToGRPCStatus(err)
	}
	if resolver == nil {
		err := fmt.Errorf("placement resolver internal service is required")
		class := PlacementResolverErrorInternal
		duration := time.Since(start)
		recordPlacementResolverOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "placement_resolver_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("method", "resolve_extent_placements"),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("offset_bytes", req.GetOffsetBytes()),
			structuredlog.F("length_bytes", req.GetLengthBytes()),
		)
		return nil, PlacementResolverErrorToGRPCStatus(err)
	}
	resolverStart := time.Now()
	var stats metadata.ResolveExtentPlacementsStats
	var placements []metadata.ResolvedExtentPlacement
	var err error
	if resolverWithStats, ok := resolver.(PlacementResolverInternalServiceWithStats); ok {
		placements, stats, err = resolverWithStats.ResolveExtentPlacementsWithStats(ctx, req.GetVolumeId(), req.GetOffsetBytes(), req.GetLengthBytes())
	} else {
		placements, err = resolver.ResolveExtentPlacements(ctx, req.GetVolumeId(), req.GetOffsetBytes(), req.GetLengthBytes())
	}
	resolverDuration := time.Since(resolverStart)
	if err != nil {
		class := ClassifyPlacementResolverError(err)
		duration := time.Since(start)
		recordPlacementResolverOutcome(record, string(class), duration)
		fields := []structuredlog.Field{
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("method", "resolve_extent_placements"),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("offset_bytes", req.GetOffsetBytes()),
			structuredlog.F("length_bytes", req.GetLengthBytes()),
			structuredlog.F("resolver_duration_ms", resolverDuration.Milliseconds()),
		}
		fields = append(fields, resolveExtentPlacementStatsFields(stats)...)
		structuredlog.Error("sbs.service", "placement_resolver_failed", err, fields...)
		return nil, PlacementResolverErrorToGRPCStatus(err)
	}
	protoStart := time.Now()
	out := make([]*internalv1.ResolvedExtentPlacement, 0, len(placements))
	for _, placement := range placements {
		out = append(out, ResolvedExtentPlacementToProto(placement))
	}
	protoDuration := time.Since(protoStart)
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, placementResolverServiceOutcomeOK, duration)
	fields := []structuredlog.Field{
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_extent_placements"),
		structuredlog.F("volume_id", req.GetVolumeId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("placement_count", len(out)),
		structuredlog.F("resolver_duration_ms", resolverDuration.Milliseconds()),
		structuredlog.F("proto_build_duration_ms", protoDuration.Milliseconds()),
	}
	fields = append(fields, resolveExtentPlacementStatsFields(stats)...)
	structuredlog.Info("sbs.service", "placement_resolver_resolved", fields...)
	return &internalv1.ResolveExtentPlacementsResponse{Placements: out}, nil
}

func resolveExtentPlacementStatsFields(stats metadata.ResolveExtentPlacementsStats) []structuredlog.Field {
	return []structuredlog.Field{
		structuredlog.F("mapping_lookup_duration_ms", stats.MappingLookupDuration.Milliseconds()),
		structuredlog.F("replica_set_lookup_duration_ms", stats.ReplicaSetLookupDuration.Milliseconds()),
		structuredlog.F("node_lookup_duration_ms", stats.NodeLookupDuration.Milliseconds()),
		structuredlog.F("index_build_duration_ms", stats.IndexBuildDuration.Milliseconds()),
		structuredlog.F("range_filter_duration_ms", stats.RangeFilterDuration.Milliseconds()),
		structuredlog.F("mapping_count_total", stats.MappingCountTotal),
		structuredlog.F("mapping_count_selected", stats.MappingCountSelected),
		structuredlog.F("replica_set_count", stats.ReplicaSetCount),
		structuredlog.F("node_count", stats.NodeCount),
	}
}

// ServeResolveAllocationPages handles the server side of the internal
// allocation page resolver gRPC façade.
func ServeResolveAllocationPages(ctx context.Context, req *internalv1.ResolveAllocationPagesRequest, resolver PlacementResolverInternalService, record PlacementResolverOutcomeRecorder) (*internalv1.ResolveAllocationPagesResponse, error) {
	start := time.Now()
	if err := ValidatePlacementResolverRange(req.GetVolumeId(), req.GetOffsetBytes(), req.GetLengthBytes()); err != nil {
		return failResolveAllocationPages(req, record, start, err)
	}
	if err := ValidatePlacementResolverGeometry(req.GetPageBytes(), req.GetChunkSizeBytes()); err != nil {
		return failResolveAllocationPages(req, record, start, err)
	}
	if resolver == nil {
		return failResolveAllocationPages(req, record, start, fmt.Errorf("placement resolver internal service is required"))
	}
	pages, err := resolver.ResolveAllocationPages(ctx, req.GetVolumeId(), req.GetOffsetBytes(), req.GetLengthBytes(), req.GetPageBytes(), req.GetChunkSizeBytes())
	if err != nil {
		return failResolveAllocationPages(req, record, start, err)
	}
	out := make([]*internalv1.ResolvedAllocationPage, 0, len(pages))
	for _, page := range pages {
		out = append(out, ResolvedAllocationPageToProto(page))
	}
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, placementResolverServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "placement_resolver_resolved",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_allocation_pages"),
		structuredlog.F("volume_id", req.GetVolumeId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
		structuredlog.F("allocation_page_count", len(out)),
	)
	return &internalv1.ResolveAllocationPagesResponse{AllocationPages: out}, nil
}

// ServeResolveSnapshotAllocationPages handles the server side of the internal
// snapshot allocation page resolver gRPC façade.
func ServeResolveSnapshotAllocationPages(ctx context.Context, req *internalv1.ResolveSnapshotAllocationPagesRequest, resolver PlacementResolverInternalService, record PlacementResolverOutcomeRecorder) (*internalv1.ResolveSnapshotAllocationPagesResponse, error) {
	start := time.Now()
	if err := ValidatePlacementResolverSnapshotRange(req.GetSnapshotId(), req.GetOffsetBytes(), req.GetLengthBytes()); err != nil {
		return failResolveSnapshotAllocationPages(req, record, start, err)
	}
	if err := ValidatePlacementResolverGeometry(req.GetPageBytes(), req.GetChunkSizeBytes()); err != nil {
		return failResolveSnapshotAllocationPages(req, record, start, err)
	}
	if resolver == nil {
		return failResolveSnapshotAllocationPages(req, record, start, fmt.Errorf("placement resolver internal service is required"))
	}
	pages, err := resolver.ResolveSnapshotAllocationPages(ctx, req.GetSnapshotId(), req.GetOffsetBytes(), req.GetLengthBytes(), req.GetPageBytes(), req.GetChunkSizeBytes())
	if err != nil {
		return failResolveSnapshotAllocationPages(req, record, start, err)
	}
	out := make([]*internalv1.ResolvedAllocationPage, 0, len(pages))
	for _, page := range pages {
		out = append(out, ResolvedAllocationPageToProto(page))
	}
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, placementResolverServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "placement_resolver_resolved",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_snapshot_allocation_pages"),
		structuredlog.F("snapshot_id", req.GetSnapshotId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
		structuredlog.F("allocation_page_count", len(out)),
	)
	return &internalv1.ResolveSnapshotAllocationPagesResponse{AllocationPages: out}, nil
}

// ServeResolveCloneAllocationPages handles the server side of the internal
// clone allocation page resolver gRPC façade.
func ServeResolveCloneAllocationPages(ctx context.Context, req *internalv1.ResolveCloneAllocationPagesRequest, resolver PlacementResolverInternalService, record PlacementResolverOutcomeRecorder) (*internalv1.ResolveCloneAllocationPagesResponse, error) {
	start := time.Now()
	if err := ValidatePlacementResolverCloneRange(req.GetCloneId(), req.GetOffsetBytes(), req.GetLengthBytes()); err != nil {
		return failResolveCloneAllocationPages(req, record, start, err)
	}
	if err := ValidatePlacementResolverGeometry(req.GetPageBytes(), req.GetChunkSizeBytes()); err != nil {
		return failResolveCloneAllocationPages(req, record, start, err)
	}
	if resolver == nil {
		return failResolveCloneAllocationPages(req, record, start, fmt.Errorf("placement resolver internal service is required"))
	}
	pages, err := resolver.ResolveCloneAllocationPages(ctx, req.GetCloneId(), req.GetOffsetBytes(), req.GetLengthBytes(), req.GetPageBytes(), req.GetChunkSizeBytes())
	if err != nil {
		return failResolveCloneAllocationPages(req, record, start, err)
	}
	out := make([]*internalv1.ResolvedAllocationPage, 0, len(pages))
	for _, page := range pages {
		out = append(out, ResolvedAllocationPageToProto(page))
	}
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, placementResolverServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "placement_resolver_resolved",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_clone_allocation_pages"),
		structuredlog.F("clone_id", req.GetCloneId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
		structuredlog.F("allocation_page_count", len(out)),
	)
	return &internalv1.ResolveCloneAllocationPagesResponse{AllocationPages: out}, nil
}

func failResolveAllocationPages(req *internalv1.ResolveAllocationPagesRequest, record PlacementResolverOutcomeRecorder, start time.Time, err error) (*internalv1.ResolveAllocationPagesResponse, error) {
	class := ClassifyPlacementResolverError(err)
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, string(class), duration)
	structuredlog.Error("sbs.service", "placement_resolver_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_allocation_pages"),
		structuredlog.F("volume_id", req.GetVolumeId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
	)
	return nil, PlacementResolverErrorToGRPCStatus(err)
}

func failResolveCloneAllocationPages(req *internalv1.ResolveCloneAllocationPagesRequest, record PlacementResolverOutcomeRecorder, start time.Time, err error) (*internalv1.ResolveCloneAllocationPagesResponse, error) {
	class := ClassifyPlacementResolverError(err)
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, string(class), duration)
	structuredlog.Error("sbs.service", "placement_resolver_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_clone_allocation_pages"),
		structuredlog.F("clone_id", req.GetCloneId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
	)
	return nil, PlacementResolverErrorToGRPCStatus(err)
}

func failResolveSnapshotAllocationPages(req *internalv1.ResolveSnapshotAllocationPagesRequest, record PlacementResolverOutcomeRecorder, start time.Time, err error) (*internalv1.ResolveSnapshotAllocationPagesResponse, error) {
	class := ClassifyPlacementResolverError(err)
	duration := time.Since(start)
	recordPlacementResolverOutcome(record, string(class), duration)
	structuredlog.Error("sbs.service", "placement_resolver_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("method", "resolve_snapshot_allocation_pages"),
		structuredlog.F("snapshot_id", req.GetSnapshotId()),
		structuredlog.F("offset_bytes", req.GetOffsetBytes()),
		structuredlog.F("length_bytes", req.GetLengthBytes()),
		structuredlog.F("page_bytes", req.GetPageBytes()),
		structuredlog.F("chunk_size_bytes", req.GetChunkSizeBytes()),
	)
	return nil, PlacementResolverErrorToGRPCStatus(err)
}

func recordPlacementResolverOutcome(record PlacementResolverOutcomeRecorder, class string, duration time.Duration) {
	if record != nil {
		record(class, duration)
	}
}
