package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

const placementApplyServiceOutcomeOK = "ok"

// PlacementApplyOutcomeRecorder records classified internal gRPC service
// outcomes. The caller keeps owning metrics storage.
type PlacementApplyOutcomeRecorder func(class string, duration time.Duration)

// ServeApplyPlacementChanges handles the server side of the internal placement
// apply gRPC façade.
func ServeApplyPlacementChanges(ctx context.Context, req *internalv1.ApplyPlacementChangesRequest, service PlacementApplyInternalService, record PlacementApplyOutcomeRecorder) (*internalv1.ApplyPlacementChangesResponse, error) {
	start := time.Now()
	placementReq, err := PlacementApplyRequestFromProto(req)
	if err != nil {
		class := ClassifyPlacementApplyError(err)
		duration := time.Since(start)
		recordPlacementApplyOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "placement_apply_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("committed_revision", req.GetCommittedRevision()),
		)
		return nil, PlacementApplyErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("placement apply internal service is required")
		class := PlacementApplyErrorInternal
		duration := time.Since(start)
		recordPlacementApplyOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "placement_apply_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", placementReq.VolumeID),
			structuredlog.F("committed_revision", placementReq.CommittedRevision),
			structuredlog.F("allocation_page_count", len(placementReq.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(placementReq.NormalizeExtentIDs)),
		)
		return nil, PlacementApplyErrorToGRPCStatus(err)
	}
	if err := service.ApplyPlacementChanges(ctx, placementReq); err != nil {
		class := ClassifyPlacementApplyError(err)
		duration := time.Since(start)
		recordPlacementApplyOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "placement_apply_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", placementReq.VolumeID),
			structuredlog.F("committed_revision", placementReq.CommittedRevision),
			structuredlog.F("allocation_page_count", len(placementReq.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(placementReq.NormalizeExtentIDs)),
		)
		return nil, PlacementApplyErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordPlacementApplyOutcome(record, placementApplyServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "placement_apply_applied",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", placementReq.VolumeID),
		structuredlog.F("committed_revision", placementReq.CommittedRevision),
		structuredlog.F("allocation_page_count", len(placementReq.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(placementReq.NormalizeExtentIDs)),
	)
	return &internalv1.ApplyPlacementChangesResponse{
		VolumeId:          placementReq.VolumeID,
		CommittedRevision: placementReq.CommittedRevision,
	}, nil
}

func recordPlacementApplyOutcome(record PlacementApplyOutcomeRecorder, class string, duration time.Duration) {
	if record != nil {
		record(class, duration)
	}
}
