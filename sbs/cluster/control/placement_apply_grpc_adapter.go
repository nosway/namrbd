package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

type GRPCPlacementApplyAdapter struct {
	client internalv1.PlacementApplyServiceClient
}

func NewGRPCPlacementApplyAdapter(client internalv1.PlacementApplyServiceClient) *GRPCPlacementApplyAdapter {
	return &GRPCPlacementApplyAdapter{client: client}
}

func (a *GRPCPlacementApplyAdapter) ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error {
	start := time.Now()
	if a.client == nil {
		err := fmt.Errorf("placement apply grpc client is required")
		logPlacementApplyGRPCFailure(err, PlacementApplyErrorInternal, time.Since(start), req)
		return err
	}
	if err := req.Validate(); err != nil {
		logPlacementApplyGRPCFailure(err, ClassifyPlacementApplyError(err), time.Since(start), req)
		return err
	}
	resp, err := a.client.ApplyPlacementChanges(ctx, PlacementApplyRequestToProto(req))
	if err != nil {
		logPlacementApplyGRPCFailure(err, ClassifyPlacementApplyTransportError(err), time.Since(start), req)
		return PlacementApplyTransportErrorToMetadataError(err)
	}
	if resp.GetVolumeId() != req.VolumeID || resp.GetCommittedRevision() != req.CommittedRevision {
		err := fmt.Errorf("%w: placement apply response identity mismatch: got volume_id=%q committed_revision=%d want volume_id=%q committed_revision=%d",
			metadata.ErrInvalidPlacementApplyRequest,
			resp.GetVolumeId(),
			resp.GetCommittedRevision(),
			req.VolumeID,
			req.CommittedRevision,
		)
		logPlacementApplyGRPCFailure(err, PlacementApplyErrorInvalidArgument, time.Since(start), req)
		return err
	}
	return nil
}

func logPlacementApplyGRPCFailure(err error, class PlacementApplyErrorClass, duration time.Duration, req metadata.PlacementApplyRequest) {
	structuredlog.Error("sbs.cluster.control", "placement_apply_grpc_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(req.NormalizeExtentIDs)),
	)
}
