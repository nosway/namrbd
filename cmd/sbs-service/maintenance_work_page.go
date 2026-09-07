package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maintenanceWorkPageTokenVersion = 1

type maintenanceWorkPageToken struct {
	Version            int    `json:"version"`
	Reason             string `json:"reason"`
	State              string `json:"state"`
	Cursor             string `json:"cursor,omitempty"`
	ProjectionRevision string `json:"projection_revision"`
}

type maintenanceWorkServicePage struct {
	Work         []clustermeta.MaintenanceWorkRecord
	NextState    string
	NextCursor   string
	ScannedCount int
}

func encodeMaintenanceWorkPageToken(reason, state, cursor, revision string) (string, error) {
	token := maintenanceWorkPageToken{
		Version: maintenanceWorkPageTokenVersion, Reason: strings.TrimSpace(reason), State: strings.TrimSpace(state),
		Cursor: strings.TrimSpace(cursor), ProjectionRevision: strings.TrimSpace(revision),
	}
	if !validMaintenanceWorkPageReason(token.Reason) || token.State != clustermeta.MaintenanceWorkStateReady && token.State != clustermeta.MaintenanceWorkStateLeased || token.ProjectionRevision == "" || token.State == clustermeta.MaintenanceWorkStateReady && token.Cursor == "" {
		return "", fmt.Errorf("invalid maintenance work page token")
	}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeMaintenanceWorkPageToken(raw, reason string) (maintenanceWorkPageToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return maintenanceWorkPageToken{}, fmt.Errorf("decode maintenance work page token: %w", err)
	}
	var token maintenanceWorkPageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return maintenanceWorkPageToken{}, fmt.Errorf("unmarshal maintenance work page token: %w", err)
	}
	if token.Version != maintenanceWorkPageTokenVersion || token.Reason != reason || token.State != clustermeta.MaintenanceWorkStateReady && token.State != clustermeta.MaintenanceWorkStateLeased || token.ProjectionRevision == "" || token.State == clustermeta.MaintenanceWorkStateReady && token.Cursor == "" {
		return maintenanceWorkPageToken{}, fmt.Errorf("invalid maintenance work page token payload")
	}
	return token, nil
}

func validMaintenanceWorkPageReason(reason string) bool {
	return reason == "repair" || reason == "rebalance"
}

func (s *server) listActiveMaintenanceWorkPage(ctx context.Context, reason, state, cursor string, limit int) (maintenanceWorkServicePage, error) {
	result := maintenanceWorkServicePage{}
	if state == "" {
		state = clustermeta.MaintenanceWorkStateReady
	}
	remaining := limit
	if state == clustermeta.MaintenanceWorkStateReady {
		page, err := s.repo.ListMaintenanceWorkDetailPage(ctx, reason, state, cursor, remaining)
		if err != nil {
			return result, err
		}
		result.Work = append(result.Work, page.Work...)
		result.ScannedCount += page.ScannedCount
		remaining -= page.ScannedCount
		if page.NextCursor != "" {
			result.NextState, result.NextCursor = state, page.NextCursor
			return result, nil
		}
		if remaining == 0 {
			result.NextState = clustermeta.MaintenanceWorkStateLeased
			return result, nil
		}
		state, cursor = clustermeta.MaintenanceWorkStateLeased, ""
	}
	if state == clustermeta.MaintenanceWorkStateLeased {
		page, err := s.repo.ListMaintenanceWorkDetailPage(ctx, reason, state, cursor, remaining)
		if err != nil {
			return result, err
		}
		result.Work = append(result.Work, page.Work...)
		result.ScannedCount += page.ScannedCount
		if page.NextCursor != "" {
			result.NextState, result.NextCursor = state, page.NextCursor
		}
	}
	return result, nil
}

func (s *server) listMaintenanceWorkPage(ctx context.Context, clusterRef *adminv1.ClusterRef, reason string, pageSize uint32, pageToken string) (*adminv1.ClusterRef, maintenanceWorkServicePage, clustermeta.MaintenanceWorkListProjection, string, time.Time, error) {
	cluster, err := s.clusterRef(clusterRef)
	if err != nil {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, err
	}
	limit := int(pageSize)
	if limit == 0 {
		limit = clustermeta.MaintenanceWorkPageDefault
	}
	if limit < 1 || limit > clustermeta.MaintenanceWorkPageMaximum {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, status.Errorf(codes.InvalidArgument, "page_size %d exceeds maximum %d", pageSize, clustermeta.MaintenanceWorkPageMaximum)
	}
	state, cursor, expectedRevision := "", "", ""
	if strings.TrimSpace(pageToken) != "" {
		token, tokenErr := decodeMaintenanceWorkPageToken(pageToken, reason)
		if tokenErr != nil {
			return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, status.Errorf(codes.InvalidArgument, "invalid %s page_token: %v", reason, tokenErr)
		}
		state, cursor, expectedRevision = token.State, token.Cursor, token.ProjectionRevision
	}
	before, err := s.repo.GetMaintenanceWorkListProjection(ctx, reason)
	if err != nil {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, maintenanceWorkPageStatusError(reason, "read projection", err)
	}
	if expectedRevision != "" && expectedRevision != before.RevisionDigest {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, status.Errorf(codes.FailedPrecondition, "%s page_token revision mismatch", reason)
	}
	page, err := s.listActiveMaintenanceWorkPage(ctx, reason, state, cursor, limit)
	if err != nil {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, maintenanceWorkPageStatusError(reason, "list page", err)
	}
	after, err := s.repo.GetMaintenanceWorkListProjection(ctx, reason)
	if err != nil {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, maintenanceWorkPageStatusError(reason, "recheck projection", err)
	}
	if before.RevisionDigest != after.RevisionDigest {
		return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, status.Errorf(codes.FailedPrecondition, "%s work projection changed during page read", reason)
	}
	nextToken := ""
	if page.NextState != "" {
		nextToken, err = encodeMaintenanceWorkPageToken(reason, page.NextState, page.NextCursor, after.RevisionDigest)
		if err != nil {
			return nil, maintenanceWorkServicePage{}, clustermeta.MaintenanceWorkListProjection{}, "", time.Time{}, status.Errorf(codes.Internal, "encode %s page_token: %v", reason, err)
		}
	}
	return cluster, page, after, nextToken, s.currentTime().UTC(), nil
}

func maintenanceWorkPageStatusError(reason, stage string, err error) error {
	switch {
	case errors.Is(err, clustermeta.ErrMaintenanceWorkListRebuildRequired):
		return status.Errorf(codes.FailedPrecondition, "%s work projection rebuild required", reason)
	case errors.Is(err, clustermeta.ErrMaintenanceWorkListChanged), errors.Is(err, clustermeta.ErrMaintenanceIndexChanged):
		return status.Errorf(codes.FailedPrecondition, "%s work projection changed during page read", reason)
	case errors.Is(err, clustermeta.ErrMaintenanceIndexInvalid):
		return status.Errorf(codes.FailedPrecondition, "%s work projection invalid: %v", reason, err)
	default:
		return status.Errorf(codes.Internal, "%s %s: %v", stage, reason, err)
	}
}

func maintenanceWorkFreshnessMillis(now time.Time, projection clustermeta.MaintenanceWorkListProjection) int64 {
	age := now.Sub(time.Unix(projection.UpdatedAtUnix, 0).UTC())
	if age < 0 {
		return 0
	}
	return age.Milliseconds()
}

func maintenanceWorkToRepairSummary(work clustermeta.MaintenanceWorkRecord) *adminv1.RepairSummary {
	state := string(clustermeta.PlacementTransitionQueued)
	if work.State == clustermeta.MaintenanceWorkStateLeased {
		state = string(clustermeta.PlacementTransitionRunning)
	}
	return &adminv1.RepairSummary{
		VolumeId: work.VolumeID, PlacementRef: work.PlacementRef, CurrentReplicaSetId: work.CurrentReplicaSetID,
		TargetReplicaSetId: work.TargetReplicaSetID, State: state,
	}
}

func (s *server) ListRepairsPage(ctx context.Context, req *adminv1.ListRepairsPageRequest) (*adminv1.ListRepairsPageResponse, error) {
	cluster, page, projection, nextToken, now, err := s.listMaintenanceWorkPage(ctx, req.GetCluster(), "repair", req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &adminv1.ListRepairsPageResponse{
		Cluster: cluster, ProjectionRevision: projection.RevisionDigest, NextPageToken: nextToken,
		FreshnessAgeMillis: maintenanceWorkFreshnessMillis(now, projection), ProjectionHealth: "healthy",
		ScannedRecords: uint32(page.ScannedCount), GeneratedAt: timestamppb.New(now),
	}
	for _, work := range page.Work {
		resp.Repairs = append(resp.Repairs, maintenanceWorkToRepairSummary(work))
	}
	return resp, nil
}

func (s *server) ListRebalancesPage(ctx context.Context, req *adminv1.ListRebalancesPageRequest) (*adminv1.ListRebalancesPageResponse, error) {
	cluster, page, projection, nextToken, now, err := s.listMaintenanceWorkPage(ctx, req.GetCluster(), "rebalance", req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	resp := &adminv1.ListRebalancesPageResponse{
		Cluster: cluster, ProjectionRevision: projection.RevisionDigest, NextPageToken: nextToken,
		FreshnessAgeMillis: maintenanceWorkFreshnessMillis(now, projection), ProjectionHealth: "healthy",
		ScannedRecords: uint32(page.ScannedCount), GeneratedAt: timestamppb.New(now),
	}
	for _, work := range page.Work {
		repair := maintenanceWorkToRepairSummary(work)
		resp.Rebalances = append(resp.Rebalances, &adminv1.RebalanceSummary{
			VolumeId: repair.GetVolumeId(), PlacementRef: repair.GetPlacementRef(), CurrentReplicaSetId: repair.GetCurrentReplicaSetId(),
			TargetReplicaSetId: repair.GetTargetReplicaSetId(), State: repair.GetState(),
		})
	}
	return resp, nil
}
