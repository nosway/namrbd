package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/sbsgrpc"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	clusterpayload "github.com/nosway/namrbd/sbs/cluster/payload"
	"github.com/nosway/namrbd/sbs/cluster/replication"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
	"github.com/nosway/namrbd/sbs/local"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestProductDefaultRuntimeKnobs(t *testing.T) {
	if !defaultServiceOwnedWriteEffects {
		t.Fatal("service-owned write effects must be the product default")
	}
	if !defaultNativeAllocationFastPath {
		t.Fatal("native allocation fast path must be the product default")
	}
	if defaultServiceRuntimeWriteEffectsBatchCoalesceWait != time.Millisecond {
		t.Fatalf("write effects batch coalesce wait=%v want 1ms", defaultServiceRuntimeWriteEffectsBatchCoalesceWait)
	}
}

func TestJoinNodePreservesDrainingLifecycle(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleDraining,
		HealthState:       clustermeta.NodeHealthHealthy,
		Zone:              "zone-a",
		Capabilities:      []string{"sbs-grpc", "admin-http"},
		LastHeartbeatUnix: 123,
		AdminHTTPEndpoint: "http://old-admin",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "old-sbs", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	resp, err := srv.JoinNode(ctx, &adminv1.JoinNodeRequest{
		Cluster:           &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		NodeId:            "node-a",
		GrpcEndpoint:      "127.0.0.1:9461",
		AdminHttpEndpoint: "http://127.0.0.1:9081",
		Zone:              "zone-a",
	})
	if err != nil {
		t.Fatalf("JoinNode: %v", err)
	}
	if !resp.GetOperation().GetAccepted() {
		t.Fatalf("JoinNode operation not accepted: %+v", resp.GetOperation())
	}
	if !strings.Contains(resp.GetOperation().GetMessage(), "preserved draining") {
		t.Fatalf("JoinNode message=%q, want draining preservation", resp.GetOperation().GetMessage())
	}

	rec, err := srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership: %v", err)
	}
	if rec.LifecycleState != clustermeta.NodeLifecycleDraining {
		t.Fatalf("lifecycle=%q want %q", rec.LifecycleState, clustermeta.NodeLifecycleDraining)
	}
	if rec.AdminHTTPEndpoint != "http://127.0.0.1:9081" {
		t.Fatalf("admin endpoint=%q", rec.AdminHTTPEndpoint)
	}
	if len(rec.SBSEndpoints) != 1 || rec.SBSEndpoints[0].Address != "127.0.0.1" || rec.SBSEndpoints[0].Port != 9461 {
		t.Fatalf("sbs endpoints=%+v", rec.SBSEndpoints)
	}
}

func TestServiceSpecFromVolumeSpecRecordPreservesProtectedState(t *testing.T) {
	record := volumeSpecRecord{
		VolumeID:        "00000075",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  64 << 10,
		ExtentPageBytes: 4 << 20,
		ProtectedState: &clustermeta.VolumeProtectedStateRecord{
			State:            " sealed ",
			ReasonCode:       " worm_sealed_read_only ",
			SealedObjectID:   " sealed-image-001 ",
			SealOperationID:  " seal-op-001 ",
			PolicySnapshotID: " policy-snap-001 ",
			LifecycleState:   " materialized ",
			SourceVolumeID:   " 00000065 ",
		},
	}
	spec := serviceSpecFromVolumeSpecRecord(record)
	if spec.ProtectedState == nil {
		t.Fatalf("ProtectedState is nil")
	}
	if spec.ProtectedState.State != service.VolumeProtectedStateSealed ||
		spec.ProtectedState.ReasonCode != service.ProtectedWriteReasonSealedReadOnly ||
		spec.ProtectedState.SourceVolumeID != "00000065" {
		t.Fatalf("unexpected protected state: %+v", spec.ProtectedState)
	}
	protoState := protectedStateToProto(record.ProtectedState)
	if protoState == nil ||
		protoState.GetState() != "sealed" ||
		protoState.GetReasonCode() != service.ProtectedWriteReasonSealedReadOnly ||
		protoState.GetSourceVolumeId() != "00000065" {
		t.Fatalf("unexpected proto protected state: %+v", protoState)
	}
}

type fakePlacementApplyInternalService struct{}

func (fakePlacementApplyInternalService) ApplyPlacementChanges(context.Context, clustermeta.PlacementApplyRequest) error {
	return nil
}

type recordingPlacementApplyInternalService struct {
	req clustermeta.PlacementApplyRequest
	err error
}

func (s *recordingPlacementApplyInternalService) ApplyPlacementChanges(_ context.Context, req clustermeta.PlacementApplyRequest) error {
	s.req = req
	return s.err
}

type recordingWriteSessionInternalService struct {
	req     clustermeta.CommitWriteStateRequest
	pageReq clustermeta.CommitWriteMetadataRequest
	state   clustermeta.VolumeState
	record  clustermeta.IdempotencyRecord
	err     error
}

func (s *recordingWriteSessionInternalService) GetVolumeState(context.Context, string) (clustermeta.VolumeState, error) {
	return clustermeta.VolumeState{}, nil
}

func (s *recordingWriteSessionInternalService) PutVolumeState(context.Context, clustermeta.VolumeState) error {
	return nil
}

func (s *recordingWriteSessionInternalService) GetIdempotencyRecord(context.Context, string, string) (clustermeta.IdempotencyRecord, error) {
	return clustermeta.IdempotencyRecord{}, nil
}

func (s *recordingWriteSessionInternalService) PutIdempotencyRecord(context.Context, clustermeta.IdempotencyRecord) error {
	return nil
}

func (s *recordingWriteSessionInternalService) GetMutationOperation(context.Context, string, string) (clustermeta.MutationOperationRecord, error) {
	return clustermeta.MutationOperationRecord{}, nil
}

func (s *recordingWriteSessionInternalService) PutMutationOperation(context.Context, clustermeta.MutationOperationRecord) error {
	return nil
}

func (s *recordingWriteSessionInternalService) PutWriteIntent(context.Context, clustermeta.IdempotencyRecord, clustermeta.MutationOperationRecord) error {
	return nil
}

func (s *recordingWriteSessionInternalService) CommitWriteState(_ context.Context, req clustermeta.CommitWriteStateRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	s.req = req
	return s.state, s.record, s.err
}

func (s *recordingWriteSessionInternalService) CommitPageScopedWriteMetadata(_ context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	s.pageReq = req
	return s.state, s.record, s.err
}

func (s *recordingWriteSessionInternalService) CommitRangeLocalWriteState(_ context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	s.pageReq = req
	return s.state, s.record, s.err
}

func (s *recordingWriteSessionInternalService) CommitAppendOnlyWriteStateAndQueueEffects(_ context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	s.pageReq = req
	return s.state, s.record, s.err
}

type blockingWriteSessionInternalService struct {
	recordingWriteSessionInternalService

	mu           sync.Mutex
	calls        int
	started      chan int
	releaseFirst chan struct{}
}

func (s *blockingWriteSessionInternalService) CommitWriteState(ctx context.Context, req clustermeta.CommitWriteStateRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	s.started <- call
	if call == 1 {
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, ctx.Err()
		}
	}
	return clustermeta.VolumeState{VolumeID: req.VolumeID, Epoch: req.ExpectedEpoch, Revision: req.CommittedRevision}, clustermeta.IdempotencyRecord{
		VolumeID:       req.VolumeID,
		IdempotencyKey: req.IdempotencyKey,
		ResultState:    clustermeta.IdempotencyCommitted,
		Revision:       req.CommittedRevision,
	}, nil
}

type recordingChunkIDAllocatorInternalService struct {
	req   clustermeta.ChunkIDAllocationRequest
	start uint64
	err   error
}

func (s *recordingChunkIDAllocatorInternalService) AllocateChunkIDs(_ context.Context, req clustermeta.ChunkIDAllocationRequest) (uint64, error) {
	s.req = req
	return s.start, s.err
}

type recordingPlacementResolverInternalService struct {
	extentVolumeID     string
	allocationVolumeID string
	snapshotID         string
	cloneID            string
	placements         []clustermeta.ResolvedExtentPlacement
	allocationPages    []clustermeta.ResolvedAllocationPage
	err                error
}

func (s *recordingPlacementResolverInternalService) ResolveExtentPlacements(_ context.Context, volumeID string, _, _ uint64) ([]clustermeta.ResolvedExtentPlacement, error) {
	s.extentVolumeID = volumeID
	return s.placements, s.err
}

func (s *recordingPlacementResolverInternalService) ResolveAllocationPages(_ context.Context, volumeID string, _, _ uint64, _, _ uint32) ([]clustermeta.ResolvedAllocationPage, error) {
	s.allocationVolumeID = volumeID
	return s.allocationPages, s.err
}

func (s *recordingPlacementResolverInternalService) ResolveSnapshotAllocationPages(_ context.Context, snapshotID string, _, _ uint64, _, _ uint32) ([]clustermeta.ResolvedAllocationPage, error) {
	s.snapshotID = snapshotID
	return s.allocationPages, s.err
}

func (s *recordingPlacementResolverInternalService) ResolveCloneAllocationPages(_ context.Context, cloneID string, _, _ uint64, _, _ uint32) ([]clustermeta.ResolvedAllocationPage, error) {
	s.cloneID = cloneID
	return s.allocationPages, s.err
}

func TestServerNewPlacementApplyInternalServiceUsesOwnedService(t *testing.T) {
	expected := fakePlacementApplyInternalService{}
	srv := &server{placementApplyInternalService: expected}

	if got := srv.newPlacementApplyInternalService(); got != expected {
		t.Fatalf("newPlacementApplyInternalService returned %T, want owned service", got)
	}
}

func TestServerNewWriteSessionInternalServiceUsesOwnedService(t *testing.T) {
	expected := &recordingWriteSessionInternalService{}
	srv := &server{writeSessionInternalService: expected}

	if got := srv.newWriteSessionInternalService(); got != expected {
		t.Fatalf("newWriteSessionInternalService returned %T, want owned service", got)
	}
}

func TestServerNewChunkIDAllocatorInternalServiceUsesOwnedService(t *testing.T) {
	expected := &recordingChunkIDAllocatorInternalService{}
	srv := &server{chunkIDAllocatorService: expected}

	if got := srv.newChunkIDAllocatorInternalService(); got != expected {
		t.Fatalf("newChunkIDAllocatorInternalService returned %T, want owned service", got)
	}
}

func TestServerNewPlacementResolverInternalServiceUsesOwnedService(t *testing.T) {
	expected := &recordingPlacementResolverInternalService{}
	srv := &server{placementResolverService: expected}

	if got := srv.newPlacementResolverInternalService(); got != expected {
		t.Fatalf("newPlacementResolverInternalService returned %T, want owned service", got)
	}
}

func TestServerECProfileLifecycle(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	cluster := &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}

	createResp, err := srv.CreateECProfile(ctx, &adminv1.CreateECProfileRequest{
		Cluster:      cluster,
		Meta:         &adminv1.RequestMeta{Actor: "tester", Reason: "phase-k-slice-001"},
		ProfileId:    "ec-6-3",
		DataShards:   6,
		ParityShards: 3,
	})
	if err != nil {
		t.Fatalf("CreateECProfile: %v", err)
	}
	if createResp.GetProfile().GetCodecId() != clustermeta.ECCodecRSVandGF8 {
		t.Fatalf("codec_id=%q", createResp.GetProfile().GetCodecId())
	}
	if createResp.GetProfile().GetStripeUnitBytes() != clustermeta.DefaultECStripeUnitBytes {
		t.Fatalf("stripe_unit_bytes=%d", createResp.GetProfile().GetStripeUnitBytes())
	}

	getResp, err := srv.GetECProfile(ctx, &adminv1.GetECProfileRequest{Cluster: cluster, ProfileId: "ec-6-3"})
	if err != nil {
		t.Fatalf("GetECProfile: %v", err)
	}
	if getResp.GetProfile().GetLifecycle() != adminv1.ECProfileLifecycle_EC_PROFILE_LIFECYCLE_ACTIVE {
		t.Fatalf("profile lifecycle=%s", getResp.GetProfile().GetLifecycle())
	}

	disableResp, err := srv.DisableECProfile(ctx, &adminv1.DisableECProfileRequest{
		Cluster:   cluster,
		Meta:      &adminv1.RequestMeta{Actor: "tester", Reason: "disable"},
		ProfileId: "ec-6-3",
	})
	if err != nil {
		t.Fatalf("DisableECProfile: %v", err)
	}
	if disableResp.GetProfile().GetLifecycle() != adminv1.ECProfileLifecycle_EC_PROFILE_LIFECYCLE_DISABLED {
		t.Fatalf("disabled lifecycle=%s", disableResp.GetProfile().GetLifecycle())
	}

	activeOnly, err := srv.ListECProfiles(ctx, &adminv1.ListECProfilesRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("ListECProfiles active: %v", err)
	}
	if len(activeOnly.GetProfiles()) != 0 {
		t.Fatalf("active profiles=%v want none", activeOnly.GetProfiles())
	}
	all, err := srv.ListECProfiles(ctx, &adminv1.ListECProfilesRequest{Cluster: cluster, IncludeDisabled: true})
	if err != nil {
		t.Fatalf("ListECProfiles all: %v", err)
	}
	if len(all.GetProfiles()) != 1 || all.GetProfiles()[0].GetProfileId() != "ec-6-3" {
		t.Fatalf("all profiles=%v", all.GetProfiles())
	}
}

func TestGetMaintenanceStatusReflectsThrottleAndPauseState(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	cluster := &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}

	initial, err := srv.GetMaintenanceStatus(ctx, &adminv1.GetMaintenanceStatusRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("GetMaintenanceStatus initial: %v", err)
	}
	if got := initial.GetThrottle(); got.GetAuthority() != "sbs-service-maintenance-throttle" ||
		got.GetGeneration() != 1 || got.GetMaxConcurrentRepairs() != 1 ||
		got.GetMaxConcurrentRebalances() != 1 || got.GetMaxConcurrentDrains() != 1 {
		t.Fatalf("unexpected initial maintenance throttle: %+v", got)
	}

	if _, err := srv.SetMaintenanceThrottle(ctx, &adminv1.SetMaintenanceThrottleRequest{
		Cluster:                 cluster,
		Meta:                    &adminv1.RequestMeta{Actor: "tester", Reason: "phase-o-budget"},
		MaxConcurrentRepairs:    3,
		MaxConcurrentRebalances: 2,
		MaxConcurrentDrains:     1,
	}); err != nil {
		t.Fatalf("SetMaintenanceThrottle: %v", err)
	}
	afterThrottle, err := srv.GetMaintenanceStatus(ctx, &adminv1.GetMaintenanceStatusRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("GetMaintenanceStatus after throttle: %v", err)
	}
	if got := afterThrottle.GetThrottle(); got.GetGeneration() != 2 ||
		got.GetMaxConcurrentRepairs() != 3 || got.GetMaxConcurrentRebalances() != 2 ||
		got.GetMaxConcurrentDrains() != 1 {
		t.Fatalf("unexpected throttled maintenance state: %+v", got)
	}

	if _, err := srv.PauseMaintenance(ctx, &adminv1.PauseMaintenanceRequest{
		Cluster:         cluster,
		Meta:            &adminv1.RequestMeta{Actor: "tester", Reason: "phase-o-budget-pause"},
		PauseRepairs:    true,
		PauseRebalances: true,
		PauseDrains:     false,
	}); err != nil {
		t.Fatalf("PauseMaintenance: %v", err)
	}
	paused, err := srv.GetMaintenanceStatus(ctx, &adminv1.GetMaintenanceStatusRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("GetMaintenanceStatus paused: %v", err)
	}
	if got := paused.GetThrottle(); !got.GetPauseRepairs() || !got.GetPauseRebalances() || got.GetPauseDrains() {
		t.Fatalf("unexpected paused maintenance state: %+v", got)
	}

	if _, err := srv.ResumeMaintenance(ctx, &adminv1.ResumeMaintenanceRequest{
		Cluster:          cluster,
		Meta:             &adminv1.RequestMeta{Actor: "tester", Reason: "phase-o-budget-resume"},
		ResumeRepairs:    true,
		ResumeRebalances: false,
		ResumeDrains:     false,
	}); err != nil {
		t.Fatalf("ResumeMaintenance: %v", err)
	}
	resumed, err := srv.GetMaintenanceStatus(ctx, &adminv1.GetMaintenanceStatusRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("GetMaintenanceStatus resumed: %v", err)
	}
	if got := resumed.GetThrottle(); got.GetPauseRepairs() || !got.GetPauseRebalances() || got.GetPauseDrains() {
		t.Fatalf("unexpected resumed maintenance state: %+v", got)
	}
}

func TestServerCreateECProfileRejectsCapOverflow(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	_, err := srv.CreateECProfile(ctx, &adminv1.CreateECProfileRequest{
		Cluster:      &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		ProfileId:    "ec-too-wide",
		DataShards:   30,
		ParityShards: 3,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateECProfile status=%v err=%v want InvalidArgument", status.Code(err), err)
	}
}

func TestServerEffectivePlacementApplyTimeout(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "default", in: 0, want: defaultPlacementApplyTimeout},
		{name: "custom", in: 2 * time.Second, want: 2 * time.Second},
		{name: "disabled", in: -time.Second, want: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := &server{placementApplyTimeout: tt.in}
			if got := srv.effectivePlacementApplyTimeout(); got != tt.want {
				t.Fatalf("effectivePlacementApplyTimeout=%v want=%v", got, tt.want)
			}
		})
	}
}

func TestServerApplyPlacementChangesDelegatesToInternalService(t *testing.T) {
	placementApply := &recordingPlacementApplyInternalService{}
	srv := &server{placementApplyInternalService: placementApply}

	resp, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 7,
		AllocationPages: []*internalv1.AllocationPage{
			{
				PageNo:         1,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []*internalv1.AllocationExtent{
					{
						LogicalChunkStart:  0,
						ChunkCount:         4,
						Kind:               internalv1.AllocationKind_ALLOCATION_KIND_DATA,
						PhysicalChunkStart: 99,
						BackingRef:         "store-a",
						Generation:         2,
					},
				},
			},
		},
		NormalizeExtentIds: []uint64{2, 3},
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}
	if resp.GetVolumeId() != "00a1b2c3" || resp.GetCommittedRevision() != 7 {
		t.Fatalf("response=(%q,%d) want=(00a1b2c3,7)", resp.GetVolumeId(), resp.GetCommittedRevision())
	}
	if placementApply.req.VolumeID != "00a1b2c3" || placementApply.req.CommittedRevision != 7 {
		t.Fatalf("delegated request identity=(%q,%d)", placementApply.req.VolumeID, placementApply.req.CommittedRevision)
	}
	if len(placementApply.req.AllocationPages) != 1 || placementApply.req.AllocationPages[0].PageNo != 1 {
		t.Fatalf("delegated allocation pages=%#v", placementApply.req.AllocationPages)
	}
	if len(placementApply.req.NormalizeExtentIDs) != 2 || placementApply.req.NormalizeExtentIDs[0] != 2 || placementApply.req.NormalizeExtentIDs[1] != 3 {
		t.Fatalf("delegated normalize extent ids=%v", placementApply.req.NormalizeExtentIDs)
	}
}

func TestServerCommitWriteStateDelegatesToInternalService(t *testing.T) {
	writeSession := &recordingWriteSessionInternalService{
		state: clustermeta.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    1,
			Revision: 8,
			Status:   clustermeta.VolumeStatusHealthy,
		},
		record: clustermeta.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-1",
			ResultState:    clustermeta.IdempotencyCommitted,
			Revision:       8,
		},
	}
	srv := &server{writeSessionInternalService: writeSession}

	resp, err := srv.CommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
	})
	if err != nil {
		t.Fatalf("CommitWriteState: %v", err)
	}
	if writeSession.req.VolumeID != "00a1b2c3" || writeSession.req.CommittedRevision != 8 {
		t.Fatalf("unexpected request: %+v", writeSession.req)
	}
	if resp.GetVolumeState().GetRevision() != 8 || resp.GetIdempotencyRecord().GetResultState() != internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_COMMITTED {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerCommitPageScopedWriteMetadataDelegatesToInternalService(t *testing.T) {
	writeSession := &recordingWriteSessionInternalService{
		state: clustermeta.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    1,
			Revision: 7,
			Status:   clustermeta.VolumeStatusHealthy,
		},
		record: clustermeta.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-page",
			ResultState:    clustermeta.IdempotencyCommitted,
			Revision:       4,
		},
	}
	srv := &server{writeSessionInternalService: writeSession}

	resp, err := srv.CommitPageScopedWriteMetadata(context.Background(), &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-page",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{
			{
				VolumeId:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
				Extents: []*internalv1.AllocationExtent{
					{
						LogicalChunkStart:  0,
						ChunkCount:         1,
						Kind:               internalv1.AllocationKind_ALLOCATION_KIND_DATA,
						PhysicalChunkStart: 101,
					},
				},
			},
		},
		MutationOperationId:   "write-idem-page",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		AffectedPageNos:       []uint64{0},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata: %v", err)
	}
	if writeSession.pageReq.VolumeID != "00a1b2c3" || writeSession.pageReq.IdempotencyKey != "idem-page" {
		t.Fatalf("unexpected request: %+v", writeSession.pageReq)
	}
	if len(writeSession.pageReq.AllocationPages) != 1 || writeSession.pageReq.AllocationPages[0].Revision != 3 {
		t.Fatalf("unexpected allocation pages: %+v", writeSession.pageReq.AllocationPages)
	}
	if resp.GetVolumeState().GetRevision() != 7 || resp.GetIdempotencyRecord().GetRevision() != 4 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerCommitRangeLocalWriteStateDelegatesToInternalService(t *testing.T) {
	writeSession := &recordingWriteSessionInternalService{
		state: clustermeta.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    1,
			Revision: 7,
			Status:   clustermeta.VolumeStatusHealthy,
		},
		record: clustermeta.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-range",
			ResultState:    clustermeta.IdempotencyCommitted,
			Revision:       4,
		},
	}
	srv := &server{writeSessionInternalService: writeSession}

	resp, err := srv.CommitRangeLocalWriteState(context.Background(), &internalv1.CommitRangeLocalWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-range",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{
			{
				VolumeId:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
			},
		},
		MutationOperationId:   "write-idem-range",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		AffectedPageNos:       []uint64{0},
	})
	if err != nil {
		t.Fatalf("CommitRangeLocalWriteState: %v", err)
	}
	if writeSession.pageReq.VolumeID != "00a1b2c3" || writeSession.pageReq.IdempotencyKey != "idem-range" {
		t.Fatalf("unexpected request: %+v", writeSession.pageReq)
	}
	if len(writeSession.pageReq.AllocationPages) != 1 || writeSession.pageReq.AllocationPages[0].Revision != 3 {
		t.Fatalf("unexpected allocation pages: %+v", writeSession.pageReq.AllocationPages)
	}
	if resp.GetVolumeState().GetRevision() != 7 || resp.GetIdempotencyRecord().GetRevision() != 4 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerCommitAppendOnlyWriteStateAndQueueEffectsRequiresServiceOwnedQueue(t *testing.T) {
	writeSession := &recordingWriteSessionInternalService{}
	srv := &server{writeSessionInternalService: writeSession}

	_, err := srv.CommitAppendOnlyWriteStateAndQueueEffects(context.Background(), &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		MutationOperationId:      "write-idem-append",
		ExpectedMutationState:    internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status=%v err=%v want failed precondition", status.Code(err), err)
	}
	if writeSession.pageReq.VolumeID != "" {
		t.Fatalf("unexpected internal service call: %+v", writeSession.pageReq)
	}
}

func TestServerCommitWriteStateSerializesSameVolume(t *testing.T) {
	writeSession := &blockingWriteSessionInternalService{
		started:      make(chan int, 2),
		releaseFirst: make(chan struct{}),
	}
	srv := &server{writeSessionInternalService: writeSession}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errs := make(chan error, 2)
	go func() {
		_, err := srv.CommitWriteState(ctx, &internalv1.CommitWriteStateRequest{
			VolumeId:                 "00a1b2c3",
			ExpectedEpoch:            1,
			ExpectedRevision:         7,
			IdempotencyKey:           "idem-1",
			ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
			CommittedRevision:        8,
		})
		errs <- err
	}()
	if got := <-writeSession.started; got != 1 {
		t.Fatalf("first call number=%d want 1", got)
	}

	go func() {
		_, err := srv.CommitWriteState(ctx, &internalv1.CommitWriteStateRequest{
			VolumeId:                 "00a1b2c3",
			ExpectedEpoch:            1,
			ExpectedRevision:         8,
			IdempotencyKey:           "idem-2",
			ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
			CommittedRevision:        9,
		})
		errs <- err
	}()
	select {
	case got := <-writeSession.started:
		t.Fatalf("second same-volume commit entered before first released: call=%d", got)
	case <-time.After(25 * time.Millisecond):
	}

	close(writeSession.releaseFirst)
	if got := <-writeSession.started; got != 2 {
		t.Fatalf("second call number=%d want 2", got)
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("CommitWriteState call %d: %v", i+1, err)
		}
	}
}

func TestServerCommitWriteStateMapsCASError(t *testing.T) {
	srv := &server{writeSessionInternalService: &recordingWriteSessionInternalService{err: clustermeta.ErrCASConflict}}

	_, err := srv.CommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
}

func TestServerCommitWriteStateRecordsObservability(t *testing.T) {
	srv := &server{writeSessionInternalService: &recordingWriteSessionInternalService{
		state:  clustermeta.VolumeState{VolumeID: "00a1b2c3", Revision: 8},
		record: clustermeta.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1", ResultState: clustermeta.IdempotencyCommitted},
	}}

	_, err := srv.CommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		IdempotencyKey:           "idem-1",
		CommittedRevision:        8,
	})
	if err != nil {
		t.Fatalf("CommitWriteState: %v", err)
	}
	_, err = srv.CommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}

	stats := srv.writeSessionObservability.snapshot()
	if stats.RequestsTotal != 2 {
		t.Fatalf("requests total=%d want 2", stats.RequestsTotal)
	}
	if stats.FailuresTotal != 1 {
		t.Fatalf("failures=%d want 1", stats.FailuresTotal)
	}
	if stats.RequestsByClass[writeSessionOutcomeOK] != 1 {
		t.Fatalf("ok count=%d want 1", stats.RequestsByClass[writeSessionOutcomeOK])
	}
	if stats.RequestsByClass[string(clustercontrol.WriteSessionErrorInvalidArgument)] != 1 {
		t.Fatalf("invalid count=%d want 1", stats.RequestsByClass[string(clustercontrol.WriteSessionErrorInvalidArgument)])
	}
}

func TestServerAllocateChunkIDsDelegatesToInternalService(t *testing.T) {
	allocator := &recordingChunkIDAllocatorInternalService{start: 17}
	srv := &server{chunkIDAllocatorService: allocator}

	resp, err := srv.AllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "00a1b2c3",
		Count:    4,
	})
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	if allocator.req.VolumeID != "00a1b2c3" || allocator.req.Count != 4 {
		t.Fatalf("unexpected request: %+v", allocator.req)
	}
	if resp.GetVolumeId() != "00a1b2c3" || resp.GetCount() != 4 || resp.GetStartChunkId() != 17 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerAllocateChunkIDsMapsInvalidRequest(t *testing.T) {
	srv := &server{chunkIDAllocatorService: &recordingChunkIDAllocatorInternalService{}}
	_, err := srv.AllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "not-a-volume",
		Count:    1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
}

func TestServerAllocateChunkIDsRecordsObservability(t *testing.T) {
	srv := &server{chunkIDAllocatorService: &recordingChunkIDAllocatorInternalService{start: 17}}

	_, err := srv.AllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "00a1b2c3",
		Count:    4,
	})
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	_, err = srv.AllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "not-a-volume",
		Count:    1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}

	stats := srv.chunkIDAllocatorObservability.snapshot()
	if stats.RequestsTotal != 2 {
		t.Fatalf("requests total=%d want 2", stats.RequestsTotal)
	}
	if stats.FailuresTotal != 1 {
		t.Fatalf("failures=%d want 1", stats.FailuresTotal)
	}
	if stats.RequestsByClass[chunkIDAllocatorOutcomeOK] != 1 {
		t.Fatalf("ok count=%d want 1", stats.RequestsByClass[chunkIDAllocatorOutcomeOK])
	}
	if stats.RequestsByClass[string(clustercontrol.ChunkIDAllocatorErrorInvalidArgument)] != 1 {
		t.Fatalf("invalid count=%d want 1", stats.RequestsByClass[string(clustercontrol.ChunkIDAllocatorErrorInvalidArgument)])
	}
}

func TestServerResolveExtentPlacementsDelegatesToInternalService(t *testing.T) {
	resolver := &recordingPlacementResolverInternalService{
		placements: []clustermeta.ResolvedExtentPlacement{
			{
				ExtentMapping: clustermeta.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, PlacementRef: "pl-1"},
				ReplicaSet:    clustermeta.ReplicaSetState{ReplicaSetID: "rs-1", PlacementRef: "pl-1"},
				Nodes:         map[string]clustermeta.NodeMembershipRecord{"node-a": {NodeID: "node-a", HealthState: clustermeta.NodeHealthHealthy}},
			},
		},
	}
	srv := &server{placementResolverService: resolver}

	resp, err := srv.ResolveExtentPlacements(context.Background(), &internalv1.ResolveExtentPlacementsRequest{
		VolumeId:    "00a1b2c3",
		OffsetBytes: 0,
		LengthBytes: 4096,
	})
	if err != nil {
		t.Fatalf("ResolveExtentPlacements: %v", err)
	}
	if resolver.extentVolumeID != "00a1b2c3" {
		t.Fatalf("extent volume_id=%q want 00a1b2c3", resolver.extentVolumeID)
	}
	if len(resp.GetPlacements()) != 1 || resp.GetPlacements()[0].GetExtentMapping().GetExtentId() != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestServerResolveAllocationPagesRecordsObservability(t *testing.T) {
	resolver := &recordingPlacementResolverInternalService{
		allocationPages: []clustermeta.ResolvedAllocationPage{
			{
				Page:            clustermeta.AllocationPageRecord{VolumeID: "00a1b2c3", PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 1024},
				RangeStartChunk: 4,
				RangeEndChunk:   8,
				CoversWholePage: true,
			},
		},
	}
	srv := &server{placementResolverService: resolver}

	_, err := srv.ResolveAllocationPages(context.Background(), &internalv1.ResolveAllocationPagesRequest{
		VolumeId:       "00a1b2c3",
		OffsetBytes:    4096,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	})
	if err != nil {
		t.Fatalf("ResolveAllocationPages: %v", err)
	}
	_, err = srv.ResolveAllocationPages(context.Background(), &internalv1.ResolveAllocationPagesRequest{
		VolumeId:       "not-a-volume",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}

	stats := srv.placementResolverObservability.snapshot()
	if stats.RequestsTotal != 2 {
		t.Fatalf("requests total=%d want 2", stats.RequestsTotal)
	}
	if stats.FailuresTotal != 1 {
		t.Fatalf("failures=%d want 1", stats.FailuresTotal)
	}
	if stats.RequestsByClass[placementResolverOutcomeOK] != 1 {
		t.Fatalf("ok count=%d want 1", stats.RequestsByClass[placementResolverOutcomeOK])
	}
	if stats.RequestsByClass[string(clustercontrol.PlacementResolverErrorInvalidArgument)] != 1 {
		t.Fatalf("invalid count=%d want 1", stats.RequestsByClass[string(clustercontrol.PlacementResolverErrorInvalidArgument)])
	}
}

func TestServerApplyPlacementChangesMapsValidationError(t *testing.T) {
	srv := &server{placementApplyInternalService: &recordingPlacementApplyInternalService{}}

	_, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "invalid",
		CommittedRevision: 1,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("status.Code=%v want=%v err=%v", got, codes.InvalidArgument, err)
	}
}

func TestServerApplyPlacementChangesMapsInternalServiceError(t *testing.T) {
	srv := &server{placementApplyInternalService: &recordingPlacementApplyInternalService{err: clustermeta.ErrCASConflict}}

	_, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if got := status.Code(err); got != codes.Aborted {
		t.Fatalf("status.Code=%v want=%v err=%v", got, codes.Aborted, err)
	}
}

func TestServerApplyPlacementChangesRecordsObservability(t *testing.T) {
	srv := &server{placementApplyInternalService: &recordingPlacementApplyInternalService{}}

	_, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if err != nil {
		t.Fatalf("ApplyPlacementChanges success path: %v", err)
	}
	_, err = srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "invalid",
		CommittedRevision: 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status.Code=%v want=%v err=%v", status.Code(err), codes.InvalidArgument, err)
	}

	stats := srv.placementApplyObservability.snapshot()
	if stats.RequestsTotal != 2 {
		t.Fatalf("requests_total=%d want=2", stats.RequestsTotal)
	}
	if stats.FailuresTotal != 1 {
		t.Fatalf("failures_total=%d want=1", stats.FailuresTotal)
	}
	if stats.RequestsByClass[placementApplyOutcomeOK] != 1 {
		t.Fatalf("ok count=%d want=1", stats.RequestsByClass[placementApplyOutcomeOK])
	}
	if stats.RequestsByClass[string(clustercontrol.PlacementApplyErrorInvalidArgument)] != 1 {
		t.Fatalf("invalid_argument count=%d want=1", stats.RequestsByClass[string(clustercontrol.PlacementApplyErrorInvalidArgument)])
	}
}

func TestServerApplyPlacementChangesRecordsDeadlineAsUnavailable(t *testing.T) {
	srv := &server{placementApplyInternalService: &recordingPlacementApplyInternalService{err: context.DeadlineExceeded}}

	_, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
	})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("status.Code=%v want=%v err=%v", got, codes.Unavailable, err)
	}
	stats := srv.placementApplyObservability.snapshot()
	if stats.RequestsTotal != 1 {
		t.Fatalf("requests_total=%d want=1", stats.RequestsTotal)
	}
	if stats.FailuresTotal != 1 {
		t.Fatalf("failures_total=%d want=1", stats.FailuresTotal)
	}
	if stats.RequestsByClass[string(clustercontrol.PlacementApplyErrorUnavailable)] != 1 {
		t.Fatalf("unavailable count=%d want=1", stats.RequestsByClass[string(clustercontrol.PlacementApplyErrorUnavailable)])
	}
}

func TestServerPlacementApplyObservabilityEndpoints(t *testing.T) {
	srv := newTestMaintenanceServer(t)
	srv.placementApplyInternalService = &recordingPlacementApplyInternalService{}
	srv.writeSessionInternalService = &recordingWriteSessionInternalService{
		state:  clustermeta.VolumeState{VolumeID: "00a1b2c3", Revision: 2},
		record: clustermeta.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1", ResultState: clustermeta.IdempotencyCommitted},
	}
	srv.placementResolverService = &recordingPlacementResolverInternalService{}
	srv.chunkIDAllocatorService = &recordingChunkIDAllocatorInternalService{start: 9}
	if _, err := srv.ApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
	}); err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}
	if _, err := srv.CommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        2,
	}); err != nil {
		t.Fatalf("CommitWriteState: %v", err)
	}
	if _, err := srv.ResolveExtentPlacements(context.Background(), &internalv1.ResolveExtentPlacementsRequest{
		VolumeId:    "00a1b2c3",
		LengthBytes: 4096,
	}); err != nil {
		t.Fatalf("ResolveExtentPlacements: %v", err)
	}
	if _, err := srv.AllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "00a1b2c3",
		Count:    2,
	}); err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(metricsRec, metricsReq)
	metricsBody := metricsRec.Body.String()
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metricsRec.Code, metricsBody)
	}
	for _, want := range []string{
		`sbs_service_placement_apply_requests_total{class="ok"} 1`,
		`sbs_service_placement_apply_duration_seconds_total`,
		`sbs_service_write_session_requests_total{class="ok"} 1`,
		`sbs_service_write_session_duration_seconds_total`,
		`sbs_service_chunk_id_allocator_requests_total{class="ok"} 1`,
		`sbs_service_chunk_id_allocator_duration_seconds_total`,
		`sbs_service_placement_resolver_requests_total{class="ok"} 1`,
		`sbs_service_placement_resolver_duration_seconds_total`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("metrics missing %q in\n%s", want, metricsBody)
		}
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/debug/summary", nil)
	summaryRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(summaryRec, summaryReq)
	var summary map[string]any
	if err := json.Unmarshal(summaryRec.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if got := summary["placement_apply_requests_total"]; got != float64(1) {
		t.Fatalf("placement_apply_requests_total=%v want=1", got)
	}
	if got := summary["placement_apply_failures_total"]; got != float64(0) {
		t.Fatalf("placement_apply_failures_total=%v want=0", got)
	}
	if got := summary["write_session_requests_total"]; got != float64(1) {
		t.Fatalf("write_session_requests_total=%v want=1", got)
	}
	if got := summary["write_session_failures_total"]; got != float64(0) {
		t.Fatalf("write_session_failures_total=%v want=0", got)
	}
	if got := summary["chunk_id_allocator_requests_total"]; got != float64(1) {
		t.Fatalf("chunk_id_allocator_requests_total=%v want=1", got)
	}
	if got := summary["chunk_id_allocator_failures_total"]; got != float64(0) {
		t.Fatalf("chunk_id_allocator_failures_total=%v want=0", got)
	}
	if got := summary["placement_resolver_requests_total"]; got != float64(1) {
		t.Fatalf("placement_resolver_requests_total=%v want=1", got)
	}
	if got := summary["placement_resolver_failures_total"]; got != float64(0) {
		t.Fatalf("placement_resolver_failures_total=%v want=0", got)
	}
}

func TestServerPlacementApplyGRPCAdapterIntegration(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      3,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	adapter := newBufconnPlacementApplyAdapter(t, srv)
	if err := adapter.ApplyPlacementChanges(ctx, clustermeta.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 11,
		AllocationPages: []clustermeta.AllocationPageRecord{
			{
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []clustermeta.AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 4, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 200},
				},
			},
		},
		NormalizeExtentIDs: []uint64{1},
	}); err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}

	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 || pages[0].Revision != 11 {
		t.Fatalf("allocation pages=%+v want one page at revision 11", pages)
	}
	if len(pages[0].Extents) != 1 || pages[0].Extents[0].PhysicalChunkStart != 200 {
		t.Fatalf("allocation page extents=%+v", pages[0].Extents)
	}

	mapping, err := srv.repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 11 {
		t.Fatalf("normalized mapping chunk_id=%d revision=%d want chunk_id=0 revision=11", mapping.ChunkID, mapping.Revision)
	}
}

func TestCreateVolumeInitializesZeroAllocationPages(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	now := time.Now().Unix()
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}

	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		ExtentSizeBytes:   1 << 20,
		ReplicationFactor: 3,
		Meta:              &adminv1.RequestMeta{Actor: "test", Reason: "create"},
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	spec, err := srv.getVolumeSpec(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("getVolumeSpec: %v", err)
	}
	if spec.ChunkSizeBytes != service.DefaultAllocationChunkSize {
		t.Fatalf("ChunkSizeBytes=%d want=%d", spec.ChunkSizeBytes, service.DefaultAllocationChunkSize)
	}
	if spec.ExtentPageBytes != service.DefaultAllocationPageSize {
		t.Fatalf("ExtentPageBytes=%d want=%d", spec.ExtentPageBytes, service.DefaultAllocationPageSize)
	}

	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("allocation pages=%d want=1", len(pages))
	}
	page := pages[0]
	if page.Revision != 1 {
		t.Fatalf("page revision=%d want=1", page.Revision)
	}
	if page.PageBytes != service.DefaultAllocationPageSize {
		t.Fatalf("page bytes=%d want=%d", page.PageBytes, service.DefaultAllocationPageSize)
	}
	if page.ChunkSizeBytes != service.DefaultAllocationChunkSize {
		t.Fatalf("chunk size=%d want=%d", page.ChunkSizeBytes, service.DefaultAllocationChunkSize)
	}
	if len(page.Extents) != 1 {
		t.Fatalf("page extents=%d want=1", len(page.Extents))
	}
	extent := page.Extents[0]
	if extent.Kind != clustermeta.AllocationKindZero {
		t.Fatalf("extent kind=%q want=%q", extent.Kind, clustermeta.AllocationKindZero)
	}
	if extent.LogicalChunkStart != 0 {
		t.Fatalf("logical chunk start=%d want=0", extent.LogicalChunkStart)
	}
	if extent.ChunkCount != 16 {
		t.Fatalf("chunk count=%d want=16", extent.ChunkCount)
	}

	mappings, err := srv.repo.ListExtentMappings(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListExtentMappings: %v", err)
	}
	if len(mappings) != 1 {
		t.Fatalf("extent mappings=%d want=1", len(mappings))
	}
	if mappings[0].ChunkID != 0 {
		t.Fatalf("extent chunk id=%d want=0", mappings[0].ChunkID)
	}
}

func TestCreateVolumeStrictTopologyRejectsInsufficientDistinctZones(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	now := time.Now().Unix()
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-b"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}

	_, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c4",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		ExtentSizeBytes:   1 << 20,
		ReplicationFactor: 3,
		TopologyMode:      "strict",
		Meta:              &adminv1.RequestMeta{Actor: "test", Reason: "create-strict"},
	})
	if err == nil {
		t.Fatalf("CreateVolume strict topology succeeded with only two distinct zones")
	}
}

func TestExpandVolumeGrowsSpecRevisionAndPlacementWithoutAllocatingPages(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	now := time.Now().Unix()
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    1,
		Revision: 3,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		ChunkSizeBytes:    65536,
		ExtentPageBytes:   4194304,
		ExtentSizeBytes:   1 << 20,
		ReplicationFactor: 3,
		CreatedAtUnix:     now,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "00a1b2c3-e000001-r01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "00a1b2c3-e000001-r01", Role: clustermeta.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "00a1b2c3-e000001-r02", Role: clustermeta.ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "00a1b2c3-e000001-r03", Role: clustermeta.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   1 << 20,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4194304,
		ChunkSizeBytes: 65536,
		Revision:       3,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 16, Kind: clustermeta.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	resp, err := srv.ExpandVolume(ctx, &adminv1.ExpandVolumeRequest{
		Cluster:         &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:            &adminv1.RequestMeta{Actor: "tester", Reason: "unit-test"},
		VolumeId:        "00a1b2c3",
		TargetSizeBytes: 2 << 20,
	})
	if err != nil {
		t.Fatalf("ExpandVolume: %v", err)
	}
	if resp.GetOldSizeBytes() != 1<<20 || resp.GetSizeBytes() != 2<<20 || resp.GetVolumeRevision() != 4 {
		t.Fatalf("unexpected expand response: %+v", resp)
	}
	spec, err := srv.getVolumeSpec(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("getVolumeSpec: %v", err)
	}
	if spec.SizeBytes != 2<<20 || spec.ChunkSizeBytes != 65536 || spec.ExtentPageBytes != 4194304 {
		t.Fatalf("unexpected expanded spec: %+v", spec)
	}
	state, err := srv.repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if state.Revision != 4 {
		t.Fatalf("revision=%d want=4", state.Revision)
	}
	mappings, err := srv.repo.ListExtentMappings(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListExtentMappings: %v", err)
	}
	if len(mappings) != 2 {
		t.Fatalf("extent mappings=%d want 2: %+v", len(mappings), mappings)
	}
	if mappings[1].ExtentID != 2 || mappings[1].LogicalOffset != 1<<20 || mappings[1].LengthBytes != 1<<20 || mappings[1].PlacementRef != "pl-000002" {
		t.Fatalf("unexpected expanded extent mapping: %+v", mappings[1])
	}
	replicaSet, err := srv.repo.GetReplicaSet(ctx, "00a1b2c3", "rs-000002")
	if err != nil {
		t.Fatalf("GetReplicaSet expanded extent: %v", err)
	}
	if len(replicaSet.Replicas) != 3 || replicaSet.WriteQuorum != 2 {
		t.Fatalf("unexpected expanded replica set: %+v", replicaSet)
	}
	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("allocation pages=%d want existing page only", len(pages))
	}
}

func TestExpandVolumeAllowsSameSizeNoopAndRejectsShrinkAndUnalignedTarget(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  65536,
		ExtentPageBytes: 4194304,
		CreatedAtUnix:   time.Now().Unix(),
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	resp, err := srv.ExpandVolume(ctx, &adminv1.ExpandVolumeRequest{
		Cluster:         &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId:        "00a1b2c3",
		TargetSizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("ExpandVolume same-size noop failed: %v", err)
	}
	if resp.GetOldSizeBytes() != 1<<20 || resp.GetSizeBytes() != 1<<20 || resp.GetVolumeRevision() != 1 {
		t.Fatalf("unexpected same-size noop response: %+v", resp)
	}
	for _, target := range []uint64{(1 << 20) - 4096, (2 << 20) + 1} {
		if _, err := srv.ExpandVolume(ctx, &adminv1.ExpandVolumeRequest{
			Cluster:         &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
			VolumeId:        "00a1b2c3",
			TargetSizeBytes: target,
		}); err == nil {
			t.Fatalf("ExpandVolume target=%d succeeded, want error", target)
		}
	}
}

func TestCreateVolumeUsesConfiguredAllocationGeometry(t *testing.T) {
	t.Setenv("NAMRBD_SBS_ALLOCATION_CHUNK_SIZE", "64k")
	t.Setenv("NAMRBD_SBS_ALLOCATION_PAGE_SIZE", "128k")
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	now := time.Now().Unix()
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}

	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c4",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		ExtentSizeBytes:   1 << 20,
		ReplicationFactor: 3,
		Meta:              &adminv1.RequestMeta{Actor: "test", Reason: "create"},
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	spec, err := srv.getVolumeSpec(ctx, "00a1b2c4")
	if err != nil {
		t.Fatalf("getVolumeSpec: %v", err)
	}
	if spec.ChunkSizeBytes != 65536 {
		t.Fatalf("ChunkSizeBytes=%d want=65536", spec.ChunkSizeBytes)
	}
	if spec.ExtentPageBytes != 131072 {
		t.Fatalf("ExtentPageBytes=%d want=131072", spec.ExtentPageBytes)
	}
	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c4")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 8 {
		t.Fatalf("allocation pages=%d want=8", len(pages))
	}
	if pages[0].PageBytes != 131072 || pages[0].ChunkSizeBytes != 65536 {
		t.Fatalf("allocation page geometry=%d/%d want=131072/65536", pages[0].PageBytes, pages[0].ChunkSizeBytes)
	}
}

func TestConfiguredExtentSizeBytesAcceptsLowercaseUnitEnv(t *testing.T) {
	t.Setenv("NAMRBD_SBS_EXTENT_SIZE", "64m")

	got, err := configuredExtentSizeBytes(0, 4096)
	if err != nil {
		t.Fatalf("configuredExtentSizeBytes: %v", err)
	}
	if got != 64<<20 {
		t.Fatalf("extent size=%d want=%d", got, uint64(64<<20))
	}
}

func TestParseSizeEnvUint64AcceptsUnitsAndBareBytes(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want uint64
	}{
		{"256k", 256 << 10},
		{"4M", 4 << 20},
		{"1g", 1 << 30},
		{"2T", 2 << 40},
		{"65536", 65536},
	} {
		got, err := parseSizeEnvUint64(tc.raw)
		if err != nil {
			t.Fatalf("parseSizeEnvUint64(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("parseSizeEnvUint64(%q)=%d want=%d", tc.raw, got, tc.want)
		}
	}
}

func TestCreateVolumeUsesRequestedAllocationGeometry(t *testing.T) {
	t.Setenv("NAMRBD_SBS_ALLOCATION_CHUNK_SIZE", "64k")
	t.Setenv("NAMRBD_SBS_ALLOCATION_PAGE_SIZE", "4m")
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	now := time.Now().Unix()
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}

	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:                 "00a1b2c5",
		SizeBytes:                1 << 20,
		BlockSize:                4096,
		ExtentSizeBytes:          1 << 20,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  262144,
		ReplicationFactor:        3,
		Meta:                     &adminv1.RequestMeta{Actor: "test", Reason: "create"},
	}); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	spec, err := srv.getVolumeSpec(ctx, "00a1b2c5")
	if err != nil {
		t.Fatalf("getVolumeSpec: %v", err)
	}
	if spec.ChunkSizeBytes != 65536 {
		t.Fatalf("ChunkSizeBytes=%d want=65536", spec.ChunkSizeBytes)
	}
	if spec.ExtentPageBytes != 262144 {
		t.Fatalf("ExtentPageBytes=%d want=262144", spec.ExtentPageBytes)
	}
	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c5")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 4 {
		t.Fatalf("allocation pages=%d want=4", len(pages))
	}
	if pages[0].PageBytes != 262144 || pages[0].ChunkSizeBytes != 65536 {
		t.Fatalf("allocation page geometry=%d/%d want=262144/65536", pages[0].PageBytes, pages[0].ChunkSizeBytes)
	}
}

func TestCreateECVolumeStoresImmutableGeometryWithoutReplicaPlacement(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	putTestECProfile(t, srv, "ec-6-3", clustermeta.ECProfileLifecycleActive)

	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		EcProfileId:       "ec-6-3",
		TopologyMode:      "strict",
		Meta:              &adminv1.RequestMeta{Actor: "test", Reason: "create-ec"},
	}); err != nil {
		t.Fatalf("CreateVolume ec: %v", err)
	}

	state, err := srv.repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if state.RedundancyBackend != clustermeta.RedundancyBackendEC || state.ProtectionPolicy != "ec:ec-6-3" {
		t.Fatalf("state backend/protection=%q/%q", state.RedundancyBackend, state.ProtectionPolicy)
	}
	spec, err := srv.getVolumeSpec(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("getVolumeSpec: %v", err)
	}
	if spec.RedundancyBackend != clustermeta.RedundancyBackendEC {
		t.Fatalf("spec backend=%q", spec.RedundancyBackend)
	}
	if spec.ReplicationFactor != 0 {
		t.Fatalf("ec replication_factor=%d want 0", spec.ReplicationFactor)
	}
	if spec.ECProfileID != "ec-6-3" || spec.ECDataShards != 6 || spec.ECParityShards != 3 {
		t.Fatalf("ec profile fields: %+v", spec)
	}
	if spec.ECStripeUnitBytes != clustermeta.DefaultECStripeUnitBytes {
		t.Fatalf("ec stripe unit=%d", spec.ECStripeUnitBytes)
	}
	mappings, err := srv.repo.ListExtentMappings(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListExtentMappings: %v", err)
	}
	if len(mappings) != 0 {
		t.Fatalf("ec volume should not create replica extent mappings: %+v", mappings)
	}
	pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 0 {
		t.Fatalf("ec volume should not pre-materialize allocation pages: %+v", pages)
	}

	getResp, err := srv.GetVolume(ctx, &adminv1.GetVolumeRequest{VolumeId: "00a1b2c3"})
	if err != nil {
		t.Fatalf("GetVolume: %v", err)
	}
	volume := getResp.GetVolume()
	if volume.GetRedundancyBackend() != clustermeta.RedundancyBackendEC || volume.GetEcProfileId() != "ec-6-3" {
		t.Fatalf("volume summary=%+v", volume)
	}
	if volume.GetEcDataShards() != 6 || volume.GetEcParityShards() != 3 {
		t.Fatalf("volume summary shard fields=%d/%d", volume.GetEcDataShards(), volume.GetEcParityShards())
	}
	_, err = srv.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{VolumeId: "00a1b2c3"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("GetReplicaTargetsView err=%v code=%v want FailedPrecondition", err, status.Code(err))
	}
}

func TestGetVolumeNotFoundDoesNotEmitErrorLog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	var logs bytes.Buffer
	restore := structuredlog.SetOutput(&logs)
	defer restore()

	_, err := srv.GetVolume(ctx, &adminv1.GetVolumeRequest{VolumeId: "missing-volume"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetVolume code=%v err=%v want NotFound", status.Code(err), err)
	}
	if got := strings.TrimSpace(logs.String()); got != "" {
		t.Fatalf("GetVolume NotFound emitted structured log: %s", got)
	}
}

func TestCreateECVolumeRejectsMissingOrDisabledProfile(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	putTestECProfile(t, srv, "ec-disabled", clustermeta.ECProfileLifecycleDisabled)

	_, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		EcProfileId:       "missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing profile code=%v err=%v want NotFound", status.Code(err), err)
	}
	_, err = srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		VolumeId:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		EcProfileId:       "ec-disabled",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled profile code=%v err=%v want FailedPrecondition", status.Code(err), err)
	}
}

func TestDeleteVolumeBlocksActiveTransitionsButPurgeVolumeBypassesGuard(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2cf",
		Epoch:    1,
		Revision: 7,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00a1b2cf",
		SizeBytes:         8 << 20,
		BlockSize:         4096,
		ChunkSizeBytes:    65536,
		ExtentPageBytes:   4 << 20,
		ReplicationFactor: 1,
		CreatedAtUnix:     time.Now().Unix(),
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2cf",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   65536,
		PlacementRef:  "pl-000001",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00a1b2cf",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rep-01",
		WriteQuorum:      1,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-01", Role: clustermeta.ReplicaRolePrimary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2cf",
		PageNo:         0,
		PageBytes:      4 << 20,
		ChunkSizeBytes: 65536,
		Revision:       7,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 1},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := srv.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:     "00a1b2cf",
		PlacementRef: "pl-000001",
		State:        clustermeta.PlacementTransitionRunning,
		Reason:       "rebalance",
		Attempt:      2,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}

	_, err := srv.DeleteVolume(ctx, &adminv1.DeleteVolumeRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:     &adminv1.RequestMeta{Actor: "tester", Reason: "delete"},
		VolumeId: "00a1b2cf",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteVolume err=%v code=%v want FailedPrecondition", err, status.Code(err))
	}

	_, err = srv.PurgeVolume(ctx, &adminv1.PurgeVolumeRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:     &adminv1.RequestMeta{Actor: "tester", Reason: "purge"},
		VolumeId: "00a1b2cf",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PurgeVolume without confirmation err=%v code=%v want InvalidArgument", err, status.Code(err))
	}

	resp, err := srv.PurgeVolume(ctx, &adminv1.PurgeVolumeRequest{
		Cluster:           &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:              &adminv1.RequestMeta{Actor: "tester", Reason: "purge"},
		VolumeId:          "00a1b2cf",
		ConfirmedDeletion: true,
	})
	if err != nil {
		t.Fatalf("PurgeVolume: %v", err)
	}
	if !resp.GetOperation().GetAccepted() || resp.GetOperation().GetMessage() != "volume purged" {
		t.Fatalf("PurgeVolume response=%+v", resp)
	}
	if _, err := srv.repo.GetVolumeState(ctx, "00a1b2cf"); !errors.Is(err, clustermeta.ErrNotFound) {
		t.Fatalf("GetVolumeState after purge err=%v want ErrNotFound", err)
	}
	if _, err := srv.getVolumeSpec(ctx, "00a1b2cf"); !errors.Is(err, clustermeta.ErrNotFound) {
		t.Fatalf("getVolumeSpec after purge err=%v want ErrNotFound", err)
	}
	if transitions, err := srv.repo.ListPlacementTransitions(ctx, "00a1b2cf"); err != nil {
		t.Fatalf("ListPlacementTransitions after purge: %v", err)
	} else if len(transitions) != 0 {
		t.Fatalf("transitions after purge=%+v want empty", transitions)
	}
	if mappings, err := srv.repo.ListExtentMappings(ctx, "00a1b2cf"); err != nil {
		t.Fatalf("ListExtentMappings after purge: %v", err)
	} else if len(mappings) != 0 {
		t.Fatalf("mappings after purge=%+v want empty", mappings)
	}
	if replicaSets, err := srv.repo.ListReplicaSets(ctx, "00a1b2cf"); err != nil {
		t.Fatalf("ListReplicaSets after purge: %v", err)
	} else if len(replicaSets) != 0 {
		t.Fatalf("replicaSets after purge=%+v want empty", replicaSets)
	}
	if pages, err := srv.repo.ListAllocationPages(ctx, "00a1b2cf"); err != nil {
		t.Fatalf("ListAllocationPages after purge: %v", err)
	} else if len(pages) != 0 {
		t.Fatalf("allocation pages after purge=%+v want empty", pages)
	}
}

func TestECSnapshotRequestCapturesECBackingRefs(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	fixedNow := time.Unix(1770000010, 0).UTC()
	srv.now = func() time.Time { return fixedNow }

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             1,
		Revision:          12,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		Status:            clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00a1b2c3",
		SizeBytes:         10 << 20,
		BlockSize:         4096,
		ChunkSizeBytes:    65536,
		ExtentPageBytes:   4194304,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		ECProfileID:       "ec-6-3",
		ECCodecID:         clustermeta.ECCodecRSVandGF8,
		ECDataShards:      6,
		ECParityShards:    3,
		ECStripeUnitBytes: 128 << 10,
		ECFailureDomain:   clustermeta.ECFailureDomainZone,
		CreatedAtUnix:     fixedNow.Unix(),
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4194304,
		ChunkSizeBytes: 65536,
		Revision:       12,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 12, Kind: clustermeta.AllocationKindData, BackingRef: "ec:00a1b2c3:0:1", Generation: 1, Checksum: "sha256:ec-snapshot"},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	createResp, err := srv.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		SourceVolumeId: "00a1b2c3",
		IdempotencyKey: "snap-ec",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot ec: %v", err)
	}
	if !createResp.GetOperation().GetAccepted() || createResp.GetSnapshotRootId() == "" {
		t.Fatalf("CreateSnapshot response=%+v", createResp)
	}
	snapshotPages, err := srv.repo.ListSnapshotAllocationPages(ctx, createResp.GetSnapshotRootId())
	if err != nil {
		t.Fatalf("ListSnapshotAllocationPages: %v", err)
	}
	if len(snapshotPages) != 1 {
		t.Fatalf("snapshot pages=%+v", snapshotPages)
	}
	extent := snapshotPages[0].Extents[0]
	if extent.BackingRef != "ec:00a1b2c3:0:1" || extent.PhysicalChunkStart != 0 || extent.Generation != 1 {
		t.Fatalf("captured ec extent=%+v", extent)
	}
}

func TestECCloneRequestCreatesRecord(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00a1b2c3",
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		ChunkSizeBytes:    service.DefaultAllocationChunkSize,
		ExtentPageBytes:   service.DefaultAllocationPageSize,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		ECProfileID:       "ec-6-3",
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if _, _, err := srv.repo.CreateSnapshotRecord(ctx, clustermeta.SnapshotRecord{
		SnapshotID:               "snap-ec-existing",
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           "snap-ec-existing",
		State:                    clustermeta.SnapshotStateAvailable,
		CreatedAtUnix:            1,
		UpdatedAtUnix:            1,
		CutVolumeRevision:        1,
		AllocationChunkSizeBytes: service.DefaultAllocationChunkSize,
		AllocationPageSizeBytes:  service.DefaultAllocationPageSize,
		SourceSizeBytes:          1 << 20,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord fixture: %v", err)
	}
	resp, err := srv.CreateClone(ctx, &adminv1.CreateCloneRequest{
		SourceSnapshotId: "snap-ec-existing",
		CloneId:          "clone-ec",
	})
	if err != nil {
		t.Fatalf("CreateClone ec: %v", err)
	}
	if resp.GetCloneId() != "clone-ec" || !resp.GetOperation().GetAccepted() {
		t.Fatalf("CreateClone response=%+v", resp)
	}
	clones, err := srv.repo.ListCloneRecords(ctx, "snap-ec-existing", "", true)
	if err != nil {
		t.Fatalf("ListCloneRecords: %v", err)
	}
	if len(clones) != 1 || clones[0].CloneID != "clone-ec" || clones[0].SourceVolumeID != "00a1b2c3" {
		t.Fatalf("ec clone records=%+v", clones)
	}
}

func TestSnapshotAPIsRecordMetadataAndHonorIdempotency(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	fixedNow := time.Unix(1770000000, 123).UTC()
	srv.now = func() time.Time { return fixedNow }

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    1,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       10 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  65536,
		ExtentPageBytes: 4194304,
		CreatedAtUnix:   fixedNow.Unix(),
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4194304,
		ChunkSizeBytes: 65536,
		Revision:       12,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 64, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 100},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	createResp, err := srv.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:           &adminv1.RequestMeta{Actor: "tester", Reason: "unit-test"},
		SourceVolumeId: "00a1b2c3",
		IdempotencyKey: "idem-snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if !createResp.GetOperation().GetAccepted() {
		t.Fatalf("create operation should be accepted: %+v", createResp.GetOperation())
	}
	if createResp.GetSnapshotId() == "" || !strings.Contains(createResp.GetSnapshotId(), "00a1b2c3") {
		t.Fatalf("snapshot_id=%q should include source volume id", createResp.GetSnapshotId())
	}

	getResp, err := srv.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{
		Cluster:    &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SnapshotId: createResp.GetSnapshotId(),
	})
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	snapshot := getResp.GetSnapshot()
	if snapshot.GetSourceVolumeId() != "00a1b2c3" || snapshot.GetCutVolumeRevision() != 12 {
		t.Fatalf("snapshot summary=%+v", snapshot)
	}
	if snapshot.GetState() != adminv1.SnapshotState_SNAPSHOT_STATE_AVAILABLE {
		t.Fatalf("snapshot state=%v want available", snapshot.GetState())
	}
	if snapshot.GetAllocationChunkSizeBytes() != 65536 || snapshot.GetAllocationPageSizeBytes() != 4194304 {
		t.Fatalf("snapshot geometry=%d/%d", snapshot.GetAllocationChunkSizeBytes(), snapshot.GetAllocationPageSizeBytes())
	}
	snapshotPages, err := srv.repo.ListSnapshotAllocationPages(ctx, createResp.GetSnapshotRootId())
	if err != nil {
		t.Fatalf("ListSnapshotAllocationPages: %v", err)
	}
	if len(snapshotPages) != 1 || snapshotPages[0].Revision != 12 {
		t.Fatalf("snapshot pages=%+v", snapshotPages)
	}
	if snapshotPages[0].Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("snapshot physical chunk start=%d want=100", snapshotPages[0].Extents[0].PhysicalChunkStart)
	}

	replayResp, err := srv.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceVolumeId: "00a1b2c3",
		IdempotencyKey: "idem-snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot replay: %v", err)
	}
	if replayResp.GetSnapshotId() != createResp.GetSnapshotId() {
		t.Fatalf("replay snapshot_id=%q want=%q", replayResp.GetSnapshotId(), createResp.GetSnapshotId())
	}
	if replayResp.GetOperation().GetAccepted() {
		t.Fatalf("replay operation should not be accepted as a new mutation: %+v", replayResp.GetOperation())
	}

	listResp, err := srv.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceVolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(listResp.GetSnapshots()) != 1 || listResp.GetSnapshots()[0].GetSnapshotId() != createResp.GetSnapshotId() {
		t.Fatalf("snapshots=%+v", listResp.GetSnapshots())
	}
}

func TestDeleteSnapshotMarksDeletedAndListHidesByDefault(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	rec, _, err := srv.repo.CreateSnapshotRecord(ctx, clustermeta.SnapshotRecord{
		SnapshotID:               "snap-00a1b2c3-20260521T120000.000000000Z",
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           "snap-00a1b2c3-20260521T120000.000000000Z",
		State:                    clustermeta.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        2,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 20,
	})
	if err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}

	if _, err := srv.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		Cluster:    &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SnapshotId: rec.SnapshotID,
		Meta:       &adminv1.RequestMeta{Actor: "tester", Reason: "delete"},
	}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	getResp, err := srv.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{
		Cluster:    &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SnapshotId: rec.SnapshotID,
	})
	if err != nil {
		t.Fatalf("GetSnapshot after delete: %v", err)
	}
	if getResp.GetSnapshot().GetState() != adminv1.SnapshotState_SNAPSHOT_STATE_DELETED {
		t.Fatalf("snapshot state=%v want deleted", getResp.GetSnapshot().GetState())
	}
	listResp, err := srv.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceVolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(listResp.GetSnapshots()) != 0 {
		t.Fatalf("deleted snapshot should be hidden by default: %+v", listResp.GetSnapshots())
	}
	listResp, err = srv.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceVolumeId: "00a1b2c3",
		IncludeDeleted: true,
	})
	if err != nil {
		t.Fatalf("ListSnapshots include deleted: %v", err)
	}
	if len(listResp.GetSnapshots()) != 1 {
		t.Fatalf("include-deleted snapshots=%+v", listResp.GetSnapshots())
	}
}

func TestCloneAPIsCreateReferenceAndProtectSnapshotDelete(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	fixedNow := time.Unix(1770000000, 456).UTC()
	srv.now = func() time.Time { return fixedNow }

	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := srv.repo.CreateSnapshotRecord(ctx, clustermeta.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    clustermeta.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        12,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          10 << 20,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}

	createResp, err := srv.CreateClone(ctx, &adminv1.CreateCloneRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:             &adminv1.RequestMeta{Actor: "tester", Reason: "unit-test"},
		SourceSnapshotId: snapshotID,
		IdempotencyKey:   "clone-idem-1",
	})
	if err != nil {
		t.Fatalf("CreateClone: %v", err)
	}
	if !createResp.GetOperation().GetAccepted() || createResp.GetCloneId() == "" || createResp.GetCloneBaseRootId() != snapshotID {
		t.Fatalf("CreateClone response=%+v", createResp)
	}
	getResp, err := srv.GetClone(ctx, &adminv1.GetCloneRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		CloneId: createResp.GetCloneId(),
	})
	if err != nil {
		t.Fatalf("GetClone: %v", err)
	}
	clone := getResp.GetClone()
	if clone.GetSourceSnapshotId() != snapshotID || clone.GetSourceVolumeId() != "00a1b2c3" || clone.GetSizeBytes() != 10<<20 {
		t.Fatalf("clone summary=%+v", clone)
	}
	if clone.GetState() != adminv1.CloneState_CLONE_STATE_AVAILABLE {
		t.Fatalf("clone state=%v want available", clone.GetState())
	}

	if _, err := srv.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		Cluster:    &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:       &adminv1.RequestMeta{Actor: "tester", Reason: "delete"},
		SnapshotId: snapshotID,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteSnapshot while clone exists err=%v want FailedPrecondition", err)
	}

	if _, err := srv.MaterializeClone(ctx, &adminv1.MaterializeCloneRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:           &adminv1.RequestMeta{Actor: "tester", Reason: "materialize"},
		CloneId:        createResp.GetCloneId(),
		TargetVolumeId: "00a1b2c4",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("MaterializeClone missing source spec err=%v want FailedPrecondition", err)
	}

	replayResp, err := srv.CreateClone(ctx, &adminv1.CreateCloneRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceSnapshotId: snapshotID,
		IdempotencyKey:   "clone-idem-1",
	})
	if err != nil {
		t.Fatalf("CreateClone replay: %v", err)
	}
	if replayResp.GetCloneId() != createResp.GetCloneId() || replayResp.GetOperation().GetAccepted() {
		t.Fatalf("CreateClone replay response=%+v", replayResp)
	}

	listResp, err := srv.ListClones(ctx, &adminv1.ListClonesRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceSnapshotId: snapshotID,
	})
	if err != nil {
		t.Fatalf("ListClones: %v", err)
	}
	if len(listResp.GetClones()) != 1 || listResp.GetClones()[0].GetCloneId() != createResp.GetCloneId() {
		t.Fatalf("clones=%+v", listResp.GetClones())
	}

	if _, err := srv.DeleteClone(ctx, &adminv1.DeleteCloneRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:    &adminv1.RequestMeta{Actor: "tester", Reason: "delete"},
		CloneId: createResp.GetCloneId(),
	}); err != nil {
		t.Fatalf("DeleteClone: %v", err)
	}
	if _, err := srv.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		Cluster:    &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:       &adminv1.RequestMeta{Actor: "tester", Reason: "delete"},
		SnapshotId: snapshotID,
	}); err != nil {
		t.Fatalf("DeleteSnapshot after clone delete: %v", err)
	}
}

func TestCreateVolumeFromSnapshotMaterializesVolumeAndHonorsIdempotency(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	sourceVolumeID := "00a1b2c3"
	targetVolumeID := "00a1b2c4"
	sourceSpec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            sourceVolumeID,
		Prefix:          "vol-" + sourceVolumeID,
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  service.DefaultAllocationChunkSize,
		ExtentPageBytes: service.DefaultAllocationPageSize,
	})
	nodeClient, err := local.Open(local.Config{Path: filepath.Join(t.TempDir(), "sbs-node-a")})
	if err != nil {
		t.Fatalf("local.Open: %v", err)
	}
	t.Cleanup(func() { _ = nodeClient.Close() })
	createLocalVolume := func(volumeID string, sizeBytes uint64, blockSize, chunkSize, pageBytes uint64) error {
		parsedID, err := service.ParseVolumeID(volumeID)
		if err != nil {
			return nil
		}
		_, err = nodeClient.CreateVolume(ctx, service.VolumeSpec{
			ID:              service.HexVolumeID(parsedID),
			Name:            volumeID,
			Prefix:          "sbs-" + volumeID,
			SizeBytes:       sizeBytes,
			BlockSize:       uint32(blockSize),
			ChunkSizeBytes:  uint32(chunkSize),
			ExtentPageBytes: uint32(pageBytes),
		})
		return err
	}
	ensureLocalVolumeAndReplicas := func(volumeID string, sizeBytes uint64, blockSize, chunkSize, pageBytes uint64) error {
		if err := createLocalVolume(volumeID, sizeBytes, blockSize, chunkSize, pageBytes); err != nil {
			return err
		}
		replicaSets, err := srv.repo.ListReplicaSets(ctx, volumeID)
		if err != nil {
			return err
		}
		for _, replicaSet := range replicaSets {
			for _, replica := range replicaSet.Replicas {
				if err := createLocalVolume(replica.ReplicaID, sizeBytes, blockSize, chunkSize, pageBytes); err != nil {
					return err
				}
			}
		}
		return nil
	}
	adminHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/debug/materialize-volume" {
			http.NotFound(w, r)
			return
		}
		sizeBytes, err := strconv.ParseUint(r.URL.Query().Get("size_bytes"), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		blockSize, err := strconv.ParseUint(r.URL.Query().Get("block_size"), 10, 32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		chunkSize, err := strconv.ParseUint(r.URL.Query().Get("allocation_chunk_size_bytes"), 10, 32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pageBytes, err := strconv.ParseUint(r.URL.Query().Get("allocation_page_bytes"), 10, 32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		volumeID := r.URL.Query().Get("volume_id")
		if err := ensureLocalVolumeAndReplicas(volumeID, sizeBytes, blockSize, chunkSize, pageBytes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "volume_id": volumeID})
	}))
	t.Cleanup(adminHTTP.Close)
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		Zone:              "zone-a",
		AdminHTTPEndpoint: adminHTTP.URL,
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "node-a", Port: 19001}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	srv.cache.clients["node-a:19001"] = cachedReplicaClient{client: nodeClient}

	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		Cluster:           &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:              &adminv1.RequestMeta{Actor: "tester", Reason: "source"},
		VolumeId:          sourceVolumeID,
		SizeBytes:         sourceSpec.SizeBytes,
		BlockSize:         sourceSpec.BlockSize,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateVolume source metadata: %v", err)
	}
	if err := ensureLocalVolumeAndReplicas(
		sourceVolumeID,
		sourceSpec.SizeBytes,
		uint64(sourceSpec.BlockSize),
		uint64(sourceSpec.ChunkSizeBytes),
		uint64(sourceSpec.ExtentPageBytes),
	); err != nil {
		t.Fatalf("CreateVolume source local replicas: %v", err)
	}
	sourceRecord, err := srv.getVolumeSpec(ctx, sourceVolumeID)
	if err != nil {
		t.Fatalf("get source spec: %v", err)
	}
	sourceClient, err := srv.newMaterializeClusterClient(ctx, sourceRecord, "live-source")
	if err != nil {
		t.Fatalf("source cluster client: %v", err)
	}
	sourceOpen, err := sourceClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   sourceVolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: materializeRequestContext(
			"open-live-source",
			"restore-source",
			"live-source",
			"att-live-source",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("open live source: %v", err)
	}
	defer func() {
		_, _ = sourceClient.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     sourceVolumeID,
			VolumeHandle: sourceOpen.VolumeHandle,
			Context: materializeRequestContext(
				"close-live-source",
				"restore-source",
				"live-source",
				"att-live-source",
				1,
				"",
			),
		})
	}()
	marker := []byte("phase-l-csi-source\n")
	sourcePayload := make([]byte, int(sourceSpec.BlockSize))
	copy(sourcePayload, marker)
	if _, err := sourceClient.Write(ctx, &service.WriteRequest{
		VolumeID:     sourceVolumeID,
		VolumeHandle: sourceOpen.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  uint64(len(sourcePayload)),
		Data:         sourcePayload,
		Context: materializeRequestContext(
			"write-live-source",
			"restore-source",
			"live-source",
			"att-live-source",
			1,
			"live-source-marker",
		),
	}); err != nil {
		t.Fatalf("write live source marker: %v", err)
	}
	snapshotResp, err := srv.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:           &adminv1.RequestMeta{Actor: "tester", Reason: "snapshot"},
		SourceVolumeId: sourceVolumeID,
		IdempotencyKey: "restore-source-snapshot",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	restoreResp, err := srv.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:             &adminv1.RequestMeta{Actor: "tester", Reason: "restore"},
		SourceSnapshotId: snapshotResp.GetSnapshotId(),
		VolumeId:         targetVolumeID,
		SizeBytes:        sourceSpec.SizeBytes + 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolumeFromSnapshot: %v", err)
	}
	if restoreResp.GetVolumeId() != targetVolumeID || restoreResp.GetCloneId() == "" || !restoreResp.GetOperation().GetAccepted() {
		t.Fatalf("restore response=%+v", restoreResp)
	}
	if restoreResp.GetSizeBytes() != sourceSpec.SizeBytes+4096 {
		t.Fatalf("restore size=%d want=%d", restoreResp.GetSizeBytes(), sourceSpec.SizeBytes+4096)
	}
	clone, err := srv.repo.GetCloneRecord(ctx, restoreResp.GetCloneId())
	if err != nil {
		t.Fatalf("GetCloneRecord: %v", err)
	}
	if clone.State != clustermeta.CloneStateMaterialized || clone.MaterializedVolumeID != targetVolumeID {
		t.Fatalf("clone after restore=%+v", clone)
	}
	targetRecord, err := srv.getVolumeSpec(ctx, targetVolumeID)
	if err != nil {
		t.Fatalf("get target spec: %v", err)
	}
	verifyClient, err := srv.newMaterializeClusterClient(ctx, targetRecord, "verify-restored")
	if err != nil {
		t.Fatalf("verify cluster client: %v", err)
	}
	verifyOpen, err := verifyClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   targetVolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: materializeRequestContext(
			"open-verify-restored",
			restoreResp.GetCloneId(),
			"verify-restored",
			"att-verify-restored",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("open verify target: %v", err)
	}
	defer func() {
		_, _ = verifyClient.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     targetVolumeID,
			VolumeHandle: verifyOpen.VolumeHandle,
			Context: materializeRequestContext(
				"close-verify-restored",
				restoreResp.GetCloneId(),
				"verify-restored",
				"att-verify-restored",
				1,
				"",
			),
		})
	}()
	verifyRead, err := verifyClient.Read(ctx, &service.ReadRequest{
		VolumeID:     targetVolumeID,
		VolumeHandle: verifyOpen.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  uint64(len(sourcePayload)),
		Context: materializeRequestContext(
			"read-verify-restored",
			restoreResp.GetCloneId(),
			"verify-restored",
			"att-verify-restored",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("read verify target: %v", err)
	}
	if !bytes.Contains(verifyRead.Data, marker) {
		t.Fatalf("restored payload missing marker: %q", string(verifyRead.Data[:len(marker)]))
	}

	replayResp, err := srv.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceSnapshotId: snapshotResp.GetSnapshotId(),
		VolumeId:         targetVolumeID,
		SizeBytes:        sourceSpec.SizeBytes + 4096,
	})
	if err != nil {
		t.Fatalf("CreateVolumeFromSnapshot replay: %v", err)
	}
	if replayResp.GetVolumeId() != targetVolumeID || replayResp.GetCloneId() != restoreResp.GetCloneId() || replayResp.GetOperation().GetAccepted() {
		t.Fatalf("restore replay response=%+v", replayResp)
	}
	if _, err := srv.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		SourceSnapshotId: snapshotResp.GetSnapshotId(),
		VolumeId:         "00a1b2c5",
		SizeBytes:        sourceSpec.SizeBytes - 4096,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateVolumeFromSnapshot smaller restore err=%v want InvalidArgument", err)
	}
}

func TestCreateVolumeFromECSnapshotMaterializesSnapshotReadView(t *testing.T) {
	if !enterpriseBuildForTests {
		t.Skip("EC snapshot materialization requires the Enterprise EC backend")
	}
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	profileID := "ec-2-1"
	if err := srv.repo.PutECProfile(ctx, clustermeta.ECProfileRecord{
		ProfileID:       profileID,
		DataShards:      2,
		ParityShards:    1,
		StripeUnitBytes: clustermeta.DefaultECStripeUnitBytes,
		FailureDomain:   clustermeta.ECFailureDomainZone,
		Lifecycle:       clustermeta.ECProfileLifecycleActive,
		CreatedAtUnix:   1,
	}); err != nil {
		t.Fatalf("PutECProfile: %v", err)
	}

	localClients := make([]*local.Client, 0, 3)
	for i, node := range []struct {
		id   string
		zone string
	}{
		{id: "node-a", zone: "zone-a"},
		{id: "node-b", zone: "zone-b"},
		{id: "node-c", zone: "zone-c"},
	} {
		client, err := local.Open(local.Config{Path: filepath.Join(t.TempDir(), node.id)})
		if err != nil {
			t.Fatalf("local.Open(%s): %v", node.id, err)
		}
		t.Cleanup(func() { _ = client.Close() })
		endpoint := fmt.Sprintf("%s:%d", node.id, 19001+i)
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:         node.id,
			LifecycleState: clustermeta.NodeLifecycleActive,
			HealthState:    clustermeta.NodeHealthHealthy,
			Zone:           node.zone,
			SBSEndpoints:   []clustermeta.SBSEndpoint{{Address: node.id, Port: uint16(19001 + i)}},
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
		srv.cache.clients[endpoint] = cachedReplicaClient{client: client}
		localClients = append(localClients, client)
	}

	sourceVolumeID := "00e10001"
	targetVolumeID := "00e10002"
	if _, err := srv.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		Cluster:           &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:              &adminv1.RequestMeta{Actor: "tester", Reason: "ec-source"},
		VolumeId:          sourceVolumeID,
		SizeBytes:         1 << 20,
		BlockSize:         4096,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
		EcProfileId:       profileID,
		TopologyMode:      "strict",
	}); err != nil {
		t.Fatalf("CreateVolume ec source metadata: %v", err)
	}
	sourceRecord, err := srv.getVolumeSpec(ctx, sourceVolumeID)
	if err != nil {
		t.Fatalf("get source spec: %v", err)
	}
	sourceSpec := serviceSpecFromVolumeSpecRecord(sourceRecord)
	targetSize := sourceSpec.SizeBytes + uint64(sourceSpec.BlockSize)
	targetSpec := testVolumeSpecWithID(sourceSpec, targetVolumeID, targetSize)
	for _, client := range localClients {
		if _, err := client.CreateVolume(ctx, sourceSpec); err != nil {
			t.Fatalf("CreateVolume source local: %v", err)
		}
		if _, err := client.CreateVolume(ctx, targetSpec); err != nil {
			t.Fatalf("CreateVolume target local: %v", err)
		}
	}

	sourceClient, err := srv.newMaterializeClusterClient(ctx, sourceRecord, "ec-live-source")
	if err != nil {
		t.Fatalf("source cluster client: %v", err)
	}
	sourceOpen, err := sourceClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   sourceVolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: materializeRequestContext(
			"open-ec-source",
			"restore-ec-source",
			"ec-live-source",
			"att-ec-live-source",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("open EC source: %v", err)
	}
	defer func() {
		_, _ = sourceClient.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     sourceVolumeID,
			VolumeHandle: sourceOpen.VolumeHandle,
			Context: materializeRequestContext(
				"close-ec-source",
				"restore-ec-source",
				"ec-live-source",
				"att-ec-live-source",
				1,
				"",
			),
		})
	}()

	sourcePayload := make([]byte, sourceSpec.BlockSize)
	copy(sourcePayload, []byte("ec-restore-source-before"))
	if _, err := sourceClient.Write(ctx, &service.WriteRequest{
		VolumeID:     sourceVolumeID,
		VolumeHandle: sourceOpen.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  uint64(len(sourcePayload)),
		Data:         sourcePayload,
		Context: materializeRequestContext(
			"write-ec-source-before",
			"restore-ec-source",
			"ec-live-source",
			"att-ec-live-source",
			1,
			"ec-source-before",
		),
	}); err != nil {
		t.Fatalf("write EC source before snapshot: %v", err)
	}
	snapshotResp, err := srv.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:           &adminv1.RequestMeta{Actor: "tester", Reason: "ec-snapshot"},
		SourceVolumeId: sourceVolumeID,
		IdempotencyKey: "ec-restore-source-snapshot",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot EC: %v", err)
	}

	parentOverwrite := make([]byte, sourceSpec.BlockSize)
	copy(parentOverwrite, []byte("ec-restore-source-after"))
	if _, err := sourceClient.Write(ctx, &service.WriteRequest{
		VolumeID:     sourceVolumeID,
		VolumeHandle: sourceOpen.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  uint64(len(parentOverwrite)),
		Data:         parentOverwrite,
		Context: materializeRequestContext(
			"write-ec-source-after",
			"restore-ec-source",
			"ec-live-source",
			"att-ec-live-source",
			1,
			"ec-source-after",
		),
	}); err != nil {
		t.Fatalf("write EC source after snapshot: %v", err)
	}

	restoreResp, err := srv.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
		Cluster:          &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:             &adminv1.RequestMeta{Actor: "tester", Reason: "ec-restore"},
		SourceSnapshotId: snapshotResp.GetSnapshotId(),
		VolumeId:         targetVolumeID,
		SizeBytes:        targetSize,
	})
	if err != nil {
		t.Fatalf("CreateVolumeFromSnapshot EC: %v", err)
	}
	targetRecord, err := srv.getVolumeSpec(ctx, targetVolumeID)
	if err != nil {
		t.Fatalf("get EC target spec: %v", err)
	}
	if targetRecord.RedundancyBackend != clustermeta.RedundancyBackendEC || targetRecord.ECProfileID != profileID {
		t.Fatalf("target EC shape backend/profile=%q/%q", targetRecord.RedundancyBackend, targetRecord.ECProfileID)
	}

	verifyClient, err := srv.newMaterializeClusterClient(ctx, targetRecord, "verify-ec-restored")
	if err != nil {
		t.Fatalf("verify EC cluster client: %v", err)
	}
	verifyOpen, err := verifyClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   targetVolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: materializeRequestContext(
			"open-verify-ec-restored",
			restoreResp.GetCloneId(),
			"verify-ec-restored",
			"att-verify-ec-restored",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("open EC target: %v", err)
	}
	defer func() {
		_, _ = verifyClient.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     targetVolumeID,
			VolumeHandle: verifyOpen.VolumeHandle,
			Context: materializeRequestContext(
				"close-verify-ec-restored",
				restoreResp.GetCloneId(),
				"verify-ec-restored",
				"att-verify-ec-restored",
				1,
				"",
			),
		})
	}()
	verifyRead, err := verifyClient.Read(ctx, &service.ReadRequest{
		VolumeID:     targetVolumeID,
		VolumeHandle: verifyOpen.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  uint64(len(sourcePayload)),
		Context: materializeRequestContext(
			"read-verify-ec-restored",
			restoreResp.GetCloneId(),
			"verify-ec-restored",
			"att-verify-ec-restored",
			1,
			"",
		),
	})
	if err != nil {
		t.Fatalf("read EC target: %v", err)
	}
	if !bytes.Equal(verifyRead.Data, sourcePayload) {
		t.Fatalf("restored EC payload=%q want snapshot payload=%q", verifyRead.Data[:32], sourcePayload[:32])
	}
}

func testVolumeSpecWithID(base service.VolumeSpec, volumeID string, sizeBytes uint64) service.VolumeSpec {
	parsed, _ := service.ParseVolumeID(volumeID)
	spec := base
	spec.ID = service.HexVolumeID(parsed)
	spec.Name = volumeID
	spec.Prefix = "vol-" + volumeID
	spec.SizeBytes = sizeBytes
	return service.NormalizeVolumeSpec(spec)
}

func TestCloneMaterializeCopyRangesSkipZeroPagesAndOverlayDelta(t *testing.T) {
	chunkSize := uint32(128 << 10)
	pageBytes := uint32(4 << 20)
	chunksPerPage := uint64(pageBytes / chunkSize)
	clone := clustermeta.CloneRecord{
		CloneID:                  "clone-sparse",
		SourceSizeBytes:          1 << 30,
		SizeBytes:                2 << 30,
		AllocationChunkSizeBytes: chunkSize,
		AllocationPageSizeBytes:  pageBytes,
	}
	basePages := []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
				{LogicalChunkStart: 1, ChunkCount: 2, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 101},
				{LogicalChunkStart: 3, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2 * chunksPerPage, ChunkCount: 1, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 201},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         20,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 20 * chunksPerPage, ChunkCount: 4, Kind: clustermeta.AllocationKindZero},
			},
		},
	}
	deltaPages := []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "clone-sparse",
			PageNo:         2,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2 * chunksPerPage, ChunkCount: 4, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "clone-sparse",
			PageNo:         3,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 3*chunksPerPage + 1, ChunkCount: 1, Kind: clustermeta.AllocationKindShared, PhysicalChunkStart: 301},
			},
		},
	}

	ranges, err := cloneMaterializeCopyRangesFromPages(clone, basePages, deltaPages)
	if err != nil {
		t.Fatalf("cloneMaterializeCopyRangesFromPages: %v", err)
	}
	want := []materializeCopyRange{
		{OffsetBytes: uint64(chunkSize), LengthBytes: 2 * uint64(chunkSize)},
		{OffsetBytes: (3*chunksPerPage + 1) * uint64(chunkSize), LengthBytes: uint64(chunkSize)},
	}
	if len(ranges) != len(want) {
		t.Fatalf("ranges=%+v want %+v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("range[%d]=%+v want %+v", i, ranges[i], want[i])
		}
	}
}

func TestCloneMaterializeCopyRangesRejectOutOfPageExtent(t *testing.T) {
	chunkSize := uint32(128 << 10)
	pageBytes := uint32(4 << 20)
	chunksPerPage := uint64(pageBytes / chunkSize)
	clone := clustermeta.CloneRecord{
		CloneID:                  "clone-bad-page",
		SourceSizeBytes:          1 << 30,
		SizeBytes:                1 << 30,
		AllocationChunkSizeBytes: chunkSize,
		AllocationPageSizeBytes:  pageBytes,
	}
	basePages := []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSize,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: chunksPerPage, ChunkCount: 1, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 101},
			},
		},
	}

	if _, err := cloneMaterializeCopyRangesFromPages(clone, basePages, nil); err == nil {
		t.Fatalf("cloneMaterializeCopyRangesFromPages accepted out-of-page extent")
	}
}

func TestMaterializedCloneCreateVolumeRequestPreservesECShape(t *testing.T) {
	clone := clustermeta.CloneRecord{
		CloneID:                  "clone-ec",
		SizeBytes:                2 << 20,
		AllocationChunkSizeBytes: 128 << 10,
		AllocationPageSizeBytes:  4 << 20,
	}
	sourceSpec := volumeSpecRecord{
		VolumeID:             "00a1b2c3",
		SizeBytes:            1 << 20,
		BlockSize:            4096,
		ChunkSizeBytes:       128 << 10,
		ExtentPageBytes:      4 << 20,
		ExtentSizeBytes:      64 << 20,
		TopologyMode:         "strict",
		RedundancyBackend:    clustermeta.RedundancyBackendEC,
		ECProfileID:          "ec-6-3",
		WeakPlacementAllowed: true,
	}

	req := materializedCloneCreateVolumeRequest(&adminv1.MaterializeCloneRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Meta:    &adminv1.RequestMeta{Actor: "tester", Reason: "materialize"},
	}, clone, sourceSpec, "00a1b2c4")

	if req.GetVolumeId() != "00a1b2c4" || req.GetSizeBytes() != clone.SizeBytes {
		t.Fatalf("target identity/size volume=%q size=%d", req.GetVolumeId(), req.GetSizeBytes())
	}
	if req.GetRedundancyBackend() != clustermeta.RedundancyBackendEC || req.GetEcProfileId() != "ec-6-3" {
		t.Fatalf("target backend/profile=%q/%q", req.GetRedundancyBackend(), req.GetEcProfileId())
	}
	if req.GetTopologyMode() != "strict" || !req.GetWeakPlacementAllowed() {
		t.Fatalf("target topology weak=%q/%t", req.GetTopologyMode(), req.GetWeakPlacementAllowed())
	}
	if req.GetAllocationChunkSizeBytes() != clone.AllocationChunkSizeBytes || req.GetAllocationPageSizeBytes() != clone.AllocationPageSizeBytes {
		t.Fatalf("target allocation geometry chunk=%d page=%d", req.GetAllocationChunkSizeBytes(), req.GetAllocationPageSizeBytes())
	}
}

func TestMaterializeContextStatusCode(t *testing.T) {
	if got := materializeContextStatusCode(context.DeadlineExceeded); got != codes.DeadlineExceeded {
		t.Fatalf("deadline code=%v want %v", got, codes.DeadlineExceeded)
	}
	if got := materializeContextStatusCode(context.Canceled); got != codes.Canceled {
		t.Fatalf("canceled code=%v want %v", got, codes.Canceled)
	}
	if got := materializeContextStatusCode(fmt.Errorf("other")); got != codes.Internal {
		t.Fatalf("other code=%v want %v", got, codes.Internal)
	}
}

func TestPersistZeroAllocationPagesWritesExpectedPages(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	spec := clustermeta.ZeroAllocationPersistSpec{
		VolumeID:        "00a1b2c3",
		SizeBytes:       1 << 20,
		ChunkSizeBytes:  service.DefaultAllocationChunkSize,
		ExtentPageBytes: service.DefaultAllocationPageSize,
	}
	if err := clustermeta.PersistZeroAllocationPages(ctx, srv.repo, spec); err != nil {
		t.Fatalf("PersistZeroAllocationPages: %v", err)
	}

	pages, err := srv.repo.ListAllocationPages(ctx, spec.VolumeID)
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("allocation pages=%d want=1", len(pages))
	}
	if pages[0].Revision != 1 {
		t.Fatalf("allocation revision=%d want=1", pages[0].Revision)
	}
	if len(pages[0].Extents) != 1 {
		t.Fatalf("allocation extents=%d want=1", len(pages[0].Extents))
	}
	if pages[0].Extents[0].Kind != clustermeta.AllocationKindZero {
		t.Fatalf("allocation kind=%q want=%q", pages[0].Extents[0].Kind, clustermeta.AllocationKindZero)
	}
}

func TestPersistZeroAllocationPagesRejectsInvalidGeometry(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	err := clustermeta.PersistZeroAllocationPages(ctx, srv.repo, clustermeta.ZeroAllocationPersistSpec{
		VolumeID:        "00a1b2c3",
		SizeBytes:       1 << 20,
		ChunkSizeBytes:  4096,
		ExtentPageBytes: 0,
	})
	if err == nil {
		t.Fatal("PersistZeroAllocationPages succeeded with invalid geometry")
	}
}

func TestMaintenanceRunOnceAppliesDrainTransitionViaGRPCSBSData(t *testing.T) {
	ctx := context.Background()
	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x65),
		Name:      "vol-smoke",
		Prefix:    "vol-smoke-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})

	sourceA, addrA := startTestSBSData(t, "node-a", spec)
	sourceB, addrB := startTestSBSData(t, "node-b", spec)
	sourceC, addrC := startTestSBSData(t, "node-c", spec)
	_, addrD := startTestSBSData(t, "node-d", spec)

	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}
	seedSourcePayload(t, ctx, "00000065", replicaClients)

	kv, err := clustermeta.OpenPebbleKV(filepath.Join(t.TempDir(), "cluster-meta"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000065",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00000065",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00000065",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet source: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001-drain-node-a",
		VolumeID:         "00000065",
		PlacementRef:     "pl-000001-drain-node-a",
		Epoch:            2,
		PrimaryReplicaID: "rep-drain-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-drain-a", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-d"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet target: %v", err)
	}
	for _, node := range []struct {
		id   string
		addr string
		zone string
	}{
		{"node-a", addrA, "zone-a"},
		{"node-b", addrB, "zone-b"},
		{"node-c", addrC, "zone-c"},
		{"node-d", addrD, "zone-d"},
	} {
		if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
			SBSEndpoints:      []clustermeta.SBSEndpoint{parseEndpoint(node.addr)},
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:            "00000065",
		PlacementRef:        "pl-000001",
		State:               clustermeta.PlacementTransitionQueued,
		Reason:              "drain",
		CurrentReplicaSetID: "rs-000001",
		TargetReplicaSetID:  "rs-000001-drain-node-a",
		StartedAtUnix:       time.Now().Unix(),
		LastProgressAtUnix:  time.Now().Unix(),
		Attempt:             1,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}

	srv := &server{
		clusterID:    "test-cluster",
		sbsClusterID: "test-sbs",
		nodeID:       "svc-1",
		root:         defaultMetadataRoot,
		startedAt:    time.Now(),
		kv:           kv,
		repo:         repo,
		ops:          newOperationStore(kv, defaultMetadataRoot),
		cache:        newReplicaClientCache(),
		maint:        newMaintenanceSettings(),
	}
	defer srv.cache.Close()
	srv.ready.Store(true)

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	mapping, err := repo.GetExtentMapping(ctx, "00000065", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.PlacementRef != "pl-000001-drain-node-a" {
		t.Fatalf("placement_ref=%q want=%q", mapping.PlacementRef, "pl-000001-drain-node-a")
	}
	transition, err := repo.GetPlacementTransition(ctx, "00000065", "pl-000001")
	if err != nil {
		t.Fatalf("GetPlacementTransition: %v", err)
	}
	if transition.State != clustermeta.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, clustermeta.PlacementTransitionCompleted)
	}

	verifyClient := dialTestSBSClient(t, addrD)
	openResp, err := verifyClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   "00000065",
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "verify-open",
			GatewayID:    "verify",
			HostID:       "svc-1",
			SessionID:    "verify-target-rep-drain-a",
			AttachmentID: "verify-rs-000001-drain-node-a",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("Open verify: %v", err)
	}
	payloadResp, err := verifyClient.Read(ctx, &service.ReadRequest{
		VolumeID:     "00000065",
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Context: service.SBSRequestContext{
			RequestID:    "verify-read",
			GatewayID:    "verify",
			HostID:       "svc-1",
			SessionID:    "verify-target-rep-drain-a",
			AttachmentID: "verify-rs-000001-drain-node-a",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("Read verify: %v", err)
	}
	if string(payloadResp.Data[:len("drain-smoke")]) != "drain-smoke" {
		t.Fatalf("payload prefix=%q", payloadResp.Data[:len("drain-smoke")])
	}
}

func TestRunMaintenanceOnceSweepsRetiredPayloadBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true
	srv.payloadRoot = t.TempDir()

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		ReplicaID:         "rep-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-000001",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []clustermeta.AllocationExtentRecord{
			{
				LogicalChunkStart: 0,
				ChunkCount:        2,
				Kind:              clustermeta.AllocationKindZero,
			},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             "write-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   clustermeta.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{500},
		StartedAtUnix:           time.Now().Unix(),
		LastUpdatedAtUnix:       time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(write): %v", err)
	}

	replicaStores, err := clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	payloadStore := replicaStores.ObjectStores()["rep-a"]
	payloadKey := fmt.Sprintf("replicas/%s/volumes/%s/extents/%020d/chunks/%020d", "rep-a", "00a1b2c3", uint64(1), uint64(500))
	if err := payloadStore.Put(ctx, payloadKey, []byte("stale")); err != nil {
		t.Fatalf("Put(payload): %v", err)
	}
	if err := replicaStores.Close(); err != nil {
		t.Fatalf("Close(seed stores): %v", err)
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	replicaStores, err = clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("ReopenReplicaStores: %v", err)
	}
	defer replicaStores.Close()
	payloadStore = replicaStores.ObjectStores()["rep-a"]
	if _, found, err := payloadStore.Get(ctx, payloadKey); err != nil {
		t.Fatalf("Get(payload): %v", err)
	} else if found {
		t.Fatalf("expected retired payload chunk to be deleted")
	}
	operation, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", "payload-gc-00a1b2c3")
	if err != nil {
		t.Fatalf("GetMutationOperation(payload-gc): %v", err)
	}
	if operation.State != clustermeta.MutationOperationCommitted {
		t.Fatalf("payload-gc state=%q want=%q", operation.State, clustermeta.MutationOperationCommitted)
	}
	if len(operation.RetiredPhysicalChunkIDs) != 1 || operation.RetiredPhysicalChunkIDs[0] != 500 {
		t.Fatalf("payload-gc retired chunks=%v want=[500]", operation.RetiredPhysicalChunkIDs)
	}
}

func TestRunMaintenanceOnceSkipsRetiredPayloadSweepWhenPaused(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true
	srv.maint.pausePayloadGCs = true
	srv.payloadRoot = t.TempDir()

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		ReplicaID:         "rep-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-000001",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents:        []clustermeta.AllocationExtentRecord{{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindZero}},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             "write-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   clustermeta.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{500},
		StartedAtUnix:           time.Now().Unix(),
		LastUpdatedAtUnix:       time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(write): %v", err)
	}

	replicaStores, err := clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	payloadStore := replicaStores.ObjectStores()["rep-a"]
	payloadKey := fmt.Sprintf("replicas/%s/volumes/%s/extents/%020d/chunks/%020d", "rep-a", "00a1b2c3", uint64(1), uint64(500))
	if err := payloadStore.Put(ctx, payloadKey, []byte("stale")); err != nil {
		t.Fatalf("Put(payload): %v", err)
	}
	if err := replicaStores.Close(); err != nil {
		t.Fatalf("Close(seed stores): %v", err)
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	replicaStores, err = clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("ReopenReplicaStores: %v", err)
	}
	defer replicaStores.Close()
	payloadStore = replicaStores.ObjectStores()["rep-a"]
	if _, found, err := payloadStore.Get(ctx, payloadKey); err != nil {
		t.Fatalf("Get(payload): %v", err)
	} else if !found {
		t.Fatalf("expected retired payload chunk to remain while payload gc is paused")
	}
	if _, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", clustermeta.PayloadGCMutationOperationID("00a1b2c3")); err == nil {
		t.Fatalf("expected no payload-gc operation while payload gc is paused")
	}
}

func TestRunMaintenanceOnceRetriesFailedRetiredPayloadBatch(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true
	srv.payloadRoot = t.TempDir()

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		ReplicaID:         "rep-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-000001",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents:        []clustermeta.AllocationExtentRecord{{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindZero}},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	nowUnix := time.Now().Unix()
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             clustermeta.PayloadGCMutationOperationID("00a1b2c3"),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc",
		State:                   clustermeta.MutationOperationFailed,
		RetiredPhysicalChunkIDs: []uint64{500, 501},
		StartedAtUnix:           nowUnix - 120,
		LastUpdatedAtUnix:       nowUnix - 60,
	}); err != nil {
		t.Fatalf("PutMutationOperation(payload-gc): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             clustermeta.PayloadGCBatchMutationOperationID("00a1b2c3", 0),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc_batch",
		State:                   clustermeta.MutationOperationCommitted,
		IdempotencyKey:          clustermeta.PayloadGCMutationOperationID("00a1b2c3"),
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{500},
		StartedAtUnix:           nowUnix - 120,
		LastUpdatedAtUnix:       nowUnix - 90,
	}); err != nil {
		t.Fatalf("PutMutationOperation(batch0): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             clustermeta.PayloadGCBatchMutationOperationID("00a1b2c3", 1),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc_batch",
		State:                   clustermeta.MutationOperationFailed,
		IdempotencyKey:          clustermeta.PayloadGCMutationOperationID("00a1b2c3"),
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{501},
		StartedAtUnix:           nowUnix - 120,
		LastUpdatedAtUnix:       nowUnix - 60,
	}); err != nil {
		t.Fatalf("PutMutationOperation(batch1): %v", err)
	}

	replicaStores, err := clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	payloadStore := replicaStores.ObjectStores()["rep-a"]
	payloadKey := fmt.Sprintf("replicas/%s/volumes/%s/extents/%020d/chunks/%020d", "rep-a", "00a1b2c3", uint64(1), uint64(501))
	if err := payloadStore.Put(ctx, payloadKey, []byte("stale")); err != nil {
		t.Fatalf("Put(payload): %v", err)
	}
	if err := replicaStores.Close(); err != nil {
		t.Fatalf("Close(seed stores): %v", err)
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	replicaStores, err = clusterpayload.OpenReplicaStores(srv.payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("ReopenReplicaStores: %v", err)
	}
	defer replicaStores.Close()
	payloadStore = replicaStores.ObjectStores()["rep-a"]
	if _, found, err := payloadStore.Get(ctx, payloadKey); err != nil {
		t.Fatalf("Get(payload): %v", err)
	} else if found {
		t.Fatalf("expected retried payload-gc batch chunk to be deleted")
	}
	parentOp, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", clustermeta.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(parent): %v", err)
	}
	if parentOp.State != clustermeta.MutationOperationCommitted {
		t.Fatalf("payload-gc parent state=%q want=%q", parentOp.State, clustermeta.MutationOperationCommitted)
	}
	batch1Op, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", clustermeta.PayloadGCBatchMutationOperationID("00a1b2c3", 1))
	if err != nil {
		t.Fatalf("GetMutationOperation(batch1): %v", err)
	}
	if batch1Op.State != clustermeta.MutationOperationCommitted {
		t.Fatalf("payload-gc batch1 state=%q want=%q", batch1Op.State, clustermeta.MutationOperationCommitted)
	}
}

func TestRunMaintenanceOnceReconcilesMutationScopesFromRetiredChunks(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-000001",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             "write-retired-only",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   clustermeta.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{500},
		StartedAtUnix:           time.Now().Unix() - 60,
		LastUpdatedAtUnix:       time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(write): %v", err)
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	operation, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", "write-retired-only")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if len(operation.AffectedPageNos) != 1 || operation.AffectedPageNos[0] != 0 {
		t.Fatalf("affected pages=%v want=[0]", operation.AffectedPageNos)
	}
	if len(operation.AffectedExtentIDs) != 1 || operation.AffectedExtentIDs[0] != 1 {
		t.Fatalf("affected extents=%v want=[1]", operation.AffectedExtentIDs)
	}
}

func TestBuildRetiredPayloadSweepCandidatesPrioritizesLargestBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	for _, spec := range []volumeSpecRecord{
		{VolumeID: "vol-a", SizeBytes: 8, BlockSize: 4096, ChunkSizeBytes: 4, ExtentPageBytes: 8},
		{VolumeID: "vol-b", SizeBytes: 8, BlockSize: 4096, ChunkSizeBytes: 4, ExtentPageBytes: 8},
		{VolumeID: "vol-c", SizeBytes: 8, BlockSize: 4096, ChunkSizeBytes: 8, ExtentPageBytes: 8},
	} {
		if err := srv.putVolumeSpec(ctx, spec); err != nil {
			t.Fatalf("putVolumeSpec(%s): %v", spec.VolumeID, err)
		}
		if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
			VolumeID: spec.VolumeID,
			Epoch:    1,
			Revision: 10,
			Status:   clustermeta.VolumeStatusHealthy,
		}); err != nil {
			t.Fatalf("PutVolumeState(%s): %v", spec.VolumeID, err)
		}
	}
	for _, op := range []clustermeta.MutationOperationRecord{
		{
			OperationID:             "write-a",
			VolumeID:                "vol-a",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{100},
		},
		{
			OperationID:             "write-b",
			VolumeID:                "vol-b",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{200, 201},
		},
		{
			OperationID:             "write-c",
			VolumeID:                "vol-c",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{300, 301},
		},
		{
			OperationID:             clustermeta.PayloadGCMutationOperationID("vol-c"),
			VolumeID:                "vol-c",
			Kind:                    "payload_gc",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{300},
		},
	} {
		if err := srv.repo.PutMutationOperation(ctx, op); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", op.OperationID, err)
		}
	}

	candidates := srv.buildRetiredPayloadSweepCandidates(ctx, []clustermeta.VolumeState{
		{VolumeID: "vol-a"},
		{VolumeID: "vol-b"},
		{VolumeID: "vol-c"},
	})
	if len(candidates) != 3 {
		t.Fatalf("candidates=%d want=3", len(candidates))
	}
	if candidates[0].VolumeID != "vol-b" || candidates[0].Backlog.Chunks != 2 {
		t.Fatalf("candidate[0]=%+v want vol-b with 2 chunks", candidates[0])
	}
	if candidates[1].VolumeID != "vol-c" || candidates[1].Backlog.Chunks != 1 {
		t.Fatalf("candidate[1]=%+v want vol-c with 1 chunk", candidates[1])
	}
	if candidates[2].VolumeID != "vol-a" || candidates[2].Backlog.Chunks != 1 {
		t.Fatalf("candidate[2]=%+v want vol-a with 1 chunk", candidates[2])
	}
}

func TestBuildRetiredPayloadSweepCandidatesPrioritizesFailedBatches(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	nowUnix := time.Now().Unix()

	for _, spec := range []volumeSpecRecord{
		{VolumeID: "vol-a", SizeBytes: 8, BlockSize: 4096, ChunkSizeBytes: 4, ExtentPageBytes: 8},
		{VolumeID: "vol-b", SizeBytes: 8, BlockSize: 4096, ChunkSizeBytes: 4, ExtentPageBytes: 8},
	} {
		if err := srv.putVolumeSpec(ctx, spec); err != nil {
			t.Fatalf("putVolumeSpec(%s): %v", spec.VolumeID, err)
		}
		if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
			VolumeID: spec.VolumeID,
			Epoch:    1,
			Revision: 10,
			Status:   clustermeta.VolumeStatusHealthy,
		}); err != nil {
			t.Fatalf("PutVolumeState(%s): %v", spec.VolumeID, err)
		}
	}
	for _, op := range []clustermeta.MutationOperationRecord{
		{
			OperationID:             "write-a",
			VolumeID:                "vol-a",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{100, 101},
		},
		{
			OperationID:             "write-b",
			VolumeID:                "vol-b",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{200},
		},
		{
			OperationID:             clustermeta.PayloadGCMutationOperationID("vol-b"),
			VolumeID:                "vol-b",
			Kind:                    "payload_gc",
			State:                   clustermeta.MutationOperationFailed,
			RetiredPhysicalChunkIDs: []uint64{200},
		},
		{
			OperationID:             clustermeta.PayloadGCBatchMutationOperationID("vol-b", 0),
			VolumeID:                "vol-b",
			Kind:                    "payload_gc_batch",
			State:                   clustermeta.MutationOperationFailed,
			IdempotencyKey:          clustermeta.PayloadGCMutationOperationID("vol-b"),
			RetiredPhysicalChunkIDs: []uint64{200},
			LastUpdatedAtUnix:       nowUnix - 120,
		},
	} {
		if err := srv.repo.PutMutationOperation(ctx, op); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", op.OperationID, err)
		}
	}

	candidates := srv.buildRetiredPayloadSweepCandidates(ctx, []clustermeta.VolumeState{
		{VolumeID: "vol-a"},
		{VolumeID: "vol-b"},
	})
	if len(candidates) != 2 {
		t.Fatalf("candidates=%d want=2", len(candidates))
	}
	if candidates[0].VolumeID != "vol-b" || candidates[0].Backlog.FailedBatches != 1 {
		t.Fatalf("candidate[0]=%+v want vol-b with failed batch priority", candidates[0])
	}
	if candidates[1].VolumeID != "vol-a" {
		t.Fatalf("candidate[1]=%+v want vol-a", candidates[1])
	}
}

func seedRebalanceCandidateForTest(t *testing.T, ctx context.Context, srv *server, volumeID string) {
	t.Helper()
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: volumeID,
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		placementRef string
		replicaSetID string
	}{
		{1, "pl-000001", "rs-000001"},
		{2, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      volumeID,
			ExtentID:      extent.id,
			LogicalOffset: (extent.id - 1) * 4096,
			LengthBytes:   4096,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         volumeID,
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
}

func TestRunMaintenanceOnceHonorsPersistedRebalancePauseAfterRestart(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	volumeID := "00000066"
	seedRebalanceCandidateForTest(t, ctx, srv, volumeID)

	cluster := &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}
	if _, err := srv.PauseMaintenance(ctx, &adminv1.PauseMaintenanceRequest{
		Cluster:         cluster,
		Meta:            &adminv1.RequestMeta{Actor: "tester", Reason: "persist-rebalance-pause"},
		PauseRepairs:    true,
		PauseRebalances: true,
		PauseDrains:     true,
	}); err != nil {
		t.Fatalf("PauseMaintenance: %v", err)
	}

	restarted := newTestMaintenanceServerWithKV(t, srv.kv, "svc-2")
	restarted.leader = &leaderLeaseManager{}
	restarted.leader.isLeader.Store(true)
	if restarted.maint.snapshot().pauseRebalances {
		t.Fatalf("new server should start with an unpaused local default")
	}

	if err := restarted.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}
	transitions, err := restarted.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("transitions=%d want=0 while persisted rebalance pause is active: %+v", len(transitions), transitions)
	}
	statusResp, err := restarted.GetMaintenanceStatus(ctx, &adminv1.GetMaintenanceStatusRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("GetMaintenanceStatus: %v", err)
	}
	if !statusResp.GetThrottle().GetPauseRebalances() {
		t.Fatalf("persisted pause_rebalances was not reflected in status: %+v", statusResp.GetThrottle())
	}
}

func TestRunMaintenanceOnceContinuesAfterStaleECVolumeState(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          "00000065",
		Epoch:             1,
		Revision:          1,
		Status:            clustermeta.VolumeStatusHealthy,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
	}); err != nil {
		t.Fatalf("PutVolumeState(stale EC): %v", err)
	}
	volumeID := "00000066"
	seedRebalanceCandidateForTest(t, ctx, srv, volumeID)

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1 after stale EC volume was skipped", len(transitions))
	}
	if transitions[0].Reason != "rebalance" {
		t.Fatalf("transition reason=%q want=rebalance", transitions[0].Reason)
	}
}

func TestRunMaintenanceOnceReloadsRebalancePauseBeforeEnqueue(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	volumeID := "00000067"
	seedRebalanceCandidateForTest(t, ctx, srv, volumeID)

	cluster := &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}
	var hookErr error
	hookCalled := false
	srv.beforeMaintenanceVolume = func(ctx context.Context, currentVolumeID string) {
		if hookCalled || currentVolumeID != volumeID {
			return
		}
		hookCalled = true
		_, hookErr = srv.PauseMaintenance(ctx, &adminv1.PauseMaintenanceRequest{
			Cluster:         cluster,
			Meta:            &adminv1.RequestMeta{Actor: "tester", Reason: "mid-cycle-rebalance-pause"},
			PauseRepairs:    true,
			PauseRebalances: true,
			PauseDrains:     true,
		})
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("PauseMaintenance from hook: %v", hookErr)
	}
	if !hookCalled {
		t.Fatalf("maintenance volume hook was not called")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("transitions=%d want=0 while mid-cycle rebalance pause is active: %+v", len(transitions), transitions)
	}
}

func TestEnqueueVolumeRebalanceUsesUnusedActiveNode(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000066",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		placementRef string
		replicaSetID string
	}{
		{1, "pl-000001", "rs-000001"},
		{2, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00000066",
			ExtentID:      extent.id,
			LogicalOffset: (extent.id - 1) * 4096,
			LengthBytes:   4096,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00000066",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "00000066")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000066")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1", len(transitions))
	}
	if transitions[0].Reason != "rebalance" {
		t.Fatalf("transition reason=%q want=rebalance", transitions[0].Reason)
	}
	target, err := srv.repo.GetReplicaSet(ctx, "00000066", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-c") {
		t.Fatalf("target replica set should include node-c: %+v", target.Replicas)
	}
}

func TestEnqueueVolumeRebalanceSkipsFreshVolume(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	fixedNow := time.Unix(1000, 0)
	srv.now = func() time.Time { return fixedNow }
	srv.autoRebalanceMinVolumeAge = time.Minute
	const volumeID = "00000067"

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:      volumeID,
		SizeBytes:     8192,
		BlockSize:     4096,
		CreatedAtUnix: fixedNow.Unix(),
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	seedRebalanceCandidateForTest(t, ctx, srv, volumeID)

	enqueued, err := srv.enqueueVolumeRebalance(ctx, volumeID)
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance fresh: %v", err)
	}
	if enqueued {
		t.Fatalf("fresh volume rebalance enqueued before min age")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions fresh: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("fresh transitions=%d want=0", len(transitions))
	}

	fixedNow = fixedNow.Add(time.Minute + time.Second)
	enqueued, err = srv.enqueueVolumeRebalance(ctx, volumeID)
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance aged: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected aged volume rebalance enqueue")
	}
	transitions, err = srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions aged: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("aged transitions=%d want=1", len(transitions))
	}
	if transitions[0].Reason != "rebalance" {
		t.Fatalf("transition reason=%q want=rebalance", transitions[0].Reason)
	}
}

func TestEnqueueVolumeRebalanceSkipsActiveForegroundWrite(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	fixedNow := time.Unix(5000, 0)
	srv.now = func() time.Time { return fixedNow }
	srv.autoRebalanceForegroundWriteSettleAge = 15 * time.Minute
	const volumeID = "00000068"
	seedRebalanceCandidateForTest(t, ctx, srv, volumeID)

	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "write-active",
		VolumeID:          volumeID,
		Kind:              "write",
		State:             clustermeta.MutationOperationRunning,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		StartedAtUnix:     fixedNow.Unix(),
		LastUpdatedAtUnix: fixedNow.Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(write-active): %v", err)
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, volumeID)
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance active write: %v", err)
	}
	if enqueued {
		t.Fatalf("active foreground write should block auto-rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions active write: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("active write transitions=%d want=0", len(transitions))
	}

	op, err := srv.repo.GetMutationOperation(ctx, volumeID, "write-active")
	if err != nil {
		t.Fatalf("GetMutationOperation(write-active): %v", err)
	}
	op.State = clustermeta.MutationOperationCommitted
	op.LastUpdatedAtUnix = fixedNow.Unix()
	if err := srv.repo.PutMutationOperation(ctx, op); err != nil {
		t.Fatalf("PutMutationOperation(write-active committed): %v", err)
	}

	enqueued, err = srv.enqueueVolumeRebalance(ctx, volumeID)
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance settling write: %v", err)
	}
	if enqueued {
		t.Fatalf("settling foreground write should block auto-rebalance enqueue")
	}
	transitions, err = srv.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions settling write: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("settling write transitions=%d want=0", len(transitions))
	}

	fixedNow = fixedNow.Add(srv.autoRebalanceForegroundWriteSettleAge + time.Second)
	enqueued, err = srv.enqueueVolumeRebalance(ctx, volumeID)
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance settled write: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue after foreground write settle window")
	}
}

func TestEnqueueVolumeRebalanceStrictTopologyBlocksDuplicateZoneTarget(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:     "0000006c",
		Epoch:        1,
		Revision:     1,
		Status:       clustermeta.VolumeStatusHealthy,
		TopologyMode: "strict",
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-b"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "0000006c",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "0000006c",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rs-000001-rep-01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rs-000001-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rs-000001-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "0000006c")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if enqueued {
		t.Fatalf("strict topology enqueued duplicate-zone rebalance")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "0000006c")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("transitions=%d want=0", len(transitions))
	}
}

func TestEnqueueVolumeRebalanceSkipsSinglePlacementIdleClusterChurn(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "0000006d",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
		{"node-d", "zone-d"},
		{"node-e", "zone-e"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "0000006d",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "0000006d",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rs-000001-rep-01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rs-000001-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rs-000001-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rs-000001-rep-03", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "0000006d")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if enqueued {
		t.Fatalf("single-placement idle-cluster rebalance enqueued")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "0000006d")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("transitions=%d want=0", len(transitions))
	}
}

func TestEnqueueVolumeRebalanceSkipsUnusedNodeWithoutWritableStores(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000066",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
		{"node-d", "zone-d"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:             "node-c",
		StoreCount:         1,
		WritableStoreCount: 0,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}
	for _, extent := range []struct {
		id           uint64
		placementRef string
		replicaSetID string
	}{
		{1, "pl-000001", "rs-000001"},
		{2, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00000066",
			ExtentID:      extent.id,
			LogicalOffset: (extent.id - 1) * 4096,
			LengthBytes:   4096,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00000066",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "00000066")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000066")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1", len(transitions))
	}
	target, err := srv.repo.GetReplicaSet(ctx, "00000066", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if replicaSetContainsNode(target, "node-c") {
		t.Fatalf("target replica set should skip node-c without writable stores: %+v", target.Replicas)
	}
	if !replicaSetContainsNode(target, "node-d") {
		t.Fatalf("target replica set should include node-d: %+v", target.Replicas)
	}
}

func TestEnqueueVolumeRebalancePrioritizesZeroOnlyExtent(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00000066",
		SizeBytes:       24,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000066",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		offset       uint64
		placementRef string
		replicaSetID string
	}{
		{1, 0, "pl-000001", "rs-000001"},
		{2, 8, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00000066",
			ExtentID:      extent.id,
			LogicalOffset: extent.offset,
			LengthBytes:   8,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00000066",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00000066",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       1,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00000066",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       1,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "00000066")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000066")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1", len(transitions))
	}
	if transitions[0].PlacementRef != "pl-000001" {
		t.Fatalf("placement_ref=%q want=pl-000001", transitions[0].PlacementRef)
	}
}

func TestEnqueueVolumeRebalancePrefersSourceLocalTargetForRecentMutation(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000068",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
		host string
	}{
		{"node-a", "zone-a", "host-a"},
		{"node-b", "zone-b", "host-b"},
		{"node-c", "zone-c", "host-c"},
		{"node-d", "zone-b", "host-b"},
		{"node-e", "zone-a", "host-a"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			Host:              node.host,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		placementRef string
		replicaSetID string
	}{
		{1, "pl-000001", "rs-000001"},
		{2, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00000068",
			ExtentID:      extent.id,
			LogicalOffset: (extent.id - 1) * 4096,
			LengthBytes:   4096,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00000068",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "write-recent",
		VolumeID:          "00000068",
		Kind:              "write",
		State:             clustermeta.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(write-recent): %v", err)
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "00000068")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000068")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	target, err := srv.repo.GetReplicaSet(ctx, "00000068", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-e") {
		t.Fatalf("target replica set should include source-local node-e: %+v", target.Replicas)
	}
}

func TestEnqueueVolumeRebalancePrefersSourceLocalTargetForSmallRetryWindowCost(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "0000006a",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id   string
		zone string
		host string
	}{
		{"node-a", "zone-a", "host-a"},
		{"node-b", "zone-b", "host-b"},
		{"node-c", "zone-c", "host-c"},
		{"node-d", "zone-b", "host-b"},
		{"node-e", "zone-a", "host-a"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			Host:              node.host,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		placementRef string
		replicaSetID string
	}{
		{1, "pl-000001", "rs-000001"},
		{2, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "0000006a",
			ExtentID:      extent.id,
			LogicalOffset: (extent.id - 1) * 4096,
			LengthBytes:   4096,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "0000006a",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-retry-cost",
		VolumeID:          "0000006a",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 0, EndPageNo: 0, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-retry-cost): %v", err)
	}

	enqueued, err := srv.enqueueVolumeRebalance(ctx, "0000006a")
	if err != nil {
		t.Fatalf("enqueueVolumeRebalance: %v", err)
	}
	if !enqueued {
		t.Fatalf("expected rebalance enqueue")
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "0000006a")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	target, err := srv.repo.GetReplicaSet(ctx, "0000006a", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-e") {
		t.Fatalf("target replica set should include source-local node-e for small retry window cost: %+v", target.Replicas)
	}
}

func TestEnqueueDrainTransitionsPrioritizesZeroOnlyExtent(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00000067",
		SizeBytes:       24,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000067",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id     string
		zone   string
		state  clustermeta.NodeLifecycleState
		health clustermeta.NodeHealthState
	}{
		{"node-a", "zone-a", clustermeta.NodeLifecycleDraining, clustermeta.NodeHealthHealthy},
		{"node-b", "zone-b", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-c", "zone-c", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-d", "zone-d", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    node.state,
			HealthState:       node.health,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		offset       uint64
		placementRef string
		replicaSetID string
	}{
		{1, 0, "pl-000001", "rs-000001"},
		{2, 8, "pl-000002", "rs-000002"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00000067",
			ExtentID:      extent.id,
			LogicalOffset: extent.offset,
			LengthBytes:   8,
			ChunkID:       extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00000067",
			PlacementRef:     extent.placementRef,
			Epoch:            1,
			PrimaryReplicaID: extent.replicaSetID + "-rep-01",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
				{NodeID: "node-c", ReplicaID: extent.replicaSetID + "-rep-03", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00000067",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       1,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00000067",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       1,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}

	if err := srv.enqueueDrainTransitions(ctx, "node-a"); err != nil {
		t.Fatalf("enqueueDrainTransitions: %v", err)
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000067")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 2 {
		t.Fatalf("transitions=%d want=2", len(transitions))
	}
	if transitions[0].PlacementRef != "pl-000001" && transitions[1].PlacementRef != "pl-000001" {
		t.Fatalf("zero-only transition missing: %+v", transitions)
	}
}

func TestEnqueueDrainTransitionsMaterializesPlacementForAllocationOnlyVolume(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00000068",
		SizeBytes:         24,
		BlockSize:         4096,
		ChunkSizeBytes:    4,
		ExtentPageBytes:   8,
		ExtentSizeBytes:   8,
		ReplicationFactor: 3,
		RedundancyBackend: clustermeta.RedundancyBackendReplicated,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          "00000068",
		Epoch:             1,
		Revision:          1,
		ProtectionPolicy:  "rf3",
		RedundancyBackend: clustermeta.RedundancyBackendReplicated,
		Status:            clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id    string
		zone  string
		state clustermeta.NodeLifecycleState
	}{
		{"node-a", "zone-a", clustermeta.NodeLifecycleDraining},
		{"node-b", "zone-b", clustermeta.NodeLifecycleActive},
		{"node-c", "zone-c", clustermeta.NodeLifecycleActive},
		{"node-d", "zone-d", clustermeta.NodeLifecycleActive},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    node.state,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00000068",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       2,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, PhysicalChunkStart: 700, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if mappings, err := srv.repo.ListExtentMappings(ctx, "00000068"); err != nil {
		t.Fatalf("ListExtentMappings before drain: %v", err)
	} else if len(mappings) != 0 {
		t.Fatalf("precondition mappings=%d want=0", len(mappings))
	}

	progress, err := srv.computeDrainProgress(ctx, "node-a")
	if err != nil {
		t.Fatalf("computeDrainProgress before enqueue: %v", err)
	}
	if progress.extentsRemaining != 3 || progress.bytesRemaining != 8 {
		t.Fatalf("progress before enqueue extents=%d bytes=%d want extents=3 bytes=8", progress.extentsRemaining, progress.bytesRemaining)
	}
	if mappings, err := srv.repo.ListExtentMappings(ctx, "00000068"); err != nil {
		t.Fatalf("ListExtentMappings after progress: %v", err)
	} else if len(mappings) != 3 {
		t.Fatalf("progress materialized mappings=%d want=3", len(mappings))
	}

	if err := srv.enqueueDrainTransitions(ctx, "node-a"); err != nil {
		t.Fatalf("enqueueDrainTransitions: %v", err)
	}
	mappings, err := srv.repo.ListExtentMappings(ctx, "00000068")
	if err != nil {
		t.Fatalf("ListExtentMappings after drain: %v", err)
	}
	if len(mappings) != 3 {
		t.Fatalf("materialized mappings=%d want=3", len(mappings))
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000068")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 3 {
		t.Fatalf("transitions=%d want=3: %+v", len(transitions), transitions)
	}
	progress, err = srv.computeDrainProgress(ctx, "node-a")
	if err != nil {
		t.Fatalf("computeDrainProgress: %v", err)
	}
	if progress.extentsRemaining != 3 || progress.bytesRemaining != 8 {
		t.Fatalf("progress extents=%d bytes=%d want extents=3 bytes=8", progress.extentsRemaining, progress.bytesRemaining)
	}
}

func TestRefreshDrainOperationCancelsWhenNodeIsActiveAgain(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		Zone:              "zone-a",
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	op, err := srv.ops.create("node.drain", "node-a", "", "evacuating", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, err := srv.ops.update(op.GetOperationId(), func(cur *adminv1.OperationStatus) {
		cur.ExtentsRemaining = 32
		cur.BytesRemaining = 2 << 30
		cur.BlockingReason = "awaiting replica evacuation"
	}); err != nil {
		t.Fatalf("seed operation progress: %v", err)
	}

	refreshed := srv.refreshDrainOperation(ctx, op)
	if refreshed.GetState() != adminv1.OperationState_OPERATION_STATE_CANCELED {
		t.Fatalf("state=%s want canceled", refreshed.GetState())
	}
	if refreshed.GetPhase() != "not_draining" || refreshed.GetBlockingReason() != "" {
		t.Fatalf("phase=%q blocking=%q", refreshed.GetPhase(), refreshed.GetBlockingReason())
	}
	if refreshed.GetExtentsRemaining() != 0 || refreshed.GetBytesRemaining() != 0 {
		t.Fatalf("remaining extents=%d bytes=%d want zero", refreshed.GetExtentsRemaining(), refreshed.GetBytesRemaining())
	}
}

func TestRunMaintenanceOnceRequeuesDrainPlanningAfterReplacementRecoveryCooldown(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Now()
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000068",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id    string
		zone  string
		state clustermeta.NodeLifecycleState
	}{
		{"node-a", "zone-a", clustermeta.NodeLifecycleDraining},
		{"node-b", "zone-b", clustermeta.NodeLifecycleActive},
		{"node-c", "zone-c", clustermeta.NodeLifecycleActive},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    node.state,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now.Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                 "node-c",
		RecoveryEligibleAtUnix: time.Now().Add(30 * time.Second).Unix(),
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail future: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00000068",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00000068",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rs-000001-rep-01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rs-000001-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rs-000001-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	if err := srv.enqueueDrainTransitions(ctx, "node-a"); err != nil {
		t.Fatalf("enqueueDrainTransitions initial: %v", err)
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000068")
	if err != nil {
		t.Fatalf("ListPlacementTransitions initial: %v", err)
	}
	if len(transitions) != 0 {
		t.Fatalf("initial transitions=%d want=0 while replacement is in recovery cooldown", len(transitions))
	}

	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                 "node-c",
		RecoveryEligibleAtUnix: time.Now().Add(-time.Second).Unix(),
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail past: %v", err)
	}
	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatalf("runMaintenanceOnce: %v", err)
	}

	transitions, err = srv.repo.ListPlacementTransitions(ctx, "00000068")
	if err != nil {
		t.Fatalf("ListPlacementTransitions after maintenance: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1 after replacement cooldown clears", len(transitions))
	}
	if transitions[0].Reason != "drain" {
		t.Fatalf("transition reason=%q want=drain", transitions[0].Reason)
	}
	target, err := srv.repo.GetReplicaSet(ctx, "00000068", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-c") {
		t.Fatalf("target replica set should include recovered node-c: %+v", target.Replicas)
	}
}

func TestEnqueueDrainTransitionsPrefersSourceLocalTargetForRecentMutation(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000069",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id     string
		zone   string
		host   string
		state  clustermeta.NodeLifecycleState
		health clustermeta.NodeHealthState
	}{
		{"node-a", "zone-a", "host-a", clustermeta.NodeLifecycleDraining, clustermeta.NodeHealthHealthy},
		{"node-b", "zone-b", "host-b", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-c", "zone-c", "host-c", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-d", "zone-b", "host-b", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-e", "zone-a", "host-a", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    node.state,
			HealthState:       node.health,
			Zone:              node.zone,
			Host:              node.host,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00000069",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00000069",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rs-000001-rep-01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rs-000001-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rs-000001-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rs-000001-rep-03", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "write-recent",
		VolumeID:          "00000069",
		Kind:              "write",
		State:             clustermeta.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(write-recent): %v", err)
	}

	if err := srv.enqueueDrainTransitions(ctx, "node-a"); err != nil {
		t.Fatalf("enqueueDrainTransitions: %v", err)
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "00000069")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1", len(transitions))
	}
	target, err := srv.repo.GetReplicaSet(ctx, "00000069", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-e") {
		t.Fatalf("target replica set should include source-local node-e: %+v", target.Replicas)
	}
}

func TestEnqueueDrainTransitionsPrefersSourceLocalTargetForSmallRetryWindowCost(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "0000006b",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, node := range []struct {
		id     string
		zone   string
		host   string
		state  clustermeta.NodeLifecycleState
		health clustermeta.NodeHealthState
	}{
		{"node-a", "zone-a", "host-a", clustermeta.NodeLifecycleDraining, clustermeta.NodeHealthHealthy},
		{"node-b", "zone-b", "host-b", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-c", "zone-c", "host-c", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-d", "zone-b", "host-b", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
		{"node-e", "zone-a", "host-a", clustermeta.NodeLifecycleActive, clustermeta.NodeHealthHealthy},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    node.state,
			HealthState:       node.health,
			Zone:              node.zone,
			Host:              node.host,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "0000006b",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "0000006b",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rs-000001-rep-01",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rs-000001-rep-01", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rs-000001-rep-02", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rs-000001-rep-03", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-retry-cost",
		VolumeID:          "0000006b",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 0, EndPageNo: 0, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-retry-cost): %v", err)
	}

	if err := srv.enqueueDrainTransitions(ctx, "node-a"); err != nil {
		t.Fatalf("enqueueDrainTransitions: %v", err)
	}
	transitions, err := srv.repo.ListPlacementTransitions(ctx, "0000006b")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions=%d want=1", len(transitions))
	}
	target, err := srv.repo.GetReplicaSet(ctx, "0000006b", transitions[0].TargetReplicaSetID)
	if err != nil {
		t.Fatalf("GetReplicaSet(target): %v", err)
	}
	if !replicaSetContainsNode(target, "node-e") {
		t.Fatalf("target replica set should include source-local node-e for small retry window cost: %+v", target.Replicas)
	}
}

func TestCleanupCompletedTransitionsRemovesTransitionAndOldReplicaSet(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000067",
		Epoch:    2,
		Revision: 2,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00000067",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001-rebalance-node-a",
		Revision:      2,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00000067",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet(old): %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001-rebalance-node-a",
		VolumeID:         "00000067",
		PlacementRef:     "pl-000001-rebalance-node-a",
		Epoch:            2,
		PrimaryReplicaID: "rep-c",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet(target): %v", err)
	}
	if err := srv.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:            "00000067",
		PlacementRef:        "pl-000001",
		State:               clustermeta.PlacementTransitionCompleted,
		Reason:              "rebalance",
		CurrentReplicaSetID: "rs-000001",
		TargetReplicaSetID:  "rs-000001-rebalance-node-a",
		StartedAtUnix:       time.Now().Unix(),
		LastProgressAtUnix:  time.Now().Unix(),
		Attempt:             1,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}

	if err := srv.cleanupCompletedTransitions(ctx, "00000067"); err != nil {
		t.Fatalf("cleanupCompletedTransitions: %v", err)
	}
	if _, err := srv.repo.GetPlacementTransition(ctx, "00000067", "pl-000001"); err == nil {
		t.Fatalf("completed transition should be removed")
	}
	if _, err := srv.repo.GetReplicaSet(ctx, "00000067", "rs-000001"); err == nil {
		t.Fatalf("obsolete replica set should be removed")
	}
	if _, err := srv.repo.GetReplicaSet(ctx, "00000067", "rs-000001-rebalance-node-a"); err != nil {
		t.Fatalf("target replica set should remain: %v", err)
	}
}

func TestTopologyZoneLifecycleCRUD(t *testing.T) {
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	ctx := context.Background()
	cluster := &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}

	createResp, err := srv.CreateTopologyZone(ctx, &adminv1.CreateTopologyZoneRequest{
		Cluster:     cluster,
		Meta:        &adminv1.RequestMeta{Actor: "test", Reason: "create-zone"},
		ZoneId:      "zone-a",
		DisplayName: "Zone A",
		Labels:      map[string]string{"purpose": "smoke"},
	})
	if err != nil {
		t.Fatalf("CreateTopologyZone: %v", err)
	}
	if createResp.GetZone().GetLifecycle() != adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_ACTIVE {
		t.Fatalf("unexpected lifecycle: %s", createResp.GetZone().GetLifecycle())
	}

	disabled := adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED
	updateResp, err := srv.UpdateTopologyZone(ctx, &adminv1.UpdateTopologyZoneRequest{
		Cluster:   cluster,
		Meta:      &adminv1.RequestMeta{Actor: "test", Reason: "disable-zone"},
		ZoneId:    "zone-a",
		Lifecycle: &disabled,
		Labels:    map[string]string{"state": "disabled"},
	})
	if err != nil {
		t.Fatalf("UpdateTopologyZone: %v", err)
	}
	if updateResp.GetZone().GetLifecycle() != adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED {
		t.Fatalf("unexpected updated lifecycle: %s", updateResp.GetZone().GetLifecycle())
	}
	if updateResp.GetZone().GetLabels()["purpose"] != "smoke" || updateResp.GetZone().GetLabels()["state"] != "disabled" {
		t.Fatalf("labels were not merged: %+v", updateResp.GetZone().GetLabels())
	}

	listResp, err := srv.ListTopologyZones(ctx, &adminv1.ListTopologyZonesRequest{Cluster: cluster})
	if err != nil {
		t.Fatalf("ListTopologyZones: %v", err)
	}
	if len(listResp.GetZones()) != 1 || listResp.GetZones()[0].GetZoneId() != "zone-a" {
		t.Fatalf("unexpected zones: %+v", listResp.GetZones())
	}

	if _, err := srv.CreateTopologyZone(ctx, &adminv1.CreateTopologyZoneRequest{
		Cluster: cluster,
		Meta:    &adminv1.RequestMeta{Actor: "test", Reason: "create-zone"},
		ZoneId:  "zone-b",
	}); err != nil {
		t.Fatalf("CreateTopologyZone(zone-b): %v", err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:         "node-a",
		LifecycleState: clustermeta.NodeLifecycleActive,
		Zone:           "zone-a",
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	nodeUpdate, err := srv.UpdateNodeTopology(ctx, &adminv1.UpdateNodeTopologyRequest{
		Cluster: cluster,
		Meta:    &adminv1.RequestMeta{Actor: "test", Reason: "move-node"},
		NodeId:  "node-a",
		Zone:    "zone-b",
	})
	if err != nil {
		t.Fatalf("UpdateNodeTopology: %v", err)
	}
	if nodeUpdate.GetNode().GetZone() != "zone-b" {
		t.Fatalf("updated node zone=%q", nodeUpdate.GetNode().GetZone())
	}
	_, err = srv.DeleteTopologyZone(ctx, &adminv1.DeleteTopologyZoneRequest{
		Cluster: cluster,
		Meta:    &adminv1.RequestMeta{Actor: "test", Reason: "delete-zone"},
		ZoneId:  "zone-b",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteTopologyZone with node ref code=%s err=%v", status.Code(err), err)
	}

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00000070",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:     "00000070",
		ExtentID:     1,
		PlacementRef: "pl-000001",
		Revision:     1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-000001",
		VolumeID:         "00000070",
		PlacementRef:     "pl-000001",
		Epoch:            1,
		PrimaryReplicaID: "00000070-e000001-r01",
		WriteQuorum:      1,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "00000070-e000001-r01", Role: clustermeta.ReplicaRolePrimary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	_, err = srv.UpdateNodeTopology(ctx, &adminv1.UpdateNodeTopologyRequest{
		Cluster: cluster,
		Meta:    &adminv1.RequestMeta{Actor: "test", Reason: "move-node-with-placement"},
		NodeId:  "node-a",
		Zone:    "zone-a",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateNodeTopology with active placement code=%s err=%v", status.Code(err), err)
	}
}

func newTestMaintenanceServer(t *testing.T) *server {
	t.Helper()
	kv, err := clustermeta.OpenPebbleKV(filepath.Join(t.TempDir(), "cluster-meta"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return newTestMaintenanceServerWithKV(t, kv, "svc-1")
}

func newTestMaintenanceServerWithKV(t *testing.T, kv clustermeta.KV, nodeID string) *server {
	t.Helper()
	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	srv := &server{
		clusterID:    "test-cluster",
		sbsClusterID: "test-sbs",
		nodeID:       nodeID,
		root:         defaultMetadataRoot,
		startedAt:    time.Now(),
		kv:           kv,
		repo:         repo,
		ops:          newOperationStore(kv, defaultMetadataRoot),
		cache:        newReplicaClientCache(),
		maint:        newMaintenanceSettings(),
		placementApplyInternalService: clustercontrol.NewRepositoryBackedPlacementApplyInternalService(
			repo,
		),
		now:                        time.Now,
		maintenanceVolumeCooldown:  5 * time.Second,
		lastMaintenanceRunByVolume: make(map[string]int64),
		healthCheckInterval:        2 * time.Second,
		healthCheckTimeout:         time.Second,
		healthSuspectAfter:         1,
		healthDownAfter:            3,
		healthRecoverAfter:         2,
		healthRecoveryCooldown:     5 * time.Second,
	}
	t.Cleanup(func() { srv.cache.Close() })
	srv.ready.Store(true)
	return srv
}

func newBufconnPlacementApplyAdapter(t *testing.T, srv *server) clustercontrol.PlacementApplyAdapter {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	internalv1.RegisterPlacementApplyServiceServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///placement-apply-bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return clustercontrol.NewGRPCPlacementApplyAdapter(internalv1.NewPlacementApplyServiceClient(conn))
}

func TestSortMaintenanceJobsPrioritizesFailedRecentAndSmallBacklog(t *testing.T) {
	jobs := []maintenanceJob{
		{
			volumeID:     "vol-c",
			reason:       "repair",
			reasonCount:  1,
			reasonBytes:  8,
			reasonChunks: 2,
			smallBatches: 1,
		},
		{
			volumeID:           "vol-b",
			reason:             "repair",
			reasonCount:        1,
			reasonBytes:        16,
			reasonChunks:       4,
			recentBatches:      1,
			oldestFailedAgeSec: 10,
		},
		{
			volumeID:           "vol-a",
			reason:             "repair",
			reasonCount:        1,
			reasonBytes:        32,
			reasonChunks:       8,
			failedBatches:      1,
			oldestFailedAgeSec: 30,
		},
		{
			volumeID:      "vol-d",
			reason:        "rebalance",
			reasonCount:   1,
			reasonBytes:   4,
			reasonChunks:  1,
			failedBatches: 0,
		},
	}

	sortMaintenanceJobs(jobs)

	got := []string{jobs[0].volumeID, jobs[1].volumeID, jobs[2].volumeID, jobs[3].volumeID}
	want := []string{"vol-d", "vol-a", "vol-b", "vol-c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job[%d]=%s want=%s full=%v", i, got[i], want[i], got)
		}
	}
}

func TestSortMaintenanceJobsPrioritizesSmallerRetryWindowBacklog(t *testing.T) {
	jobs := []maintenanceJob{
		{
			volumeID:          "vol-b",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       8,
			reasonChunks:      2,
			retryWindows:      3,
			retryWindowBytes:  24,
			retryWindowChunks: 6,
		},
		{
			volumeID:          "vol-a",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       16,
			reasonChunks:      4,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
		},
		{
			volumeID:     "vol-c",
			reason:       "repair",
			reasonCount:  1,
			reasonBytes:  4,
			reasonChunks: 1,
			retryWindows: 0,
		},
	}

	sortMaintenanceJobs(jobs)

	got := []string{jobs[0].volumeID, jobs[1].volumeID, jobs[2].volumeID}
	want := []string{"vol-a", "vol-b", "vol-c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job[%d]=%s want=%s full=%v", i, got[i], want[i], got)
		}
	}
}

func TestSortMaintenanceJobsDeprioritizesLargeRetryWindowBacklogForFairness(t *testing.T) {
	jobs := []maintenanceJob{
		{
			volumeID:          "vol-large-retry",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       128,
			reasonChunks:      32,
			retryWindows:      2,
			retryWindowBytes:  2 * 1024 * 1024,
			retryWindowChunks: 512,
		},
		{
			volumeID:     "vol-no-retry",
			reason:       "repair",
			reasonCount:  1,
			reasonBytes:  64,
			reasonChunks: 16,
		},
		{
			volumeID:          "vol-small-retry",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       32,
			reasonChunks:      8,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
		},
	}

	sortMaintenanceJobs(jobs)

	got := []string{jobs[0].volumeID, jobs[1].volumeID, jobs[2].volumeID}
	want := []string{"vol-small-retry", "vol-no-retry", "vol-large-retry"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job[%d]=%s want=%s full=%v", i, got[i], want[i], got)
		}
	}
}

func TestSortMaintenanceJobsDeprioritizesCooledDownVolume(t *testing.T) {
	jobs := []maintenanceJob{
		{
			volumeID:          "vol-cooled",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       8,
			reasonChunks:      2,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
			cooldownActive:    true,
			cooldownRemaining: 5,
		},
		{
			volumeID:          "vol-ready",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       8,
			reasonChunks:      2,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
		},
	}

	sortMaintenanceJobs(jobs)

	if jobs[0].volumeID != "vol-ready" {
		t.Fatalf("job[0]=%s want=vol-ready", jobs[0].volumeID)
	}
}

func TestSortMaintenanceJobsPrefersSoonerCooldownExpiry(t *testing.T) {
	jobs := []maintenanceJob{
		{
			volumeID:          "vol-later",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       8,
			reasonChunks:      2,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
			cooldownActive:    true,
			cooldownRemaining: 9,
		},
		{
			volumeID:          "vol-sooner",
			reason:            "repair",
			reasonCount:       1,
			reasonBytes:       8,
			reasonChunks:      2,
			retryWindows:      1,
			retryWindowBytes:  8,
			retryWindowChunks: 2,
			cooldownActive:    true,
			cooldownRemaining: 3,
		},
	}

	sortMaintenanceJobs(jobs)

	if jobs[0].volumeID != "vol-sooner" {
		t.Fatalf("job[0]=%s want=vol-sooner", jobs[0].volumeID)
	}
}

func TestRequeueRetryableFailedTransitionsMarksTransitionQueuedAndParentPending(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}

	if err := srv.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		State:               clustermeta.PlacementTransitionFailed,
		Reason:              "repair",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-2",
		StartedAtUnix:       100,
		LastProgressAtUnix:  200,
		Attempt:             2,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{0},
		ErrorMessage:      "copy failed",
		LastUpdatedAtUnix: 200,
	}); err != nil {
		t.Fatalf("PutMutationOperation(parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-1-pages-00000000000000000001-00000000000000000001",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{1},
		ErrorMessage:      "page failed",
		LastUpdatedAtUnix: 201,
	}); err != nil {
		t.Fatalf("PutMutationOperation(batch): %v", err)
	}

	if err := srv.requeueRetryableFailedTransitions(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("requeueRetryableFailedTransitions: %v", err)
	}

	transition, err := srv.repo.GetPlacementTransition(ctx, "00a1b2c3", "pl-1")
	if err != nil {
		t.Fatalf("GetPlacementTransition: %v", err)
	}
	if transition.State != clustermeta.PlacementTransitionQueued {
		t.Fatalf("transition state=%q want=%q", transition.State, clustermeta.PlacementTransitionQueued)
	}
	parent, err := srv.repo.GetMutationOperation(ctx, "00a1b2c3", "transition-pl-1")
	if err != nil {
		t.Fatalf("GetMutationOperation(parent): %v", err)
	}
	if parent.State != clustermeta.MutationOperationPending {
		t.Fatalf("parent state=%q want=%q", parent.State, clustermeta.MutationOperationPending)
	}
	if parent.ErrorMessage != "" {
		t.Fatalf("parent error=%q want empty", parent.ErrorMessage)
	}
	if len(parent.RetryPageWindows) != 1 {
		t.Fatalf("retry page windows=%v want 1 window", parent.RetryPageWindows)
	}
	if parent.RetryPageWindows[0].ExtentID != 1 || parent.RetryPageWindows[0].StartPageNo != 1 || parent.RetryPageWindows[0].EndPageNo != 1 {
		t.Fatalf("retry page window=%+v want extent=1 pages=1-1", parent.RetryPageWindows[0])
	}
	if parent.RetryPageWindows[0].DataBytes != 8 || parent.RetryPageWindows[0].DataChunks != 2 {
		t.Fatalf("retry page window cost=%+v want bytes=8 chunks=2", parent.RetryPageWindows[0])
	}
}

func TestRunNodeHealthReconcilerTransitionsNodeToSuspectAndDown(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(2000, 0)
	srv.now = func() time.Time { return now }
	srv.healthCheckTimeout = 50 * time.Millisecond
	srv.healthSuspectAfter = 1
	srv.healthDownAfter = 2
	srv.healthRecoverAfter = 2
	srv.probeNodeHealth = func(context.Context, clustermeta.NodeMembershipRecord) error {
		return fmt.Errorf("probe failed")
	}

	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: now.Unix(),
		AdminHTTPEndpoint: "http://127.0.0.1:1",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: 1}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	if err := srv.runNodeHealthReconcilerOnce(ctx); err != nil {
		t.Fatalf("runNodeHealthReconcilerOnce suspect: %v", err)
	}
	rec, err := srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership suspect: %v", err)
	}
	if rec.HealthState != clustermeta.NodeHealthSuspect {
		t.Fatalf("health=%q want=%q", rec.HealthState, clustermeta.NodeHealthSuspect)
	}
	detail, err := srv.repo.GetNodeHealthDetail(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeHealthDetail suspect: %v", err)
	}
	if detail.ConsecutiveProbeFailures != 1 {
		t.Fatalf("failures=%d want=1", detail.ConsecutiveProbeFailures)
	}

	now = now.Add(time.Second)
	if err := srv.runNodeHealthReconcilerOnce(ctx); err != nil {
		t.Fatalf("runNodeHealthReconcilerOnce down: %v", err)
	}
	rec, err = srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership down: %v", err)
	}
	if rec.HealthState != clustermeta.NodeHealthDown {
		t.Fatalf("health=%q want=%q", rec.HealthState, clustermeta.NodeHealthDown)
	}
	detail, err = srv.repo.GetNodeHealthDetail(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeHealthDetail down: %v", err)
	}
	if detail.ConsecutiveProbeFailures != 2 {
		t.Fatalf("failures=%d want=2", detail.ConsecutiveProbeFailures)
	}
	if detail.HealthUpdatedBy != clustermeta.HealthUpdatedByReconciler {
		t.Fatalf("updated_by=%q want=%q", detail.HealthUpdatedBy, clustermeta.HealthUpdatedByReconciler)
	}
	if detail.LastProbeError == "" {
		t.Fatalf("last_probe_error should be populated")
	}
}

func TestRunNodeHealthReconcilerRecoversNodeToHealthyAfterSuccessStreak(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(3000, 0)
	srv.now = func() time.Time { return now }
	srv.healthCheckTimeout = 200 * time.Millisecond
	srv.healthSuspectAfter = 1
	srv.healthDownAfter = 2
	srv.healthRecoverAfter = 2
	srv.probeNodeHealth = func(context.Context, clustermeta.NodeMembershipRecord) error {
		return nil
	}

	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthDown,
		LastHeartbeatUnix: now.Unix() - 100,
		AdminHTTPEndpoint: "http://127.0.0.1:9082",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	if err := srv.runNodeHealthReconcilerOnce(ctx); err != nil {
		t.Fatalf("runNodeHealthReconcilerOnce first success: %v", err)
	}
	rec, err := srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership first success: %v", err)
	}
	if rec.HealthState != clustermeta.NodeHealthDown {
		t.Fatalf("health after first success=%q want down", rec.HealthState)
	}

	now = now.Add(time.Second)
	if err := srv.runNodeHealthReconcilerOnce(ctx); err != nil {
		t.Fatalf("runNodeHealthReconcilerOnce second success: %v", err)
	}
	rec, err = srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership second success: %v", err)
	}
	if rec.HealthState != clustermeta.NodeHealthHealthy {
		t.Fatalf("health=%q want=%q", rec.HealthState, clustermeta.NodeHealthHealthy)
	}
	if rec.LastHeartbeatUnix <= now.Unix()-100 {
		t.Fatalf("last_heartbeat=%d want updated value", rec.LastHeartbeatUnix)
	}
	detail, err := srv.repo.GetNodeHealthDetail(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeHealthDetail: %v", err)
	}
	if detail.ConsecutiveProbeSuccesses != 2 {
		t.Fatalf("successes=%d want=2", detail.ConsecutiveProbeSuccesses)
	}
	if detail.LastProbeError != "" {
		t.Fatalf("last_probe_error=%q want empty", detail.LastProbeError)
	}
	if detail.RecoveryEligibleAtUnix <= now.Unix() {
		t.Fatalf("recovery_eligible_at_unix=%d want future value", detail.RecoveryEligibleAtUnix)
	}
}

func TestRunNodeHealthReconcilerPersistsStoreCapacitySummary(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(3500, 0)
	srv.now = func() time.Time { return now }
	srv.healthCheckTimeout = time.Second
	srv.healthRecoverAfter = 1

	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			_, _ = w.Write([]byte("ok\n"))
		case "/debug/summary":
			_, _ = w.Write([]byte(`{"stores":[{"state":"healthy","weight":100,"capacity_bytes":1000,"available_bytes":700,"used_bytes":300,"compaction_pending_bytes":11,"compaction_in_progress_bytes":7},{"state":"read_only","weight":80,"capacity_bytes":500,"available_bytes":100,"used_bytes":400,"compaction_pending_bytes":5},{"state":"draining","weight":0,"capacity_bytes":250,"available_bytes":50,"used_bytes":200,"compaction_pending_bytes":3,"compaction_in_progress_bytes":2}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer admin.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer lis.Close()
	_, portText, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("Atoi port: %v", err)
	}

	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: now.Unix() - 100,
		AdminHTTPEndpoint: admin.URL,
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: uint16(port)}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	if err := srv.runNodeHealthReconcilerOnce(ctx); err != nil {
		t.Fatalf("runNodeHealthReconcilerOnce: %v", err)
	}
	detail, err := srv.repo.GetNodeHealthDetail(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeHealthDetail: %v", err)
	}
	if detail.StoreCount != 3 || detail.HealthyStoreCount != 1 || detail.WritableStoreCount != 1 {
		t.Fatalf("unexpected store counts: %+v", detail)
	}
	if !detail.StoreAllocationWeightObserved || detail.AllocatableStoreCount != 1 || detail.StoreAllocationWeightTotal != 180 {
		t.Fatalf("unexpected allocation weight summary: %+v", detail)
	}
	if detail.StoreCapacityBytes != 1750 || detail.StoreAvailableBytes != 850 || detail.StoreUsedBytes != 900 {
		t.Fatalf("unexpected store capacity summary: %+v", detail)
	}
	if detail.StoreCompactionPendingBytes != 19 || detail.StoreCompactionInProgressBytes != 9 {
		t.Fatalf("unexpected compaction summary: %+v", detail)
	}
	rec, err := srv.repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatalf("GetNodeMembership: %v", err)
	}
	if rec.CapacityBytes != 1750 || rec.UsedBytes != 900 {
		t.Fatalf("membership capacity=(%d,%d) want (1750,900)", rec.CapacityBytes, rec.UsedBytes)
	}
}

func TestSelectPlacementNodesSkipsNodeWithoutPositiveAllocationWeight(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(4150, 0)
	srv.now = func() time.Time { return now }

	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
		{"node-d", "zone-d"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now.Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                        "node-d",
		StoreCount:                    2,
		WritableStoreCount:            2,
		AllocatableStoreCount:         0,
		StoreAllocationWeightTotal:    0,
		StoreAllocationWeightObserved: true,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}

	selected, err := srv.selectPlacementNodes(ctx, 3)
	if err != nil {
		t.Fatalf("selectPlacementNodes: %v", err)
	}
	for _, node := range selected {
		if node.NodeID == "node-d" {
			t.Fatalf("selected node without positive allocation weight: %+v", selected)
		}
	}
}

func TestMaterializingSBSClientPreservesPhysicalChunkCapability(t *testing.T) {
	next := &physicalChunkForwardingFakeSBSClient{}
	wrapped := newMaterializingSBSClient(next, "http://127.0.0.1:1", volumeSpecRecord{VolumeID: "00000065"})
	physical, ok := wrapped.(service.PhysicalChunkSBSClient)
	if !ok {
		t.Fatalf("materializing client dropped PhysicalChunkSBSClient capability")
	}

	if _, err := physical.ReadPhysicalChunk(context.Background(), &service.ReadPhysicalChunkRequest{PhysicalChunkID: 7, LengthBytes: 4096}); err != nil {
		t.Fatalf("ReadPhysicalChunk: %v", err)
	}
	if next.readPhysicalChunkID != 7 {
		t.Fatalf("forwarded read physical chunk id=%d want=7", next.readPhysicalChunkID)
	}
	if _, err := physical.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{PhysicalChunkID: 8, LengthBytes: 4096, Data: make([]byte, 4096)}); err != nil {
		t.Fatalf("WritePhysicalChunk: %v", err)
	}
	if next.writePhysicalChunkID != 8 {
		t.Fatalf("forwarded write physical chunk id=%d want=8", next.writePhysicalChunkID)
	}
}

func TestMaterializingSBSClientMaterializesECShardReadOnNotFound(t *testing.T) {
	next := &physicalChunkForwardingFakeSBSClient{ecShardReadData: []byte("ec-shard")}
	materializeCalls := 0
	oldDefaultClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		materializeCalls++
		if r.Method != http.MethodPost {
			t.Fatalf("materialize method=%s want POST", r.Method)
		}
		if r.URL.Path != "/debug/materialize-volume" {
			t.Fatalf("materialize path=%s", r.URL.Path)
		}
		if got := r.URL.Query().Get("volume_id"); got != "00000065" {
			t.Fatalf("materialize volume_id=%q want 00000065", got)
		}
		next.markECVolumeMaterialized()
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: http.NoBody}, nil
	})}
	defer func() {
		http.DefaultClient = oldDefaultClient
	}()

	wrapped := newMaterializingSBSClient(next, "http://materializer.test", volumeSpecRecord{
		VolumeID:        "00000065",
		SizeBytes:       1 << 20,
		BlockSize:       4096,
		ChunkSizeBytes:  65536,
		ExtentPageBytes: 4 << 20,
	})
	ecClient, ok := wrapped.(service.ECShardSBSClient)
	if !ok {
		t.Fatalf("materializing client dropped ECShardSBSClient capability")
	}

	resp, err := ecClient.ReadECShard(context.Background(), &service.ReadECShardRequest{
		VolumeID:         "00000065",
		VolumeHandle:     "vh-00000065",
		ObjectID:         "ec:00000065:0:1:test",
		StripeID:         "0",
		StripeGeneration: 1,
		ShardID:          1,
		StoreID:          "node-a/default",
		LengthBytes:      8,
		Context: service.SBSRequestContext{
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "sess-a",
			AttachmentID: "att-a",
			Generation:   1,
		},
	})
	if err != nil {
		t.Fatalf("ReadECShard: %v", err)
	}
	if string(resp.Data) != "ec-shard" {
		t.Fatalf("ReadECShard data=%q want ec-shard", resp.Data)
	}
	if materializeCalls != 1 {
		t.Fatalf("materialize calls=%d want 1", materializeCalls)
	}
	if next.ecShardReadAttempts != 2 {
		t.Fatalf("ReadECShard attempts=%d want 2", next.ecShardReadAttempts)
	}
}

type physicalChunkForwardingFakeSBSClient struct {
	readPhysicalChunkID  uint64
	writePhysicalChunkID uint64

	mu                   sync.Mutex
	ecVolumeMaterialized bool
	ecShardReadAttempts  int
	ecShardReadData      []byte
}

func (c *physicalChunkForwardingFakeSBSClient) OpenVolume(context.Context, *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	return &service.OpenVolumeResponse{Status: "ok"}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) CloseVolume(context.Context, *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return &service.CloseVolumeResponse{Status: "ok"}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) GetVolumeProfile(context.Context, *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return &service.GetVolumeProfileResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) GetVolumeStatus(context.Context, *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return &service.GetVolumeStatusResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) Read(context.Context, *service.ReadRequest) (*service.ReadResponse, error) {
	return &service.ReadResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) Write(context.Context, *service.WriteRequest) (*service.WriteResponse, error) {
	return &service.WriteResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) Flush(context.Context, *service.FlushRequest) (*service.FlushResponse, error) {
	return &service.FlushResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) Discard(context.Context, *service.DiscardRequest) (*service.DiscardResponse, error) {
	return &service.DiscardResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) Zero(context.Context, *service.ZeroRequest) (*service.ZeroResponse, error) {
	return &service.ZeroResponse{}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) ReadPhysicalChunk(_ context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	c.readPhysicalChunkID = req.PhysicalChunkID
	return &service.ReadPhysicalChunkResponse{Data: make([]byte, req.LengthBytes)}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) WritePhysicalChunk(_ context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	c.writePhysicalChunkID = req.PhysicalChunkID
	return &service.WritePhysicalChunkResponse{Status: "ok"}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) WriteECShard(context.Context, *service.WriteECShardRequest) (*service.WriteECShardResponse, error) {
	return &service.WriteECShardResponse{Status: "ok"}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) ReadECShard(_ context.Context, req *service.ReadECShardRequest) (*service.ReadECShardResponse, error) {
	c.mu.Lock()
	c.ecShardReadAttempts++
	materialized := c.ecVolumeMaterialized
	data := append([]byte(nil), c.ecShardReadData...)
	c.mu.Unlock()
	if !materialized {
		return nil, &service.SBSError{Code: service.SBSErrorCodeNotFound, Message: "volume not found"}
	}
	return &service.ReadECShardResponse{
		VolumeID:         req.VolumeID,
		ObjectID:         req.ObjectID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardID:          req.ShardID,
		StoreID:          req.StoreID,
		LengthBytes:      uint64(len(data)),
		Data:             data,
	}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) DeleteECShard(context.Context, *service.DeleteECShardRequest) (*service.DeleteECShardResponse, error) {
	return &service.DeleteECShardResponse{Status: "ok"}, nil
}

func (c *physicalChunkForwardingFakeSBSClient) markECVolumeMaterialized() {
	c.mu.Lock()
	c.ecVolumeMaterialized = true
	c.mu.Unlock()
}

func TestSelectPlacementNodesSkipsRecoveredCooldownNode(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(4000, 0)
	srv.now = func() time.Time { return now }

	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
		{"node-d", "zone-d"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now.Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                 "node-c",
		RecoveryEligibleAtUnix: now.Unix() + 30,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}

	selected, err := srv.selectPlacementNodes(ctx, 3)
	if err != nil {
		t.Fatalf("selectPlacementNodes: %v", err)
	}
	for _, node := range selected {
		if node.NodeID == "node-c" {
			t.Fatalf("selected recovered cooldown node: %+v", selected)
		}
	}
}

func TestSelectPlacementNodesSkipsNodeWithoutWritableStores(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	now := time.Unix(4100, 0)
	srv.now = func() time.Time { return now }

	for _, node := range []struct {
		id   string
		zone string
	}{
		{"node-a", "zone-a"},
		{"node-b", "zone-b"},
		{"node-c", "zone-c"},
		{"node-d", "zone-d"},
	} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            node.id,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			Zone:              node.zone,
			LastHeartbeatUnix: now.Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.id, err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:             "node-d",
		StoreCount:         1,
		WritableStoreCount: 0,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}

	selected, err := srv.selectPlacementNodes(ctx, 3)
	if err != nil {
		t.Fatalf("selectPlacementNodes: %v", err)
	}
	for _, node := range selected {
		if node.NodeID == "node-d" {
			t.Fatalf("selected node without writable stores: %+v", selected)
		}
	}
}

func startTestSBSData(t *testing.T, nodeID string, spec service.VolumeSpec) (service.SBSClient, string) {
	t.Helper()
	client, err := local.Open(local.Config{Path: filepath.Join(t.TempDir(), nodeID)})
	if err != nil {
		t.Fatalf("local.Open(%s): %v", nodeID, err)
	}
	if _, err := client.CreateVolume(context.Background(), spec); err != nil {
		t.Fatalf("CreateVolume(%s): %v", nodeID, err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(%s): %v", nodeID, err)
	}
	grpcServer := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcServer, sbsgrpc.NewServer(client))
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
		_ = client.Close()
	})
	return client, lis.Addr().String()
}

func dialTestSBSClient(t *testing.T, endpoint string) service.SBSClient {
	t.Helper()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s): %v", endpoint, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return sbsgrpc.NewClient(sbsv1.NewVolumeServiceClient(conn))
}

func seedSourcePayload(t *testing.T, ctx context.Context, volumeID string, replicas map[string]service.SBSClient) {
	t.Helper()
	rems, err := replication.OpenReplicaSessions(ctx, replicas, replication.OpenReplicaSessionsRequest{
		VolumeID:      volumeID,
		GatewayID:     "seed",
		HostID:        "seed-host",
		AttachmentID:  "seed-" + volumeID,
		Generation:    1,
		SessionPrefix: "seed",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	writer := replication.NewRemoteReplicaWriter(rems)
	payload := make([]byte, 4096)
	copy(payload, []byte("drain-smoke"))
	if _, err := writer.WriteExtent(ctx, replication.ExtentWritePlan{
		Extent: metadataExtent(),
		WriteTargets: []replication.ReplicaTarget{
			{ReplicaID: "rep-a"},
			{ReplicaID: "rep-b"},
			{ReplicaID: "rep-c"},
		},
	}, replication.ReplicaWriteRequest{
		RequestID:      "seed-1",
		VolumeID:       volumeID,
		AttachmentID:   "seed-" + volumeID,
		Generation:     1,
		IdempotencyKey: "seed-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           payload,
	}); err != nil {
		t.Fatalf("WriteExtent seed: %v", err)
	}
	for _, replica := range rems {
		if _, err := replica.Client.CloseVolume(ctx, &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		}); err != nil {
			t.Fatalf("CloseVolume seed: %v", err)
		}
	}
}

func metadataExtent() clustermeta.ExtentMappingRecord {
	return clustermeta.ExtentMappingRecord{
		VolumeID:      "00000065",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1,
		PlacementRef:  "pl-000001",
		Revision:      1,
	}
}

func putTestECProfile(t *testing.T, srv *server, profileID string, lifecycle clustermeta.ECProfileLifecycle) {
	t.Helper()
	if err := srv.repo.PutECProfile(context.Background(), clustermeta.ECProfileRecord{
		ProfileID:       profileID,
		DataShards:      6,
		ParityShards:    3,
		StripeUnitBytes: clustermeta.DefaultECStripeUnitBytes,
		FailureDomain:   clustermeta.ECFailureDomainZone,
		Lifecycle:       lifecycle,
		CreatedAtUnix:   1,
	}); err != nil {
		t.Fatalf("PutECProfile(%s): %v", profileID, err)
	}
}

func TestValidateMetadataRuntimeConfigRejectsTiKVWithoutPDEndpoints(t *testing.T) {
	err := validateMetadataRuntimeConfig(metadataRuntimeConfig{
		backendName:     "tikv",
		tikvPDEndpoints: nil,
	})
	if err == nil {
		t.Fatal("expected error for tikv backend without PD endpoints")
	}
}

func TestValidateMetadataRuntimeConfigAllowsTiKVWithPDEndpoints(t *testing.T) {
	if err := validateMetadataRuntimeConfig(metadataRuntimeConfig{
		backendName:     "tikv",
		tikvPDEndpoints: []string{"127.0.0.1:2379"},
	}); err != nil {
		t.Fatalf("validateMetadataRuntimeConfig: %v", err)
	}
}

func TestValidateMetadataRuntimeConfigRejectsPebbleWithoutMetadataPath(t *testing.T) {
	err := validateMetadataRuntimeConfig(metadataRuntimeConfig{
		backendName:  "pebble",
		metadataPath: "",
	})
	if err == nil {
		t.Fatal("expected error for pebble backend without metadata path")
	}
}

func TestValidateMetadataRuntimeConfigAllowsPebbleWithMetadataPath(t *testing.T) {
	if err := validateMetadataRuntimeConfig(metadataRuntimeConfig{
		backendName:  "pebble",
		metadataPath: "./var/sbs-metadata",
	}); err != nil {
		t.Fatalf("validateMetadataRuntimeConfig: %v", err)
	}
}
