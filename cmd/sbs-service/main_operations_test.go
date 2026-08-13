package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/metadata"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGetOperationFallsBackToMutationOperation(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:        "payload-gc-00a1b2c3",
		VolumeID:           "00a1b2c3",
		Kind:               "payload_gc",
		State:              clustermeta.MutationOperationCommitted,
		AllocationRevision: 12,
		WriterFencingEpoch: 5,
		IdempotencyKey:     "00a1b2c3",
		StartedAtUnix:      1000,
		LastUpdatedAtUnix:  1005,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
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

	resp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: "payload-gc-00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if resp.GetOperation().GetKind() != "payload_gc" {
		t.Fatalf("kind=%q want=payload_gc", resp.GetOperation().GetKind())
	}
	if resp.GetOperation().GetState() != adminv1.OperationState_OPERATION_STATE_COMPLETED {
		t.Fatalf("state=%s want=COMPLETED", resp.GetOperation().GetState())
	}
	if resp.GetOperation().GetTargetVolumeId() != "00a1b2c3" {
		t.Fatalf("target_volume_id=%q", resp.GetOperation().GetTargetVolumeId())
	}
}

func TestGetNodeIncludesHealthReconcilerDetail(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthSuspect,
		LastHeartbeatUnix: 1000,
		AdminHTTPEndpoint: "http://127.0.0.1:9082",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                    "node-a",
		LastProbeUnix:             1001,
		LastProbeError:            "healthz probe failed",
		ConsecutiveProbeFailures:  2,
		ConsecutiveProbeSuccesses: 0,
		HealthReason:              "healthz probe failed",
		HealthUpdatedBy:           clustermeta.HealthUpdatedByReconciler,
		RecoveryEligibleAtUnix:    1005,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
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

	resp, err := srv.GetNode(ctx, &adminv1.GetNodeRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		NodeId:  "node-a",
	})
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	node := resp.GetNode()
	if node.GetAdminHttpEndpoint() != "http://127.0.0.1:9082" {
		t.Fatalf("admin_http_endpoint=%q want=http://127.0.0.1:9082", node.GetAdminHttpEndpoint())
	}
	if node.GetLastProbeError() != "healthz probe failed" {
		t.Fatalf("last_probe_error=%q want=healthz probe failed", node.GetLastProbeError())
	}
	if node.GetConsecutiveProbeFailures() != 2 {
		t.Fatalf("consecutive_probe_failures=%d want=2", node.GetConsecutiveProbeFailures())
	}
	if node.GetHealthUpdatedBy() != string(clustermeta.HealthUpdatedByReconciler) {
		t.Fatalf("health_updated_by=%q want=%q", node.GetHealthUpdatedBy(), clustermeta.HealthUpdatedByReconciler)
	}
	if node.GetRecoveryEligibleTime() == nil {
		t.Fatalf("recovery_eligible_time should be populated")
	}
}

func TestListNodesIncludesHealthReconcilerDetail(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: 1000,
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                    "node-a",
		LastProbeUnix:             1001,
		ConsecutiveProbeSuccesses: 3,
		HealthReason:              "healthy",
		HealthUpdatedBy:           clustermeta.HealthUpdatedByReconciler,
		RecoveryEligibleAtUnix:    1005,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
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

	resp, err := srv.ListNodes(ctx, &adminv1.ListNodesRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
	})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(resp.GetNodes()) != 1 {
		t.Fatalf("nodes=%d want=1", len(resp.GetNodes()))
	}
	if resp.GetNodes()[0].GetConsecutiveProbeSuccesses() != 3 {
		t.Fatalf("consecutive_probe_successes=%d want=3", resp.GetNodes()[0].GetConsecutiveProbeSuccesses())
	}
	if resp.GetNodes()[0].GetRecoveryEligibleTime() == nil {
		t.Fatalf("recovery_eligible_time should be populated")
	}
}

func TestUpdateNodeStoreWeightsForwardsRuntimeWeightUpdateToNodeAdminEndpoint(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	var called bool

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		AdminHTTPEndpoint: "http://node-a:9082",
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
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
		httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			called = true
			if r.URL.String() != "http://node-a:9082/admin/store-weights" {
				t.Fatalf("url=%q want=http://node-a:9082/admin/store-weights", r.URL.String())
			}
			if r.Method != http.MethodPost {
				t.Fatalf("method=%s want=POST", r.Method)
			}
			var payload struct {
				Stores []struct {
					StoreID string `json:"store_id"`
					Weight  int    `json:"weight"`
				} `json:"stores"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(payload.Stores) != 1 || payload.Stores[0].StoreID != "fast" || payload.Stores[0].Weight != 200 {
				t.Fatalf("unexpected payload: %+v", payload)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"persisted":true}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	defer srv.cache.Close()

	resp, err := srv.UpdateNodeStoreWeights(ctx, &adminv1.UpdateNodeStoreWeightsRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		NodeId:  "node-a",
		Stores: []*adminv1.StoreWeightSummary{
			{StoreId: "fast", Weight: 200},
		},
	})
	if err != nil {
		t.Fatalf("UpdateNodeStoreWeights: %v", err)
	}
	if resp == nil || resp.GetOperation() == nil {
		t.Fatalf("expected response payload")
	}
	if !resp.GetOperation().GetAccepted() {
		t.Fatalf("accepted=%t want=true", resp.GetOperation().GetAccepted())
	}
	if !strings.Contains(resp.GetOperation().GetMessage(), "persisted") {
		t.Fatalf("message=%q does not describe persisted semantics", resp.GetOperation().GetMessage())
	}
	if !called {
		t.Fatalf("expected node admin endpoint call")
	}
}

func TestUpdateNodeStoreTuningForwardsRuntimeTuningUpdateToNodeAdminEndpoint(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	var called bool

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		AdminHTTPEndpoint: "http://node-a:9082",
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
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
		httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			called = true
			if r.URL.String() != "http://node-a:9082/admin/store-tuning" {
				t.Fatalf("url=%q want=http://node-a:9082/admin/store-tuning", r.URL.String())
			}
			if r.Method != http.MethodPost {
				t.Fatalf("method=%s want=POST", r.Method)
			}
			var payload struct {
				Stores []struct {
					StoreID string `json:"store_id"`
					Weight  int    `json:"weight"`
				} `json:"stores"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(payload.Stores) != 1 || payload.Stores[0].StoreID != "fast" || payload.Stores[0].Weight != 200 {
				t.Fatalf("unexpected payload: %+v", payload)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"persisted":true}`)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	defer srv.cache.Close()

	resp, err := srv.UpdateNodeStoreTuning(ctx, &adminv1.UpdateNodeStoreTuningRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		NodeId:  "node-a",
		Stores: []*adminv1.StoreTuningSummary{
			{StoreId: "fast", Weight: 200},
		},
	})
	if err != nil {
		t.Fatalf("UpdateNodeStoreTuning: %v", err)
	}
	if resp == nil || resp.GetOperation() == nil {
		t.Fatalf("expected response payload")
	}
	if !resp.GetOperation().GetAccepted() {
		t.Fatalf("accepted=%t want=true", resp.GetOperation().GetAccepted())
	}
	if !strings.Contains(resp.GetOperation().GetMessage(), "persisted") {
		t.Fatalf("message=%q does not describe persisted semantics", resp.GetOperation().GetMessage())
	}
	if !called {
		t.Fatalf("expected node admin endpoint call")
	}
}

func TestUpdateNodeStoreTuningFallsBackToDebugEndpointWhenAdminPathMissing(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	var urls []string

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		AdminHTTPEndpoint: "http://node-a:9082",
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
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
		httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			urls = append(urls, r.URL.String())
			if strings.HasSuffix(r.URL.String(), "/admin/store-tuning") {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader(`not found`)),
					Header:     make(http.Header),
				}, nil
			}
			if strings.HasSuffix(r.URL.String(), "/debug/store-tuning") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"ok":true,"persisted":false}`)),
					Header:     make(http.Header),
				}, nil
			}
			t.Fatalf("unexpected url %q", r.URL.String())
			return nil, nil
		})},
	}
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)
	defer srv.cache.Close()

	resp, err := srv.UpdateNodeStoreTuning(ctx, &adminv1.UpdateNodeStoreTuningRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		NodeId:  "node-a",
		Stores: []*adminv1.StoreTuningSummary{
			{StoreId: "fast", Weight: 200},
		},
	})
	if err != nil {
		t.Fatalf("UpdateNodeStoreTuning: %v", err)
	}
	if resp == nil || resp.GetOperation() == nil {
		t.Fatalf("expected response payload")
	}
	if len(urls) != 2 {
		t.Fatalf("urls=%v want 2 attempts", urls)
	}
	if !strings.HasSuffix(urls[0], "/admin/store-tuning") || !strings.HasSuffix(urls[1], "/debug/store-tuning") {
		t.Fatalf("unexpected fallback order: %v", urls)
	}
}

func TestGetReplicaTargetsViewPublishesUsableTargetsAndReasonCodes(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    1,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}

	now := time.Unix(1000, 0).UTC()
	nodes := []clustermeta.NodeMembershipRecord{
		{
			NodeID:            "node-a",
			ReplicaID:         "rep-a",
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: now.Unix(),
			AdminHTTPEndpoint: "http://node-a:9082",
			SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "node-a", Port: 9460}},
		},
		{
			NodeID:            "node-b",
			ReplicaID:         "rep-b",
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthSuspect,
			LastHeartbeatUnix: now.Unix(),
			AdminHTTPEndpoint: "http://node-b:9082",
			SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "node-b", Port: 9460}},
		},
		{
			NodeID:            "node-c",
			ReplicaID:         "rep-c",
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: now.Unix(),
			AdminHTTPEndpoint: "http://node-c:9082",
			SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "node-c", Port: 9460}},
		},
		{
			NodeID:            "node-d",
			ReplicaID:         "rep-d",
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: now.Unix(),
		},
	}
	for _, node := range nodes {
		if err := repo.PutNodeMembership(ctx, node); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", node.NodeID, err)
		}
	}
	if err := repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                 "node-c",
		RecoveryEligibleAtUnix: now.Unix() + 30,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail(node-c): %v", err)
	}

	srv := &server{
		clusterID:           "test-cluster",
		sbsClusterID:        "test-sbs",
		nodeID:              "svc-1",
		root:                defaultMetadataRoot,
		startedAt:           now,
		kv:                  kv,
		repo:                repo,
		ops:                 newOperationStore(kv, defaultMetadataRoot),
		cache:               newReplicaClientCache(),
		maint:               newMaintenanceSettings(),
		healthCheckInterval: 5 * time.Second,
		now:                 func() time.Time { return now },
	}
	defer srv.cache.Close()

	resp, err := srv.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetReplicaTargetsView: %v", err)
	}
	if resp.GetRevision() != 12 {
		t.Fatalf("revision=%d want=12", resp.GetRevision())
	}
	if resp.GetCacheTtlSeconds() != 5 {
		t.Fatalf("cache_ttl_seconds=%d want=5", resp.GetCacheTtlSeconds())
	}

	byID := make(map[string]*adminv1.ReplicaTargetView)
	for _, target := range resp.GetTargets() {
		byID[target.GetTargetId()] = target
	}
	if got := byID["rep-a"]; got == nil || !got.GetUsable() || got.GetPriority() != 100 || got.GetReasonCode() != adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_READY {
		t.Fatalf("rep-a=%+v want usable priority=100 reason=READY", got)
	}
	if got := byID["node-a"]; got == nil || !got.GetUsable() {
		t.Fatalf("node-a alias missing or unusable: %+v", got)
	}
	if got := byID["rep-b"]; got == nil || !got.GetUsable() || got.GetPriority() != 50 || got.GetReasonCode() != adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_SUSPECT {
		t.Fatalf("rep-b=%+v want usable priority=50 reason=NODE_SUSPECT", got)
	}
	if got := byID["rep-c"]; got == nil || got.GetUsable() || got.GetReasonCode() != adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_RECOVERY_COOLDOWN {
		t.Fatalf("rep-c=%+v want unusable reason=RECOVERY_COOLDOWN", got)
	}
	if got := byID["rep-d"]; got == nil || got.GetUsable() || got.GetReasonCode() != adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_ENDPOINT_MISSING {
		t.Fatalf("rep-d=%+v want unusable reason=ENDPOINT_MISSING", got)
	}
}

func TestGetVolumePlacementViewReturnsExtentMappingsAndReplicaSets(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    3,
		Revision: 11,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            3,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"zone-a", "zone-b"},
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "zone-b"},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 << 20,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
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

	resp, err := srv.GetVolumePlacementView(ctx, &adminv1.GetVolumePlacementViewRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetVolumePlacementView: %v", err)
	}
	if resp.GetRevision() != 11 {
		t.Fatalf("revision=%d want=11", resp.GetRevision())
	}
	if len(resp.GetExtentMappings()) != 1 {
		t.Fatalf("extent_mappings=%d want=1", len(resp.GetExtentMappings()))
	}
	if resp.GetExtentMappings()[0].GetPlacementRef() != "pl-1" || resp.GetExtentMappings()[0].GetChunkId() != 101 {
		t.Fatalf("unexpected extent mapping: %+v", resp.GetExtentMappings()[0])
	}
	if len(resp.GetReplicaSets()) != 1 {
		t.Fatalf("replica_sets=%d want=1", len(resp.GetReplicaSets()))
	}
	rs := resp.GetReplicaSets()[0]
	if rs.GetReplicaSetId() != "rs-1" || rs.GetPlacementRef() != "pl-1" || rs.GetPrimaryReplicaId() != "rep-a" {
		t.Fatalf("unexpected replica set summary: %+v", rs)
	}
	if len(rs.GetReplicas()) != 2 || rs.GetReplicas()[0].GetNodeId() != "node-a" || rs.GetReplicas()[1].GetReplicaId() != "rep-b" {
		t.Fatalf("unexpected replica members: %+v", rs.GetReplicas())
	}
}

func TestGetVolumePlacementViewUsesPublishedViewCache(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    3,
		Revision: 11,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 << 20,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
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
		viewCache:    newPublishedViewCache(time.Minute),
		maint:        newMaintenanceSettings(),
	}
	defer srv.cache.Close()

	req := &adminv1.GetVolumePlacementViewRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	}
	first, err := srv.GetVolumePlacementView(ctx, req)
	if err != nil {
		t.Fatalf("first GetVolumePlacementView: %v", err)
	}
	if got := first.GetExtentMappings()[0].GetPlacementRef(); got != "pl-1" {
		t.Fatalf("first placement_ref=%q want pl-1", got)
	}

	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 << 20,
		ChunkID:       202,
		PlacementRef:  "pl-2",
		Revision:      12,
	}); err != nil {
		t.Fatalf("update PutExtentMapping: %v", err)
	}
	cached, err := srv.GetVolumePlacementView(ctx, req)
	if err != nil {
		t.Fatalf("cached GetVolumePlacementView: %v", err)
	}
	if got := cached.GetExtentMappings()[0].GetPlacementRef(); got != "pl-1" {
		t.Fatalf("cached placement_ref=%q want pl-1", got)
	}

	srv.viewCache.invalidateVolume("00a1b2c3")
	refreshed, err := srv.GetVolumePlacementView(ctx, req)
	if err != nil {
		t.Fatalf("refreshed GetVolumePlacementView: %v", err)
	}
	if got := refreshed.GetExtentMappings()[0].GetPlacementRef(); got != "pl-2" {
		t.Fatalf("refreshed placement_ref=%q want pl-2", got)
	}
}

func TestGetVolumeAllocationPageViewReturnsCompatiblePage(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    3,
		Revision: 11,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 << 20,
		ChunkSizeBytes: 4096,
		Revision:       11,
		Extents: []clustermeta.AllocationExtentRecord{
			{
				LogicalChunkStart:  0,
				ChunkCount:         2,
				Kind:               clustermeta.AllocationKindData,
				PhysicalChunkStart: 101,
				Encryption: &clustermeta.PayloadEncryptionHeader{
					HeaderVersion:    clustermeta.PayloadEncryptionHeaderVersion,
					CipherSuite:      "aes_256_gcm",
					EncryptionScope:  "volume",
					KeyProviderID:    "provider-a",
					DataKeyID:        "data-key-a",
					KeyID:            "key-a",
					ObjectID:         "replicated:00a1b2c3:101",
					BackendType:      clustermeta.PhysicalObjectBackendReplicated,
					NonceHex:         "00112233445566778899aabb",
					NonceSource:      "random_stored",
					PlaintextLength:  4096,
					CiphertextLength: 4096,
					AuthTagBytes:     16,
					AuthTagHex:       "00112233445566778899aabbccddeeff",
				},
			},
			{LogicalChunkStart: 2, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
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

	resp, err := srv.GetVolumeAllocationPageView(ctx, &adminv1.GetVolumeAllocationPageViewRequest{
		Cluster:        &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 << 20,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("GetVolumeAllocationPageView: %v", err)
	}
	if resp.GetRevision() != 11 {
		t.Fatalf("revision=%d want=11", resp.GetRevision())
	}
	page := resp.GetAllocationPage()
	if page == nil {
		t.Fatalf("allocation page missing")
	}
	if page.GetPageNo() != 0 || page.GetPageBytes() != 4<<20 || page.GetChunkSizeBytes() != 4096 || page.GetRevision() != 11 {
		t.Fatalf("unexpected allocation page header: %+v", page)
	}
	if len(page.GetExtents()) != 2 || page.GetExtents()[0].GetPhysicalChunkStart() != 101 || page.GetExtents()[1].GetKind() != string(clustermeta.AllocationKindZero) {
		t.Fatalf("unexpected allocation page extents: %+v", page.GetExtents())
	}
	if header := page.GetExtents()[0].GetEncryption(); header == nil || header.GetAuthTagHex() != "00112233445566778899aabbccddeeff" || header.GetBackendType() != string(clustermeta.PhysicalObjectBackendReplicated) {
		t.Fatalf("unexpected allocation page encryption header: %+v", header)
	}
}

func TestGetReplicaTargetsViewAllowsClusterWideBootstrapView(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	now := time.Unix(1000, 0).UTC()
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		ReplicaID:         "rep-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: now.Unix(),
		AdminHTTPEndpoint: "http://node-a:9082",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "node-a", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	srv := &server{
		clusterID:           "test-cluster",
		sbsClusterID:        "test-sbs",
		nodeID:              "svc-1",
		root:                defaultMetadataRoot,
		startedAt:           now,
		kv:                  kv,
		repo:                repo,
		ops:                 newOperationStore(kv, defaultMetadataRoot),
		cache:               newReplicaClientCache(),
		maint:               newMaintenanceSettings(),
		healthCheckInterval: 5 * time.Second,
		now:                 func() time.Time { return now },
	}
	defer srv.cache.Close()

	resp, err := srv.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
	})
	if err != nil {
		t.Fatalf("GetReplicaTargetsView: %v", err)
	}
	if resp.GetVolumeId() != "" {
		t.Fatalf("volume_id=%q want empty bootstrap view", resp.GetVolumeId())
	}
	if resp.GetRevision() != 0 {
		t.Fatalf("revision=%d want=0 for cluster-wide bootstrap view", resp.GetRevision())
	}
	if len(resp.GetTargets()) == 0 {
		t.Fatalf("expected bootstrap targets")
	}
}

func TestHandleDebugVolumeIncludesMutationOperations(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:        "transition-pl-1",
		VolumeID:           "00a1b2c3",
		Kind:               "transition",
		State:              clustermeta.MutationOperationPending,
		PlacementRevision:  1,
		AllocationRevision: 12,
		IdempotencyKey:     "pl-1",
		AffectedExtentIDs:  []uint64{1},
		AffectedPageNos:    []uint64{0, 1},
		CompletedPageNos:   []uint64{0},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 1, EndPageNo: 1},
		},
		StartedAtUnix:     1000,
		LastUpdatedAtUnix: 1001,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
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
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	req := httptest.NewRequest(http.MethodGet, "/debug/volume?volume_id=00a1b2c3", nil)
	rec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(rec, req)

	var payload struct {
		Volume                   map[string]any   `json:"volume"`
		MutationOperations       []map[string]any `json:"mutation_operations"`
		MutationOperationRecords []map[string]any `json:"mutation_operation_records"`
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if payload.Volume == nil {
		t.Fatalf("volume payload missing")
	}
	if len(payload.MutationOperations) != 1 {
		t.Fatalf("mutation_operations=%v", payload.MutationOperations)
	}
	if payload.MutationOperations[0]["operation_id"] != "transition-pl-1" {
		t.Fatalf("mutation op=%v", payload.MutationOperations[0])
	}
	if len(payload.MutationOperationRecords) != 1 {
		t.Fatalf("mutation_operation_records=%v", payload.MutationOperationRecords)
	}
	windows, ok := payload.MutationOperationRecords[0]["retry_page_windows"].([]any)
	if !ok || len(windows) != 1 {
		t.Fatalf("retry_page_windows=%v", payload.MutationOperationRecords[0]["retry_page_windows"])
	}
	window, ok := windows[0].(map[string]any)
	if !ok {
		t.Fatalf("retry page window=%T", windows[0])
	}
	if window["extent_id"] != float64(1) || window["start_page_no"] != float64(1) || window["end_page_no"] != float64(1) {
		t.Fatalf("retry page window=%v want extent=1 pages=1-1", window)
	}
}

func TestListOperationsIncludesMutationOperations(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:        "payload-gc-00a1b2c3",
		VolumeID:           "00a1b2c3",
		Kind:               "payload_gc",
		State:              clustermeta.MutationOperationCommitted,
		AllocationRevision: 12,
		StartedAtUnix:      1000,
		LastUpdatedAtUnix:  1005,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
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

	resp, err := srv.ListOperations(ctx, &adminv1.ListOperationsRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
	})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 1 {
		t.Fatalf("operations=%v", resp.GetOperations())
	}
	if resp.GetOperations()[0].GetOperationId() != "payload-gc-00a1b2c3" {
		t.Fatalf("operation=%v", resp.GetOperations()[0])
	}
}

func TestListOperationsFiltersMutationOperationsByKindAndState(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, rec := range []clustermeta.MutationOperationRecord{
		{
			OperationID:       "transition-pl-1",
			VolumeID:          "00a1b2c3",
			Kind:              "transition",
			State:             clustermeta.MutationOperationRunning,
			StartedAtUnix:     1000,
			LastUpdatedAtUnix: 1001,
		},
		{
			OperationID:       "payload-gc-00a1b2c3",
			VolumeID:          "00a1b2c3",
			Kind:              "payload_gc",
			State:             clustermeta.MutationOperationCommitted,
			StartedAtUnix:     1002,
			LastUpdatedAtUnix: 1003,
		},
	} {
		if err := repo.PutMutationOperation(ctx, rec); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", rec.OperationID, err)
		}
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

	resp, err := srv.ListOperations(ctx, &adminv1.ListOperationsRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		Kind:    "transition",
		State:   adminv1.OperationState_OPERATION_STATE_RUNNING,
	})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(resp.GetOperations()) != 1 {
		t.Fatalf("operations=%v", resp.GetOperations())
	}
	if resp.GetOperations()[0].GetOperationId() != "transition-pl-1" {
		t.Fatalf("operation=%v", resp.GetOperations()[0])
	}
}

func TestGetOperationShowsPayloadGCBatchProgress(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, rec := range []clustermeta.MutationOperationRecord{
		{
			OperationID:             "payload-gc-00a1b2c3",
			VolumeID:                "00a1b2c3",
			Kind:                    "payload_gc",
			State:                   clustermeta.MutationOperationRunning,
			AllocationRevision:      12,
			RetiredPhysicalChunkIDs: []uint64{500, 501},
			StartedAtUnix:           1000,
			LastUpdatedAtUnix:       1001,
		},
		{
			OperationID:             "payload-gc-00a1b2c3-batch-000000",
			VolumeID:                "00a1b2c3",
			Kind:                    "payload_gc_batch",
			State:                   clustermeta.MutationOperationCommitted,
			IdempotencyKey:          "payload-gc-00a1b2c3",
			RetiredPhysicalChunkIDs: []uint64{500},
			StartedAtUnix:           1002,
			LastUpdatedAtUnix:       1003,
		},
		{
			OperationID:             "payload-gc-00a1b2c3-batch-000001",
			VolumeID:                "00a1b2c3",
			Kind:                    "payload_gc_batch",
			State:                   clustermeta.MutationOperationRunning,
			IdempotencyKey:          "payload-gc-00a1b2c3",
			RetiredPhysicalChunkIDs: []uint64{501},
			StartedAtUnix:           1004,
			LastUpdatedAtUnix:       1005,
		},
	} {
		if err := repo.PutMutationOperation(ctx, rec); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", rec.OperationID, err)
		}
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

	resp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: "payload-gc-00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	phase := resp.GetOperation().GetPhase()
	for _, want := range []string{"batches=2", "completed=1", "running=1", "chunks=2"} {
		if !strings.Contains(phase, want) {
			t.Fatalf("phase=%q missing %q", phase, want)
		}
	}

	batchResp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: "payload-gc-00a1b2c3-batch-000001",
	})
	if err != nil {
		t.Fatalf("GetOperation(batch): %v", err)
	}
	batchPhase := batchResp.GetOperation().GetPhase()
	for _, want := range []string{"parent=payload-gc-00a1b2c3", "chunks=1"} {
		if !strings.Contains(batchPhase, want) {
			t.Fatalf("batch phase=%q missing %q", batchPhase, want)
		}
	}
}

func TestGetOperationShowsTransitionBatchProgress(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	parentID := "transition-pl-1"
	for _, rec := range []clustermeta.MutationOperationRecord{
		{
			OperationID:       parentID,
			VolumeID:          "00a1b2c3",
			Kind:              "transition",
			State:             clustermeta.MutationOperationRunning,
			PlacementRevision: 12,
			AffectedExtentIDs: []uint64{1},
			AffectedPageNos:   []uint64{0, 1},
			CompletedPageNos:  []uint64{0},
			StartedAtUnix:     1000,
			LastUpdatedAtUnix: 1001,
		},
		{
			OperationID:       "transition-pl-1-page-00000000000000000000",
			VolumeID:          "00a1b2c3",
			Kind:              "transition_batch",
			State:             clustermeta.MutationOperationCommitted,
			IdempotencyKey:    parentID,
			AffectedExtentIDs: []uint64{1},
			AffectedPageNos:   []uint64{0},
			CompletedPageNos:  []uint64{0},
			StartedAtUnix:     1002,
			LastUpdatedAtUnix: 1003,
		},
		{
			OperationID:       "transition-pl-1-page-00000000000000000001",
			VolumeID:          "00a1b2c3",
			Kind:              "transition_batch",
			State:             clustermeta.MutationOperationRunning,
			IdempotencyKey:    parentID,
			AffectedExtentIDs: []uint64{1},
			AffectedPageNos:   []uint64{1},
			StartedAtUnix:     1004,
			LastUpdatedAtUnix: 1005,
		},
		{
			OperationID:       "write-recent-page-1",
			VolumeID:          "00a1b2c3",
			Kind:              "write",
			State:             clustermeta.MutationOperationCommitted,
			AffectedExtentIDs: []uint64{1},
			AffectedPageNos:   []uint64{1},
			StartedAtUnix:     1006,
			LastUpdatedAtUnix: 1007,
		},
	} {
		if err := repo.PutMutationOperation(ctx, rec); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", rec.OperationID, err)
		}
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

	resp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: parentID,
	})
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	phase := resp.GetOperation().GetPhase()
	for _, want := range []string{"batches=2", "completed=1", "running=1", "recent=1", "small=2", "pages=2", "completed_pages=1", "remaining_retry_pages=1", "remaining_retry_batches=1"} {
		if !strings.Contains(phase, want) {
			t.Fatalf("phase=%q missing %q", phase, want)
		}
	}

	batchResp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: "transition-pl-1-page-00000000000000000001",
	})
	if err != nil {
		t.Fatalf("GetOperation(batch): %v", err)
	}
	batchPhase := batchResp.GetOperation().GetPhase()
	for _, want := range []string{"parent=transition-pl-1", "pages=1", "completed_pages=0", "remaining_pages=1"} {
		if !strings.Contains(batchPhase, want) {
			t.Fatalf("batch phase=%q missing %q", batchPhase, want)
		}
	}
}

func TestGetOperationShowsRequeuedTransitionRetryProgress(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	parentID := "transition-pl-2"
	for _, rec := range []clustermeta.MutationOperationRecord{
		{
			OperationID:       parentID,
			VolumeID:          "00a1b2c3",
			Kind:              "transition",
			State:             clustermeta.MutationOperationPending,
			PlacementRevision: 12,
			AffectedExtentIDs: []uint64{2},
			AffectedPageNos:   []uint64{2, 3},
			CompletedPageNos:  []uint64{2},
			RetryPageWindows: []clustermeta.MutationPageWindowRecord{
				{ExtentID: 2, StartPageNo: 3, EndPageNo: 3, DataBytes: 8, DataChunks: 2},
			},
			StartedAtUnix:     1000,
			LastUpdatedAtUnix: 1008,
		},
		{
			OperationID:       "transition-pl-2-page-00000000000000000003",
			VolumeID:          "00a1b2c3",
			Kind:              "transition_batch",
			State:             clustermeta.MutationOperationFailed,
			IdempotencyKey:    parentID,
			AffectedExtentIDs: []uint64{2},
			AffectedPageNos:   []uint64{3},
			StartedAtUnix:     1004,
			LastUpdatedAtUnix: 1007,
			ErrorMessage:      "retry me",
		},
	} {
		if err := repo.PutMutationOperation(ctx, rec); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", rec.OperationID, err)
		}
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

	resp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: parentID,
	})
	if err != nil {
		t.Fatalf("GetOperation(parent): %v", err)
	}
	phase := resp.GetOperation().GetPhase()
	for _, want := range []string{"retry=requeued", "remaining_retry_pages=1", "remaining_retry_batches=1", "retry_windows=1", "retry_window_bytes=8", "retry_window_chunks=2", "next_retry_window=extent:2 pages:3-3 bytes:8 chunks:2"} {
		if !strings.Contains(phase, want) {
			t.Fatalf("parent phase=%q missing %q", phase, want)
		}
	}

	batchResp, err := srv.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		OperationId: "transition-pl-2-page-00000000000000000003",
	})
	if err != nil {
		t.Fatalf("GetOperation(batch): %v", err)
	}
	batchPhase := batchResp.GetOperation().GetPhase()
	for _, want := range []string{"parent=transition-pl-2", "remaining_pages=1", "parent_retry=requeued"} {
		if !strings.Contains(batchPhase, want) {
			t.Fatalf("batch phase=%q missing %q", batchPhase, want)
		}
	}
}

func TestObservabilitySnapshotTracksAllocationAwareTransitionBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	baseNow := time.Unix(2000, 0)
	srv.now = func() time.Time { return baseNow }
	srv.maintenanceVolumeCooldown = 10 * time.Second
	srv.lastMaintenanceRunByVolume["00a1b2c3"] = baseNow.Add(-2 * time.Second).Unix()

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("PutVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            nodeID,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		offset       uint64
		placementRef string
		replicaSetID string
	}{
		{1, 0, "pl-1", "rs-1"},
		{2, 8, "pl-2", "rs-2"},
		{3, 16, "pl-3", "rs-3"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00a1b2c3",
			ExtentID:      extent.id,
			LogicalOffset: extent.offset,
			LengthBytes:   8,
			ChunkID:       100 + extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      12,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00a1b2c3",
			PlacementRef:     extent.placementRef,
			Epoch:            5,
			PrimaryReplicaID: extent.replicaSetID + "-rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-a", Role: clustermeta.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-b", Role: clustermeta.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: extent.replicaSetID + "-rep-c", Role: clustermeta.ReplicaRoleSecondary},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, PhysicalChunkStart: 500, ChunkCount: 1, Kind: clustermeta.AllocationKindData},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	for _, tr := range []clustermeta.PlacementTransitionRecord{
		{VolumeID: "00a1b2c3", PlacementRef: "pl-1", State: clustermeta.PlacementTransitionQueued, Reason: "repair", CurrentReplicaSetID: "rs-1", TargetReplicaSetID: "rs-1-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-2", State: clustermeta.PlacementTransitionRunning, Reason: "rebalance", CurrentReplicaSetID: "rs-2", TargetReplicaSetID: "rs-2-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-3", State: clustermeta.PlacementTransitionQueued, Reason: "drain", CurrentReplicaSetID: "rs-3", TargetReplicaSetID: "rs-3-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
	} {
		if err := srv.repo.PutPlacementTransition(ctx, tr); err != nil {
			t.Fatalf("PutPlacementTransition(%s): %v", tr.PlacementRef, err)
		}
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2-page-00000000000000000002",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix() - 120,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-batch): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "write-recent-page-1",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             clustermeta.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(write-recent): %v", err)
	}

	snapshot, _ := srv.observabilitySnapshot(ctx)
	if snapshot.RepairBacklog != 1 || snapshot.RepairBacklogBytes != 4 || snapshot.RepairBacklogChunks != 1 {
		t.Fatalf("repair backlog=%d bytes=%d chunks=%d", snapshot.RepairBacklog, snapshot.RepairBacklogBytes, snapshot.RepairBacklogChunks)
	}
	if snapshot.RebalanceBacklog != 1 || snapshot.RebalanceBacklogBytes != 8 || snapshot.RebalanceBacklogChunks != 2 {
		t.Fatalf("rebalance backlog=%d bytes=%d chunks=%d", snapshot.RebalanceBacklog, snapshot.RebalanceBacklogBytes, snapshot.RebalanceBacklogChunks)
	}
	if snapshot.DrainBacklog != 1 || snapshot.DrainBacklogBytes != 0 || snapshot.DrainBacklogChunks != 0 {
		t.Fatalf("drain backlog=%d bytes=%d chunks=%d", snapshot.DrainBacklog, snapshot.DrainBacklogBytes, snapshot.DrainBacklogChunks)
	}
	if snapshot.TransitionFailedBatches != 1 || snapshot.TransitionRecentBatches != 1 || snapshot.TransitionSmallBatches != 1 || snapshot.TransitionRequeued != 1 || snapshot.TransitionRetryPages != 1 || snapshot.TransitionRetryWindows != 1 || snapshot.TransitionRetryWindowBytes != 8 || snapshot.TransitionRetryWindowChunks != 2 {
		t.Fatalf("transition batch snapshot failed=%d recent=%d small=%d requeued=%d retry_pages=%d retry_windows=%d retry_bytes=%d retry_chunks=%d want=1/1/1/1/1/1/8/2", snapshot.TransitionFailedBatches, snapshot.TransitionRecentBatches, snapshot.TransitionSmallBatches, snapshot.TransitionRequeued, snapshot.TransitionRetryPages, snapshot.TransitionRetryWindows, snapshot.TransitionRetryWindowBytes, snapshot.TransitionRetryWindowChunks)
	}
	if snapshot.MaintenanceCooldownVolumes != 1 || snapshot.MaintenanceCooldownMaxSec != 8 {
		t.Fatalf("maintenance cooldown volumes=%d max=%d want=1/8", snapshot.MaintenanceCooldownVolumes, snapshot.MaintenanceCooldownMaxSec)
	}
}

func TestObservabilityEndpointsExposeAllocationAwareTransitionBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.metadataBackendName = "tikv"
	srv.metadataRuntimeMode = "primary-tikv"
	srv.tikvPDEndpointsConfigured = true
	srv.ready.Store(true)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("PutVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            nodeID,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            5,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	if err := srv.repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, PhysicalChunkStart: 500, ChunkCount: 1, Kind: clustermeta.AllocationKindData},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := srv.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		State:               clustermeta.PlacementTransitionQueued,
		Reason:              "repair",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-1-r",
		StartedAtUnix:       1000,
		LastProgressAtUnix:  1000,
		Attempt:             1,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/debug/summary", nil)
	summaryRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(summaryRec, summaryReq)
	if summaryRec.Code != http.StatusOK {
		t.Fatalf("summary status=%d body=%s", summaryRec.Code, summaryRec.Body.String())
	}
	var summary map[string]any
	if err := json.NewDecoder(summaryRec.Body).Decode(&summary); err != nil {
		t.Fatalf("Decode summary: %v", err)
	}
	if got := summary["repair_backlog_bytes"]; got != float64(4) {
		t.Fatalf("repair_backlog_bytes=%v want=4", got)
	}
	if got := summary["repair_backlog_chunks"]; got != float64(1) {
		t.Fatalf("repair_backlog_chunks=%v want=1", got)
	}
	if got := summary["repair_backlog_current"]; got != float64(1) {
		t.Fatalf("repair_backlog_current=%v want=1", got)
	}
	if got := summary["rebalance_backlog_current"]; got != float64(0) {
		t.Fatalf("rebalance_backlog_current=%v want=0", got)
	}
	if got := summary["metadata_backend"]; got != "tikv" {
		t.Fatalf("metadata_backend=%v want=tikv", got)
	}
	if got := summary["runtime_mode"]; got != "primary-tikv" {
		t.Fatalf("runtime_mode=%v want=primary-tikv", got)
	}
	if got := summary["tikv_pd_endpoints_configured"]; got != true {
		t.Fatalf("tikv_pd_endpoints_configured=%v want=true", got)
	}
	if got := summary["metadata_path_configured"]; got != false {
		t.Fatalf("metadata_path_configured=%v want=false", got)
	}
	if got := summary["control_plane_owner"]; got != "sbs-service" {
		t.Fatalf("control_plane_owner=%v want=sbs-service", got)
	}
	if got := summary["cluster_metadata_owner"]; got != "tikv" {
		t.Fatalf("cluster_metadata_owner=%v want=tikv", got)
	}
	if got := summary["dev_metadata_owner"]; got != "local-pebble" {
		t.Fatalf("dev_metadata_owner=%v want=local-pebble", got)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(metricsRec, metricsReq)
	body := metricsRec.Body.String()
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metricsRec.Code, body)
	}
	for _, want := range []string{
		"sbs_service_transition_backlog{reason=\"repair\"} 1",
		"sbs_service_repair_backlog_current 1",
		"sbs_service_rebalance_backlog_current 0",
		"sbs_service_transition_backlog_bytes{reason=\"repair\"} 4",
		"sbs_service_transition_backlog_chunks{reason=\"repair\"} 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in\n%s", want, body)
		}
	}
}

func TestGetClusterStatusIncludesAllocationAwareBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	baseNow := time.Unix(2000, 0)
	srv.now = func() time.Time { return baseNow }
	srv.maintenanceVolumeCooldown = 10 * time.Second
	srv.lastMaintenanceRunByVolume["00a1b2c3"] = baseNow.Add(-2 * time.Second).Unix()

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("PutVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            nodeID,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		offset       uint64
		placementRef string
		replicaSetID string
	}{
		{1, 0, "pl-1", "rs-1"},
		{2, 8, "pl-2", "rs-2"},
		{3, 16, "pl-3", "rs-3"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00a1b2c3",
			ExtentID:      extent.id,
			LogicalOffset: extent.offset,
			LengthBytes:   8,
			ChunkID:       100 + extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      12,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00a1b2c3",
			PlacementRef:     extent.placementRef,
			Epoch:            5,
			PrimaryReplicaID: extent.replicaSetID + "-rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-a", Role: clustermeta.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-b", Role: clustermeta.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: extent.replicaSetID + "-rep-c", Role: clustermeta.ReplicaRoleSecondary},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, PhysicalChunkStart: 500, ChunkCount: 1, Kind: clustermeta.AllocationKindData},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	for _, tr := range []clustermeta.PlacementTransitionRecord{
		{VolumeID: "00a1b2c3", PlacementRef: "pl-1", State: clustermeta.PlacementTransitionQueued, Reason: "repair", CurrentReplicaSetID: "rs-1", TargetReplicaSetID: "rs-1-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-2", State: clustermeta.PlacementTransitionRunning, Reason: "rebalance", CurrentReplicaSetID: "rs-2", TargetReplicaSetID: "rs-2-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-3", State: clustermeta.PlacementTransitionQueued, Reason: "drain", CurrentReplicaSetID: "rs-3", TargetReplicaSetID: "rs-3-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
	} {
		if err := srv.repo.PutPlacementTransition(ctx, tr); err != nil {
			t.Fatalf("PutPlacementTransition(%s): %v", tr.PlacementRef, err)
		}
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2-page-00000000000000000002",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix() - 120,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-batch): %v", err)
	}

	resp, err := srv.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
	})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if resp.GetRepairBacklog() != 1 || resp.GetRepairBacklogBytes() != 4 || resp.GetRepairBacklogChunks() != 1 {
		t.Fatalf("repair backlog=%d bytes=%d chunks=%d", resp.GetRepairBacklog(), resp.GetRepairBacklogBytes(), resp.GetRepairBacklogChunks())
	}
	if resp.GetRebalanceBacklog() != 1 || resp.GetRebalanceBacklogBytes() != 8 || resp.GetRebalanceBacklogChunks() != 2 {
		t.Fatalf("rebalance backlog=%d bytes=%d chunks=%d", resp.GetRebalanceBacklog(), resp.GetRebalanceBacklogBytes(), resp.GetRebalanceBacklogChunks())
	}
	if resp.GetDrainBacklog() != 1 || resp.GetDrainBacklogBytes() != 0 || resp.GetDrainBacklogChunks() != 0 {
		t.Fatalf("drain backlog=%d bytes=%d chunks=%d", resp.GetDrainBacklog(), resp.GetDrainBacklogBytes(), resp.GetDrainBacklogChunks())
	}
	if resp.GetTransitionFailedBatches() != 1 {
		t.Fatalf("transition failed batches=%d want=1", resp.GetTransitionFailedBatches())
	}
	if resp.GetTransitionOldestFailedBatchAgeSeconds() < 60 {
		t.Fatalf("transition oldest failed batch age=%d want>=60", resp.GetTransitionOldestFailedBatchAgeSeconds())
	}
	if resp.GetTransitionRecentBatches() != 0 {
		t.Fatalf("transition recent batches=%d want=0", resp.GetTransitionRecentBatches())
	}
	if resp.GetTransitionSmallBatches() != 1 {
		t.Fatalf("transition small batches=%d want=1", resp.GetTransitionSmallBatches())
	}
	if resp.GetTransitionRequeued() != 1 {
		t.Fatalf("transition requeued=%d want=1", resp.GetTransitionRequeued())
	}
	if resp.GetTransitionRetryPages() != 1 {
		t.Fatalf("transition retry pages=%d want=1", resp.GetTransitionRetryPages())
	}
	if resp.GetTransitionRetryWindows() != 1 {
		t.Fatalf("transition retry windows=%d want=1", resp.GetTransitionRetryWindows())
	}
	if resp.GetTransitionRetryWindowBytes() != 8 {
		t.Fatalf("transition retry window bytes=%d want=8", resp.GetTransitionRetryWindowBytes())
	}
	if resp.GetTransitionRetryWindowChunks() != 2 {
		t.Fatalf("transition retry window chunks=%d want=2", resp.GetTransitionRetryWindowChunks())
	}
	if resp.GetMaintenanceCooldownVolumes() != 1 {
		t.Fatalf("maintenance cooldown volumes=%d want=1", resp.GetMaintenanceCooldownVolumes())
	}
	if resp.GetMaintenanceCooldownMaxRemainingSeconds() != 8 {
		t.Fatalf("maintenance cooldown max remaining=%d want=8", resp.GetMaintenanceCooldownMaxRemainingSeconds())
	}
}

func TestGetVolumeIncludesAllocationAwareBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	baseNow := time.Unix(2000, 0)
	srv.now = func() time.Time { return baseNow }
	srv.maintenanceVolumeCooldown = 10 * time.Second
	srv.lastMaintenanceRunByVolume["00a1b2c3"] = baseNow.Add(-2 * time.Second).Unix()

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       24,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            nodeID,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	for _, extent := range []struct {
		id           uint64
		offset       uint64
		placementRef string
		replicaSetID string
	}{
		{1, 0, "pl-1", "rs-1"},
		{2, 8, "pl-2", "rs-2"},
		{3, 16, "pl-3", "rs-3"},
	} {
		if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
			VolumeID:      "00a1b2c3",
			ExtentID:      extent.id,
			LogicalOffset: extent.offset,
			LengthBytes:   8,
			ChunkID:       100 + extent.id,
			PlacementRef:  extent.placementRef,
			Revision:      12,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", extent.id, err)
		}
		if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
			ReplicaSetID:     extent.replicaSetID,
			VolumeID:         "00a1b2c3",
			PlacementRef:     extent.placementRef,
			Epoch:            5,
			PrimaryReplicaID: extent.replicaSetID + "-rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []clustermeta.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: extent.replicaSetID + "-rep-a", Role: clustermeta.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: extent.replicaSetID + "-rep-b", Role: clustermeta.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: extent.replicaSetID + "-rep-c", Role: clustermeta.ReplicaRoleSecondary},
			},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", extent.replicaSetID, err)
		}
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, PhysicalChunkStart: 500, ChunkCount: 1, Kind: clustermeta.AllocationKindData},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: clustermeta.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 2, Kind: clustermeta.AllocationKindZero},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	for _, tr := range []clustermeta.PlacementTransitionRecord{
		{VolumeID: "00a1b2c3", PlacementRef: "pl-1", State: clustermeta.PlacementTransitionQueued, Reason: "repair", CurrentReplicaSetID: "rs-1", TargetReplicaSetID: "rs-1-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-2", State: clustermeta.PlacementTransitionRunning, Reason: "rebalance", CurrentReplicaSetID: "rs-2", TargetReplicaSetID: "rs-2-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
		{VolumeID: "00a1b2c3", PlacementRef: "pl-3", State: clustermeta.PlacementTransitionQueued, Reason: "drain", CurrentReplicaSetID: "rs-3", TargetReplicaSetID: "rs-3-r", StartedAtUnix: 1000, LastProgressAtUnix: 1000, Attempt: 1},
	} {
		if err := srv.repo.PutPlacementTransition(ctx, tr); err != nil {
			t.Fatalf("PutPlacementTransition(%s): %v", tr.PlacementRef, err)
		}
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 30,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-2-page-00000000000000000002",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: time.Now().Unix() - 120,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-batch): %v", err)
	}

	resp, err := srv.GetVolume(ctx, &adminv1.GetVolumeRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetVolume: %v", err)
	}
	vol := resp.GetVolume()
	if vol.GetRepairBacklog() != 1 || vol.GetRepairBacklogBytes() != 4 || vol.GetRepairBacklogChunks() != 1 {
		t.Fatalf("repair backlog=%d bytes=%d chunks=%d", vol.GetRepairBacklog(), vol.GetRepairBacklogBytes(), vol.GetRepairBacklogChunks())
	}
	if vol.GetRebalanceBacklog() != 1 || vol.GetRebalanceBacklogBytes() != 8 || vol.GetRebalanceBacklogChunks() != 2 {
		t.Fatalf("rebalance backlog=%d bytes=%d chunks=%d", vol.GetRebalanceBacklog(), vol.GetRebalanceBacklogBytes(), vol.GetRebalanceBacklogChunks())
	}
	if vol.GetDrainBacklog() != 1 || vol.GetDrainBacklogBytes() != 0 || vol.GetDrainBacklogChunks() != 0 {
		t.Fatalf("drain backlog=%d bytes=%d chunks=%d", vol.GetDrainBacklog(), vol.GetDrainBacklogBytes(), vol.GetDrainBacklogChunks())
	}
	if vol.GetTransitionFailedBatches() != 1 {
		t.Fatalf("transition failed batches=%d want=1", vol.GetTransitionFailedBatches())
	}
	if vol.GetTransitionOldestFailedBatchAgeSeconds() < 60 {
		t.Fatalf("transition oldest failed batch age=%d want>=60", vol.GetTransitionOldestFailedBatchAgeSeconds())
	}
	if vol.GetTransitionRecentBatches() != 0 {
		t.Fatalf("transition recent batches=%d want=0", vol.GetTransitionRecentBatches())
	}
	if vol.GetTransitionSmallBatches() != 1 {
		t.Fatalf("transition small batches=%d want=1", vol.GetTransitionSmallBatches())
	}
	if vol.GetTransitionRequeued() != 1 {
		t.Fatalf("transition requeued=%d want=1", vol.GetTransitionRequeued())
	}
	if vol.GetTransitionRetryPages() != 1 {
		t.Fatalf("transition retry pages=%d want=1", vol.GetTransitionRetryPages())
	}
	if vol.GetTransitionRetryWindows() != 1 {
		t.Fatalf("transition retry windows=%d want=1", vol.GetTransitionRetryWindows())
	}
	if vol.GetTransitionRetryWindowBytes() != 8 {
		t.Fatalf("transition retry window bytes=%d want=8", vol.GetTransitionRetryWindowBytes())
	}
	if vol.GetTransitionRetryWindowChunks() != 2 {
		t.Fatalf("transition retry window chunks=%d want=2", vol.GetTransitionRetryWindowChunks())
	}
	if !vol.GetMaintenanceCooldownActive() {
		t.Fatalf("maintenance cooldown active=false want=true")
	}
	if vol.GetMaintenanceCooldownRemainingSeconds() != 8 {
		t.Fatalf("maintenance cooldown remaining=%d want=8", vol.GetMaintenanceCooldownRemainingSeconds())
	}

	if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       32,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("PutVolumeSpec expanded: %v", err)
	}
	specOnlyCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(adminVolumeSummaryModeMetadataKey, adminVolumeSummaryModeSpecOnly))
	specOnlyResp, err := srv.GetVolume(specOnlyCtx, &adminv1.GetVolumeRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetVolume spec-only: %v", err)
	}
	specOnlyVol := specOnlyResp.GetVolume()
	if specOnlyVol.GetVolumeId() != "00a1b2c3" || specOnlyVol.GetSizeBytes() != 32 || specOnlyVol.GetBlockSize() != 4096 {
		t.Fatalf("unexpected spec-only volume geometry: %+v", specOnlyVol)
	}
	if specOnlyVol.GetRepairBacklog() != 0 || specOnlyVol.GetRebalanceBacklog() != 0 || specOnlyVol.GetDrainBacklog() != 0 {
		t.Fatalf("spec-only response should not compute backlog: %+v", specOnlyVol)
	}
}

func TestSummarizeVolumeTransitionsUsesIncompleteTransitionBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.ready.Store(true)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       16,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:         nodeID,
			LifecycleState: clustermeta.NodeLifecycleActive,
			HealthState:    clustermeta.NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	if err := srv.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   16,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := srv.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            5,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	for _, page := range []clustermeta.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 0, PhysicalChunkStart: 500, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []clustermeta.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: clustermeta.AllocationKindData},
			},
		},
	} {
		if err := srv.repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	if err := srv.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		State:               clustermeta.PlacementTransitionRunning,
		Reason:              "rebalance",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-2",
		StartedAtUnix:       1000,
		LastProgressAtUnix:  1000,
		Attempt:             1,
	}); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationRunning,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{0},
		LastUpdatedAtUnix: 1001,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition): %v", err)
	}

	backlog := srv.summarizeVolumeTransitions(ctx, "00a1b2c3")
	if backlog.RebalanceCount != 1 || backlog.RebalanceBytes != 8 || backlog.RebalanceChunks != 2 {
		t.Fatalf("rebalance backlog=%+v want count=1 bytes=8 chunks=2", backlog)
	}
}

func TestRetiredPayloadBacklogExposedInClusterAndVolumeStatus(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.ready.Store(true)

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusDegraded,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       24,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:            nodeID,
			LifecycleState:    clustermeta.NodeLifecycleActive,
			HealthState:       clustermeta.NodeHealthHealthy,
			LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	for _, op := range []clustermeta.MutationOperationRecord{
		{
			OperationID:             "write-hint-1",
			VolumeID:                "00a1b2c3",
			Kind:                    "write",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{500, 501},
		},
		{
			OperationID:             "transition-hint-1",
			VolumeID:                "00a1b2c3",
			Kind:                    "transition",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{700},
		},
		{
			OperationID:             "payload-gc-00a1b2c3",
			VolumeID:                "00a1b2c3",
			Kind:                    "payload_gc",
			State:                   clustermeta.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{500},
		},
		{
			OperationID:             clustermeta.PayloadGCBatchMutationOperationID("00a1b2c3", 1),
			VolumeID:                "00a1b2c3",
			Kind:                    "payload_gc_batch",
			State:                   clustermeta.MutationOperationFailed,
			IdempotencyKey:          "payload-gc-00a1b2c3",
			RetiredPhysicalChunkIDs: []uint64{700},
			LastUpdatedAtUnix:       time.Now().Unix() - 120,
		},
	} {
		if err := srv.repo.PutMutationOperation(ctx, op); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", op.OperationID, err)
		}
	}

	clusterResp, err := srv.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{
		Cluster: &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
	})
	if err != nil {
		t.Fatalf("GetClusterStatus: %v", err)
	}
	if clusterResp.GetRetiredPayloadBacklogChunks() != 2 || clusterResp.GetRetiredPayloadBacklogBytes() != 8 {
		t.Fatalf("cluster retired backlog chunks=%d bytes=%d", clusterResp.GetRetiredPayloadBacklogChunks(), clusterResp.GetRetiredPayloadBacklogBytes())
	}
	if clusterResp.GetRetiredPayloadFailedBatches() != 1 {
		t.Fatalf("cluster retired failed batches=%d want=1", clusterResp.GetRetiredPayloadFailedBatches())
	}
	if clusterResp.GetRetiredPayloadOldestFailedBatchAgeSeconds() < 100 {
		t.Fatalf("cluster retired failed batch age=%d want>=100", clusterResp.GetRetiredPayloadOldestFailedBatchAgeSeconds())
	}

	volumeResp, err := srv.GetVolume(ctx, &adminv1.GetVolumeRequest{
		Cluster:  &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"},
		VolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("GetVolume: %v", err)
	}
	if volumeResp.GetVolume().GetRetiredPayloadBacklogChunks() != 2 || volumeResp.GetVolume().GetRetiredPayloadBacklogBytes() != 8 {
		t.Fatalf("volume retired backlog chunks=%d bytes=%d", volumeResp.GetVolume().GetRetiredPayloadBacklogChunks(), volumeResp.GetVolume().GetRetiredPayloadBacklogBytes())
	}
	if volumeResp.GetVolume().GetRetiredPayloadFailedBatches() != 1 {
		t.Fatalf("volume retired failed batches=%d want=1", volumeResp.GetVolume().GetRetiredPayloadFailedBatches())
	}
	if volumeResp.GetVolume().GetRetiredPayloadOldestFailedBatchAgeSeconds() < 100 {
		t.Fatalf("volume retired failed batch age=%d want>=100", volumeResp.GetVolume().GetRetiredPayloadOldestFailedBatchAgeSeconds())
	}
}

func TestObservabilityEndpointsExposeRetiredPayloadBacklog(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.ready.Store(true)
	srv.maint.maxConcurrentPayloadGCs = 3
	srv.maint.pausePayloadGCs = true
	baseNow := time.Unix(2000, 0)
	srv.now = func() time.Time { return baseNow }
	srv.maintenanceVolumeCooldown = 10 * time.Second
	srv.lastMaintenanceRunByVolume["00a1b2c3"] = baseNow.Add(-2 * time.Second).Unix()

	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4096,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             "write-hint-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   clustermeta.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{500},
	}); err != nil {
		t.Fatalf("PutMutationOperation(write): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             clustermeta.PayloadGCBatchMutationOperationID("00a1b2c3", 0),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc_batch",
		State:                   clustermeta.MutationOperationFailed,
		IdempotencyKey:          clustermeta.PayloadGCMutationOperationID("00a1b2c3"),
		RetiredPhysicalChunkIDs: []uint64{500},
		LastUpdatedAtUnix:       time.Now().Unix() - 90,
	}); err != nil {
		t.Fatalf("PutMutationOperation(batch): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             clustermeta.MutationOperationPending,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{0},
		RetryPageWindows: []clustermeta.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: time.Now().Unix() - 20,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-parent): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "transition-pl-1-page-00000000000000000000",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             clustermeta.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: time.Now().Unix() - 120,
	}); err != nil {
		t.Fatalf("PutMutationOperation(transition-batch): %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:       "write-recent-page-0",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             clustermeta.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: time.Now().Unix() - 10,
	}); err != nil {
		t.Fatalf("PutMutationOperation(write-recent): %v", err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthSuspect,
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                   "node-a",
		LastProbeUnix:            time.Now().Unix() - 5,
		LastProbeError:           "healthz probe failed",
		ConsecutiveProbeFailures: 2,
		HealthReason:             "healthz probe failed",
		HealthUpdatedBy:          clustermeta.HealthUpdatedByReconciler,
		RecoveryEligibleAtUnix:   baseNow.Unix() + 7,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/debug/summary", nil)
	summaryRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(summaryRec, summaryReq)
	if summaryRec.Code != http.StatusOK {
		t.Fatalf("summary status=%d body=%s", summaryRec.Code, summaryRec.Body.String())
	}
	var summary map[string]any
	if err := json.NewDecoder(summaryRec.Body).Decode(&summary); err != nil {
		t.Fatalf("Decode summary: %v", err)
	}
	if got := summary["retired_payload_backlog_chunks"]; got != float64(1) {
		t.Fatalf("retired_payload_backlog_chunks=%v want=1", got)
	}
	if got := summary["retired_payload_backlog_bytes"]; got != float64(4) {
		t.Fatalf("retired_payload_backlog_bytes=%v want=4", got)
	}
	if got := summary["retired_payload_failed_batches"]; got != float64(1) {
		t.Fatalf("retired_payload_failed_batches=%v want=1", got)
	}
	if got := summary["retired_payload_oldest_failed_batch_age_seconds"]; got.(float64) < 60 {
		t.Fatalf("retired_payload_oldest_failed_batch_age_seconds=%v want>=60", got)
	}
	if got := summary["transition_failed_batches"]; got != float64(1) {
		t.Fatalf("transition_failed_batches=%v want=1", got)
	}
	if got := summary["transition_recent_batches"]; got != float64(1) {
		t.Fatalf("transition_recent_batches=%v want=1", got)
	}
	if got := summary["transition_small_batches"]; got != float64(1) {
		t.Fatalf("transition_small_batches=%v want=1", got)
	}
	if got := summary["transition_requeued"]; got != float64(1) {
		t.Fatalf("transition_requeued=%v want=1", got)
	}
	if got := summary["transition_retry_pages"]; got != float64(1) {
		t.Fatalf("transition_retry_pages=%v want=1", got)
	}
	if got := summary["transition_retry_windows"]; got != float64(1) {
		t.Fatalf("transition_retry_windows=%v want=1", got)
	}
	if got := summary["transition_retry_window_bytes"]; got != float64(8) {
		t.Fatalf("transition_retry_window_bytes=%v want=8", got)
	}
	if got := summary["transition_retry_window_chunks"]; got != float64(2) {
		t.Fatalf("transition_retry_window_chunks=%v want=2", got)
	}
	if got := summary["maintenance_cooldown_volumes"]; got != float64(1) {
		t.Fatalf("maintenance_cooldown_volumes=%v want=1", got)
	}
	if got := summary["maintenance_cooldown_max_remaining_seconds"]; got != float64(8) {
		t.Fatalf("maintenance_cooldown_max_remaining_seconds=%v want=8", got)
	}
	if got := summary["nodes_with_probe_failures"]; got != float64(1) {
		t.Fatalf("nodes_with_probe_failures=%v want=1", got)
	}
	if got := summary["max_consecutive_probe_failures"]; got != float64(2) {
		t.Fatalf("max_consecutive_probe_failures=%v want=2", got)
	}
	if got := summary["nodes_in_recovery_cooldown"]; got != float64(1) {
		t.Fatalf("nodes_in_recovery_cooldown=%v want=1", got)
	}
	if got := summary["max_recovery_cooldown_remaining_seconds"]; got != float64(7) {
		t.Fatalf("max_recovery_cooldown_remaining_seconds=%v want=7", got)
	}
	if got := summary["transition_oldest_failed_batch_age_seconds"]; got.(float64) < 60 {
		t.Fatalf("transition_oldest_failed_batch_age_seconds=%v want>=60", got)
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(metricsRec, metricsReq)
	body := metricsRec.Body.String()
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metricsRec.Code, body)
	}
	for _, want := range []string{
		"sbs_service_retired_payload_backlog_chunks 1",
		"sbs_service_retired_payload_backlog_bytes 4",
		"sbs_service_retired_payload_failed_batches 1",
		"sbs_service_retired_payload_oldest_failed_batch_age_seconds ",
		"sbs_service_transition_failed_batches 1",
		"sbs_service_transition_recent_batches 1",
		"sbs_service_transition_small_batches 1",
		"sbs_service_transition_requeued 1",
		"sbs_service_transition_retry_pages 1",
		"sbs_service_transition_retry_windows 1",
		"sbs_service_transition_retry_window_bytes 8",
		"sbs_service_transition_retry_window_chunks 2",
		"sbs_service_maintenance_cooldown_volumes 1",
		"sbs_service_maintenance_cooldown_max_remaining_seconds 8",
		"sbs_service_nodes_with_probe_failures 1",
		"sbs_service_max_consecutive_probe_failures 2",
		"sbs_service_nodes_in_recovery_cooldown 1",
		"sbs_service_max_recovery_cooldown_remaining_seconds 7",
		"sbs_service_transition_oldest_failed_batch_age_seconds ",
		"sbs_service_throttle_config{kind=\"payload_gc\"} 3",
		"sbs_service_pause_state{kind=\"payload_gc\"} 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in\n%s", want, body)
		}
	}
}
