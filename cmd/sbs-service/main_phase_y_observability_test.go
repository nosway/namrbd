package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/observability"
)

func TestPhaseYOperationsAPIExposesSharedSBSObservability(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.now = func() time.Time { return time.Unix(200, 0) }
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		Zone:              "zone-a",
		CapacityBytes:     1000,
		UsedBytes:         300,
		LastHeartbeatUnix: 123,
		Capabilities:      []string{"sbs-grpc", "admin-http"},
		AdminHTTPEndpoint: "http://127.0.0.1:9081",
		SBSEndpoints:      []clustermeta.SBSEndpoint{{Address: "127.0.0.1", Port: 9460}},
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID:                         "node-a",
		StoreCount:                     2,
		HealthyStoreCount:              2,
		WritableStoreCount:             2,
		AllocatableStoreCount:          2,
		StoreCapacityBytes:             2000,
		StoreAvailableBytes:            1400,
		StoreUsedBytes:                 600,
		StoreAllocationWeightTotal:     200,
		StoreAllocationWeightObserved:  true,
		StoreCompactionPendingBytes:    12,
		StoreCompactionInProgressBytes: 4,
	}); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             1,
		Revision:          2,
		Status:            clustermeta.VolumeStatusHealthy,
		RedundancyBackend: clustermeta.RedundancyBackendReplicated,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:          "00a1b2c3",
		SizeBytes:         8192,
		BlockSize:         4096,
		ChunkSizeBytes:    4096,
		ExtentSizeBytes:   65536,
		ReplicationFactor: 3,
		RedundancyBackend: clustermeta.RedundancyBackendReplicated,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:             "mut-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   clustermeta.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{10, 11},
		StartedAtUnix:           100,
		LastUpdatedAtUnix:       100,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	handler := observabilityMux(srv)
	clusterReq := httptest.NewRequest(http.MethodGet, "/api/v1/sbs/cluster", nil)
	clusterRec := httptest.NewRecorder()
	handler.ServeHTTP(clusterRec, clusterReq)
	if clusterRec.Code != http.StatusOK {
		t.Fatalf("cluster status=%d body=%s", clusterRec.Code, clusterRec.Body.String())
	}
	var snapshot observability.Snapshot
	if err := json.Unmarshal(clusterRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode cluster snapshot: %v", err)
	}
	if snapshot.SchemaVersion != observability.SchemaVersion {
		t.Fatalf("schema_version=%q want %q", snapshot.SchemaVersion, observability.SchemaVersion)
	}
	if snapshot.CollectionStatus != observability.StatusOK {
		t.Fatalf("collection_status=%q warnings=%v first=%q last=%q", snapshot.CollectionStatus, snapshot.Warnings, snapshot.FirstError, snapshot.LastError)
	}
	if snapshot.Capacity.TotalBytes != 2000 || snapshot.Capacity.PhysicalFreeBytes != 1400 || snapshot.Capacity.LogicalBytes != 8192 {
		t.Fatalf("unexpected capacity: %+v", snapshot.Capacity)
	}
	if snapshot.Reclaim.PendingChunks != 2 || snapshot.Reclaim.PendingBytes != 8192 || snapshot.Reclaim.CompletedClaimed {
		t.Fatalf("unexpected reclaim: %+v", snapshot.Reclaim)
	}
	if snapshot.Membership.ActiveNodes != 1 || snapshot.Membership.HealthyNodes != 1 {
		t.Fatalf("membership counts do not match node view: %+v", snapshot.Membership)
	}
	if !snapshot.Membership.NAMRBDGatewayMembershipReady || !snapshot.Membership.ISCSIGatewayMembershipReady || !snapshot.Membership.GatewaySBSViewFresh {
		t.Fatalf("membership readiness not represented: %+v", snapshot.Membership)
	}
	if !snapshot.ReadOnlyModeEnforced || !snapshot.RBACChecked || !snapshot.RedactionApplied || !snapshot.UnsupportedClaimVisible {
		t.Fatalf("operator safety envelope missing: %+v", snapshot)
	}

	mcp := getPhaseYView[observability.MCPSurface](t, handler, "/api/v1/mcp/tools", "mcp.tools")
	if !mcp.ToolRegistered || !mcp.ReadOnly || mcp.MutatingToolsEnabled {
		t.Fatalf("mcp descriptor is not observe-first: %+v", mcp)
	}
	if !mcp.ServerReady || !mcp.ProviderReady || mcp.Transport != "stdio-jsonrpc-content-length" {
		t.Fatalf("mcp descriptor must reflect the read-only stdio transport/provider: %+v", mcp)
	}

	gui := getPhaseYView[struct {
		GUI observability.GUISurface `json:"gui"`
	}](t, handler, "/api/v1/gui/summary", "gui.summary")
	if !gui.GUI.ViewContractReady || !gui.GUI.ReadOnlyModeEnforced || !gui.GUI.MutationControlsHidden {
		t.Fatalf("gui descriptor is not read-only: %+v", gui.GUI)
	}

	workflow := getPhaseYView[observability.WorkflowState](t, handler, "/api/v1/workflow/hardening", "workflow.hardening")
	if !workflow.Hardened || !workflow.EvidenceBundleReady || !workflow.DangerousActionsBlocked {
		t.Fatalf("workflow hardening not represented: %+v", workflow)
	}
}

func TestPhaseYOperationsAPIMembershipCountsUseNodeViewWhenBoundedSummaryDisabled(t *testing.T) {
	t.Setenv("NAMRBD_OBSERVABILITY_SNAPSHOT_TIMEOUT", "0s")
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	for _, rec := range []clustermeta.NodeMembershipRecord{
		{NodeID: "node-a", LifecycleState: clustermeta.NodeLifecycleActive, HealthState: clustermeta.NodeHealthHealthy},
		{NodeID: "node-b", LifecycleState: clustermeta.NodeLifecycleDraining, HealthState: clustermeta.NodeHealthSuspect},
		{NodeID: "node-c", LifecycleState: clustermeta.NodeLifecycleRemoved, HealthState: clustermeta.NodeHealthDown},
	} {
		if err := srv.repo.PutNodeMembership(ctx, rec); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", rec.NodeID, err)
		}
	}

	snapshot := srv.phaseYOperationsSnapshot(ctx)
	if len(snapshot.Nodes) != 3 {
		t.Fatalf("nodes=%d want 3", len(snapshot.Nodes))
	}
	if snapshot.Membership.ActiveNodes != 1 ||
		snapshot.Membership.DrainingNodes != 1 ||
		snapshot.Membership.RemovedNodes != 1 ||
		snapshot.Membership.HealthyNodes != 1 ||
		snapshot.Membership.SuspectNodes != 1 ||
		snapshot.Membership.DownNodes != 1 {
		t.Fatalf("membership counts do not match node records: %+v", snapshot.Membership)
	}
}

func TestPhaseYOperationsAPIRejectsMutationMethods(t *testing.T) {
	srv := newTestMaintenanceServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sbs/cluster", nil)
	observabilityMux(srv).ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestObservabilityMuxServesReadOnlyOperationsConsole(t *testing.T) {
	srv := newTestMaintenanceServer(t)
	handler := observabilityMux(srv)

	consoleRec := httptest.NewRecorder()
	consoleReq := httptest.NewRequest(http.MethodGet, "/console/", nil)
	handler.ServeHTTP(consoleRec, consoleReq)
	if consoleRec.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", consoleRec.Code, consoleRec.Body.String())
	}
	if body := consoleRec.Body.String(); !strings.Contains(body, "NAMRBD Operations") {
		t.Fatalf("console body did not contain dashboard title: %s", body)
	}
	if got := consoleRec.Header().Get("X-NAMRBD-Dashboard"); got != "read-only" {
		t.Fatalf("dashboard header=%q", got)
	}

	clusterRec := httptest.NewRecorder()
	clusterReq := httptest.NewRequest(http.MethodGet, "/api/v1/sbs/cluster", nil)
	handler.ServeHTTP(clusterRec, clusterReq)
	if clusterRec.Code != http.StatusOK {
		t.Fatalf("cluster status=%d body=%s", clusterRec.Code, clusterRec.Body.String())
	}
	if got := clusterRec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("cluster Content-Type=%q", got)
	}
	var snapshot observability.Snapshot
	if err := json.Unmarshal(clusterRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode cluster JSON after console registration: %v", err)
	}

	postRec := httptest.NewRecorder()
	postReq := httptest.NewRequest(http.MethodPost, "/console/", nil)
	handler.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("console mutation status=%d body=%s", postRec.Code, postRec.Body.String())
	}
}

func getPhaseYView[T any](t *testing.T, handler http.Handler, path string, wantViewID string) T {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
	}
	var view struct {
		SchemaVersion           string          `json:"schema_version"`
		ViewID                  string          `json:"view_id"`
		ReadOnlyModeEnforced    bool            `json:"read_only_mode_enforced"`
		UnsupportedClaimVisible bool            `json:"unsupported_claim_visible"`
		Data                    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode %s view: %v", path, err)
	}
	if view.SchemaVersion != observability.SchemaVersion || view.ViewID != wantViewID {
		t.Fatalf("%s envelope schema=%q view=%q", path, view.SchemaVersion, view.ViewID)
	}
	if !view.ReadOnlyModeEnforced || !view.UnsupportedClaimVisible {
		t.Fatalf("%s safety envelope missing: %+v", path, view)
	}
	var out T
	if err := json.Unmarshal(view.Data, &out); err != nil {
		t.Fatalf("decode %s data: %v", path, err)
	}
	return out
}
