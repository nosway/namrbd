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
	if snapshot.CollectionStatus != observability.StatusDegraded {
		t.Fatalf("collection_status=%q warnings=%v first=%q last=%q", snapshot.CollectionStatus, snapshot.Warnings, snapshot.FirstError, snapshot.LastError)
	}
	if snapshot.Projection.Health != string(clustermeta.SummaryHealthRebuildRequired) || snapshot.Projection.Reason != "aggregate_missing" || !snapshot.Projection.Partial || !snapshot.Projection.RebuildRequired {
		t.Fatalf("missing aggregate must remain visible: %+v", snapshot.Projection)
	}
	if !snapshot.Detail.DefaultPoll || snapshot.Detail.NodeDetailIncluded || snapshot.Detail.StoreDetailIncluded || snapshot.Detail.VolumeDetailIncluded || len(snapshot.Nodes) != 0 || len(snapshot.Stores) != 0 || len(snapshot.Volumes) != 0 {
		t.Fatalf("default poll leaked detail: detail=%+v nodes=%d stores=%d volumes=%d", snapshot.Detail, len(snapshot.Nodes), len(snapshot.Stores), len(snapshot.Volumes))
	}
	if len(snapshot.FleetHealth) != 1 || snapshot.FleetHealth[0].Code != "SBS_FLEET_CHECK_STALE" {
		t.Fatalf("missing projection health code=%+v", snapshot.FleetHealth)
	}
	if snapshot.Membership.SBSMembershipSyncCompleted || snapshot.Membership.GatewaySBSViewFresh {
		t.Fatalf("missing projection appeared fresh: %+v", snapshot.Membership)
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

func TestPhaseADFleetSummaryUsesBoundedAggregateWithoutDetailFanout(t *testing.T) {
	srv, guard, cleanup := newEnforcedSummaryServer(t)
	defer cleanup()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/sbs/cluster", nil)
	observabilityMux(srv).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot observability.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.CollectionStatus != observability.StatusOK || snapshot.Projection.Health != "ready" || snapshot.Projection.Partial || snapshot.Projection.Stale {
		t.Fatalf("projection=%+v collection_status=%q", snapshot.Projection, snapshot.CollectionStatus)
	}
	if snapshot.Fleet.KnownNodes != 2 || snapshot.Fleet.ActiveNodes != 1 || snapshot.Fleet.DrainingNodes != 1 || snapshot.Fleet.SuspectNodes != 1 || snapshot.Fleet.VolumeCount != 1 || snapshot.Fleet.DegradedVolumes != 1 {
		t.Fatalf("fleet=%+v", snapshot.Fleet)
	}
	if snapshot.Membership.ActiveNodes != 1 || snapshot.Membership.DrainingNodes != 1 || snapshot.Membership.SuspectNodes != 1 || !snapshot.Membership.SBSMembershipSyncCompleted || !snapshot.Membership.GatewaySBSViewFresh {
		t.Fatalf("membership=%+v", snapshot.Membership)
	}
	if snapshot.Capacity.TotalBytes != 2000 || snapshot.Capacity.PhysicalFreeBytes != 1400 || snapshot.Capacity.ReservedBytes != 100 || snapshot.Capacity.Freshness != "fresh" || snapshot.FleetControl.ManifestRevision != "manifest-enforced" || snapshot.FleetControl.SourceRevision != 300 {
		t.Fatalf("capacity=%+v fleet_control=%+v", snapshot.Capacity, snapshot.FleetControl)
	}
	if snapshot.RequestClass.PointGetCount != 3 || snapshot.RequestClass.BatchGetCount != 2 || snapshot.RequestClass.BatchGetKeyCount != 2*clustermeta.SummaryVirtualShardCount || snapshot.RequestClass.RangePageCount != 0 || snapshot.RequestClass.BackendFullScanCount != 0 || snapshot.RequestClass.FullCompletionCount != 0 || snapshot.RequestClass.NestedCompletionCount != 0 {
		t.Fatalf("request_class=%+v", snapshot.RequestClass)
	}
	if !snapshot.Detail.DefaultPoll || snapshot.Detail.NodeDetailIncluded || snapshot.Detail.StoreDetailIncluded || snapshot.Detail.VolumeDetailIncluded || len(snapshot.Nodes) != 0 || len(snapshot.Stores) != 0 || len(snapshot.Volumes) != 0 {
		t.Fatalf("detail scope=%+v nodes=%d stores=%d volumes=%d", snapshot.Detail, len(snapshot.Nodes), len(snapshot.Stores), len(snapshot.Volumes))
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("forbidden calls list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}

func TestPhaseADFleetSummaryEmitsStableHealthCodes(t *testing.T) {
	srv, guard, cleanup := newEnforcedSummaryServer(t)
	defer cleanup()
	digest := "sha256:" + strings.Repeat("b", 64)
	now := time.Now().UTC()
	observation, err := clustermeta.NewFleetObservation(clustermeta.FleetObservation{
		SourceRevision: 301, ManifestRevision: "manifest-301", ManifestDigest: digest,
		BinaryDigest: digest, ConfigDigest: digest, StoreDigest: digest,
		ApplyOperationID: "apply-301", ApplyState: "paused", ApplyPaused: true,
		HostCheckFailedCount: 1, ConfigDriftCount: 2, StrayNodeCount: 3, StorageClaimMismatchCount: 4,
		StoreCount: 160, UsableBytes: 1000, FreeBytes: 700, ReservedBytes: 100,
		Zones:                  []clustermeta.FleetZoneObservation{{Zone: "zone-a", ActiveNodes: 20}, {Zone: "zone-b", DrainingNodes: 1, SuspectNodes: 1}},
		CapacityObservedAtUnix: now.Unix(), ObservedAtUnix: now.Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	writeRepo := clustermeta.NewRepository(guard.base, defaultMetadataRoot)
	if err := writeRepo.PutFleetObservation(context.Background(), observation); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/sbs/cluster", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot observability.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.CollectionStatus != observability.StatusDegraded || len(snapshot.FleetHealth) != 5 {
		t.Fatalf("status=%q fleet_health=%+v", snapshot.CollectionStatus, snapshot.FleetHealth)
	}
	want := map[string]uint64{
		"SBS_APPLY_PAUSED": 1, "SBS_HOST_CHECK_FAILED": 1, "SBS_CONFIG_DRIFT": 2,
		"SBS_STRAY_NODE": 3, "SBS_STORAGE_CLAIM_MISMATCH": 4,
	}
	for _, health := range snapshot.FleetHealth {
		if want[health.Code] != health.Count || health.SourceRevision != 301 || health.Freshness != "fresh" {
			t.Fatalf("health=%+v want_count=%d", health, want[health.Code])
		}
		delete(want, health.Code)
	}
	if len(want) != 0 {
		t.Fatalf("missing health codes=%v", want)
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("GET made forbidden calls list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}

func TestPhaseADNodeAndVolumeViewsRequireBoundedPageOrPointReads(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID: nodeID, LifecycleState: clustermeta.NodeLifecycleActive,
			HealthState: clustermeta.NodeHealthHealthy, Zone: "zone-a",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := srv.repo.PutNodeHealthDetail(ctx, clustermeta.NodeHealthDetailRecord{
		NodeID: "node-a", StoreCount: 2, HealthyStoreCount: 2, WritableStoreCount: 2,
		AllocatableStoreCount: 2, StoreCapacityBytes: 2000, StoreAvailableBytes: 1400, StoreUsedBytes: 600,
	}); err != nil {
		t.Fatal(err)
	}
	for index, volumeID := range []string{"00a1b2c3", "00a1b2c4"} {
		if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
			VolumeID: volumeID, Epoch: 1, Revision: uint64(index + 1), Status: clustermeta.VolumeStatusHealthy,
			RedundancyBackend: clustermeta.RedundancyBackendReplicated,
		}); err != nil {
			t.Fatal(err)
		}
		if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
			VolumeID: volumeID, SizeBytes: uint64(8192 * (index + 1)), BlockSize: 4096,
			ChunkSizeBytes: 4096, ExtentSizeBytes: 65536, ReplicationFactor: 3,
			RedundancyBackend: clustermeta.RedundancyBackendReplicated,
		}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := srv.repo.RunVolumeCatalogRebuildPage(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Ready {
		result, err = srv.repo.RunVolumeCatalogRebuildPage(ctx, 2)
	}
	if err != nil || !result.Ready {
		t.Fatalf("volume catalog rebuild result=%+v err=%v", result, err)
	}

	handler := observabilityMux(srv)
	firstNodes := getPhaseADPageView[struct {
		Nodes                   []observability.Node       `json:"nodes"`
		NextPageToken           string                     `json:"next_page_token"`
		AutomaticPageCompletion bool                       `json:"automatic_page_completion"`
		RequestClass            observability.RequestClass `json:"request_class"`
	}](t, handler, "/api/v1/sbs/nodes?page_size=2", "sbs.nodes")
	if len(firstNodes.Nodes) != 2 || firstNodes.NextPageToken == "" || firstNodes.AutomaticPageCompletion || firstNodes.RequestClass.RangePageCount != 1 || firstNodes.RequestClass.FullCompletionCount != 0 {
		t.Fatalf("first node page=%+v", firstNodes)
	}
	secondNodes := getPhaseADPageView[struct {
		Nodes         []observability.Node `json:"nodes"`
		NextPageToken string               `json:"next_page_token"`
	}](t, handler, "/api/v1/sbs/nodes?page_size=2&page_token="+firstNodes.NextPageToken, "sbs.nodes")
	if len(secondNodes.Nodes) != 1 || secondNodes.NextPageToken != "" {
		t.Fatalf("second node page=%+v", secondNodes)
	}
	nodePoint := getPhaseADPageView[struct {
		Node         observability.Node         `json:"node"`
		Stores       []observability.Store      `json:"stores"`
		RequestClass observability.RequestClass `json:"request_class"`
	}](t, handler, "/api/v1/sbs/node?id=node-a", "sbs.node")
	if nodePoint.Node.NodeID != "node-a" || len(nodePoint.Stores) != 1 || nodePoint.RequestClass.PointGetCount != 2 || nodePoint.RequestClass.RangePageCount != 0 {
		t.Fatalf("node point=%+v", nodePoint)
	}

	firstVolumes := getPhaseADPageView[struct {
		Volumes                 []observability.Volume     `json:"volumes"`
		NextPageToken           string                     `json:"next_page_token"`
		AutomaticPageCompletion bool                       `json:"automatic_page_completion"`
		RequestClass            observability.RequestClass `json:"request_class"`
	}](t, handler, "/api/v1/sbs/volumes?page_size=1", "sbs.volumes")
	if len(firstVolumes.Volumes) != 1 || firstVolumes.NextPageToken == "" || firstVolumes.AutomaticPageCompletion || firstVolumes.RequestClass.RangePageCount != 1 || firstVolumes.RequestClass.NestedCompletionCount != 0 {
		t.Fatalf("first volume page=%+v", firstVolumes)
	}
	secondVolumes := getPhaseADPageView[struct {
		Volumes       []observability.Volume `json:"volumes"`
		NextPageToken string                 `json:"next_page_token"`
	}](t, handler, "/api/v1/sbs/volumes?page_size=1&page_token="+firstVolumes.NextPageToken, "sbs.volumes")
	if len(secondVolumes.Volumes) != 1 {
		t.Fatalf("second volume page=%+v", secondVolumes)
	}
	if secondVolumes.NextPageToken != "" {
		terminalVolumes := getPhaseADPageView[struct {
			Volumes       []observability.Volume `json:"volumes"`
			NextPageToken string                 `json:"next_page_token"`
		}](t, handler, "/api/v1/sbs/volumes?page_size=1&page_token="+secondVolumes.NextPageToken, "sbs.volumes")
		if len(terminalVolumes.Volumes) != 0 || terminalVolumes.NextPageToken != "" {
			t.Fatalf("terminal volume page=%+v", terminalVolumes)
		}
	}
	volumePoint := getPhaseADPageView[struct {
		Volume       observability.Volume       `json:"volume"`
		RequestClass observability.RequestClass `json:"request_class"`
	}](t, handler, "/api/v1/sbs/volume?id=00a1b2c3", "sbs.volume")
	if volumePoint.Volume.VolumeID != "00a1b2c3" || volumePoint.RequestClass.PointGetCount != 2 || volumePoint.RequestClass.RangePageCount != 0 || volumePoint.RequestClass.NestedCompletionCount != 0 {
		t.Fatalf("volume point=%+v", volumePoint)
	}
}

func getPhaseADPageView[T any](t *testing.T, handler http.Handler, path, wantViewID string) T {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
	}
	var envelope struct {
		SchemaVersion        string          `json:"schema_version"`
		ViewID               string          `json:"view_id"`
		ReadOnlyModeEnforced bool            `json:"read_only_mode_enforced"`
		Data                 json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaVersion != observability.SchemaVersion || envelope.ViewID != wantViewID || !envelope.ReadOnlyModeEnforced {
		t.Fatalf("envelope=%+v", envelope)
	}
	var out T
	if err := json.Unmarshal(envelope.Data, &out); err != nil {
		t.Fatal(err)
	}
	return out
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
