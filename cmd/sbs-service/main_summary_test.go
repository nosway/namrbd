package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type summaryNoCompletionKV struct {
	base          clustermeta.KV
	pointGets     int
	batchGets     int
	batchGetKeys  int
	listCalls     int
	mutationCalls int
}

func (kv *summaryNoCompletionKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	kv.pointGets++
	return kv.base.Get(ctx, key)
}

func (kv *summaryNoCompletionKV) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	kv.batchGets++
	kv.batchGetKeys += len(keys)
	batcher, ok := kv.base.(interface {
		BatchGet(context.Context, []string) (map[string][]byte, error)
	})
	if !ok {
		return nil, fmt.Errorf("fixture base has no BatchGet")
	}
	return batcher.BatchGet(ctx, keys)
}

func (kv *summaryNoCompletionKV) Set(context.Context, string, []byte) error {
	kv.mutationCalls++
	return fmt.Errorf("enforced summary read attempted a mutation")
}

func (kv *summaryNoCompletionKV) Delete(context.Context, string) error {
	kv.mutationCalls++
	return fmt.Errorf("enforced summary read attempted a mutation")
}

func (kv *summaryNoCompletionKV) List(context.Context, string, string, int) ([]string, string, error) {
	kv.listCalls++
	return nil, "", fmt.Errorf("enforced summary read attempted range/full completion")
}

type serverSummaryFixtureSource struct {
	revision      uint64
	contributions []clustermeta.SummaryContribution
}

func (source serverSummaryFixtureSource) ListSummaryContributions(_ context.Context, kind, cursor string, limit int) (clustermeta.SummaryRebuildSourcePage, error) {
	if kind != clustermeta.SummaryKindCluster || cursor != "" || limit < len(source.contributions) {
		return clustermeta.SummaryRebuildSourcePage{}, fmt.Errorf("unexpected summary rebuild request kind=%q cursor=%q limit=%d", kind, cursor, limit)
	}
	return clustermeta.SummaryRebuildSourcePage{
		Contributions:  append([]clustermeta.SummaryContribution(nil), source.contributions...),
		SourceRevision: source.revision,
	}, nil
}

func seedClusterSummaryAggregateForTest(t *testing.T, repo *clustermeta.Repository, revision uint64, counters clustermeta.SummaryCounters) {
	t.Helper()
	contribution, err := clustermeta.NewSummaryContribution("fixture:cluster-summary", counters)
	if err != nil {
		t.Fatalf("new cluster summary contribution: %v", err)
	}
	source := serverSummaryFixtureSource{
		revision:      revision,
		contributions: []clustermeta.SummaryContribution{contribution},
	}
	epoch := fmt.Sprintf("epoch-fixture-%d", revision)
	if _, err := repo.RunSummaryRebuildPage(context.Background(), source, clustermeta.SummaryKindCluster, epoch, 1); err != nil {
		t.Fatalf("run cluster summary rebuild: %v", err)
	}
	if _, err := repo.PromoteSummaryRebuild(context.Background(), clustermeta.SummaryKindCluster, epoch, revision); err != nil {
		t.Fatalf("promote cluster summary rebuild: %v", err)
	}
}

func TestAdminOperationStoreUpdatesPromotedSummaryAtomically(t *testing.T) {
	ctx := context.Background()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	source := serverSummaryFixtureSource{revision: 1}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, clustermeta.SummaryKindCluster, "admin-live", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, clustermeta.SummaryKindCluster, "admin-live", 1); err != nil {
		t.Fatal(err)
	}
	store := newOperationStore(kv, defaultMetadataRoot)
	op, err := store.create("node.join", "node1", "", "running", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		t.Fatal(err)
	}
	read, err := repo.GetClusterSummary(ctx, clustermeta.SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.Counters.OperationCount != 1 || read.Counters.RunningOperations != 1 {
		t.Fatalf("created operation counters=%+v", read.Counters)
	}
	if _, err := store.update(op.GetOperationId(), func(current *adminv1.OperationStatus) {
		current.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
	}); err != nil {
		t.Fatal(err)
	}
	read, err = repo.GetClusterSummary(ctx, clustermeta.SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.Counters.OperationCount != 1 || read.Counters.RunningOperations != 0 || read.Counters.CompletedOperations != 1 {
		t.Fatalf("completed operation counters=%+v", read.Counters)
	}
}

func newEnforcedSummaryServer(t *testing.T) (*server, *summaryNoCompletionKV, func()) {
	t.Helper()
	ctx := context.Background()
	base, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() { _ = base.Close() }
	writeRepo := clustermeta.NewRepository(base, defaultMetadataRoot)
	counters := []struct {
		subject string
		value   clustermeta.SummaryCounters
	}{
		{subject: "node:1", value: clustermeta.SummaryCounters{KnownNodes: 1, ActiveNodes: 1, HealthyNodes: 1}},
		{subject: "node:2", value: clustermeta.SummaryCounters{KnownNodes: 1, DrainingNodes: 1, SuspectNodes: 1}},
		{subject: "operation:1", value: clustermeta.SummaryCounters{OperationCount: 1, RunningOperations: 1}},
		{subject: "volume:1", value: clustermeta.SummaryCounters{
			VolumeCount: 1, DegradedVolumes: 1, DegradedExtents: 7,
			RepairBacklog: 2, RepairBacklogBytes: 200, RepairBacklogChunks: 20,
			RebalanceBacklog: 3, RebalanceBacklogBytes: 300, RebalanceBacklogChunks: 30,
			DrainBacklog: 4, DrainBacklogBytes: 400, DrainBacklogChunks: 40,
		}},
	}
	contributions := make([]clustermeta.SummaryContribution, 0, len(counters))
	for _, item := range counters {
		contribution, err := clustermeta.NewSummaryContribution(item.subject, item.value)
		if err != nil {
			cleanup()
			t.Fatalf("summary contribution %s: %v", item.subject, err)
		}
		contributions = append(contributions, contribution)
	}
	sort.Slice(contributions, func(i, j int) bool { return contributions[i].SubjectID < contributions[j].SubjectID })
	source := serverSummaryFixtureSource{revision: 300, contributions: contributions}
	if _, err := writeRepo.RunSummaryRebuildPage(ctx, source, clustermeta.SummaryKindCluster, "epoch-enforced", len(contributions)); err != nil {
		cleanup()
		t.Fatal(err)
	}
	if _, err := writeRepo.PromoteSummaryRebuild(ctx, clustermeta.SummaryKindCluster, "epoch-enforced", source.revision); err != nil {
		cleanup()
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	observedAt := time.Now().UTC()
	fleetObservation, err := clustermeta.NewFleetObservation(clustermeta.FleetObservation{
		SourceRevision: source.revision, ManifestRevision: "manifest-enforced", ManifestDigest: digest,
		BinaryDigest: digest, ConfigDigest: digest, StoreDigest: digest, ApplyState: "idle",
		StoreCount: 2, UsableBytes: 2000, FreeBytes: 1400, ReservedBytes: 100,
		Zones:                  []clustermeta.FleetZoneObservation{{Zone: "zone-a", ActiveNodes: 1}, {Zone: "zone-b", DrainingNodes: 1, SuspectNodes: 1}},
		RepairOldestAgeSeconds: 12, RebalanceOldestAgeSeconds: 13, DrainOldestAgeSeconds: 14,
		RepairClaimLatencyMillis: 21, RebalanceClaimLatencyMillis: 22, DrainClaimLatencyMillis: 23,
		CapacityObservedAtUnix: observedAt.Unix(), ObservedAtUnix: observedAt.Unix(),
	})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if err := writeRepo.PutFleetObservation(ctx, fleetObservation); err != nil {
		cleanup()
		t.Fatal(err)
	}

	guard := &summaryNoCompletionKV{base: base}
	readRepo := clustermeta.NewRepository(guard, defaultMetadataRoot)
	srv := &server{
		clusterID: "cluster-a", sbsClusterID: "sbs-a", nodeID: "service-1",
		root: defaultMetadataRoot, kv: guard, repo: readRepo, maint: newMaintenanceSettings(),
		clusterSummaryState: clusterSummaryStateEnforced,
		startedAt:           time.Unix(1, 0).UTC(),
		now:                 time.Now,
	}
	return srv, guard, cleanup
}

func TestEnforcedClusterStatusUsesBoundedAggregateOnly(t *testing.T) {
	srv, guard, cleanup := newEnforcedSummaryServer(t)
	defer cleanup()
	response, err := srv.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetActiveNodes() != 1 || response.GetDrainingNodes() != 1 || response.GetDegradedExtents() != 7 {
		t.Fatalf("aggregate status=%+v", response)
	}
	if response.GetKnownNodes() != 2 || response.GetHealthyNodes() != 1 || response.GetSuspectNodes() != 1 || response.GetDownNodes() != 0 || response.GetRemovedNodes() != 0 {
		t.Fatalf("aggregate node health=%+v", response)
	}
	if response.GetRepairBacklog() != 2 || response.GetRebalanceBacklog() != 3 || response.GetDrainBacklog() != 4 {
		t.Fatalf("aggregate backlog=%+v", response)
	}
	if response.GetClusterSummaryHealth() != adminv1.ClusterSummaryHealth_CLUSTER_SUMMARY_HEALTH_READY || response.GetClusterSummaryReason() != "none" || response.GetClusterSummaryPartial() || response.GetClusterSummaryStale() || response.GetClusterSummaryRebuildRequired() || response.GetClusterSummarySourceRevision() != 300 {
		t.Fatalf("aggregate health=%+v", response)
	}
	if guard.pointGets != 2 || guard.batchGets != 2 || guard.batchGetKeys != 2*clustermeta.SummaryVirtualShardCount {
		t.Fatalf("request class point=%d batch=%d keys=%d", guard.pointGets, guard.batchGets, guard.batchGetKeys)
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("forbidden calls list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}

func TestEnforcedMetricsUsesBoundedAggregateOnly(t *testing.T) {
	srv, guard, cleanup := newEnforcedSummaryServer(t)
	defer cleanup()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	observabilityMux(srv).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, line := range []string{
		`sbs_service_cluster_summary_health{state="ready"} 1`,
		`sbs_service_cluster_summary_health{state="rebuild_required"} 0`,
		`sbs_service_cluster_summary_partial 0`,
		`sbs_service_nodes{state="known"} 2`,
		`sbs_service_volumes{state="degraded"} 1`,
		`sbs_service_transition_backlog{reason="repair"} 2`,
		`sbs_service_transition_backlog{reason="rebalance"} 3`,
		`sbs_service_transition_backlog{reason="drain"} 4`,
		`sbs_service_fleet_health{health_code="SBS_FLEET_CHECK_STALE"} 0`,
		`sbs_service_fleet_apply_state{state="idle"} 1`,
		`sbs_service_fleet_manifest_source_revision 300`,
		`sbs_service_fleet_capacity_bytes{state="usable"} 2000`,
		`sbs_service_fleet_capacity_bytes{state="free"} 1400`,
		`sbs_service_fleet_capacity_bytes{state="reserved"} 100`,
		`sbs_service_fleet_zone_nodes{zone="zone-a",state="active"} 1`,
		`sbs_service_maintenance_oldest_age_seconds{reason="repair"} 12`,
		`sbs_service_maintenance_claim_latency_seconds{reason="drain"} 0.023`,
		`sbs_service_metadata_completion_total{state="nested"} 0`,
	} {
		if !strings.Contains(recorder.Body.String(), line) {
			t.Errorf("metrics missing %q", line)
		}
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("forbidden calls list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}

func TestEnforcedClusterStatusRejectsMissingAggregateWithoutLegacyFallback(t *testing.T) {
	base, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	guard := &summaryNoCompletionKV{base: base}
	srv := &server{
		clusterID: "cluster-a", sbsClusterID: "sbs-a", nodeID: "service-1",
		root: defaultMetadataRoot, kv: guard, repo: clustermeta.NewRepository(guard, defaultMetadataRoot), maint: newMaintenanceSettings(),
		clusterSummaryState: clusterSummaryStateEnforced,
	}
	response, err := srv.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetClusterSummaryHealth() != adminv1.ClusterSummaryHealth_CLUSTER_SUMMARY_HEALTH_REBUILD_REQUIRED || response.GetClusterSummaryReason() != "aggregate_missing" || !response.GetClusterSummaryPartial() || !response.GetClusterSummaryRebuildRequired() || response.GetQuorumHealth() != adminv1.QuorumHealth_QUORUM_HEALTH_UNAVAILABLE {
		t.Fatalf("missing aggregate status=%+v", response)
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("missing aggregate fell back: list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}

	recorder := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range []string{
		`sbs_service_cluster_summary_health{state="ready"} 0`,
		`sbs_service_cluster_summary_health{state="rebuild_required"} 1`,
		`sbs_service_cluster_summary_partial 1`,
		`sbs_service_cluster_summary_rebuild_required 1`,
	} {
		if !strings.Contains(recorder.Body.String(), line) {
			t.Errorf("missing aggregate metrics missing %q", line)
		}
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("missing aggregate metrics fell back: list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}

func TestEnforcedClusterStatusClassifiesStaleAndExpiredWithoutFallback(t *testing.T) {
	srv, guard, cleanup := newEnforcedSummaryServer(t)
	defer cleanup()
	srv.clusterSummaryDegradedAfter = 10 * time.Second
	srv.clusterSummaryRebuildRequiredAfter = 20 * time.Second
	baseNow := time.Now().UTC()
	srv.now = func() time.Time { return baseNow.Add(15 * time.Second) }
	degraded, err := srv.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if degraded.GetClusterSummaryHealth() != adminv1.ClusterSummaryHealth_CLUSTER_SUMMARY_HEALTH_DEGRADED || degraded.GetClusterSummaryReason() != "freshness_stale" || !degraded.GetClusterSummaryStale() || degraded.GetClusterSummaryPartial() || degraded.GetClusterSummaryRebuildRequired() || degraded.GetQuorumHealth() != adminv1.QuorumHealth_QUORUM_HEALTH_DEGRADED {
		t.Fatalf("degraded status=%+v", degraded)
	}
	srv.now = func() time.Time { return baseNow.Add(25 * time.Second) }
	expired, err := srv.GetClusterStatus(context.Background(), &adminv1.GetClusterStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if expired.GetClusterSummaryHealth() != adminv1.ClusterSummaryHealth_CLUSTER_SUMMARY_HEALTH_REBUILD_REQUIRED || expired.GetClusterSummaryReason() != "freshness_expired" || !expired.GetClusterSummaryStale() || expired.GetClusterSummaryPartial() || !expired.GetClusterSummaryRebuildRequired() || expired.GetQuorumHealth() != adminv1.QuorumHealth_QUORUM_HEALTH_UNAVAILABLE {
		t.Fatalf("expired status=%+v", expired)
	}
	if guard.listCalls != 0 || guard.mutationCalls != 0 {
		t.Fatalf("stale aggregate fell back: list=%d mutation=%d", guard.listCalls, guard.mutationCalls)
	}
}
