package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaintenanceWorkPagePinsRevisionAndRejectsOversize(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	seedServerMaintenanceWork(t, ctx, srv.repo, "00a1b2e1", "pl-repair-a", "repair")
	seedServerMaintenanceWork(t, ctx, srv.repo, "00a1b2e2", "pl-repair-b", "repair")
	seedServerMaintenanceWork(t, ctx, srv.repo, "00a1b2e3", "pl-rebalance-a", "rebalance")
	promoteServerMaintenanceProjection(t, ctx, srv.repo)

	first, err := srv.ListRepairsPage(ctx, &adminv1.ListRepairsPageRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetRepairs()) != 1 || first.GetNextPageToken() == "" || first.GetProjectionRevision() == "" || first.GetScannedRecords() != 1 || first.GetProjectionHealth() != "healthy" {
		t.Fatalf("first page=%+v", first)
	}
	if _, err := srv.ListRebalancesPage(ctx, &adminv1.ListRebalancesPageRequest{PageSize: 1, PageToken: first.GetNextPageToken()}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-reason token code=%s err=%v", status.Code(err), err)
	}
	ready, err := srv.repo.ListMaintenanceWorkPage(ctx, "repair", clustermeta.MaintenanceWorkStateReady, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.repo.ClaimMaintenanceWork(ctx, ready.Records[0], "worker-page-test", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.ListRepairsPage(ctx, &adminv1.ListRepairsPageRequest{PageSize: 1, PageToken: first.GetNextPageToken()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale token code=%s err=%v", status.Code(err), err)
	}
	fresh, err := srv.ListRepairsPage(ctx, &adminv1.ListRepairsPageRequest{PageSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]int{}
	for _, repair := range fresh.GetRepairs() {
		states[repair.GetState()]++
	}
	if len(fresh.GetRepairs()) != 2 || states["queued"] != 1 || states["running"] != 1 || fresh.GetNextPageToken() != "" || fresh.GetScannedRecords() != 2 {
		t.Fatalf("ready+leased page=%+v states=%+v", fresh, states)
	}
	if _, err := srv.ListRepairsPage(ctx, &adminv1.ListRepairsPageRequest{PageSize: 513}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversize code=%s err=%v", status.Code(err), err)
	}
}

func TestLegacyExpensiveListsRequireAdmissionDeadlineAndBudget(t *testing.T) {
	srv := newTestMaintenanceServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	admission := &adminv1.ExpensiveCallAdmission{Reason: "diagnostic export", RecordBudget: 10}
	calls := []struct {
		surface string
		call    func(context.Context, *adminv1.ExpensiveCallAdmission) error
	}{
		{"list_volumes", func(callCtx context.Context, value *adminv1.ExpensiveCallAdmission) error {
			_, err := srv.ListVolumes(callCtx, &adminv1.ListVolumesRequest{Admission: value})
			return err
		}},
		{"list_operations", func(callCtx context.Context, value *adminv1.ExpensiveCallAdmission) error {
			_, err := srv.ListOperations(callCtx, &adminv1.ListOperationsRequest{Admission: value})
			return err
		}},
		{"list_repairs", func(callCtx context.Context, value *adminv1.ExpensiveCallAdmission) error {
			_, err := srv.ListRepairs(callCtx, &adminv1.ListRepairsRequest{Admission: value})
			return err
		}},
		{"list_rebalances", func(callCtx context.Context, value *adminv1.ExpensiveCallAdmission) error {
			_, err := srv.ListRebalances(callCtx, &adminv1.ListRebalancesRequest{Admission: value})
			return err
		}},
	}
	for _, item := range calls {
		if err := item.call(context.Background(), nil); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("%s missing admission code=%s err=%v", item.surface, status.Code(err), err)
		}
		if err := item.call(ctx, admission); err != nil {
			t.Fatalf("%s admitted empty list: %v", item.surface, err)
		}
		stats := srv.phaseADCurrentObservability.snapshot().LegacyExpensiveBySurface[item.surface]
		if stats["requested"] != 2 || stats["rejected"] != 1 || stats["admitted"] != 1 || stats["completed"] != 1 {
			t.Fatalf("%s legacy counters=%+v", item.surface, stats)
		}
	}
	recorder := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, item := range calls {
		for _, outcome := range []string{"admitted", "rejected", "completed"} {
			want := "sbs_service_phase_ad_legacy_expensive_calls_total{surface=\"" + item.surface + "\",outcome=\"" + outcome + "\"} 1"
			if !strings.Contains(recorder.Body.String(), want) {
				t.Fatalf("metrics missing %q", want)
			}
		}
	}
}

func seedServerMaintenanceWork(t *testing.T, ctx context.Context, repo *clustermeta.Repository, volumeID, placementRef, reason string) {
	t.Helper()
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{VolumeID: volumeID, Epoch: 7, Revision: 11, Status: clustermeta.VolumeStatusHealthy}); err != nil {
		t.Fatal(err)
	}
	current := clustermeta.ReplicaSetState{
		ReplicaSetID: "rs-current-" + placementRef, VolumeID: volumeID, PlacementRef: placementRef, Epoch: 3,
		PrimaryReplicaID: "rep-a", WriteQuorum: 2, ReadQuorum: 1,
		Replicas: []clustermeta.ReplicaDescriptor{{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary}, {NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary}, {NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary}},
	}
	target := clustermeta.ReplicaSetState{
		ReplicaSetID: "rs-target-" + placementRef, VolumeID: volumeID, PlacementRef: "target-" + placementRef, Epoch: 4,
		PrimaryReplicaID: "rep-d", WriteQuorum: 2, ReadQuorum: 1,
		Replicas: []clustermeta.ReplicaDescriptor{{NodeID: "node-d", ReplicaID: "rep-d", Role: clustermeta.ReplicaRolePrimary}, {NodeID: "node-e", ReplicaID: "rep-e", Role: clustermeta.ReplicaRoleSecondary}, {NodeID: "node-f", ReplicaID: "rep-f", Role: clustermeta.ReplicaRoleSecondary}},
	}
	if err := repo.PutReplicaSet(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutReplicaSet(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
		VolumeID: volumeID, PlacementRef: placementRef, State: clustermeta.PlacementTransitionQueued, Reason: reason,
		CurrentReplicaSetID: current.ReplicaSetID, TargetReplicaSetID: target.ReplicaSetID, StartedAtUnix: time.Now().Add(-time.Minute).Unix(), LastProgressAtUnix: time.Now().Add(-time.Minute).Unix(),
	}); err != nil {
		t.Fatal(err)
	}
}

func promoteServerMaintenanceProjection(t *testing.T, ctx context.Context, repo *clustermeta.Repository) {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		page, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-paged-work", 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		if page.Completed {
			if _, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-paged-work"); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("maintenance projection rebuild did not complete")
}
