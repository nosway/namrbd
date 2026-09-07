package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMaintenanceWorkReadyClaimLeaseRecoveryAndCompletion(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1_800_000_000, 0).UTC()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-work-ready")
	repo.now = func() time.Time { return clock }
	transition := seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2c3", "pl-a", "repair")

	ready, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateReady, "", 1)
	if err != nil || len(ready.Records) != 1 || ready.Records[0].VolumeID != transition.VolumeID || ready.NextCursor != "" || ready.RangePageCount != 1 || ready.BatchGetKeyCount+ready.PointGetCount != 1 || ready.BackendFullScanCount != 0 || ready.FullCompletionCount != 0 || ready.NestedCompletionCount != 0 {
		t.Fatalf("ready page=%+v err=%v", ready, err)
	}
	first, err := repo.ClaimMaintenanceWork(ctx, ready.Records[0], "worker-a", 30*time.Second)
	if err != nil || first.State != MaintenanceWorkStateLeased || first.LeaseOwner != "worker-a" || first.LeaseGeneration != 1 || first.LeaseExpiresAtUnix != clock.Add(30*time.Second).Unix() {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	if page, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateReady, "", 1); err != nil || len(page.Records) != 0 {
		t.Fatalf("ready after claim=%+v err=%v", page, err)
	}
	leased, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateLeased, "", 1)
	if err != nil || len(leased.Records) != 1 {
		t.Fatalf("leased page=%+v err=%v", leased, err)
	}
	replayed, err := repo.ClaimMaintenanceWork(ctx, leased.Records[0], "worker-a", 30*time.Second)
	if err != nil || replayed.WorkDigest != first.WorkDigest {
		t.Fatalf("same-owner replay=%+v err=%v", replayed, err)
	}
	if _, err := repo.ClaimMaintenanceWork(ctx, leased.Records[0], "worker-b", 30*time.Second); !errors.Is(err, ErrMaintenanceWorkLeaseHeld) {
		t.Fatalf("live competing lease error=%v", err)
	}

	clock = clock.Add(10 * time.Second)
	renewed, err := repo.RenewMaintenanceWorkLease(ctx, first, "worker-a", 30*time.Second)
	if err != nil || renewed.LeaseGeneration != first.LeaseGeneration || renewed.WorkRevision != first.WorkRevision+1 || renewed.LeaseExpiresAtUnix != clock.Add(30*time.Second).Unix() {
		t.Fatalf("renewed lease=%+v err=%v", renewed, err)
	}
	if _, err := repo.ValidateMaintenanceWorkLease(ctx, first, "worker-a"); !errors.Is(err, ErrMaintenanceWorkLeaseLost) {
		t.Fatalf("pre-renewal digest remained valid: %v", err)
	}
	leased, err = repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateLeased, "", 1)
	if err != nil || len(leased.Records) != 1 || leased.Records[0].WorkRevision != renewed.WorkRevision {
		t.Fatalf("renewed leased page=%+v err=%v", leased, err)
	}
	clock = clock.Add(31 * time.Second)
	second, err := repo.ClaimMaintenanceWork(ctx, leased.Records[0], "worker-b", 30*time.Second)
	if err != nil || second.LeaseOwner != "worker-b" || second.LeaseGeneration != first.LeaseGeneration+1 || second.WorkRevision != renewed.WorkRevision+1 {
		t.Fatalf("expired reclaim=%+v err=%v", second, err)
	}
	if _, err := repo.ValidateMaintenanceWorkLease(ctx, first, "worker-a"); !errors.Is(err, ErrMaintenanceWorkLeaseLost) {
		t.Fatalf("old lease validation error=%v", err)
	}
	if current, err := repo.ValidateMaintenanceWorkLease(ctx, second, "worker-b"); err != nil || current.WorkDigest != second.WorkDigest {
		t.Fatalf("current lease=%+v err=%v", current, err)
	}

	transition.State = PlacementTransitionRunning
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	volume, err := repo.GetVolumeState(ctx, transition.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	volume.Epoch++
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	current, err := repo.GetMaintenanceWork(ctx, second.WorkID)
	if err != nil || current.WorkDigest != second.WorkDigest {
		t.Fatalf("running transition changed lease=%+v err=%v", current, err)
	}
	if _, err := repo.ValidateMaintenanceWorkLease(ctx, second, "worker-b"); err != nil {
		t.Fatalf("running transition could not resume after placement epoch advanced: %v", err)
	}
	transition.State = PlacementTransitionCompleted
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	completed, err := repo.GetMaintenanceWork(ctx, second.WorkID)
	if err != nil || completed.State != MaintenanceWorkStateCompleted || completed.LeaseOwner != "" || completed.WorkRevision != second.WorkRevision+1 {
		t.Fatalf("completed work=%+v err=%v", completed, err)
	}
	if page, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateLeased, "", 1); err != nil || len(page.Records) != 0 {
		t.Fatalf("leased after completion=%+v err=%v", page, err)
	}
	if err := repo.DeletePlacementTransition(ctx, transition.VolumeID, transition.PlacementRef); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetMaintenanceWork(ctx, second.WorkID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("work survived transition deletion: %v", err)
	}
}

func TestMaintenanceWorkClaimRejectsStaleAuthorityWithoutMutation(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-work-stale")
	transition := seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2c4", "pl-b", "rebalance")
	page, err := repo.ListMaintenanceWorkPage(ctx, "rebalance", MaintenanceWorkStateReady, "", 8)
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("ready page=%+v err=%v", page, err)
	}
	before, err := repo.GetMaintenanceWork(ctx, page.Records[0].WorkID)
	if err != nil {
		t.Fatal(err)
	}
	volume, err := repo.GetVolumeState(ctx, transition.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	volume.Epoch++
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimMaintenanceWork(ctx, page.Records[0], "worker-stale", time.Minute); !errors.Is(err, ErrMaintenanceWorkStale) {
		t.Fatalf("stale claim error=%v", err)
	}
	after, err := repo.GetMaintenanceWork(ctx, before.WorkID)
	if err != nil || after.WorkDigest != before.WorkDigest || after.State != MaintenanceWorkStateReady || after.LeaseGeneration != 0 {
		t.Fatalf("stale claim mutated work before=%+v after=%+v err=%v", before, after, err)
	}
	ready, err := repo.ListMaintenanceWorkPage(ctx, "rebalance", MaintenanceWorkStateReady, "", 8)
	if err != nil || len(ready.Records) != 1 || ready.Records[0].IndexDigest != page.Records[0].IndexDigest {
		t.Fatalf("stale claim mutated index=%+v err=%v", ready, err)
	}
}

func TestMaintenanceWorkReasonPriority(t *testing.T) {
	for reason, want := range map[string]int{"drain": 0, "repair": 1, "rebalance": 2} {
		if got := maintenanceWorkPriority(reason); got != want {
			t.Fatalf("reason=%s priority=%d want=%d", reason, got, want)
		}
	}
}

func seedMaintenanceWorkTransition(t *testing.T, ctx context.Context, repo *Repository, volumeID, placementRef, reason string) PlacementTransitionRecord {
	t.Helper()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: volumeID, Epoch: 7, Revision: 11, Status: VolumeStatusHealthy}); err != nil {
		t.Fatal(err)
	}
	current := rebuildReplicaSet(volumeID, "rs-current-"+placementRef, placementRef, 3, "node-a", "node-b", "node-c")
	target := rebuildReplicaSet(volumeID, "rs-target-"+placementRef, "target-"+placementRef, 4, "node-d", "node-e", "node-f")
	if err := repo.PutReplicaSet(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutReplicaSet(ctx, target); err != nil {
		t.Fatal(err)
	}
	transition := PlacementTransitionRecord{
		VolumeID: volumeID, PlacementRef: placementRef, State: PlacementTransitionQueued, Reason: reason,
		CurrentReplicaSetID: current.ReplicaSetID, TargetReplicaSetID: target.ReplicaSetID,
		StartedAtUnix: 1_799_999_900, LastProgressAtUnix: 1_799_999_900,
	}
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	return transition
}
