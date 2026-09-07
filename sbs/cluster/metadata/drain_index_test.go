package metadata

import (
	"context"
	"errors"
	"testing"
)

func TestExtentByPlacementIndexTracksMappingIdentity(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-drain")
	mappings := []ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, PlacementRef: "pl-a", Revision: 1},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 4096, LengthBytes: 4096, PlacementRef: "pl-a", Revision: 1},
	}
	for _, mapping := range mappings {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatal(err)
		}
	}
	first, err := repo.ListExtentMappingsByPlacementPage(ctx, mappings[0].VolumeID, "pl-a", "", 1)
	if err != nil || len(first.Records) != 1 || first.Records[0].ExtentID != 1 || first.NextCursor == "" || first.RangePageCount != 1 || first.BackendFullScanCount != 0 || first.FullCompletionCount != 0 || first.NestedCompletionCount != 0 {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	second, err := repo.ListExtentMappingsByPlacementPage(ctx, mappings[0].VolumeID, "pl-a", first.NextCursor, 1)
	if err != nil || len(second.Records) != 1 || second.Records[0].ExtentID != 2 || second.NextCursor != "" {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	mappings[0].PlacementRef = "pl-b"
	mappings[0].Revision++
	if err := repo.PutExtentMapping(ctx, mappings[0]); err != nil {
		t.Fatal(err)
	}
	oldPage, err := repo.ListExtentMappingsByPlacementPage(ctx, mappings[0].VolumeID, "pl-a", "", 10)
	if err != nil || len(oldPage.Records) != 1 || oldPage.Records[0].ExtentID != 2 {
		t.Fatalf("old placement page=%+v err=%v", oldPage, err)
	}
	if err := repo.DeleteExtentMapping(ctx, mappings[0].VolumeID, mappings[0].ExtentID); err != nil {
		t.Fatal(err)
	}
	deletedPage, err := repo.ListExtentMappingsByPlacementPage(ctx, mappings[0].VolumeID, "pl-b", "", 10)
	if err != nil || len(deletedPage.Records) != 0 {
		t.Fatalf("deleted placement page=%+v err=%v", deletedPage, err)
	}
}

func TestDrainWorkProgressIsIdempotentAndCompletesWithTransition(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-drain")
	progress, err := repo.BeginDrainProgress(ctx, "node-a", "op-drain-1")
	if err != nil {
		t.Fatal(err)
	}
	volumeID := "00a1b2c3"
	target := rebuildReplicaSet(volumeID, "rs-target", "pl-target", 2, "node-b", "node-c")
	transition := PlacementTransitionRecord{
		VolumeID: volumeID, PlacementRef: "pl-a",
		State: PlacementTransitionQueued, Reason: "drain", CurrentReplicaSetID: "rs-source", TargetReplicaSetID: target.ReplicaSetID,
	}
	base := EnqueueDrainWorkRequest{
		OperationID: progress.OperationID, NodeID: progress.NodeID,
		VolumeID: volumeID, PlacementRef: "pl-a", ReplicaSetID: "rs-source",
	}
	partial := base
	partial.Extents = []DrainWorkExtent{{ExtentID: 1, DataBytes: 2048}}
	progress, created, err := repo.EnqueueDrainWork(ctx, partial)
	if err != nil || !created || progress.TotalExtents != 1 || progress.RemainingExtents != 1 || progress.TotalBytes != 2048 || progress.RemainingBytes != 2048 {
		t.Fatalf("partial enqueue progress=%+v created=%t err=%v", progress, created, err)
	}
	if _, err := repo.GetPlacementTransition(ctx, volumeID, "pl-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial enqueue exposed transition: %v", err)
	}
	final := base
	final.Extents = []DrainWorkExtent{{ExtentID: 2, DataBytes: 1024}}
	final.FinalizePlacement = true
	final.TargetReplicaSet = target
	final.Transition = transition
	progress, created, err = repo.EnqueueDrainWork(ctx, final)
	if err != nil || !created || progress.TotalExtents != 2 || progress.RemainingExtents != 2 || progress.TotalBytes != 3072 || progress.RemainingBytes != 3072 {
		t.Fatalf("final enqueue progress=%+v created=%t err=%v", progress, created, err)
	}
	point, err := repo.GetDrainProgressPoint(ctx, progress.NodeID)
	if err != nil || point.PointGetCount != 1 || point.Progress.ProgressDigest != progress.ProgressDigest || point.BackendFullScanCount != 0 || point.FullCompletionCount != 0 || point.NestedCompletionCount != 0 {
		t.Fatalf("progress point=%+v err=%v", point, err)
	}
	again, created, err := repo.EnqueueDrainWork(ctx, final)
	if err != nil || created || again.ProgressDigest != progress.ProgressDigest {
		t.Fatalf("replay progress=%+v created=%t err=%v", again, created, err)
	}
	advanced, err := repo.AdvanceDrainEnqueueCursor(ctx, progress, "", "", true)
	if err != nil || !advanced.EnqueueCompleted {
		t.Fatalf("advance=%+v err=%v", advanced, err)
	}
	transition.State = PlacementTransitionCompleted
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	completed, err := repo.GetDrainProgress(ctx, "node-a")
	if err != nil || completed.RemainingExtents != 0 || completed.RemainingBytes != 0 || !completed.EnqueueCompleted {
		t.Fatalf("completed progress=%+v err=%v", completed, err)
	}
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	replayed, err := repo.GetDrainProgress(ctx, "node-a")
	if err != nil || replayed.ProgressDigest != completed.ProgressDigest {
		t.Fatalf("completion replay progress=%+v err=%v", replayed, err)
	}
	staleReplay, created, err := repo.EnqueueDrainWork(ctx, final)
	if err != nil || created || staleReplay.ProgressDigest != completed.ProgressDigest {
		t.Fatalf("stale enqueue replay progress=%+v created=%t err=%v", staleReplay, created, err)
	}
	storedTransition, err := repo.GetPlacementTransition(ctx, transition.VolumeID, transition.PlacementRef)
	if err != nil || storedTransition.State != PlacementTransitionCompleted {
		t.Fatalf("transition regressed after stale replay: %+v err=%v", storedTransition, err)
	}
}

func TestDrainWorkEnqueueRollsBackTargetTransitionAndProgress(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := &conflictInjectingMembershipKV{fakeTransactionalKV: base}
	repo := NewRepository(kv, "phase-ad-drain")
	progress, err := repo.BeginDrainProgress(ctx, "node-a", "op-drain-rollback")
	if err != nil {
		t.Fatal(err)
	}
	volumeID := "00a1b2c3"
	target := rebuildReplicaSet(volumeID, "rs-target", "pl-target", 2, "node-b")
	transition := PlacementTransitionRecord{VolumeID: volumeID, PlacementRef: "pl-a", State: PlacementTransitionQueued, Reason: "drain", CurrentReplicaSetID: "rs-source", TargetReplicaSetID: target.ReplicaSetID}
	kv.conflictsRemaining = 1
	_, _, err = repo.EnqueueDrainWork(ctx, EnqueueDrainWorkRequest{
		OperationID: progress.OperationID, NodeID: progress.NodeID,
		VolumeID: volumeID, PlacementRef: "pl-a", ReplicaSetID: "rs-source",
		Extents: []DrainWorkExtent{{ExtentID: 1, DataBytes: 512}}, FinalizePlacement: true,
		TargetReplicaSet: target, Transition: transition,
	})
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("enqueue error=%v", err)
	}
	if _, err := repo.GetReplicaSet(ctx, target.VolumeID, target.ReplicaSetID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("target survived rollback: %v", err)
	}
	if _, err := repo.GetPlacementTransition(ctx, transition.VolumeID, transition.PlacementRef); !errors.Is(err, ErrNotFound) {
		t.Fatalf("transition survived rollback: %v", err)
	}
	after, err := repo.GetDrainProgress(ctx, progress.NodeID)
	if err != nil || after.TotalExtents != 0 || after.RemainingExtents != 0 {
		t.Fatalf("progress after rollback=%+v err=%v", after, err)
	}
}
