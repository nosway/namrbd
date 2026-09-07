package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMaintenanceIndexRebuildRepairsResumesAndPromotes(t *testing.T) {
	ctx := context.Background()
	const root = "phase-ad-index-rebuild"
	kv := &listCountingTransactionalKV{fakeTransactionalKV: newFakeTransactionalKV()}
	repo := NewRepository(kv, root)

	legacyReplica := rebuildReplicaSet("00a1b2c3", "rs-legacy", "pl-legacy", 1, "node-a", "node-b")
	if err := writeReplicaSet(ctx, kv, root, legacyReplica); err != nil {
		t.Fatal(err)
	}
	legacyOperation := MutationOperationRecord{OperationID: "op-legacy", VolumeID: "00a1b2c3", Kind: "repair", State: MutationOperationRunning, LastUpdatedAtUnix: 100}
	if err := writeMutationOperation(ctx, kv, root, legacyOperation); err != nil {
		t.Fatal(err)
	}

	staleReplica := rebuildReplicaSet("00a1b2c4", "rs-stale", "pl-stale", 1, "node-old", "node-keep")
	if err := repo.PutReplicaSet(ctx, staleReplica); err != nil {
		t.Fatal(err)
	}
	staleReplica.Epoch = 2
	staleReplica.PrimaryReplicaID = "rep-node-new"
	staleReplica.Replicas = rebuildReplicas("node-keep", "node-new")
	if err := writeReplicaSet(ctx, kv, root, staleReplica); err != nil {
		t.Fatal(err)
	}
	staleOperation := MutationOperationRecord{OperationID: "op-stale", VolumeID: "00a1b2c4", Kind: "repair", State: MutationOperationRunning, LastUpdatedAtUnix: 101}
	if err := repo.PutMutationOperation(ctx, staleOperation); err != nil {
		t.Fatal(err)
	}
	staleOperation.State = MutationOperationCommitted
	staleOperation.LastUpdatedAtUnix = 102
	if err := writeMutationOperation(ctx, kv, root, staleOperation); err != nil {
		t.Fatal(err)
	}

	danglingReplica := rebuildReplicaSet("00a1b2c5", "rs-dangling", "pl-dangling", 1, "node-dangling")
	if err := repo.PutReplicaSet(ctx, danglingReplica); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, replicaSetKey(root, danglingReplica.VolumeID, danglingReplica.ReplicaSetID)); err != nil {
		t.Fatal(err)
	}
	danglingOperation := MutationOperationRecord{OperationID: "op-dangling", VolumeID: "00a1b2c5", Kind: "repair", State: MutationOperationRunning, LastUpdatedAtUnix: 103}
	if err := repo.PutMutationOperation(ctx, danglingOperation); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, mutationOperationKey(root, danglingOperation.VolumeID, danglingOperation.OperationID)); err != nil {
		t.Fatal(err)
	}

	first, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-001", 2, 8)
	if err != nil {
		t.Fatalf("first rebuild page: %v", err)
	}
	if first.RangePageCount != 1 || first.InputCount > 2 || first.ProcessedKeyCount == 0 || first.RepairCount > 8 || first.Completed {
		t.Fatalf("first page=%+v", first)
	}
	paused, err := repo.SetMaintenanceIndexRebuildPaused(ctx, "epoch-001", true)
	if err != nil || !paused.Paused {
		t.Fatalf("pause checkpoint=%+v err=%v", paused, err)
	}
	pausedPage, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-001", 2, 8)
	if err != nil || !pausedPage.Paused || pausedPage.RangePageCount != 0 {
		t.Fatalf("paused page=%+v err=%v", pausedPage, err)
	}

	repo = NewRepository(kv, root)
	if _, err := repo.SetMaintenanceIndexRebuildPaused(ctx, "epoch-001", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	for attempt := 0; attempt < 100; attempt++ {
		page, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-001", 2, 8)
		if err != nil {
			t.Fatalf("rebuild attempt %d: %v", attempt, err)
		}
		if page.InputCount > 2 || page.RepairCount > 8 || page.BackendFullScanCount != 0 || page.FullCompletionCount != 0 || page.NestedCompletionCount != 0 {
			t.Fatalf("unbounded page=%+v", page)
		}
		if page.Completed {
			break
		}
		if attempt == 99 {
			t.Fatal("rebuild did not complete")
		}
		repo = NewRepository(kv, root)
	}
	checkpoint, err := repo.GetMaintenanceIndexRebuildCheckpoint(ctx, "epoch-001")
	if err != nil || !checkpoint.Completed || checkpoint.Promoted || !checkpoint.WorkProjectionRebuilt || checkpoint.MismatchCount == 0 || checkpoint.RepairCount != checkpoint.MismatchCount || checkpoint.DeletedCount != 0 {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	antiEntropy, err := repo.GetMaintenanceIndexAntiEntropyCheckpoint(ctx, "epoch-001")
	if err != nil || !antiEntropy.Completed || antiEntropy.Promoted || antiEntropy.RepairCount != antiEntropy.MismatchCount || antiEntropy.DeletedCount < 2 {
		t.Fatalf("anti-entropy checkpoint=%+v err=%v", antiEntropy, err)
	}
	state, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-001")
	if err != nil || state.State != maintenanceIndexStateReady || state.ActiveEpoch != "epoch-001" || !state.DrainProjectionReady || !state.HealthProjectionReady || !state.WorkProjectionReady {
		t.Fatalf("promoted state=%+v err=%v", state, err)
	}

	if point, err := repo.GetMutationOperationByIDPoint(ctx, legacyOperation.OperationID); err != nil || point.Operation.VolumeID != legacyOperation.VolumeID {
		t.Fatalf("legacy operation point=%+v err=%v", point, err)
	}
	if point, err := repo.GetMutationOperationByIDPoint(ctx, staleOperation.OperationID); err != nil || point.Operation.State != MutationOperationCommitted {
		t.Fatalf("stale operation point=%+v err=%v", point, err)
	}
	if page := mustPlacementPage(t, repo, "node-a"); len(page.Records) != 1 || page.Records[0].ReplicaSetID != legacyReplica.ReplicaSetID {
		t.Fatalf("rebuilt legacy placement=%+v", page.Records)
	}
	if page := mustPlacementPage(t, repo, "node-old"); len(page.Records) != 0 {
		t.Fatalf("stale placement survived=%+v", page.Records)
	}
	if page := mustPlacementPage(t, repo, "node-new"); len(page.Records) != 1 || page.Records[0].ReplicaSetEpoch != 2 {
		t.Fatalf("replacement placement=%+v", page.Records)
	}
	if page := mustPlacementPage(t, repo, "node-dangling"); len(page.Records) != 0 {
		t.Fatalf("dangling placement survived=%+v", page.Records)
	}
	if _, err := repo.GetMutationOperationByID(ctx, danglingOperation.OperationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dangling operation survived: %v", err)
	}

	kv.listCalls = 0
	if _, err := repo.FindMutationOperationByID(ctx, "op-absent"); !errors.Is(err, ErrNotFound) || kv.listCalls != 0 {
		t.Fatalf("promoted missing lookup err=%v list_calls=%d", err, kv.listCalls)
	}
	completedPage, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-001", 2, 8)
	if err != nil || !completedPage.Completed || completedPage.RangePageCount != 0 {
		t.Fatalf("completed page=%+v err=%v", completedPage, err)
	}
}

func TestMaintenanceIndexPromotionWaitsForSeparateAntiEntropyCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-rebuild-separated")
	page, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-separated", 8, 8)
	if err != nil || page.JobClass != maintenanceIndexJobRebuild || page.Completed {
		t.Fatalf("rebuild page=%+v err=%v", page, err)
	}
	rebuild, err := repo.GetMaintenanceIndexRebuildCheckpoint(ctx, "epoch-separated")
	if err != nil || !rebuild.Completed || !rebuild.WorkProjectionRebuilt || rebuild.PageCount != 1 {
		t.Fatalf("rebuild checkpoint=%+v err=%v", rebuild, err)
	}
	if _, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-separated"); !errors.Is(err, ErrMaintenanceIndexRebuildIncomplete) {
		t.Fatalf("promotion before anti-entropy err=%v", err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		page, err = repo.RunMaintenanceIndexAntiEntropyPage(ctx, "epoch-separated", 8, 8)
		if err != nil || page.JobClass != maintenanceIndexJobAntiEntropy {
			t.Fatalf("anti-entropy attempt=%d page=%+v err=%v", attempt, page, err)
		}
	}
	if !page.Completed {
		t.Fatalf("anti-entropy page=%+v", page)
	}
	state, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-separated")
	if err != nil || !state.WorkProjectionReady {
		t.Fatalf("promotion state=%+v err=%v", state, err)
	}
}

func TestLegacyMaintenanceIndexStateKeepsAffectedSetProjectionsDisabled(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-rebuild")
	legacy := MaintenanceIndexState{
		SchemaVersion: MaintenanceIndexRebuildSchemaVersion,
		State:         maintenanceIndexStateReady,
		ActiveEpoch:   "epoch-004b",
		UpdatedAtUnix: 1,
	}
	legacy.StateDigest = digestMaintenanceIndexState(legacy)
	if err := repo.putJSON(ctx, maintenanceIndexStateKey(repo.root), legacy); err != nil {
		t.Fatal(err)
	}
	state, err := repo.GetMaintenanceIndexState(ctx)
	if err != nil || state.DrainProjectionReady || state.HealthProjectionReady || state.WorkProjectionReady {
		t.Fatalf("legacy state=%+v err=%v", state, err)
	}
}

func TestLegacyCompletedRebuildCannotEnableWorkProjection(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-rebuild-legacy-complete")
	checkpoint := MaintenanceIndexRebuildCheckpoint{
		SchemaVersion: MaintenanceIndexRebuildSchemaVersion, Epoch: "epoch-004b-complete",
		Phase: maintenanceIndexRebuildPhaseComplete, PageCount: 3, Completed: true,
		StartedAtUnix: 1, UpdatedAtUnix: 2,
	}
	checkpoint.CheckpointDigest = digestMaintenanceIndexRebuildCheckpoint(checkpoint)
	if err := repo.putJSON(ctx, maintenanceIndexRebuildCheckpointKey(repo.root, checkpoint.Epoch), checkpoint); err != nil {
		t.Fatal(err)
	}
	state, err := repo.PromoteMaintenanceIndexRebuild(ctx, checkpoint.Epoch)
	if err != nil || !state.DrainProjectionReady || !state.HealthProjectionReady || state.WorkProjectionReady {
		t.Fatalf("legacy promotion state=%+v err=%v", state, err)
	}
}

func TestMaintenanceIndexRebuildRepairsWorkReadyAndDeletesDanglingIndex(t *testing.T) {
	ctx := context.Background()
	const root = "phase-ad-index-rebuild-work"
	clock := time.Unix(1_800_001_000, 0).UTC()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, root)
	repo.now = func() time.Time { return clock }
	transition := seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2c6", "pl-work", "repair")
	workID := MaintenanceWorkID(transition.VolumeID, transition.PlacementRef)
	work, err := repo.GetMaintenanceWork(ctx, workID)
	if err != nil {
		t.Fatal(err)
	}
	index := maintenanceWorkIndexRecord(work)
	if err := kv.Delete(ctx, maintenanceWorkKey(root, workID)); err != nil {
		t.Fatal(err)
	}
	dangling := index
	dangling.WorkID = "00dead00:pl-dead"
	dangling.VolumeID = "00dead00"
	dangling.PlacementRef = "pl-dead"
	dangling.VirtualShard = MaintenanceWorkIndexVirtualShard(dangling.WorkID)
	dangling.IndexDigest = digestMaintenanceWorkIndexRecord(dangling)
	danglingKey := maintenanceWorkIndexKey(root, dangling)
	if err := putJSONStore(ctx, kv, danglingKey, dangling); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 100; attempt++ {
		page, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-work", 2, 8)
		if err != nil {
			t.Fatalf("rebuild attempt %d: %v", attempt, err)
		}
		if page.InputCount > 2 || page.RepairCount > 8 || page.BackendFullScanCount != 0 || page.FullCompletionCount != 0 || page.NestedCompletionCount != 0 {
			t.Fatalf("unbounded page=%+v", page)
		}
		if page.Completed {
			break
		}
		if attempt == 99 {
			t.Fatal("work rebuild did not complete")
		}
	}
	rebuilt, err := repo.GetMaintenanceWork(ctx, workID)
	if err != nil || rebuilt.WorkDigest != work.WorkDigest {
		t.Fatalf("rebuilt work=%+v err=%v", rebuilt, err)
	}
	ready, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateReady, "", 8)
	if err != nil || len(ready.Records) != 1 || ready.Records[0].WorkID != workID {
		t.Fatalf("rebuilt ready page=%+v err=%v", ready, err)
	}
	if _, found, err := kv.Get(ctx, danglingKey); err != nil || found {
		t.Fatalf("dangling work index found=%t err=%v", found, err)
	}
	state, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-work")
	if err != nil || !state.WorkProjectionReady {
		t.Fatalf("promoted state=%+v err=%v", state, err)
	}
}

func TestMaintenanceIndexRebuildMigratesLegacyWorkShardSchema(t *testing.T) {
	ctx := context.Background()
	const root = "phase-ad-index-rebuild-work-v1"
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, root)
	transition := seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2c6", "pl-work", "repair")
	workID := MaintenanceWorkID(transition.VolumeID, transition.PlacementRef)
	work, err := repo.GetMaintenanceWork(ctx, workID)
	if err != nil {
		t.Fatal(err)
	}
	currentIndex := maintenanceWorkIndexRecord(work)
	currentIndexKey := maintenanceWorkIndexKey(root, currentIndex)

	work.SchemaVersion = 1
	work.VirtualShard = metadataVirtualShard(work.WorkID, 64)
	if work.VirtualShard < MaintenanceWorkIndexShardCount {
		t.Fatalf("fixture does not cross shard boundary: shard=%d", work.VirtualShard)
	}
	work.WorkDigest = digestMaintenanceWorkRecord(work)
	legacyIndex := maintenanceWorkIndexRecord(work)
	legacyIndex.SchemaVersion = 1
	legacyIndex.WorkDigest = work.WorkDigest
	legacyIndex.IndexDigest = digestMaintenanceWorkIndexRecord(legacyIndex)
	legacyIndexKey := maintenanceWorkIndexKey(root, legacyIndex)
	if legacyIndexKey == currentIndexKey {
		t.Fatal("legacy and current work index keys must differ")
	}
	if err := kv.Delete(ctx, currentIndexKey); err != nil {
		t.Fatal(err)
	}
	if err := putJSONStore(ctx, kv, maintenanceWorkKey(root, workID), work); err != nil {
		t.Fatal(err)
	}
	if err := putJSONStore(ctx, kv, legacyIndexKey, legacyIndex); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 100; attempt++ {
		page, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-work-v2", 8, 8)
		if err != nil {
			t.Fatalf("rebuild attempt %d: %v", attempt, err)
		}
		if page.Completed {
			break
		}
		if attempt == 99 {
			t.Fatal("work shard migration did not complete")
		}
	}
	if _, err := repo.PromoteMaintenanceIndexRebuild(ctx, "epoch-work-v2"); err != nil {
		t.Fatalf("promote migrated work projection: %v", err)
	}
	migrated, err := repo.GetMaintenanceWork(ctx, workID)
	if err != nil || migrated.SchemaVersion != MaintenanceWorkSchemaVersion || migrated.VirtualShard != MaintenanceWorkIndexVirtualShard(workID) {
		t.Fatalf("migrated work=%+v err=%v", migrated, err)
	}
	if _, found, err := kv.Get(ctx, legacyIndexKey); err != nil || found {
		t.Fatalf("legacy 64-shard index found=%t err=%v", found, err)
	}
	ready, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateReady, "", 8)
	if err != nil || len(ready.Records) != 1 || ready.Records[0].VirtualShard != migrated.VirtualShard {
		t.Fatalf("migrated ready page=%+v err=%v", ready, err)
	}
}

func TestMaintenanceIndexRebuildRejectsTooSmallRepairBudget(t *testing.T) {
	ctx := context.Background()
	const root = "phase-ad-index-rebuild-budget"
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, root)
	record := rebuildReplicaSet("00a1b2c3", "rs-wide", "pl-wide", 1, "node-a", "node-b", "node-c")
	if err := writeReplicaSet(ctx, kv, root, record); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-budget", 1, 2); !errors.Is(err, ErrMaintenanceIndexRebuildBudget) {
		t.Fatalf("budget error=%v", err)
	}
	if _, err := repo.GetMaintenanceIndexRebuildCheckpoint(ctx, "epoch-budget"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint advanced on budget error: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		if page := mustPlacementPage(t, repo, nodeID); len(page.Records) != 0 {
			t.Fatalf("partial repair for %s=%+v", nodeID, page.Records)
		}
	}
}

func TestMaintenanceIndexRebuildRejectsGlobalOperationCollision(t *testing.T) {
	ctx := context.Background()
	const root = "phase-ad-index-rebuild-collision"
	base := newFakeTransactionalKV()
	kv := &conflictInjectingMembershipKV{fakeTransactionalKV: base}
	repo := NewRepository(kv, root)
	for _, volumeID := range []string{"00a1b2c3", "00a1b2c4"} {
		if err := writeMutationOperation(ctx, kv, root, MutationOperationRecord{OperationID: "op-collision", VolumeID: volumeID, Kind: "repair", State: MutationOperationRunning}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-collision", 8, 8); !errors.Is(err, ErrMaintenanceIndexConflict) {
		t.Fatalf("collision error=%v", err)
	}
	if _, err := repo.GetMaintenanceIndexRebuildCheckpoint(ctx, "epoch-collision"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint advanced after collision: %v", err)
	}
	if _, found, err := kv.Get(ctx, operationByIDKey(root, "op-collision")); err != nil || found {
		t.Fatalf("ambiguous index committed found=%v err=%v", found, err)
	}
}

func rebuildReplicaSet(volumeID, replicaSetID, placementRef string, epoch uint64, nodeIDs ...string) ReplicaSetState {
	replicas := rebuildReplicas(nodeIDs...)
	primary := ""
	if len(replicas) != 0 {
		primary = replicas[0].ReplicaID
	}
	return ReplicaSetState{
		VolumeID: volumeID, ReplicaSetID: replicaSetID, PlacementRef: placementRef,
		Epoch: epoch, PrimaryReplicaID: primary, Replicas: replicas, WriteQuorum: 1, ReadQuorum: 1,
	}
}

func rebuildReplicas(nodeIDs ...string) []ReplicaDescriptor {
	replicas := make([]ReplicaDescriptor, 0, len(nodeIDs))
	for index, nodeID := range nodeIDs {
		role := ReplicaRoleSecondary
		if index == 0 {
			role = ReplicaRolePrimary
		}
		replicas = append(replicas, ReplicaDescriptor{NodeID: nodeID, ReplicaID: "rep-" + nodeID, Role: role})
	}
	return replicas
}
