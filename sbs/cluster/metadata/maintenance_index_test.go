package metadata

import (
	"context"
	"errors"
	"testing"
)

type listCountingTransactionalKV struct {
	*fakeTransactionalKV
	listCalls int
}

func (kv *listCountingTransactionalKV) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	kv.listCalls++
	return kv.fakeTransactionalKV.List(ctx, prefix, cursor, limit)
}

func TestPlacementByNodeIndexTracksReplicaSetChanges(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-index")

	first := ReplicaSetState{
		VolumeID: "00a1b2c3", ReplicaSetID: "rs-001", PlacementRef: "pl-001", Epoch: 1,
		PrimaryReplicaID: "rep-a",
		Replicas: []ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: ReplicaRoleSecondary},
		},
	}
	if err := repo.PutReplicaSet(ctx, first); err != nil {
		t.Fatalf("PutReplicaSet(first): %v", err)
	}
	second := first
	second.ReplicaSetID = "rs-002"
	second.PlacementRef = "pl-002"
	second.PrimaryReplicaID = "rep-a-2"
	second.Replicas = []ReplicaDescriptor{{NodeID: "node-a", ReplicaID: "rep-a-2", Role: ReplicaRolePrimary}}
	if err := repo.PutReplicaSet(ctx, second); err != nil {
		t.Fatalf("PutReplicaSet(second): %v", err)
	}

	page1, err := repo.ListPlacementByNodePage(ctx, "node-a", "", 1)
	if err != nil {
		t.Fatalf("ListPlacementByNodePage(first): %v", err)
	}
	if len(page1.Records) != 1 || page1.NextCursor == "" || page1.RangePageCount != 1 || page1.PointGetCount != 1 || page1.BatchGetCount != 0 {
		t.Fatalf("first page=%+v", page1)
	}
	page2, err := repo.ListPlacementByNodePage(ctx, "node-a", page1.NextCursor, 1)
	if err != nil {
		t.Fatalf("ListPlacementByNodePage(second): %v", err)
	}
	if len(page2.Records) != 1 || page2.Records[0].PlacementRef == page1.Records[0].PlacementRef {
		t.Fatalf("second page=%+v", page2)
	}

	updated := first
	updated.Epoch = 2
	updated.PrimaryReplicaID = "rep-c"
	updated.Replicas = []ReplicaDescriptor{
		{NodeID: "node-c", ReplicaID: "rep-c", Role: ReplicaRolePrimary},
		{NodeID: "node-d", ReplicaID: "rep-d", Role: ReplicaRoleSecondary},
	}
	if err := repo.PutReplicaSet(ctx, updated); err != nil {
		t.Fatalf("PutReplicaSet(updated): %v", err)
	}
	if page := mustPlacementPage(t, repo, "node-b"); len(page.Records) != 0 {
		t.Fatalf("removed node-b index=%+v", page.Records)
	}
	if page := mustPlacementPage(t, repo, "node-d"); len(page.Records) != 1 || page.Records[0].ReplicaSetEpoch != 2 {
		t.Fatalf("added node-d index=%+v", page.Records)
	}
	if page := mustPlacementPage(t, repo, "node-a"); len(page.Records) != 1 || page.Records[0].ReplicaSetID != "rs-002" {
		t.Fatalf("node-a retained records=%+v", page.Records)
	}

	if err := repo.DeleteReplicaSet(ctx, updated.VolumeID, updated.ReplicaSetID); err != nil {
		t.Fatalf("DeleteReplicaSet: %v", err)
	}
	for _, nodeID := range []string{"node-c", "node-d"} {
		if page := mustPlacementPage(t, repo, nodeID); len(page.Records) != 0 {
			t.Fatalf("deleted %s index=%+v", nodeID, page.Records)
		}
	}
}

func TestPlacementByNodeIndexRollsBackWithAuthorityTransaction(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := &conflictInjectingMembershipKV{fakeTransactionalKV: base, conflictsRemaining: 1}
	repo := NewRepository(kv, "phase-ad-index")
	rec := ReplicaSetState{
		VolumeID: "00a1b2c3", ReplicaSetID: "rs-rollback", PlacementRef: "pl-rollback", Epoch: 1,
		Replicas: []ReplicaDescriptor{{NodeID: "node-a", ReplicaID: "rep-a", Role: ReplicaRolePrimary}},
	}
	if err := repo.PutReplicaSet(ctx, rec); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("PutReplicaSet error=%v want CAS conflict", err)
	}
	if _, err := repo.GetReplicaSet(ctx, rec.VolumeID, rec.ReplicaSetID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("authority survived rollback: %v", err)
	}
	if page := mustPlacementPage(t, repo, "node-a"); len(page.Records) != 0 {
		t.Fatalf("index survived rollback: %+v", page.Records)
	}
}

func TestPrimaryFailoverRefreshesPlacementIndexValue(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-index")
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}); err != nil {
		t.Fatal(err)
	}
	replicaSet := ReplicaSetState{
		VolumeID: "00a1b2c3", ReplicaSetID: "rs-failover", PlacementRef: "pl-failover", Epoch: 1,
		PrimaryReplicaID: "rep-a",
		Replicas: []ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: ReplicaRoleSecondary},
		},
	}
	if err := repo.PutReplicaSet(ctx, replicaSet); err != nil {
		t.Fatal(err)
	}
	_, _, err := repo.CommitPrimaryFailover(ctx, CommitPrimaryFailoverRequest{
		VolumeID: "00a1b2c3", ReplicaSetID: "rs-failover", ExpectedVolumeEpoch: 1,
		ExpectedReplicaSetEpoch: 1, ExpectedPrimaryReplicaID: "rep-a", NewPrimaryReplicaID: "rep-b",
	})
	if err != nil {
		t.Fatalf("CommitPrimaryFailover: %v", err)
	}
	for _, nodeID := range []string{"node-a", "node-b"} {
		page := mustPlacementPage(t, repo, nodeID)
		if len(page.Records) != 1 || page.Records[0].ReplicaSetEpoch != 2 || page.Records[0].PrimaryReplica != "rep-b" {
			t.Fatalf("%s index after failover=%+v", nodeID, page.Records)
		}
	}
}

func TestPlacementByNodeIndexRejectsReplicaSetIdentityCollision(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := &conflictInjectingMembershipKV{fakeTransactionalKV: base}
	repo := NewRepository(kv, "phase-ad-index")
	first := ReplicaSetState{
		VolumeID: "00a1b2c3", ReplicaSetID: "rs-first", PlacementRef: "pl-shared", Epoch: 1,
		Replicas: []ReplicaDescriptor{{NodeID: "node-a", ReplicaID: "rep-a", Role: ReplicaRolePrimary}},
	}
	if err := repo.PutReplicaSet(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ReplicaSetID = "rs-second"
	if err := repo.PutReplicaSet(ctx, second); !errors.Is(err, ErrMaintenanceIndexConflict) {
		t.Fatalf("collision error=%v", err)
	}
	if _, err := repo.GetReplicaSet(ctx, second.VolumeID, second.ReplicaSetID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting authority committed: %v", err)
	}
	page := mustPlacementPage(t, repo, "node-a")
	if len(page.Records) != 1 || page.Records[0].ReplicaSetID != first.ReplicaSetID {
		t.Fatalf("placement collision changed owner: %+v", page.Records)
	}
}

func TestMutationOperationByIDIndexUsesOnlyPointReads(t *testing.T) {
	ctx := context.Background()
	kv := &listCountingTransactionalKV{fakeTransactionalKV: newFakeTransactionalKV()}
	repo := NewRepository(kv, "phase-ad-index")
	rec := MutationOperationRecord{
		OperationID: "repair-global-001", VolumeID: "00a1b2c3", Kind: "repair",
		State: MutationOperationRunning, StartedAtUnix: 100, LastUpdatedAtUnix: 101,
	}
	if err := repo.PutMutationOperation(ctx, rec); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	kv.resetGetCalls()
	point, err := repo.GetMutationOperationByIDPoint(ctx, rec.OperationID)
	if err != nil {
		t.Fatalf("GetMutationOperationByIDPoint: %v", err)
	}
	if point.PointGetCount != 2 || point.BackendFullScanCount != 0 || point.FullCompletionCount != 0 || point.NestedCompletionCount != 0 || kv.listCalls != 0 {
		t.Fatalf("point read shape=%+v list_calls=%d", point, kv.listCalls)
	}
	if point.Operation.VolumeID != rec.VolumeID || point.Index.State != MutationOperationRunning {
		t.Fatalf("point result=%+v", point)
	}
	if _, err := repo.FindMutationOperationByID(ctx, rec.OperationID); err != nil || kv.listCalls != 0 {
		t.Fatalf("indexed compatibility lookup err=%v list_calls=%d", err, kv.listCalls)
	}

	rec.State = MutationOperationCommitted
	rec.LastUpdatedAtUnix = 102
	if err := repo.PutMutationOperation(ctx, rec); err != nil {
		t.Fatalf("update operation: %v", err)
	}
	updated, err := repo.GetMutationOperationByID(ctx, rec.OperationID)
	if err != nil || updated.State != MutationOperationCommitted {
		t.Fatalf("updated operation=%+v err=%v", updated, err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{OperationID: rec.OperationID, VolumeID: "00a1b2c4", Kind: "repair", State: MutationOperationPending}); !errors.Is(err, ErrMaintenanceIndexConflict) {
		t.Fatalf("collision error=%v", err)
	}

	if err := repo.DeleteMutationOperation(ctx, rec.VolumeID, rec.OperationID); err != nil {
		t.Fatalf("DeleteMutationOperation: %v", err)
	}
	if _, err := repo.GetMutationOperationByID(ctx, rec.OperationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted point read error=%v", err)
	}
}

func TestMutationOperationRetiredReplicaTargetsRequireCanonicalTransitionAuthority(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-index")
	base := MutationOperationRecord{
		OperationID: "transition-pl-1", VolumeID: "00a1b2c3", Kind: "transition", State: MutationOperationCommitted,
		RetiredReplicaTargetsResolved: true,
		RetiredReplicaTargets:         []MutationRetiredReplicaTarget{{NodeID: "node-a", SourceReplicaIDs: []string{"rep-a"}}},
	}
	if err := repo.PutMutationOperation(ctx, base); err != nil {
		t.Fatalf("valid retired replica targets: %v", err)
	}
	for name, mutate := range map[string]func(*MutationOperationRecord){
		"unresolved": func(rec *MutationOperationRecord) { rec.RetiredReplicaTargetsResolved = false },
		"write":      func(rec *MutationOperationRecord) { rec.Kind = "write" },
		"duplicate": func(rec *MutationOperationRecord) {
			rec.RetiredReplicaTargets = append(rec.RetiredReplicaTargets, rec.RetiredReplicaTargets[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := cloneMutationOperationRecord(base)
			rec.OperationID += "-" + name
			mutate(&rec)
			if err := repo.PutMutationOperation(ctx, rec); !errors.Is(err, ErrMaintenanceIndexInvalid) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestMutationOperationByIDIndexRejectsKindIdentityCollision(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := &conflictInjectingMembershipKV{fakeTransactionalKV: base}
	repo := NewRepository(kv, "phase-ad-index")
	first := MutationOperationRecord{OperationID: "op-kind", VolumeID: "00a1b2c3", Kind: "repair", State: MutationOperationRunning}
	if err := repo.PutMutationOperation(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Kind = "rebalance"
	if err := repo.PutMutationOperation(ctx, second); !errors.Is(err, ErrMaintenanceIndexConflict) {
		t.Fatalf("kind collision error=%v", err)
	}
	operation, err := repo.GetMutationOperationByID(ctx, first.OperationID)
	if err != nil || operation.Kind != first.Kind {
		t.Fatalf("operation after collision=%+v err=%v", operation, err)
	}
}

func TestWriteIntentCreatesOperationByIDIndexInSameTransaction(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-index")
	if err := repo.PutWriteIntent(ctx,
		IdempotencyRecord{IdempotencyKey: "idem-001", VolumeID: "00a1b2c3", Operation: "write", ResultState: IdempotencyPending},
		MutationOperationRecord{OperationID: "write-global-001", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationRunning, IdempotencyKey: "idem-001", LastUpdatedAtUnix: 100},
	); err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	if kv.runTxCalls != 1 {
		t.Fatalf("transaction calls=%d want 1", kv.runTxCalls)
	}
	point, err := repo.GetMutationOperationByIDPoint(ctx, "write-global-001")
	if err != nil || point.Operation.IdempotencyKey != "idem-001" {
		t.Fatalf("indexed write intent=%+v err=%v", point, err)
	}
}

func TestCompareAndSetMutationOperationsIsAtomicAndExact(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	repo := NewRepository(&conflictInjectingMembershipKV{fakeTransactionalKV: base}, "phase-ad-index")
	first := MutationOperationRecord{
		OperationID: "payload-gc-00a1b2c3", VolumeID: "00a1b2c3", Kind: "payload_gc",
		State: MutationOperationPending, RetiredPhysicalChunkIDs: []uint64{1, 2}, LastUpdatedAtUnix: 100,
	}
	second := MutationOperationRecord{
		OperationID: "payload-gc-00a1b2c4", VolumeID: "00a1b2c4", Kind: "payload_gc",
		State: MutationOperationPending, RetiredPhysicalChunkIDs: []uint64{3}, LastUpdatedAtUnix: 100,
	}
	for _, operation := range []MutationOperationRecord{first, second} {
		if err := repo.PutMutationOperation(ctx, operation); err != nil {
			t.Fatal(err)
		}
	}
	committedFirst := cloneMutationOperationRecord(first)
	committedFirst.State = MutationOperationCommitted
	committedFirst.LastUpdatedAtUnix = 200
	committedSecond := cloneMutationOperationRecord(second)
	committedSecond.State = MutationOperationCommitted
	committedSecond.LastUpdatedAtUnix = 200
	updates := []MutationOperationCASUpdate{
		{Expected: first, Updated: committedFirst},
		{Expected: second, Updated: committedSecond},
	}
	if err := repo.CompareAndSetMutationOperations(ctx, updates); err != nil {
		t.Fatalf("CompareAndSetMutationOperations: %v", err)
	}
	for _, operationID := range []string{first.OperationID, second.OperationID} {
		point, err := repo.GetMutationOperationByIDPoint(ctx, operationID)
		if err != nil || point.Operation.State != MutationOperationCommitted || point.Index.State != MutationOperationCommitted {
			t.Fatalf("committed operation %s point=%+v err=%v", operationID, point, err)
		}
	}

	staleFirst := cloneMutationOperationRecord(committedFirst)
	staleFirst.LastUpdatedAtUnix--
	rolledFirst := cloneMutationOperationRecord(committedFirst)
	rolledFirst.State = MutationOperationRolledBack
	rolledSecond := cloneMutationOperationRecord(committedSecond)
	rolledSecond.State = MutationOperationRolledBack
	if err := repo.CompareAndSetMutationOperations(ctx, []MutationOperationCASUpdate{
		{Expected: staleFirst, Updated: rolledFirst},
		{Expected: committedSecond, Updated: rolledSecond},
	}); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale batch error=%v want ErrCASConflict", err)
	}
	currentSecond, err := repo.GetMutationOperation(ctx, second.VolumeID, second.OperationID)
	if err != nil || currentSecond.State != MutationOperationCommitted {
		t.Fatalf("atomic rollback second=%+v err=%v", currentSecond, err)
	}
}

func TestCompareAndSetMutationOperationsRejectsUnsafeBatchShape(t *testing.T) {
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-index")
	base := MutationOperationRecord{OperationID: "op-1", VolumeID: "00a1b2c3", Kind: "repair", State: MutationOperationRunning}
	if err := repo.CompareAndSetMutationOperations(context.Background(), nil); !errors.Is(err, ErrMaintenanceIndexInvalid) {
		t.Fatalf("empty batch error=%v", err)
	}
	if err := repo.CompareAndSetMutationOperations(context.Background(), []MutationOperationCASUpdate{{Expected: base, Updated: base}, {Expected: base, Updated: base}}); !errors.Is(err, ErrMaintenanceIndexInvalid) {
		t.Fatalf("duplicate batch error=%v", err)
	}
	changed := base
	changed.OperationID = "op-2"
	if err := repo.CompareAndSetMutationOperations(context.Background(), []MutationOperationCASUpdate{{Expected: base, Updated: changed}}); !errors.Is(err, ErrMaintenanceIndexInvalid) {
		t.Fatalf("identity change error=%v", err)
	}
}

func mustPlacementPage(t *testing.T, repo *Repository, nodeID string) PlacementByNodePage {
	t.Helper()
	page, err := repo.ListPlacementByNodePage(context.Background(), nodeID, "", MaintenanceIndexPageMaximum)
	if err != nil {
		t.Fatalf("ListPlacementByNodePage(%s): %v", nodeID, err)
	}
	return page
}
