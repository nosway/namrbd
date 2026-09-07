package metadata

import (
	"context"
	"errors"
	"testing"
)

func TestOperationListProjectionAndMutationPageAreBounded(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-operation-list")
	first := MutationOperationRecord{
		OperationID: "op-a", VolumeID: "00a1b2c3", Kind: "repair", State: MutationOperationRunning, LastUpdatedAtUnix: 100,
	}
	second := MutationOperationRecord{
		OperationID: "op-b", VolumeID: "00a1b2c4", Kind: "rebalance", State: MutationOperationPending, LastUpdatedAtUnix: 101,
	}
	if err := repo.PutMutationOperation(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetOperationListProjection(ctx); !errors.Is(err, ErrOperationListRebuildRequired) {
		t.Fatalf("pre-promotion projection error=%v", err)
	}
	putReadyMaintenanceIndexState(t, kv, repo.root, "epoch-operation-list")
	projection, err := repo.GetOperationListProjection(ctx)
	if err != nil || projection.RevisionDigest == "" || projection.MaintenanceEpoch != "epoch-operation-list" {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	page, err := repo.ListMutationOperationIndexPage(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.ScannedCount != 1 || len(page.Operations) != 1 || page.NextCursor == "" || page.RangePageCount != 1 || page.BatchGetCount != 2 || page.BatchGetKeyCount != 2 || page.BackendFullScanCount != 0 || page.FullCompletionCount != 0 || page.NestedCompletionCount != 0 {
		t.Fatalf("first page=%+v", page)
	}
	second.State = MutationOperationCommitted
	second.LastUpdatedAtUnix++
	if err := repo.PutMutationOperation(ctx, second); err != nil {
		t.Fatal(err)
	}
	after, err := repo.GetOperationListProjection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.RevisionDigest == projection.RevisionDigest {
		t.Fatalf("operation update did not advance sharded projection: before=%+v after=%+v", projection, after)
	}
	if _, err := repo.ListMutationOperationIndexPage(ctx, "", MaintenanceIndexPageMaximum+1); !errors.Is(err, ErrOperationListInvalid) {
		t.Fatalf("oversized page error=%v", err)
	}
}

func TestOperationChildrenSummaryTracksStateDeltas(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-operation-children")
	child := MutationOperationRecord{
		OperationID: "batch-1", VolumeID: "00a1b2c3", Kind: "transition_batch", State: MutationOperationRunning,
		IdempotencyKey: "parent-1", AffectedPageNos: []uint64{1}, LastUpdatedAtUnix: 100,
	}
	if err := repo.PutMutationOperation(ctx, child); err != nil {
		t.Fatal(err)
	}
	summary, err := repo.GetOperationChildrenSummary(ctx, "parent-1")
	if err != nil || summary.Total != 1 || summary.Running != 1 || summary.Small != 1 {
		t.Fatalf("running summary=%+v err=%v", summary, err)
	}
	child.State = MutationOperationCommitted
	child.CompletedPageNos = []uint64{1}
	child.LastUpdatedAtUnix++
	if err := repo.PutMutationOperation(ctx, child); err != nil {
		t.Fatal(err)
	}
	summary, err = repo.GetOperationChildrenSummary(ctx, "parent-1")
	if err != nil || summary.Total != 1 || summary.Running != 0 || summary.Completed != 1 || summary.Small != 1 {
		t.Fatalf("completed summary=%+v err=%v", summary, err)
	}
	if err := repo.DeleteMutationOperation(ctx, child.VolumeID, child.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetOperationChildrenSummary(ctx, "parent-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted summary error=%v", err)
	}
}

func putReadyMaintenanceIndexState(t *testing.T, store kvReadWriter, root, epoch string) {
	t.Helper()
	state := MaintenanceIndexState{
		SchemaVersion: MaintenanceIndexRebuildSchemaVersion, State: maintenanceIndexStateReady, ActiveEpoch: epoch,
		DrainProjectionReady: true, HealthProjectionReady: true, WorkProjectionReady: true, UpdatedAtUnix: 100,
	}
	state.StateDigest = digestMaintenanceIndexState(state)
	if err := putJSONStore(context.Background(), store, maintenanceIndexStateKey(root), state); err != nil {
		t.Fatal(err)
	}
}
