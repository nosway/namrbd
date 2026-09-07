package metadata

import (
	"context"
	"errors"
	"slices"
	"testing"
)

type orphanCleanupTestKV struct {
	*fakeTransactionalKV
	beforeTransaction func(values map[string][]byte)
	snapshotCalls     int
}

func newOrphanCleanupTestKV() *orphanCleanupTestKV {
	return &orphanCleanupTestKV{fakeTransactionalKV: newFakeTransactionalKV()}
}

func (f *orphanCleanupTestKV) RunInReadSnapshot(ctx context.Context, fn func(snapshot kvReadSnapshot) error) error {
	f.mu.Lock()
	f.snapshotCalls++
	values := cloneOrphanCleanupValues(f.values)
	f.mu.Unlock()
	batchCalls := 0
	return fn(&mapMembershipReadSnapshot{values: values, batchGetCalls: &batchCalls})
}

func (f *orphanCleanupTestKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runTxCalls++
	working := cloneOrphanCleanupValues(f.values)
	if f.beforeTransaction != nil {
		f.beforeTransaction(working)
		f.beforeTransaction = nil
	}
	if err := fn(ctxLockedStore{
		values:        working,
		getCalls:      f.getCalls,
		setCalls:      f.setCalls,
		batchGetCalls: &f.batchGetCalls,
		batchGetKeys:  &f.batchGetKeys,
	}); err != nil {
		return err
	}
	f.values = working
	return nil
}

func cloneOrphanCleanupValues(values map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(values))
	for key, value := range values {
		clone[key] = slices.Clone(value)
	}
	return clone
}

func seedOrphanCleanupVolume(t *testing.T, repo *Repository, volumeID string, states ...MutationOperationState) {
	t.Helper()
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: volumeID, Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{VolumeID: volumeID, IdempotencyKey: "idem-1"}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	for index, state := range states {
		operation := MutationOperationRecord{
			OperationID: "orphan-operation-" + string(rune('a'+index)),
			VolumeID:    volumeID,
			Kind:        "write",
			State:       state,
		}
		if err := repo.PutMutationOperation(ctx, operation); err != nil {
			t.Fatalf("PutMutationOperation: %v", err)
		}
	}
}

func orphanCleanupExpectation(t *testing.T, kv *orphanCleanupTestKV, root, volumeID string) OrphanVolumeMetadataCleanupExpectation {
	t.Helper()
	var expectation OrphanVolumeMetadataCleanupExpectation
	err := kv.RunInReadSnapshot(context.Background(), func(snapshot kvReadSnapshot) error {
		keys, _, err := listOrphanCleanupKeys(context.Background(), snapshot, root+"/volumes/"+volumeID+"/")
		if err != nil {
			return err
		}
		values, err := snapshot.BatchGet(context.Background(), keys)
		if err != nil {
			return err
		}
		expectation = OrphanVolumeMetadataCleanupExpectation{
			VolumeID:                  volumeID,
			ExactKeyValueCount:        len(keys),
			ExactKeyValueDigestSHA256: digestOrphanMetadataKeyValues(keys, values),
		}
		return nil
	})
	if err != nil {
		t.Fatalf("capture expectation: %v", err)
	}
	return expectation
}

func TestOrphanMetadataCleanupPlanIsReadOnlyAndExactDeleteIsAtomic(t *testing.T) {
	ctx := context.Background()
	kv := newOrphanCleanupTestKV()
	repo := NewRepository(kv, "orphan-cleanup")
	volumeID := "00a1b2c3"
	seedOrphanCleanupVolume(t, repo, volumeID, MutationOperationCommitted, MutationOperationRolledBack)
	expectation := orphanCleanupExpectation(t, kv, "orphan-cleanup", volumeID)

	runTxBeforePlan := kv.runTxCalls
	planned, err := repo.ValidateOrphanVolumeMetadataCleanupExact(ctx, []OrphanVolumeMetadataCleanupExpectation{expectation})
	if err != nil {
		t.Fatalf("ValidateOrphanVolumeMetadataCleanupExact: %v", err)
	}
	if planned.VolumeCount != 1 || planned.VolumeKeyDeleteCount != expectation.ExactKeyValueCount || planned.MutationOperationDeleteCount != 2 || planned.TransactionCallCount != 0 || kv.runTxCalls != runTxBeforePlan {
		t.Fatalf("read-only plan=%+v runTx before=%d after=%d", planned, runTxBeforePlan, kv.runTxCalls)
	}

	result, err := repo.DeleteOrphanVolumeMetadataExact(ctx, []OrphanVolumeMetadataCleanupExpectation{expectation}, true)
	if err != nil {
		t.Fatalf("DeleteOrphanVolumeMetadataExact: %v", err)
	}
	if result.VolumeCount != 1 || result.VolumeKeyDeleteCount != expectation.ExactKeyValueCount || result.MutationOperationDeleteCount != 2 || result.TransactionCallCount != 1 || result.UniqueTiKVMutationKeyCount < expectation.ExactKeyValueCount || result.PostDeleteRemainingKeyCount != 0 {
		t.Fatalf("cleanup result=%+v", result)
	}
	keys, _, err := kv.List(ctx, "orphan-cleanup/volumes/"+volumeID+"/", "", 100)
	if err != nil || len(keys) != 0 {
		t.Fatalf("remaining volume keys=%v err=%v", keys, err)
	}
	for _, operationID := range []string{"orphan-operation-a", "orphan-operation-b"} {
		if _, err := repo.GetMutationOperationByID(ctx, operationID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("operation index %s remains: %v", operationID, err)
		}
	}
}

func TestOrphanMetadataCleanupRejectsWithoutStoppedWriters(t *testing.T) {
	ctx := context.Background()
	kv := newOrphanCleanupTestKV()
	repo := NewRepository(kv, "orphan-cleanup-stopped")
	volumeID := "00a1b2c3"
	seedOrphanCleanupVolume(t, repo, volumeID, MutationOperationCommitted)
	expectation := orphanCleanupExpectation(t, kv, "orphan-cleanup-stopped", volumeID)
	transactionCalls := kv.runTxCalls

	if _, err := repo.DeleteOrphanVolumeMetadataExact(ctx, []OrphanVolumeMetadataCleanupExpectation{expectation}, false); !errors.Is(err, ErrOrphanMetadataCleanupRejected) {
		t.Fatalf("cleanup error=%v", err)
	}
	if kv.runTxCalls != transactionCalls {
		t.Fatalf("unexpected transaction calls before=%d after=%d", transactionCalls, kv.runTxCalls)
	}
	if _, err := repo.GetVolumeState(ctx, volumeID); err != nil {
		t.Fatalf("orphan state changed: %v", err)
	}
}

func TestOrphanMetadataCleanupRejectsStaleDigestAndTransactionConflict(t *testing.T) {
	ctx := context.Background()
	kv := newOrphanCleanupTestKV()
	repo := NewRepository(kv, "orphan-cleanup-conflict")
	volumeID := "00a1b2c3"
	seedOrphanCleanupVolume(t, repo, volumeID, MutationOperationCommitted)
	expectation := orphanCleanupExpectation(t, kv, "orphan-cleanup-conflict", volumeID)

	stale := expectation
	stale.ExactKeyValueDigestSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	transactionCalls := kv.runTxCalls
	if _, err := repo.DeleteOrphanVolumeMetadataExact(ctx, []OrphanVolumeMetadataCleanupExpectation{stale}, true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale digest error=%v", err)
	}
	if kv.runTxCalls != transactionCalls {
		t.Fatalf("stale digest reached transaction before=%d after=%d", transactionCalls, kv.runTxCalls)
	}

	stateKey := volumeStateKey("orphan-cleanup-conflict", volumeID)
	kv.beforeTransaction = func(values map[string][]byte) { values[stateKey] = []byte(`{"volume_id":"00a1b2c3","revision":99}`) }
	if _, err := repo.DeleteOrphanVolumeMetadataExact(ctx, []OrphanVolumeMetadataCleanupExpectation{expectation}, true); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("transaction conflict error=%v", err)
	}
	state, err := repo.GetVolumeState(ctx, volumeID)
	if err != nil || state.Revision != 1 {
		t.Fatalf("transaction rollback state=%+v err=%v", state, err)
	}
}

func TestOrphanMetadataCleanupRejectsUnsafeOrLiveAuthority(t *testing.T) {
	for _, test := range []struct {
		name  string
		state MutationOperationState
		seed  func(t *testing.T, repo *Repository, kv *orphanCleanupTestKV, root, volumeID string)
	}{
		{name: "pending-operation", state: MutationOperationPending},
		{name: "running-operation", state: MutationOperationRunning},
		{name: "failed-operation", state: MutationOperationFailed},
		{name: "unsafe-key", state: MutationOperationCommitted, seed: func(t *testing.T, _ *Repository, kv *orphanCleanupTestKV, root, volumeID string) {
			t.Helper()
			if err := kv.Set(context.Background(), root+"/volumes/"+volumeID+"/extents/0001", []byte(`{"physical_chunk_id":1}`)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "volume-spec", state: MutationOperationCommitted, seed: func(t *testing.T, repo *Repository, _ *orphanCleanupTestKV, _ string, volumeID string) {
			t.Helper()
			if err := repo.PutVolumeSpec(context.Background(), VolumeSpecRecord{VolumeID: volumeID, SizeBytes: 4096, BlockSize: 4096, ChunkSizeBytes: 4096}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := "orphan-cleanup-reject-" + test.name
			kv := newOrphanCleanupTestKV()
			repo := NewRepository(kv, root)
			volumeID := "00a1b2c3"
			seedOrphanCleanupVolume(t, repo, volumeID, test.state)
			if test.seed != nil {
				test.seed(t, repo, kv, root, volumeID)
			}
			expectation := orphanCleanupExpectation(t, kv, root, volumeID)
			transactionCalls := kv.runTxCalls
			if _, err := repo.ValidateOrphanVolumeMetadataCleanupExact(ctx, []OrphanVolumeMetadataCleanupExpectation{expectation}); !errors.Is(err, ErrOrphanMetadataCleanupRejected) {
				t.Fatalf("validation error=%v", err)
			}
			if kv.runTxCalls != transactionCalls {
				t.Fatalf("rejection reached transaction before=%d after=%d", transactionCalls, kv.runTxCalls)
			}
		})
	}
}
