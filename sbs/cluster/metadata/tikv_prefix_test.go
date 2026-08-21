package metadata

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestPrefixedKVIsolatesKeysForV1KeyspaceFallback(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kvA := newPrefixedKV(base, "phasef-a")
	kvB := newPrefixedKV(base, "phasef-b")

	if err := kvA.Set(ctx, "sbs/cluster/bootstrap", []byte("a")); err != nil {
		t.Fatalf("kvA.Set: %v", err)
	}
	if err := kvB.Set(ctx, "sbs/cluster/bootstrap", []byte("b")); err != nil {
		t.Fatalf("kvB.Set: %v", err)
	}

	gotA, found, err := kvA.Get(ctx, "sbs/cluster/bootstrap")
	if err != nil {
		t.Fatalf("kvA.Get: %v", err)
	}
	if !found || string(gotA) != "a" {
		t.Fatalf("kvA.Get=%q found=%v, want a/true", gotA, found)
	}
	gotB, found, err := kvB.Get(ctx, "sbs/cluster/bootstrap")
	if err != nil {
		t.Fatalf("kvB.Get: %v", err)
	}
	if !found || string(gotB) != "b" {
		t.Fatalf("kvB.Get=%q found=%v, want b/true", gotB, found)
	}
	if _, found, err := base.Get(ctx, "sbs/cluster/bootstrap"); err != nil || found {
		t.Fatalf("base unprefixed Get found=%v err=%v, want false/nil", found, err)
	}
}

func TestPrefixedKVListReturnsUnprefixedKeysAndCursor(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := newPrefixedKV(base, "phasef-list")

	for _, key := range []string{
		"sbs/cluster/nodes/a",
		"sbs/cluster/nodes/b",
		"sbs/cluster/volumes/a",
	} {
		if err := kv.Set(ctx, key, []byte(key)); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	keys, cursor, err := kv.List(ctx, "sbs/cluster/nodes/", "", 1)
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if len(keys) != 1 || keys[0] != "sbs/cluster/nodes/a" || cursor != "sbs/cluster/nodes/a" {
		t.Fatalf("first page keys=%v cursor=%q", keys, cursor)
	}
	keys, cursor, err = kv.List(ctx, "sbs/cluster/nodes/", cursor, 1)
	if err != nil {
		t.Fatalf("List second page: %v", err)
	}
	if len(keys) != 1 || keys[0] != "sbs/cluster/nodes/b" || cursor != "" {
		t.Fatalf("second page keys=%v cursor=%q", keys, cursor)
	}
}

func TestPrefixedKVTransactionPrefixesKeys(t *testing.T) {
	ctx := context.Background()
	base := newFakeTransactionalKV()
	kv := newPrefixedKV(base, "phasef-tx")

	if err := RunInTransaction(ctx, kv, func(tx ReadWriter) error {
		return tx.Set(ctx, "sbs/cluster/bootstrap", []byte("tx"))
	}); err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}

	got, found, err := base.Get(ctx, "keyspaces/phasef-tx/sbs/cluster/bootstrap")
	if err != nil {
		t.Fatalf("base.Get prefixed: %v", err)
	}
	if !found || string(got) != "tx" {
		t.Fatalf("base.Get prefixed=%q found=%v, want tx/true", got, found)
	}
}

func TestPrefixedKVBatchGetPrefixesKeysAndReturnsUnprefixedResults(t *testing.T) {
	ctx := context.Background()
	base := newBatchablePrefixKV()
	if err := base.Set(ctx, "keyspaces/phasef-batch/sbs/cluster/bootstrap", []byte("batch")); err != nil {
		t.Fatalf("base.Set: %v", err)
	}
	kv := newPrefixedKV(base, "phasef-batch")
	batcher, ok := kv.(kvBatchReader)
	if !ok {
		t.Fatalf("prefixed kv does not expose kvBatchReader")
	}

	values, err := batcher.BatchGet(ctx, []string{
		"sbs/cluster/bootstrap",
		"sbs/cluster/missing",
		"sbs/cluster/bootstrap",
	})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if base.batchGetCalls != 1 || base.getCalls != 0 {
		t.Fatalf("batchGetCalls=%d getCalls=%d, want batch=1 get=0", base.batchGetCalls, base.getCalls)
	}
	wantKeys := []string{
		"keyspaces/phasef-batch/sbs/cluster/bootstrap",
		"keyspaces/phasef-batch/sbs/cluster/missing",
	}
	if !equalStrings(base.lastBatchKeys, wantKeys) {
		t.Fatalf("batch keys=%v want %v", base.lastBatchKeys, wantKeys)
	}
	got, found := values["sbs/cluster/bootstrap"]
	if !found || string(got) != "batch" {
		t.Fatalf("BatchGet bootstrap=%q found=%v, want batch/true", got, found)
	}
	if _, found := values["sbs/cluster/missing"]; found {
		t.Fatalf("BatchGet returned missing key")
	}
	if _, found := values["keyspaces/phasef-batch/sbs/cluster/bootstrap"]; found {
		t.Fatalf("BatchGet returned prefixed key to caller")
	}
}

func TestPrefixedKVTransactionBatchGetPreservesBackendBatcher(t *testing.T) {
	ctx := context.Background()
	base := newBatchablePrefixKV()
	if err := base.Set(ctx, "keyspaces/phasef-tx-batch/sbs/cluster/bootstrap", []byte("tx-batch")); err != nil {
		t.Fatalf("base.Set: %v", err)
	}
	kv := newPrefixedKV(base, "phasef-tx-batch")
	runner, ok := kv.(transactionalKV)
	if !ok {
		t.Fatalf("prefixed kv does not expose transactionalKV")
	}

	err := runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		batcher, ok := tx.(kvBatchReader)
		if !ok {
			t.Fatalf("prefixed transaction does not expose kvBatchReader")
		}
		values, err := batcher.BatchGet(ctx, []string{
			"sbs/cluster/bootstrap",
			"sbs/cluster/missing",
		})
		if err != nil {
			return err
		}
		got, found := values["sbs/cluster/bootstrap"]
		if !found || string(got) != "tx-batch" {
			t.Fatalf("BatchGet bootstrap=%q found=%v, want tx-batch/true", got, found)
		}
		if _, found := values["sbs/cluster/missing"]; found {
			t.Fatalf("BatchGet returned missing key")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}
	if base.txBatchGetCalls != 1 || base.txGetCalls != 0 {
		t.Fatalf("txBatchGetCalls=%d txGetCalls=%d, want batch=1 get=0", base.txBatchGetCalls, base.txGetCalls)
	}
	wantKeys := []string{
		"keyspaces/phasef-tx-batch/sbs/cluster/bootstrap",
		"keyspaces/phasef-tx-batch/sbs/cluster/missing",
	}
	if !equalStrings(base.lastTxBatchKeys, wantKeys) {
		t.Fatalf("tx batch keys=%v want %v", base.lastTxBatchKeys, wantKeys)
	}
}

func TestPrefixedKVReadSnapshotTranslatesKeysAndCursor(t *testing.T) {
	ctx := context.Background()
	base := &snapshotPrefixKV{batchablePrefixKV: newBatchablePrefixKV()}
	for _, key := range []string{
		"keyspaces/phaseaa-snapshot/sbs/cluster/projection/nodes/node-a",
		"keyspaces/phaseaa-snapshot/sbs/cluster/projection/nodes/node-b",
	} {
		if err := base.Set(ctx, key, []byte(key)); err != nil {
			t.Fatalf("base.Set(%s): %v", key, err)
		}
	}
	kv := newPrefixedKV(base, "phaseaa-snapshot")
	runner, ok := kv.(consistentSnapshotKV)
	if !ok {
		t.Fatalf("prefixed kv does not expose consistentSnapshotKV")
	}

	err := runner.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
		keys, next, err := snapshot.List(ctx, "sbs/cluster/projection/nodes/", "", 1)
		if err != nil {
			return err
		}
		if len(keys) != 1 || keys[0] != "sbs/cluster/projection/nodes/node-a" || next != keys[0] {
			t.Fatalf("snapshot.List keys=%v next=%q", keys, next)
		}
		values, err := snapshot.BatchGet(ctx, keys)
		if err != nil {
			return err
		}
		if _, found := values[keys[0]]; !found {
			t.Fatalf("snapshot.BatchGet did not return logical key %q", keys[0])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInReadSnapshot: %v", err)
	}
	if base.snapshotCalls != 1 {
		t.Fatalf("snapshot calls=%d want 1", base.snapshotCalls)
	}
}

type batchablePrefixKV struct {
	values          map[string][]byte
	getCalls        int
	batchGetCalls   int
	lastBatchKeys   []string
	txGetCalls      int
	txBatchGetCalls int
	lastTxBatchKeys []string
}

type snapshotPrefixKV struct {
	*batchablePrefixKV
	snapshotCalls int
}

type prefixReadSnapshot struct {
	base *batchablePrefixKV
}

func newBatchablePrefixKV() *batchablePrefixKV {
	return &batchablePrefixKV{values: make(map[string][]byte)}
}

func (kv *batchablePrefixKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	kv.getCalls++
	return kv.get(key)
}

func (kv *batchablePrefixKV) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	kv.batchGetCalls++
	kv.lastBatchKeys = append([]string(nil), keys...)
	return kv.batchGet(keys), nil
}

func (kv *batchablePrefixKV) Set(_ context.Context, key string, value []byte) error {
	kv.values[key] = append([]byte(nil), value...)
	return nil
}

func (kv *batchablePrefixKV) Delete(_ context.Context, key string) error {
	delete(kv.values, key)
	return nil
}

func (kv *batchablePrefixKV) List(context.Context, string, string, int) ([]string, string, error) {
	return nil, "", nil
}

func (kv *batchablePrefixKV) RunInTransaction(_ context.Context, fn func(tx kvReadWriter) error) error {
	return fn(batchablePrefixTxn{base: kv})
}

func (kv *snapshotPrefixKV) RunInReadSnapshot(_ context.Context, fn func(snapshot kvReadSnapshot) error) error {
	kv.snapshotCalls++
	return fn(prefixReadSnapshot{base: kv.batchablePrefixKV})
}

func (kv *batchablePrefixKV) get(key string) ([]byte, bool, error) {
	value, ok := kv.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (kv *batchablePrefixKV) batchGet(keys []string) map[string][]byte {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := kv.values[key]; ok {
			out[key] = append([]byte(nil), value...)
		}
	}
	return out
}

func (snapshot prefixReadSnapshot) Get(_ context.Context, key string) ([]byte, bool, error) {
	return snapshot.base.get(key)
}

func (snapshot prefixReadSnapshot) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	return snapshot.base.batchGet(keys), nil
}

func (snapshot prefixReadSnapshot) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	keys := make([]string, 0)
	for key := range snapshot.base.values {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
		return keys, keys[len(keys)-1], nil
	}
	return keys, "", nil
}

type batchablePrefixTxn struct {
	base *batchablePrefixKV
}

func (tx batchablePrefixTxn) Get(_ context.Context, key string) ([]byte, bool, error) {
	tx.base.txGetCalls++
	return tx.base.get(key)
}

func (tx batchablePrefixTxn) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	tx.base.txBatchGetCalls++
	tx.base.lastTxBatchKeys = append([]string(nil), keys...)
	return tx.base.batchGet(keys), nil
}

func (tx batchablePrefixTxn) Set(_ context.Context, key string, value []byte) error {
	tx.base.values[key] = append([]byte(nil), value...)
	return nil
}

func (tx batchablePrefixTxn) Delete(_ context.Context, key string) error {
	delete(tx.base.values, key)
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
