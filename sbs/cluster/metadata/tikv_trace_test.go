package metadata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
)

func TestClassifyTiKVMetadataKeyUsesCoarseClasses(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{
			key:  "keyspaces/phasek18/sbs/cluster/volumes/00a1b2c3/meta/state",
			want: "volume_state",
		},
		{
			key:  "sbs/cluster/volumes/00a1b2c3/allocation/pages/00000000000000000007",
			want: "allocation_page",
		},
		{
			key:  "sbs/cluster/volumes/00a1b2c3/idem/client-write-1",
			want: "idempotency",
		},
		{
			key:  "sbs/cluster/volumes/00a1b2c3/operations/op-1",
			want: "mutation_operation",
		},
		{
			key:  "sbs/cluster/nodes/u01/health/detail",
			want: "node_health",
		},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := classifyTiKVMetadataKey(tc.key); got != tc.want {
				t.Fatalf("classifyTiKVMetadataKey(%q)=%q want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestLogTiKVOperationEmitsCoarseStructuredEvent(t *testing.T) {
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	rawKey := "keyspaces/phasek18/sbs/cluster/volumes/00a1b2c3/allocation/pages/00000000000000000007"
	logTiKVOperation(true, "txn_get", "transaction", rawKey, "ok", time.Now(), nil,
		structuredlog.F("found", true),
	)

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("expected structured TiKV operation log")
	}
	if strings.Contains(line, rawKey) || strings.Contains(line, "00a1b2c3") {
		t.Fatalf("log leaked raw metadata key: %s", line)
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("unmarshal log: %v\n%s", err, line)
	}
	if rec["component"] != "sbs.metadata" || rec["event"] != "tikv_operation_completed" {
		t.Fatalf("unexpected event identity: %+v", rec)
	}
	if rec["operation"] != "txn_get" || rec["scope"] != "transaction" || rec["key_class"] != "allocation_page" {
		t.Fatalf("unexpected operation fields: %+v", rec)
	}
	if _, ok := rec["duration_ms"].(float64); !ok {
		t.Fatalf("duration_ms missing or not numeric: %+v", rec)
	}
}

func TestTiKVTxnBatchGetUsesPointFallbackOnlyForSmallUniqueBatches(t *testing.T) {
	tests := []struct {
		name              string
		keys              []string
		wantUniqueKeys    []string
		wantPointFallback bool
	}{
		{
			name:              "empty",
			wantUniqueKeys:    []string{},
			wantPointFallback: false,
		},
		{
			name:              "single",
			keys:              []string{"sbs/cluster/volumes/00a1/meta/state"},
			wantUniqueKeys:    []string{"sbs/cluster/volumes/00a1/meta/state"},
			wantPointFallback: true,
		},
		{
			name: "two with duplicate",
			keys: []string{
				"sbs/cluster/volumes/00a1/meta/state",
				"sbs/cluster/volumes/00a1/idem/write-1",
				"sbs/cluster/volumes/00a1/meta/state",
			},
			wantUniqueKeys: []string{
				"sbs/cluster/volumes/00a1/meta/state",
				"sbs/cluster/volumes/00a1/idem/write-1",
			},
			wantPointFallback: true,
		},
		{
			name: "three",
			keys: []string{
				"sbs/cluster/volumes/00a1/meta/state",
				"sbs/cluster/volumes/00a1/idem/write-1",
				"sbs/cluster/volumes/00a1/allocation/pages/0000000000000001",
			},
			wantUniqueKeys: []string{
				"sbs/cluster/volumes/00a1/meta/state",
				"sbs/cluster/volumes/00a1/idem/write-1",
				"sbs/cluster/volumes/00a1/allocation/pages/0000000000000001",
			},
			wantPointFallback: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uniqueKeys, rawKeys := uniqueTiKVBatchGetKeys(tc.keys)
			if !equalStrings(uniqueKeys, tc.wantUniqueKeys) {
				t.Fatalf("unique keys=%v want %v", uniqueKeys, tc.wantUniqueKeys)
			}
			if len(rawKeys) != len(tc.wantUniqueKeys) {
				t.Fatalf("raw key count=%d want %d", len(rawKeys), len(tc.wantUniqueKeys))
			}
			for i, rawKey := range rawKeys {
				if string(rawKey) != tc.wantUniqueKeys[i] {
					t.Fatalf("rawKeys[%d]=%q want %q", i, rawKey, tc.wantUniqueKeys[i])
				}
			}
			if got := useTiKVTxnBatchGetPointFallback(len(uniqueKeys)); got != tc.wantPointFallback {
				t.Fatalf("point fallback=%t want %t", got, tc.wantPointFallback)
			}
		})
	}
}

func TestTiKVTxnBatchGetTraceIncludesModeAndUniqueKeyCount(t *testing.T) {
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	logTiKVOperation(true, "txn_batch_get", "transaction", "", "ok", time.Now(), nil,
		structuredlog.F("key_count", 3),
		structuredlog.F("unique_key_count", 2),
		structuredlog.F("found_count", 2),
		structuredlog.F("batch_get_mode", "point_get_fallback"),
	)

	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("unmarshal log: %v\n%s", err, buf.String())
	}
	if rec["operation"] != "txn_batch_get" || rec["batch_get_mode"] != "point_get_fallback" {
		t.Fatalf("batch get trace mode missing: %+v", rec)
	}
	if rec["key_count"] != float64(3) || rec["unique_key_count"] != float64(2) || rec["found_count"] != float64(2) {
		t.Fatalf("batch get trace counts missing: %+v", rec)
	}
}

type fakeTiKVTxnCommitOptionSetter struct {
	asyncCommit    bool
	onePhaseCommit bool
}

func (f *fakeTiKVTxnCommitOptionSetter) SetEnableAsyncCommit(enabled bool) {
	f.asyncCommit = enabled
}

func (f *fakeTiKVTxnCommitOptionSetter) SetEnable1PC(enabled bool) {
	f.onePhaseCommit = enabled
}

func TestTiKVTxnKVConfiguresLowLatencyCommitOptions(t *testing.T) {
	kv := &tiKVTxnKV{
		enableAsyncCommit:    true,
		enableOnePhaseCommit: true,
	}
	var txn fakeTiKVTxnCommitOptionSetter

	kv.configureTxnCommitOptions(&txn)

	if !txn.asyncCommit || !txn.onePhaseCommit {
		t.Fatalf("configured async_commit=%t one_phase_commit=%t, want both true", txn.asyncCommit, txn.onePhaseCommit)
	}
}

func TestTiKVCommitProtocolFieldsExposeConfiguredMode(t *testing.T) {
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	kv := &tiKVTxnKV{
		enableAsyncCommit:    true,
		enableOnePhaseCommit: true,
	}
	logTiKVOperation(true, "transaction", "transaction", "", "ok", time.Now(), nil, kv.commitProtocolFields()...)

	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("unmarshal log: %v\n%s", err, buf.String())
	}
	if rec["async_commit_enabled"] != true || rec["one_phase_commit_enabled"] != true {
		t.Fatalf("commit protocol fields missing: %+v", rec)
	}
}

func TestTiKVTransactionLifecycleFieldsExposeRunnerSplit(t *testing.T) {
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	kv := &tiKVTxnKV{
		enableAsyncCommit:    true,
		enableOnePhaseCommit: true,
	}
	logTiKVOperation(true, "transaction", "transaction", "", "ok", time.Now(), nil,
		kv.transactionLifecycleFields(13*time.Millisecond, 17*time.Millisecond, 3*time.Millisecond)...,
	)

	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("unmarshal log: %v\n%s", err, buf.String())
	}
	if rec["operation"] != "transaction" || rec["scope"] != "transaction" {
		t.Fatalf("unexpected transaction identity: %+v", rec)
	}
	if rec["txn_begin_duration_ms"] != float64(13) ||
		rec["txn_callback_duration_ms"] != float64(17) ||
		rec["txn_commit_duration_ms"] != float64(3) {
		t.Fatalf("transaction lifecycle fields missing: %+v", rec)
	}
	if rec["async_commit_enabled"] != true || rec["one_phase_commit_enabled"] != true {
		t.Fatalf("commit protocol fields missing from lifecycle trace: %+v", rec)
	}
}

func TestLogTiKVOperationCanBeDisabled(t *testing.T) {
	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	logTiKVOperation(false, "get", "standalone", "sbs/cluster/bootstrap", "ok", time.Now(), nil)
	if strings.TrimSpace(buf.String()) != "" {
		t.Fatalf("disabled TiKV operation trace emitted log: %s", buf.String())
	}
}
