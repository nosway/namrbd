package iscsi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSizeBytes(t *testing.T) {
	tests := map[string]uint64{
		"512":  512,
		"1KiB": 1024,
		"2MiB": 2 * 1024 * 1024,
		"3GiB": 3 * 1024 * 1024 * 1024,
		"4M":   4 * 1024 * 1024,
	}
	for raw, want := range tests {
		got, err := ParseSizeBytes(raw)
		if err != nil {
			t.Fatalf("ParseSizeBytes(%q) failed: %v", raw, err)
		}
		if got != want {
			t.Fatalf("ParseSizeBytes(%q)=%d, want %d", raw, got, want)
		}
	}
}

func TestRunMemorySelfTestWritesArtifacts(t *testing.T) {
	dir := t.TempDir()
	summaryPath := filepath.Join(dir, "summary.json")
	operationsPath := filepath.Join(dir, "operations.jsonl")
	summary, err := RunMemorySelfTest(MemoryOptions{
		Portal:             "127.0.0.1:3260",
		MemoryLUNBytes:     1 * 1024 * 1024,
		ExportID:           "unit",
		SummaryJSONPath:    summaryPath,
		OperationJSONLPath: operationsPath,
	})
	if err != nil {
		t.Fatalf("RunMemorySelfTest failed: %v", err)
	}
	if summary.Result != "ok" {
		t.Fatalf("summary result=%q, want ok", summary.Result)
	}
	if summary.BackendMode != "memory" || summary.MemoryBackendPersistence != "volatile" {
		t.Fatalf("unexpected memory backend summary: %#v", summary)
	}
	if summary.MetadataAuthority != "local_fixture" {
		t.Fatalf("metadata authority=%q, want local_fixture", summary.MetadataAuthority)
	}
	if summary.UNMAPPolicy != "reject_memory_backend" {
		t.Fatalf("unmap policy=%q, want reject_memory_backend", summary.UNMAPPolicy)
	}
	if !summary.ReadbackMatched || summary.OKCount < 4 || summary.ErrorCount != 0 {
		t.Fatalf("unexpected IO result summary: %#v", summary)
	}
	rawSummary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary artifact: %v", err)
	}
	var artifact Summary
	if err := json.Unmarshal(rawSummary, &artifact); err != nil {
		t.Fatalf("summary artifact is not JSON: %v", err)
	}
	if artifact.TargetIQN != DefaultTargetIQN("unit") {
		t.Fatalf("target iqn=%q, want default", artifact.TargetIQN)
	}
	rawOps, err := os.ReadFile(operationsPath)
	if err != nil {
		t.Fatalf("read operations artifact: %v", err)
	}
	if !strings.Contains(string(rawOps), `"operation":"unmap_reject"`) {
		t.Fatalf("operations artifact does not record UNMAP rejection: %s", rawOps)
	}
}

func TestServeGotgtMemoryRejectsWildcardListenByDefault(t *testing.T) {
	summary, err := ServeGotgtMemory(context.Background(), ServeOptions{
		MemoryOptions: MemoryOptions{
			Portal:         "127.0.0.1:3260",
			MemoryLUNBytes: 1 * 1024 * 1024,
			ExportID:       "unit",
		},
	})
	if err == nil {
		t.Fatalf("ServeGotgtMemory unexpectedly allowed default wildcard listener")
	}
	if !strings.Contains(err.Error(), "wildcard") {
		t.Fatalf("ServeGotgtMemory error=%q, want wildcard guard", err)
	}
	if summary.Result != "" {
		t.Fatalf("summary=%#v, want zero summary for preflight rejection", summary)
	}
}
