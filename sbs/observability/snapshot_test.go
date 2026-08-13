package observability

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewSnapshotAggregatesDefaultSafetyFields(t *testing.T) {
	snap := NewSnapshot(BuildInput{
		GeneratedAt: time.Unix(123, 0),
		ClusterID:   "cluster-a",
		Nodes: []Node{
			{NodeID: "node-a", Lifecycle: "active", Health: "healthy"},
		},
		Capacity: Capacity{TotalBytes: 1024, PhysicalUsedBytes: 256, PhysicalFreeBytes: 768},
		Reclaim:  Reclaim{PendingChunks: 2, PendingBytes: 128},
	})

	if snap.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version=%q want %q", snap.SchemaVersion, SchemaVersion)
	}
	if snap.CollectionStatus != StatusOK {
		t.Fatalf("collection_status=%q want %q", snap.CollectionStatus, StatusOK)
	}
	if !snap.ReadOnlyModeEnforced || !snap.UnsupportedClaimVisible {
		t.Fatalf("read-only and unsupported-claim fields must be visible: %+v", snap)
	}
	if snap.Reclaim.CompletedClaimed {
		t.Fatalf("reclaim completion must not be claimed without before/after evidence")
	}
	if !snap.Reclaim.EvidenceRequired {
		t.Fatalf("reclaim evidence_required=false want true")
	}
	if !snap.Query.Registered || !snap.Query.ReadOnly || snap.Query.RawLogFallback {
		t.Fatalf("query defaults are not safe: %+v", snap.Query)
	}
	if !snap.MCP.ToolRegistered || snap.MCP.MutatingToolsEnabled {
		t.Fatalf("mcp defaults are not observe-first: %+v", snap.MCP)
	}
	if !snap.MCP.ServerReady || !snap.MCP.ProviderReady || snap.MCP.Transport != "stdio-jsonrpc-content-length" {
		t.Fatalf("mcp transport/provider defaults are not ready: %+v", snap.MCP)
	}
}

func TestNewSnapshotPreservesWarningsAndErrors(t *testing.T) {
	warn := NewSnapshot(BuildInput{Warnings: []string{"node health detail missing"}})
	if warn.CollectionStatus != StatusDegraded || warn.WarningCount != 1 {
		t.Fatalf("warning snapshot status=%q warnings=%d", warn.CollectionStatus, warn.WarningCount)
	}

	failed := NewSnapshot(BuildInput{
		FirstError: "list nodes failed",
		LastError:  "list volumes failed",
	})
	if failed.CollectionStatus != StatusError {
		t.Fatalf("error snapshot status=%q want %q", failed.CollectionStatus, StatusError)
	}
	raw, err := json.Marshal(failed)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded["schema_version"] != SchemaVersion {
		t.Fatalf("json schema_version=%v want %s", decoded["schema_version"], SchemaVersion)
	}
	if decoded["first_error"] != "list nodes failed" || decoded["last_error"] != "list volumes failed" {
		t.Fatalf("json errors not preserved: %v", decoded)
	}
}

func TestSnapshotViewCarriesEnvelope(t *testing.T) {
	snap := NewSnapshot(BuildInput{
		SourceAuthority: "sbs-service AdminService",
		Warnings:        []string{"partial"},
	})
	view := snap.View("capacity", map[string]any{"physical_free_bytes": 7})
	if view.SchemaVersion != SchemaVersion {
		t.Fatalf("view schema_version=%q", view.SchemaVersion)
	}
	if view.ViewID != "capacity" || view.SourceAuthority != "sbs-service AdminService" {
		t.Fatalf("view envelope not preserved: %+v", view)
	}
	if view.WarningCount != 1 || !view.ReadOnlyModeEnforced || !view.UnsupportedClaimVisible {
		t.Fatalf("view safety envelope not preserved: %+v", view)
	}
}
