package control

import (
	"strings"
	"testing"
)

func TestNewAdminEndpointPlacementApplyAdapterRequiresEndpoint(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointPlacementApplyAdapter("")
	if err == nil {
		t.Fatal("expected error")
	}
	if adapter != nil || cleanup != nil {
		t.Fatalf("adapter=%T cleanup=%T, want nil/nil", adapter, cleanup)
	}
	if !strings.Contains(err.Error(), "placement apply adapter requires reachable --sbs-admin-endpoint") {
		t.Fatalf("error=%v", err)
	}
}

func TestNewAdminEndpointPlacementApplyAdapterCreatesAdapter(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointPlacementApplyAdapter("passthrough:///placement-apply-test")
	if err != nil {
		t.Fatalf("NewAdminEndpointPlacementApplyAdapter: %v", err)
	}
	defer cleanup()
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
}

func TestNewAdminEndpointWriteSessionAdapterRequiresEndpoint(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointWriteSessionAdapter("")
	if err == nil {
		t.Fatal("expected error")
	}
	if adapter != nil || cleanup != nil {
		t.Fatalf("adapter=%T cleanup=%T, want nil/nil", adapter, cleanup)
	}
	if !strings.Contains(err.Error(), "write session committer requires reachable --sbs-admin-endpoint") {
		t.Fatalf("error=%v", err)
	}
}

func TestNewAdminEndpointWriteSessionAdapterCreatesAdapter(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointWriteSessionAdapter("passthrough:///write-session-test")
	if err != nil {
		t.Fatalf("NewAdminEndpointWriteSessionAdapter: %v", err)
	}
	defer cleanup()
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
}

func TestNewAdminEndpointChunkIDAllocatorRequiresEndpoint(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointChunkIDAllocator("")
	if err == nil {
		t.Fatal("expected error")
	}
	if adapter != nil || cleanup != nil {
		t.Fatalf("adapter=%T cleanup=%T, want nil/nil", adapter, cleanup)
	}
	if !strings.Contains(err.Error(), "chunk id allocator requires reachable --sbs-admin-endpoint") {
		t.Fatalf("error=%v", err)
	}
}

func TestNewAdminEndpointChunkIDAllocatorCreatesAdapter(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointChunkIDAllocator("passthrough:///chunk-id-test")
	if err != nil {
		t.Fatalf("NewAdminEndpointChunkIDAllocator: %v", err)
	}
	defer cleanup()
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
}

func TestNewAdminEndpointPhysicalChunkIDAllocatorCreatesAdapter(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointPhysicalChunkIDAllocator("passthrough:///physical-chunk-id-test")
	if err != nil {
		t.Fatalf("NewAdminEndpointPhysicalChunkIDAllocator: %v", err)
	}
	defer cleanup()
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
}

func TestNewAdminEndpointPlacementResolverRequiresEndpoint(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointPlacementResolver("")
	if err == nil {
		t.Fatal("expected error")
	}
	if adapter != nil || cleanup != nil {
		t.Fatalf("adapter=%T cleanup=%T, want nil/nil", adapter, cleanup)
	}
	if !strings.Contains(err.Error(), "placement resolver requires reachable --sbs-admin-endpoint") {
		t.Fatalf("error=%v", err)
	}
}

func TestNewAdminEndpointPlacementResolverCreatesAdapter(t *testing.T) {
	adapter, cleanup, err := NewAdminEndpointPlacementResolver("passthrough:///placement-resolver-test")
	if err != nil {
		t.Fatalf("NewAdminEndpointPlacementResolver: %v", err)
	}
	defer cleanup()
	if adapter == nil {
		t.Fatal("adapter is nil")
	}
}
