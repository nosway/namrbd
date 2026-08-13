package replication

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakePlacementResolver struct {
	resolved    []metadata.ResolvedExtentPlacement
	allocations []metadata.ResolvedAllocationPage
	err         error
}

func (f fakePlacementResolver) ResolveExtentPlacements(_ context.Context, _ string, _, _ uint64) ([]metadata.ResolvedExtentPlacement, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.resolved, nil
}

func (f fakePlacementResolver) ResolveAllocationPages(_ context.Context, _ string, _, _ uint64, _, _ uint32) ([]metadata.ResolvedAllocationPage, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.allocations, nil
}

func TestCoordinatorPlanWriteUsesExtentPlacementPrimaryAndQuorum(t *testing.T) {
	resolver := fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   4 << 20,
					PlacementRef:  "pl-1",
					Revision:      9,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-1",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-1",
					Epoch:            3,
					PrimaryReplicaID: "rep-a",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "zone-a"},
						{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "zone-b"},
						{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "zone-c"},
					},
				},
			},
		},
	}
	coordinator := NewCoordinator(resolver)

	plan, err := coordinator.PlanWrite(context.Background(), "00a1b2c3", 0, 4096, 0, 0)
	if err != nil {
		t.Fatalf("PlanWrite: %v", err)
	}
	if len(plan.Extents) != 1 {
		t.Fatalf("PlanWrite extents=%d want=1", len(plan.Extents))
	}
	got := plan.Extents[0]
	if got.Primary.ReplicaID != "rep-a" {
		t.Fatalf("primary=%q want=rep-a", got.Primary.ReplicaID)
	}
	if got.RequiredAcks != 2 {
		t.Fatalf("required acks=%d want=2", got.RequiredAcks)
	}
	if len(got.WriteTargets) != 3 {
		t.Fatalf("write targets=%d want=3", len(got.WriteTargets))
	}
}

func TestCoordinatorPlanWriteWithStatsReportsSubphases(t *testing.T) {
	resolver := fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   4 << 20,
					PlacementRef:  "pl-1",
					Revision:      9,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-1",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-1",
					Epoch:            3,
					PrimaryReplicaID: "rep-a",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "zone-a"},
						{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "zone-b"},
						{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "zone-c"},
					},
				},
			},
		},
		allocations: []metadata.ResolvedAllocationPage{
			{
				Page: metadata.AllocationPageRecord{
					VolumeID:       "00a1b2c3",
					PageNo:         0,
					PageBytes:      4096,
					ChunkSizeBytes: 4096,
				},
				RangeStartChunk: 0,
				RangeEndChunk:   1,
				CoversWholePage: true,
			},
		},
	}
	coordinator := NewCoordinator(resolver, resolver)

	plan, stats, err := coordinator.PlanWriteWithStats(context.Background(), "00a1b2c3", 0, 4096, 4096, 4096)
	if err != nil {
		t.Fatalf("PlanWriteWithStats: %v", err)
	}
	if len(plan.Extents) != 1 {
		t.Fatalf("PlanWriteWithStats extents=%d want=1", len(plan.Extents))
	}
	if stats.ResolvedPlacementCount != 1 {
		t.Fatalf("resolved placement count=%d want=1", stats.ResolvedPlacementCount)
	}
	if stats.ResolvedAllocationPageCount != 1 {
		t.Fatalf("resolved allocation page count=%d want=1", stats.ResolvedAllocationPageCount)
	}
	if stats.CopyOnWrite {
		t.Fatal("copy-on-write=true want false")
	}
	if stats.ResolvePlacementsDuration < 0 || stats.ResolveAllocationsDuration < 0 ||
		stats.SourceCOWDuration < 0 || stats.BuildTargetsDuration < 0 {
		t.Fatalf("negative plan stats durations: %+v", stats)
	}
}

func TestCoordinatorPlanReadUsesPrimaryPreferredAndFallbacks(t *testing.T) {
	coordinator := NewCoordinator(fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      2,
					LogicalOffset: 4 << 20,
					LengthBytes:   4 << 20,
					PlacementRef:  "pl-2",
					Revision:      11,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-2",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-2",
					Epoch:            4,
					PrimaryReplicaID: "rep-x",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-x", ReplicaID: "rep-x", Role: metadata.ReplicaRolePrimary},
						{NodeID: "node-y", ReplicaID: "rep-y", Role: metadata.ReplicaRoleSecondary},
						{NodeID: "node-z", ReplicaID: "rep-z", Role: metadata.ReplicaRoleSecondary},
					},
				},
			},
		},
	})

	plan, err := coordinator.PlanRead(context.Background(), "00a1b2c3", 4<<20, 4096, 0, 0)
	if err != nil {
		t.Fatalf("PlanRead: %v", err)
	}
	if len(plan.Extents) != 1 {
		t.Fatalf("PlanRead extents=%d want=1", len(plan.Extents))
	}
	got := plan.Extents[0]
	if got.Preferred.ReplicaID != "rep-x" {
		t.Fatalf("preferred=%q want=rep-x", got.Preferred.ReplicaID)
	}
	if len(got.Fallbacks) != 2 {
		t.Fatalf("fallbacks=%d want=2", len(got.Fallbacks))
	}
}

func TestCoordinatorPlanWriteFailsWithoutPrimary(t *testing.T) {
	coordinator := NewCoordinator(fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:     "00a1b2c3",
					ExtentID:     1,
					PlacementRef: "pl-1",
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID: "rs-1",
					VolumeID:     "00a1b2c3",
					PlacementRef: "pl-1",
					WriteQuorum:  2,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
					},
				},
			},
		},
	})

	if _, err := coordinator.PlanWrite(context.Background(), "00a1b2c3", 0, 4096, 0, 0); err == nil {
		t.Fatal("PlanWrite expected error, got nil")
	}
}

func TestCoordinatorPlanWriteSkipsDownReplica(t *testing.T) {
	coordinator := NewCoordinator(fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   4 << 20,
					PlacementRef:  "pl-1",
					Revision:      9,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-1",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-1",
					Epoch:            3,
					PrimaryReplicaID: "rep-a",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
						{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
						{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
					},
				},
				Nodes: map[string]metadata.NodeMembershipRecord{
					"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
					"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown},
					"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
				},
			},
		},
	})

	plan, err := coordinator.PlanWrite(context.Background(), "00a1b2c3", 0, 4096, 0, 0)
	if err != nil {
		t.Fatalf("PlanWrite: %v", err)
	}
	got := plan.Extents[0]
	if len(got.WriteTargets) != 2 {
		t.Fatalf("write targets=%d want=2: %+v", len(got.WriteTargets), got.WriteTargets)
	}
	for _, target := range got.WriteTargets {
		if target.NodeID == "node-b" {
			t.Fatalf("down node included in write targets: %+v", got.WriteTargets)
		}
	}
}

func TestCoordinatorPlanReadUsesHealthyFallbackWhenPrimaryDown(t *testing.T) {
	coordinator := NewCoordinator(fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   4 << 20,
					PlacementRef:  "pl-1",
					Revision:      9,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-1",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-1",
					Epoch:            3,
					PrimaryReplicaID: "rep-a",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
						{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
						{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
					},
				},
				Nodes: map[string]metadata.NodeMembershipRecord{
					"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown},
					"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
					"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
				},
			},
		},
	})

	plan, err := coordinator.PlanRead(context.Background(), "00a1b2c3", 0, 4096, 0, 0)
	if err != nil {
		t.Fatalf("PlanRead: %v", err)
	}
	if got := plan.Extents[0].Preferred.ReplicaID; got != "rep-b" {
		t.Fatalf("preferred=%q want rep-b", got)
	}
}

func TestCoordinatorPlanReadIncludesOverlappingAllocationPages(t *testing.T) {
	resolver := fakePlacementResolver{
		resolved: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   8 << 10,
					PlacementRef:  "pl-1",
					Revision:      9,
				},
				ReplicaSet: metadata.ReplicaSetState{
					ReplicaSetID:     "rs-1",
					VolumeID:         "00a1b2c3",
					PlacementRef:     "pl-1",
					Epoch:            3,
					PrimaryReplicaID: "rep-a",
					WriteQuorum:      2,
					ReadQuorum:       1,
					Replicas: []metadata.ReplicaDescriptor{
						{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
					},
				},
			},
		},
		allocations: []metadata.ResolvedAllocationPage{
			{
				Page: metadata.AllocationPageRecord{
					VolumeID:       "00a1b2c3",
					PageNo:         0,
					PageBytes:      8 << 10,
					ChunkSizeBytes: 4 << 10,
					Extents: []metadata.AllocationExtentRecord{
						{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
					},
				},
				RangeStartChunk: 0,
				RangeEndChunk:   2,
				CoversWholePage: true,
			},
		},
	}
	coordinator := NewCoordinator(resolver, resolver)

	plan, err := coordinator.PlanRead(context.Background(), "00a1b2c3", 0, 8<<10, 8<<10, 4<<10)
	if err != nil {
		t.Fatalf("PlanRead: %v", err)
	}
	if len(plan.Extents) != 1 || len(plan.Extents[0].AllocationPages) != 1 {
		t.Fatalf("unexpected allocation pages in plan: %+v", plan.Extents)
	}
	if plan.Extents[0].ChunkSizeBytes != 4<<10 {
		t.Fatalf("chunk size=%d want=%d", plan.Extents[0].ChunkSizeBytes, 4<<10)
	}
}
