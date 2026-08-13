package control

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestControllerSetNodeHealthTriggersRepairScan(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-a): %v", err)
	}
	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-b",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-b): %v", err)
	}
	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-c",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-c): %v", err)
	}
	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-d",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-d): %v", err)
	}
	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-e",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-e): %v", err)
	}
	if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:            "node-f",
		LifecycleState:    metadata.NodeLifecycleActive,
		HealthState:       metadata.NodeHealthHealthy,
		LastHeartbeatUnix: 100,
	}); err != nil {
		t.Fatalf("PutNodeMembership(node-f): %v", err)
	}

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             1,
		Revision:          1,
		PlacementPolicyID: "test",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       1,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            1,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet(rs-1): %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            1,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet(rs-2): %v", err)
	}

	controller := NewFromRepository(repo)
	controller.now = func() time.Time { return time.Unix(200, 0) }

	rec, failovers, enqueued, err := controller.SetNodeHealth(ctx, "node-c", metadata.NodeHealthDown)
	if err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if failovers != 0 {
		t.Fatalf("failovers=%d want=0", failovers)
	}
	if rec.HealthState != metadata.NodeHealthDown {
		t.Fatalf("health=%q want=%q", rec.HealthState, metadata.NodeHealthDown)
	}
	if rec.LastHeartbeatUnix != 200 {
		t.Fatalf("heartbeat=%d want=200", rec.LastHeartbeatUnix)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}

	transition, err := repo.GetPlacementTransition(ctx, "00a1b2c3", "pl-1")
	if err != nil {
		t.Fatalf("GetPlacementTransition: %v", err)
	}
	if transition.State != metadata.PlacementTransitionQueued || transition.TargetReplicaSetID != "rs-1-repair-node-c" {
		t.Fatalf("transition=%+v", transition)
	}
}

func TestControllerGetMetrics(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	_ = repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:         "node-a",
		LifecycleState: metadata.NodeLifecycleActive,
		HealthState:    metadata.NodeHealthHealthy,
	})
	_ = repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
		NodeID:         "node-b",
		LifecycleState: metadata.NodeLifecycleDraining,
		HealthState:    metadata.NodeHealthDown,
	})
	_ = repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Status:   metadata.VolumeStatusDegraded,
	})
	_ = repo.PutPlacementTransition(ctx, metadata.PlacementTransitionRecord{
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		State:        metadata.PlacementTransitionQueued,
		Reason:       "repair",
	})
	_ = repo.PutPlacementTransition(ctx, metadata.PlacementTransitionRecord{
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-2",
		State:        metadata.PlacementTransitionRunning,
		Reason:       "rebalance",
	})

	controller := NewFromRepository(repo)
	metrics, err := controller.GetMetrics(ctx)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if metrics.Volumes["total"] != 1 || metrics.Volumes["degraded"] != 1 {
		t.Fatalf("unexpected volume metrics: %+v", metrics.Volumes)
	}
	if metrics.Nodes["total"] != 2 || metrics.Nodes["healthy"] != 1 || metrics.Nodes["down"] != 1 || metrics.Nodes["draining"] != 1 {
		t.Fatalf("unexpected node metrics: %+v", metrics.Nodes)
	}
	if metrics.Backlog["queued"] != 1 || metrics.Backlog["running"] != 1 || metrics.Backlog["repair_like"] != 1 || metrics.Backlog["rebalance"] != 1 {
		t.Fatalf("unexpected backlog metrics: %+v", metrics.Backlog)
	}
}
