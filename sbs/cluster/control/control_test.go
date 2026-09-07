package control

import (
	"context"
	"errors"
	"fmt"
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

func TestControllerReconcileNodeHealthTransitionsForNodePagesAndResumes(t *testing.T) {
	kv, err := metadata.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()
	repo := metadata.NewRepository(kv, "phase-ad-health-affected")
	ctx := context.Background()
	for i, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"} {
		if err := repo.PutNodeMembership(ctx, metadata.NodeMembershipRecord{
			NodeID: nodeID, Zone: fmt.Sprintf("zone-%d", i), Host: fmt.Sprintf("host-%d", i),
			LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	putVolume := func(volumeID string) {
		t.Helper()
		if err := repo.PutVolumeState(ctx, metadata.VolumeState{
			VolumeID: volumeID, Epoch: 1, Revision: 1, PlacementPolicyID: "test",
			ProtectionPolicy: "rf3", Status: metadata.VolumeStatusHealthy,
		}); err != nil {
			t.Fatalf("PutVolumeState(%s): %v", volumeID, err)
		}
	}
	putPlacement := func(volumeID, placementRef, replicaSetID, primary string, extentID uint64, nodes ...string) {
		t.Helper()
		if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
			VolumeID: volumeID, ExtentID: extentID, LogicalOffset: (extentID - 1) * 8,
			LengthBytes: 8, ChunkID: extentID, PlacementRef: placementRef, Revision: 1,
		}); err != nil {
			t.Fatalf("PutExtentMapping(%s/%d): %v", volumeID, extentID, err)
		}
		replicas := make([]metadata.ReplicaDescriptor, 0, len(nodes))
		for _, nodeID := range nodes {
			role := metadata.ReplicaRoleSecondary
			if nodeID == primary {
				role = metadata.ReplicaRolePrimary
			}
			replicas = append(replicas, metadata.ReplicaDescriptor{NodeID: nodeID, ReplicaID: "rep-" + placementRef + "-" + nodeID, Role: role})
		}
		if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
			ReplicaSetID: replicaSetID, VolumeID: volumeID, PlacementRef: placementRef,
			Epoch: 1, PrimaryReplicaID: "rep-" + placementRef + "-" + primary,
			WriteQuorum: 2, ReadQuorum: 1, Replicas: replicas,
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", replicaSetID, err)
		}
	}
	putVolume("00a1b2c3")
	putPlacement("00a1b2c3", "pl-1", "rs-1", "node-c", 1, "node-a", "node-b", "node-c")
	putPlacement("00a1b2c3", "pl-2", "rs-2", "node-a", 2, "node-a", "node-b", "node-c")
	putVolume("00a1b2c4")
	putPlacement("00a1b2c4", "pl-unrelated", "rs-unrelated", "node-d", 1, "node-d", "node-e", "node-f")
	promoteMaintenanceIndexForControlTest(t, ctx, repo, "epoch-health-affected")

	controller := NewFromRepository(repo)
	controller.now = func() time.Time { return time.Unix(300, 0) }
	node, err := controller.SetNodeHealthOnly(ctx, "node-c", metadata.NodeHealthDown)
	if err != nil || node.MembershipRevision == 0 {
		t.Fatalf("SetNodeHealthOnly node=%+v err=%v", node, err)
	}
	inputCount := 0
	failovers := 0
	enqueued := 0
	completed := false
	for attempt := 0; attempt < 5; attempt++ {
		controller = NewFromRepository(repo)
		page, err := controller.ReconcileNodeHealthTransitionsForNode(ctx, "node-c", 1)
		if err != nil {
			t.Fatalf("affected page %d: %v", attempt, err)
		}
		if page.InputCount > 1 || page.ProcessedCount > 1 || page.RangePageCount > 1 || page.BackendFullScanCount != 0 || page.FullCompletionCount != 0 || page.NestedCompletionCount != 0 {
			t.Fatalf("unbounded affected page=%+v", page)
		}
		inputCount += page.InputCount
		failovers += page.PrimaryFailoverCount
		enqueued += page.RepairEnqueuedCount
		if page.Completed {
			completed = true
			break
		}
	}
	if !completed || inputCount != 2 || failovers != 1 || enqueued != 2 {
		t.Fatalf("completed=%t input=%d failovers=%d enqueued=%d", completed, inputCount, failovers, enqueued)
	}
	for _, placementRef := range []string{"pl-1", "pl-2"} {
		transition, err := repo.GetPlacementTransition(ctx, "00a1b2c3", placementRef)
		if err != nil || transition.State != metadata.PlacementTransitionQueued {
			t.Fatalf("transition(%s)=%+v err=%v", placementRef, transition, err)
		}
	}
	if _, err := repo.GetPlacementTransition(ctx, "00a1b2c4", "pl-unrelated"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("unrelated transition error=%v", err)
	}
	unrelated, err := repo.GetVolumeState(ctx, "00a1b2c4")
	if err != nil || unrelated.Status != metadata.VolumeStatusHealthy || unrelated.Epoch != 1 {
		t.Fatalf("unrelated volume=%+v err=%v", unrelated, err)
	}
	replay, err := NewFromRepository(repo).ReconcileNodeHealthTransitionsForNode(ctx, "node-c", 1)
	if err != nil || !replay.Completed || replay.InputCount != 0 || replay.RepairEnqueuedCount != 0 || replay.PrimaryFailoverCount != 0 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func promoteMaintenanceIndexForControlTest(t *testing.T, ctx context.Context, repo *metadata.Repository, epoch string) {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		page, err := repo.RunMaintenanceIndexRebuildPage(ctx, epoch, 16, 128)
		if err != nil {
			t.Fatalf("RunMaintenanceIndexRebuildPage(%d): %v", attempt, err)
		}
		if page.Completed {
			if _, err := repo.PromoteMaintenanceIndexRebuild(ctx, epoch); err != nil {
				t.Fatalf("PromoteMaintenanceIndexRebuild: %v", err)
			}
			return
		}
	}
	t.Fatal("maintenance index rebuild did not complete")
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
