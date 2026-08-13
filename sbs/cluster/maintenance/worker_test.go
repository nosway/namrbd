package maintenance

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/replication"
)

func addNodeClientAliases(clients map[string]service.SBSClient, aliases map[string]string) {
	for nodeID, replicaID := range aliases {
		clients[nodeID] = clients[replicaID]
	}
}

func TestWorkerRunOnceAppliesQueuedTransition(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c", "node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f"})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}
	sourceReplicas := map[string]service.SBSClient{
		"rep-a": replicaClients["rep-a"],
		"rep-b": replicaClients["rep-b"],
		"rep-c": replicaClients["rep-c"],
	}
	seedPayload(t, store.mappings[0], "00a1b2c3", sourceReplicas)

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
}

func TestWorkerRunOnceAutoEnqueuesAndAppliesRepair(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c", "node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f"})
	seedPayload(t, store.mappings[0], "00a1b2c3", map[string]service.SBSClient{
		"rep-a": replicaClients["rep-a"],
		"rep-b": replicaClients["rep-b"],
		"rep-c": replicaClients["rep-c"],
	})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-1-repair-node-c" {
		t.Fatalf("placement_ref=%q want=pl-1-repair-node-c", store.mappings[0].PlacementRef)
	}
}

func TestWorkerRunOnceFailsoverPrimaryBeforeRepair(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-a"] = metadata.NodeMembershipRecord{NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c", "node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f"})
	seedPayload(t, store.mappings[0], "00a1b2c3", map[string]service.SBSClient{
		"rep-a": replicaClients["rep-a"],
		"rep-b": replicaClients["rep-b"],
		"rep-c": replicaClients["rep-c"],
	})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	if store.replicaSets[0].PrimaryReplicaID != "rep-b" {
		t.Fatalf("primary=%q want=rep-b", store.replicaSets[0].PrimaryReplicaID)
	}
	if store.mappings[0].PlacementRef != "pl-1-repair-node-a" {
		t.Fatalf("placement_ref=%q want=pl-1-repair-node-a", store.mappings[0].PlacementRef)
	}
}

func TestWorkerRunOnceMarksFailureWithoutAutomaticRetry(t *testing.T) {
	store := newFakeStore()
	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-missing"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: map[string]service.SBSClient{},
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !worked {
		t.Fatal("expected worked=true on failure attempt")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionFailed {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionFailed)
	}

	worker.now = func() time.Time { return time.Unix(1002, 0) }
	worked, err = worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce after failure: %v", err)
	}
	if worked {
		t.Fatal("expected worked=false after failed transition was quarantined")
	}
}

func TestWorkerRunOnceReplansRepairTargetPreconditionFailure(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: map[string]service.SBSClient{},
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true on deferred transition attempt")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionQueued {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionQueued)
	}
	if transition.Attempt != 2 {
		t.Fatalf("transition attempt=%d want=2", transition.Attempt)
	}
	if transition.TargetReplicaSetID == "rs-2" {
		t.Fatalf("target_replica_set_id still stale: %q", transition.TargetReplicaSetID)
	}
	if transition.TargetReplicaSetID != "rs-1-repair-node-c" {
		t.Fatalf("target_replica_set_id=%q want=rs-1-repair-node-c", transition.TargetReplicaSetID)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", transition.TargetReplicaSetID)
	if err != nil {
		t.Fatalf("replanned target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-d" {
		t.Fatalf("replacement node=%q want=node-d", target.Replicas[2].NodeID)
	}
}

func TestWorkerRunOnceReplansRepairTargetWhenReplicaSetIDIsUnchanged(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1-repair-node-c",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1-repair-node-c",
		Epoch:            6,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-d", ReplicaID: "rs-1-repair-node-c-rep-03", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.transitions["pl-1"] = metadata.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-1-repair-node-c",
		Reason:              "repair",
		State:               metadata.PlacementTransitionQueued,
		StartedAtUnix:       900,
		LastProgressAtUnix:  900,
		Attempt:             1,
	}
	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: map[string]service.SBSClient{},
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true on replanned transition")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionQueued {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionQueued)
	}
	if transition.TargetReplicaSetID != "rs-1-repair-node-c" {
		t.Fatalf("target_replica_set_id=%q want rs-1-repair-node-c", transition.TargetReplicaSetID)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", transition.TargetReplicaSetID)
	if err != nil {
		t.Fatalf("replanned target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e", target.Replicas[2].NodeID)
	}
}

func TestWorkerRunOnceDoesNotOverwriteCompletedTransitionAfterStaleList(t *testing.T) {
	store := newFakeStore()
	store.transitions["pl-1"] = metadata.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-2",
		Reason:              "repair",
		State:               metadata.PlacementTransitionCompleted,
	}
	store.transitionListOverride = []metadata.PlacementTransitionRecord{
		{
			VolumeID:            "00a1b2c3",
			PlacementRef:        "pl-1",
			CurrentReplicaSetID: "rs-1",
			TargetReplicaSetID:  "rs-2",
			Reason:              "repair",
			State:               metadata.PlacementTransitionQueued,
		},
	}
	store.transitionListOverrideCount = 2
	svc := NewService(store)

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: map[string]service.SBSClient{},
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	if got := store.transitions["pl-1"].State; got != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", got, metadata.PlacementTransitionCompleted)
	}
}

func TestWorkerRunOnceCompletesObsoleteTransitionWithoutMapping(t *testing.T) {
	store := newFakeStore()
	store.mappings[0].PlacementRef = "pl-1-repair-node-c"
	store.replicaSets[0].PlacementRef = "pl-1-repair-node-c"
	store.transitions["pl-1"] = metadata.PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-1",
		CurrentReplicaSetID: "rs-1",
		TargetReplicaSetID:  "rs-2",
		Reason:              "repair",
		State:               metadata.PlacementTransitionQueued,
		StartedAtUnix:       900,
		LastProgressAtUnix:  900,
		Attempt:             1,
	}
	svc := NewService(store)

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: map[string]service.SBSClient{},
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if transition.Attempt != 2 {
		t.Fatalf("transition attempt=%d want=2", transition.Attempt)
	}
	if store.mappings[0].PlacementRef != "pl-1-repair-node-c" {
		t.Fatalf("mapping placement_ref=%q want pl-1-repair-node-c", store.mappings[0].PlacementRef)
	}
}

func TestWorkerRunOncePrioritizesTransitionWithFailedBatch(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-3",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-3",
			Epoch:            5,
			PrimaryReplicaID: "rep-g",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-g", ReplicaID: "rep-g", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-h", ReplicaID: "rep-h", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-i", ReplicaID: "rep-i", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-4",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-4",
			Epoch:            5,
			PrimaryReplicaID: "rep-j",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-j", ReplicaID: "rep-j", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-k", ReplicaID: "rep-k", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-l", ReplicaID: "rep-l", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h", "node-i", "node-j", "node-k", "node-l"} {
		store.nodes[nodeID] = metadata.NodeMembershipRecord{NodeID: nodeID, LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-g": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-h": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-i": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-j": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-k": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-l": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{
		"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c",
		"node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f",
		"node-g": "rep-g", "node-h": "rep-h", "node-i": "rep-i",
		"node-j": "rep-j", "node-k": "rep-k", "node-l": "rep-l",
	})
	seedPayload(t, store.mappings[0], "00a1b2c3", map[string]service.SBSClient{"rep-a": replicaClients["rep-a"], "rep-b": replicaClients["rep-b"], "rep-c": replicaClients["rep-c"]})
	seedPayload(t, store.mappings[1], "00a1b2c3", map[string]service.SBSClient{"rep-d": replicaClients["rep-d"], "rep-e": replicaClients["rep-e"], "rep-f": replicaClients["rep-f"]})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-3"); err != nil {
		t.Fatalf("EnqueueRepair extent1: %v", err)
	}
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 2, "rs-4"); err != nil {
		t.Fatalf("EnqueueRepair extent2: %v", err)
	}
	store.mutationOps["transition-pl-2-page-00000000000000000002"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-2-page-00000000000000000002",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{2},
		StartedAtUnix:     900,
		LastUpdatedAtUnix: 901,
	}

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	if store.transitions["pl-2"].State != metadata.PlacementTransitionCompleted {
		t.Fatalf("pl-2 state=%q want completed", store.transitions["pl-2"].State)
	}
	if store.transitions["pl-1"].State != metadata.PlacementTransitionQueued {
		t.Fatalf("pl-1 state=%q want queued", store.transitions["pl-1"].State)
	}
}

func TestWorkerRunOncePrioritizesTransitionWithRecentBatch(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-3",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-3",
			Epoch:            5,
			PrimaryReplicaID: "rep-g",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-g", ReplicaID: "rep-g", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-h", ReplicaID: "rep-h", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-i", ReplicaID: "rep-i", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-4",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-4",
			Epoch:            5,
			PrimaryReplicaID: "rep-j",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-j", ReplicaID: "rep-j", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-k", ReplicaID: "rep-k", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-l", ReplicaID: "rep-l", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h", "node-i", "node-j", "node-k", "node-l"} {
		store.nodes[nodeID] = metadata.NodeMembershipRecord{NodeID: nodeID, LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-g": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-h": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-i": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-j": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-k": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-l": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{
		"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c",
		"node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f",
		"node-g": "rep-g", "node-h": "rep-h", "node-i": "rep-i",
		"node-j": "rep-j", "node-k": "rep-k", "node-l": "rep-l",
	})
	seedPayload(t, store.mappings[0], "00a1b2c3", map[string]service.SBSClient{"rep-a": replicaClients["rep-a"], "rep-b": replicaClients["rep-b"], "rep-c": replicaClients["rep-c"]})
	seedPayload(t, store.mappings[1], "00a1b2c3", map[string]service.SBSClient{"rep-d": replicaClients["rep-d"], "rep-e": replicaClients["rep-e"], "rep-f": replicaClients["rep-f"]})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-3"); err != nil {
		t.Fatalf("EnqueueRepair extent1: %v", err)
	}
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 2, "rs-4"); err != nil {
		t.Fatalf("EnqueueRepair extent2: %v", err)
	}
	store.mutationOps["write-recent-extent-2"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent-extent-2",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 999,
	}
	store.mutationOps["transition-pl-2-page-00000000000000000001"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-2-page-00000000000000000001",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationRunning,
		IdempotencyKey:    "transition-pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		StartedAtUnix:     995,
		LastUpdatedAtUnix: 998,
	}

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	if store.transitions["pl-2"].State != metadata.PlacementTransitionCompleted {
		t.Fatalf("pl-2 state=%q want completed", store.transitions["pl-2"].State)
	}
	if store.transitions["pl-1"].State != metadata.PlacementTransitionQueued {
		t.Fatalf("pl-1 state=%q want queued", store.transitions["pl-1"].State)
	}
}

func TestWorkerRunOncePrioritizesTransitionWithSmallerRetryWindow(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-3",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-3",
			Epoch:            5,
			PrimaryReplicaID: "rep-g",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-g", ReplicaID: "rep-g", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-h", ReplicaID: "rep-h", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-i", ReplicaID: "rep-i", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-4",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-4",
			Epoch:            5,
			PrimaryReplicaID: "rep-j",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-j", ReplicaID: "rep-j", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-k", ReplicaID: "rep-k", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-l", ReplicaID: "rep-l", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f", "node-g", "node-h", "node-i", "node-j", "node-k", "node-l"} {
		store.nodes[nodeID] = metadata.NodeMembershipRecord{NodeID: nodeID, LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-g": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-h": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-i": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-j": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-k": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-l": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	addNodeClientAliases(replicaClients, map[string]string{
		"node-a": "rep-a", "node-b": "rep-b", "node-c": "rep-c",
		"node-d": "rep-d", "node-e": "rep-e", "node-f": "rep-f",
		"node-g": "rep-g", "node-h": "rep-h", "node-i": "rep-i",
		"node-j": "rep-j", "node-k": "rep-k", "node-l": "rep-l",
	})
	seedPayload(t, store.mappings[0], "00a1b2c3", map[string]service.SBSClient{"rep-a": replicaClients["rep-a"], "rep-b": replicaClients["rep-b"], "rep-c": replicaClients["rep-c"]})
	seedPayload(t, store.mappings[1], "00a1b2c3", map[string]service.SBSClient{"rep-d": replicaClients["rep-d"], "rep-e": replicaClients["rep-e"], "rep-f": replicaClients["rep-f"]})

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-3"); err != nil {
		t.Fatalf("EnqueueRepair extent1: %v", err)
	}
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 2, "rs-4"); err != nil {
		t.Fatalf("EnqueueRepair extent2: %v", err)
	}
	store.mutationOps["transition-pl-1"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationPending,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		RetryPageWindows: []metadata.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 0, EndPageNo: 0, DataBytes: 8, DataChunks: 2},
			{ExtentID: 1, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
	}
	store.mutationOps["transition-pl-2"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-2",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationPending,
		IdempotencyKey:    "pl-2",
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{2},
		RetryPageWindows: []metadata.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 2, EndPageNo: 2, DataBytes: 4, DataChunks: 1},
		},
	}

	worker := NewWorker(svc, WorkerConfig{
		VolumeID:       "00a1b2c3",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		RetryBackoff:   time.Second,
	})
	worker.now = func() time.Time { return time.Unix(1000, 0) }

	worked, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected worked=true")
	}
	if store.transitions["pl-2"].State != metadata.PlacementTransitionCompleted {
		t.Fatalf("pl-2 state=%q want completed", store.transitions["pl-2"].State)
	}
	if store.transitions["pl-1"].State != metadata.PlacementTransitionQueued {
		t.Fatalf("pl-1 state=%q want queued", store.transitions["pl-1"].State)
	}
}

func seedPayload(t *testing.T, mapping metadata.ExtentMappingRecord, volumeID string, replicas map[string]service.SBSClient) {
	t.Helper()
	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), replicas, replication.OpenReplicaSessionsRequest{
		VolumeID:      volumeID,
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed",
		Generation:    1,
		SessionPrefix: "seed",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions: %v", err)
	}
	writer := replication.NewRemoteReplicaWriter(sourceReplicas)
	replicaIDs := make([]string, 0, len(sourceReplicas))
	for replicaID := range sourceReplicas {
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Strings(replicaIDs)
	targets := make([]replication.ReplicaTarget, 0, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		targets = append(targets, replication.ReplicaTarget{ReplicaID: replicaID})
	}
	if _, err := writer.WriteExtent(context.Background(), replication.ExtentWritePlan{
		Extent:       mapping,
		WriteTargets: targets,
	}, replication.ReplicaWriteRequest{
		RequestID:      "seed-1",
		VolumeID:       volumeID,
		AttachmentID:   "seed",
		Generation:     1,
		IdempotencyKey: "seed-1",
		OffsetBytes:    mapping.LogicalOffset,
		LengthBytes:    mapping.LengthBytes,
		Data:           []byte("payload1"),
	}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
}
