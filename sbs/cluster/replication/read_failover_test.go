package replication

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestReadServiceFallsBackWhenPrimaryPayloadMissing(t *testing.T) {
	ctx := context.Background()
	repo := metadata.NewRepository(newIntegrationKV(), "phase-e-read")
	_ = repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 5, Revision: 11, Status: metadata.VolumeStatusHealthy})
	_ = repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	})
	_ = repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            5,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
		},
	})

	secondary := store.NewMemoryStore()
	key := localReplicaPayloadKey("rep-b", "00a1b2c3", 1, 101)
	if err := secondary.Put(ctx, key, []byte("fallback")); err != nil {
		t.Fatalf("secondary.Put: %v", err)
	}

	metaSvc := metadata.NewService(repo)
	readSvc := NewReadService(NewCoordinator(metaSvc, metaSvc), NewLocalReplicaReader(map[string]store.ObjectStore{
		"rep-a": store.NewMemoryStore(),
		"rep-b": secondary,
	}))
	resp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:    "00a1b2c3",
		OffsetBytes: 0,
		LengthBytes: 8,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(resp.Data) != "fallback" {
		t.Fatalf("data=%q want=%q", resp.Data, "fallback")
	}
	if len(resp.ReplicaReads) != 1 || resp.ReplicaReads[0] != "rep-b" {
		t.Fatalf("replica reads=%v", resp.ReplicaReads)
	}
}

func TestFailoverServicePromotesNewPrimary(t *testing.T) {
	ctx := context.Background()
	repo := metadata.NewRepository(newIntegrationKV(), "phase-e-failover")
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		Epoch:            5,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
		},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	failover := NewFailoverService(repo)
	volumeState, replicaSet, err := failover.FailoverExtent(ctx, "00a1b2c3", "rs-1", "rep-b")
	if err != nil {
		t.Fatalf("FailoverExtent: %v", err)
	}
	if volumeState.Epoch != 6 {
		t.Fatalf("volume epoch=%d want=6", volumeState.Epoch)
	}
	if replicaSet.Epoch != 6 || replicaSet.PrimaryReplicaID != "rep-b" {
		t.Fatalf("replica set=%+v", replicaSet)
	}
}
