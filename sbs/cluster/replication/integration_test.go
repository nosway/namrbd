package replication

import (
	"context"
	"slices"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type integrationKV struct {
	values map[string][]byte
}

func newIntegrationKV() *integrationKV {
	return &integrationKV{values: make(map[string][]byte)}
}

func (f *integrationKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	value, ok := f.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (f *integrationKV) Set(_ context.Context, key string, value []byte) error {
	f.values[key] = append([]byte(nil), value...)
	return nil
}

func (f *integrationKV) Delete(_ context.Context, key string) error {
	delete(f.values, key)
	return nil
}

func (f *integrationKV) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	keys := make([]string, 0)
	for key := range f.values {
		if len(prefix) > 0 && len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	start := 0
	if cursor != "" {
		for i, key := range keys {
			if key > cursor {
				start = i
				break
			}
			start = len(keys)
		}
	}
	if limit <= 0 || start+limit >= len(keys) {
		return keys[start:], "", nil
	}
	out := keys[start : start+limit]
	return out, out[len(out)-1], nil
}

func TestWriteServiceWithMetadataRepositoryAndService(t *testing.T) {
	ctx := context.Background()
	repo := metadata.NewRepository(newIntegrationKV(), "phase-e-int")
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          11,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3-primary",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, mapping := range []metadata.ExtentMappingRecord{
		{
			VolumeID:      "00a1b2c3",
			ExtentID:      1,
			LogicalOffset: 0,
			LengthBytes:   4 << 20,
			ChunkID:       101,
			PlacementRef:  "pl-1",
			Revision:      11,
		},
		{
			VolumeID:      "00a1b2c3",
			ExtentID:      2,
			LogicalOffset: 4 << 20,
			LengthBytes:   4 << 20,
			ChunkID:       102,
			PlacementRef:  "pl-2",
			Revision:      11,
		},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}
	for _, replicaSet := range []metadata.ReplicaSetState{
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
			PrimaryReplicaID: "rep-x",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-x", ReplicaID: "rep-x", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-y", ReplicaID: "rep-y", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-z", ReplicaID: "rep-z", Role: metadata.ReplicaRoleSecondary},
			},
		},
	} {
		if err := repo.PutReplicaSet(ctx, replicaSet); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", replicaSet.ReplicaSetID, err)
		}
	}

	metaSvc := metadata.NewService(repo)
	coordinator := NewCoordinator(metaSvc, metaSvc)
	executor := NewExecutor(repo, coordinator)
	writer := fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
			2: {AckedReplicaIDs: []string{"rep-x", "rep-y"}},
		},
		errs: map[uint64]error{},
	}
	service := NewWriteService(executor, writer)

	resp, err := service.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-int-1",
		AttachmentID:   "att-00a1b2c3-0001",
		Generation:     8,
		IdempotencyKey: "idem-int-1",
		OffsetBytes:    2 << 20,
		LengthBytes:    5 << 20,
		Data:           []byte("integration-payload"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked || resp.Revision != 12 {
		t.Fatalf("response=%+v", resp)
	}

	record, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-int-1")
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	if record.ResultState != metadata.IdempotencyCommitted || record.Revision != 12 {
		t.Fatalf("idempotency record=%+v", record)
	}

	state, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if state.Revision != 12 {
		t.Fatalf("volume revision=%d want=12", state.Revision)
	}
}
