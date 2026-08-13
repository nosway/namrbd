package metadata

import (
	"context"
	"strings"
	"testing"
)

func TestAllocateChunkIDsFromSequenceAdvancesNextID(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	startID, err := AllocateChunkIDsFromSequence(ctx, repo, "00a1b2c3", 4)
	if err != nil {
		t.Fatalf("AllocateChunkIDsFromSequence: %v", err)
	}
	if startID != 1 {
		t.Fatalf("start_id=%d want=1", startID)
	}
	nextID, err := repo.GetNextChunkID(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetNextChunkID: %v", err)
	}
	if nextID != 5 {
		t.Fatalf("next_id=%d want=5", nextID)
	}
}

func TestAllocateChunkIDsFromSequenceWithZeroCountReturnsZero(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	if err := repo.PutVolumeState(context.Background(), VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	startID, err := AllocateChunkIDsFromSequence(context.Background(), repo, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("AllocateChunkIDsFromSequence: %v", err)
	}
	if startID != 0 {
		t.Fatalf("start_id=%d want=0", startID)
	}
}

func TestChunkIDAllocationServiceAllocatesFromSequence(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	svc := NewChunkIDAllocationService(repo)
	startID, err := svc.AllocateChunkIDs(ctx, ChunkIDAllocationRequest{
		VolumeID: "00a1b2c3",
		Count:    3,
	})
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	if startID != 1 {
		t.Fatalf("start_id=%d want=1", startID)
	}
	nextID, err := repo.GetNextChunkID(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetNextChunkID: %v", err)
	}
	if nextID != 4 {
		t.Fatalf("next_id=%d want=4", nextID)
	}
}

func TestChunkIDAllocationRequestValidateRejectsInvalidVolumeID(t *testing.T) {
	err := (ChunkIDAllocationRequest{VolumeID: "not-a-volume", Count: 1}).Validate()
	if err == nil || !strings.Contains(err.Error(), "invalid chunk id allocation volume_id") {
		t.Fatalf("Validate error=%v want invalid volume_id", err)
	}
}

func TestChunkIDAllocationServiceRequiresStore(t *testing.T) {
	_, err := NewChunkIDAllocationService(nil).AllocateChunkIDs(context.Background(), ChunkIDAllocationRequest{
		VolumeID: "00a1b2c3",
		Count:    1,
	})
	if err == nil || !strings.Contains(err.Error(), "chunk id sequence store is required") {
		t.Fatalf("AllocateChunkIDs error=%v want required store", err)
	}
}
