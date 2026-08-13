package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type flakyDeleteStore struct {
	inner      store.ObjectStore
	failKey    string
	failCount  int
	deleteHits map[string]int
}

func newFlakyDeleteStore(inner store.ObjectStore, failKey string, failCount int) *flakyDeleteStore {
	return &flakyDeleteStore{
		inner:      inner,
		failKey:    failKey,
		failCount:  failCount,
		deleteHits: make(map[string]int),
	}
}

func (s *flakyDeleteStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return s.inner.Get(ctx, key)
}

func (s *flakyDeleteStore) Put(ctx context.Context, key string, value []byte) error {
	return s.inner.Put(ctx, key, value)
}

func (s *flakyDeleteStore) Delete(ctx context.Context, key string) error {
	s.deleteHits[key]++
	if key == s.failKey && s.deleteHits[key] <= s.failCount {
		return fmt.Errorf("injected delete failure for %s", key)
	}
	return s.inner.Delete(ctx, key)
}

func (s *flakyDeleteStore) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	return s.inner.List(ctx, prefix, cursor, limit)
}

func TestLocalPayloadGarbageCollectorDeletesUnreferencedLegacyChunkWhenAllocationPagesExist(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 10,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       10,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             "write-hint-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   metadata.MutationOperationCommitted,
		AllocationRevision:      12,
		WriterFencingEpoch:      5,
		RetiredPhysicalChunkIDs: []uint64{500},
		StartedAtUnix:           1000,
		LastUpdatedAtUnix:       1001,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, key := range []string{
		localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500),
		localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 501),
		localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 11),
	} {
		if err := storeA.Put(ctx, key, []byte("payload")); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 2 {
		t.Fatalf("results=%+v", results)
	}
	op, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if op.State != metadata.MutationOperationCommitted || op.Kind != "payload_gc" {
		t.Fatalf("mutation operation=%+v", op)
	}

	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 11)); err != nil {
		t.Fatalf("Get legacy key err=%v", err)
	} else if found {
		t.Fatalf("legacy key was not deleted")
	}
	for _, chunkID := range []uint64{500, 501} {
		if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID)); err != nil || !found {
			t.Fatalf("Get chunk %d found=%v err=%v", chunkID, found, err)
		}
	}
}

func TestLocalPayloadGarbageCollectorProtectsSnapshotAndCloneDeltaChunks(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 20,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      20,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       20,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 501},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            1000,
		UpdatedAtUnix:            1000,
		CutVolumeRevision:        19,
		AllocationChunkSizeBytes: 4,
		AllocationPageSizeBytes:  8,
		SourceSizeBytes:          8,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       19,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, metadata.CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    1001,
		UpdatedAtUnix:    1001,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if err := repo.PutCloneDeltaAllocationPage(ctx, clone.CloneID, metadata.AllocationPageRecord{
		VolumeID:       clone.CloneID,
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       21,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 700},
		},
	}); err != nil {
		t.Fatalf("PutCloneDeltaAllocationPage: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             "write-hint-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   metadata.MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{11, 500, 501, 700},
		StartedAtUnix:           1002,
		LastUpdatedAtUnix:       1003,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{11, 500, 501, 700} {
		key := localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID)
		if err := storeA.Put(ctx, key, []byte("payload")); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 3 {
		t.Fatalf("results=%+v", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 11)); err != nil {
		t.Fatalf("Get stale key err=%v", err)
	} else if found {
		t.Fatalf("stale key was not deleted")
	}
	for _, chunkID := range []uint64{500, 501, 700} {
		if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID)); err != nil || !found {
			t.Fatalf("Get protected chunk %d found=%v err=%v", chunkID, found, err)
		}
	}
}

func TestLocalPayloadGarbageCollectorProtectsSnapshotChunksWithoutLiveAllocationPages(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 20,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		PlacementRef:  "pl-1",
		Revision:      20,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            1000,
		UpdatedAtUnix:            1000,
		CutVolumeRevision:        19,
		AllocationChunkSizeBytes: 4,
		AllocationPageSizeBytes:  8,
		SourceSizeBytes:          8,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       19,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{500, 700} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 1 {
		t.Fatalf("results=%+v", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)); err != nil || !found {
		t.Fatalf("snapshot chunk found=%v err=%v", found, err)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 700)); err != nil {
		t.Fatalf("Get orphan chunk err=%v", err)
	} else if found {
		t.Fatalf("orphan chunk was not deleted")
	}
}

func TestLocalPayloadGarbageCollectorReleasesMaterializedCloneDeltaChunks(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 30,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		PlacementRef:  "pl-1",
		Revision:      30,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       30,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 501},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            1000,
		UpdatedAtUnix:            1000,
		CutVolumeRevision:        29,
		AllocationChunkSizeBytes: 4,
		AllocationPageSizeBytes:  8,
		SourceSizeBytes:          8,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, metadata.CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    1001,
		UpdatedAtUnix:    1001,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if err := repo.PutCloneDeltaAllocationPage(ctx, clone.CloneID, metadata.AllocationPageRecord{
		VolumeID:       clone.CloneID,
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       31,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 700},
		},
	}); err != nil {
		t.Fatalf("PutCloneDeltaAllocationPage: %v", err)
	}
	if _, err := repo.MarkCloneMaterialized(ctx, clone.CloneID, "00a1b2c4"); err != nil {
		t.Fatalf("MarkCloneMaterialized: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{501, 700} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 1 {
		t.Fatalf("results=%+v", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 501)); err != nil || !found {
		t.Fatalf("live chunk found=%v err=%v", found, err)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 700)); err != nil {
		t.Fatalf("Get materialized clone delta err=%v", err)
	} else if found {
		t.Fatalf("materialized clone delta chunk was not deleted")
	}
}

func TestLocalPayloadGarbageCollectorToleratesPhysicalChunkIDHoles(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 40,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		PlacementRef:  "pl-1",
		Revision:      40,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       40,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 900},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{500, 501, 900} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 2 {
		t.Fatalf("results=%+v", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 501)); err != nil {
		t.Fatalf("Get hole chunk err=%v", err)
	} else if found {
		t.Fatalf("unreferenced physical chunk inside ID hole was not deleted")
	}
	for _, chunkID := range []uint64{500, 900} {
		if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID)); err != nil || !found {
			t.Fatalf("Get referenced chunk %d found=%v err=%v", chunkID, found, err)
		}
	}
}

func TestLocalPayloadGarbageCollectorLoadsRetiredPayloadHintsFromCommittedMutations(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, op := range []metadata.MutationOperationRecord{
		{
			OperationID:             "write-hint-1",
			VolumeID:                "00a1b2c3",
			Kind:                    "write",
			State:                   metadata.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{500, 501},
		},
		{
			OperationID:             "transition-hint-1",
			VolumeID:                "00a1b2c3",
			Kind:                    "transition",
			State:                   metadata.MutationOperationCommitted,
			RetiredPhysicalChunkIDs: []uint64{700},
		},
		{
			OperationID:             "failed-write",
			VolumeID:                "00a1b2c3",
			Kind:                    "write",
			State:                   metadata.MutationOperationFailed,
			RetiredPhysicalChunkIDs: []uint64{999},
		},
	} {
		if err := repo.PutMutationOperation(ctx, op); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", op.OperationID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": store.NewMemoryStore()})
	hints, err := collector.retiredPayloadHintChunkIDs(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("retiredPayloadHintChunkIDs: %v", err)
	}
	for _, want := range []uint64{500, 501, 700} {
		if _, ok := hints[want]; !ok {
			t.Fatalf("missing hint %d in %v", want, hints)
		}
	}
	if _, ok := hints[999]; ok {
		t.Fatalf("failed mutation hint should be ignored: %v", hints)
	}
}

func TestLocalPayloadGarbageCollectorDeletesZeroedChunkAfterAllocationPromotion(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 501},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{500, 501} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 1 {
		t.Fatalf("results=%+v", results)
	}
	op, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if op.State != metadata.MutationOperationCommitted || op.AllocationRevision != 12 {
		t.Fatalf("mutation operation=%+v", op)
	}
	if len(op.RetiredPhysicalChunkIDs) != 1 || op.RetiredPhysicalChunkIDs[0] != 500 {
		t.Fatalf("payload-gc retired chunk ids=%v want=[500]", op.RetiredPhysicalChunkIDs)
	}

	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)); err != nil {
		t.Fatalf("Get chunk 500 err=%v", err)
	} else if found {
		t.Fatalf("zero-promoted chunk 500 was not deleted")
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 501)); err != nil || !found {
		t.Fatalf("Get chunk 501 found=%v err=%v", found, err)
	}
}

func TestLocalPayloadGarbageCollectorScopesSweepToAffectedExtents(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, mapping := range []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 12},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-2", Revision: 12},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}
	for _, page := range []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindZero},
			},
		},
	} {
		if err := repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             metadata.PayloadGCMutationOperationID("00a1b2c3"),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc",
		State:                   metadata.MutationOperationPending,
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{500},
	}); err != nil {
		t.Fatalf("PutMutationOperation(payload-gc): %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, seeded := range []struct {
		extentID uint64
		chunkID  uint64
	}{
		{extentID: 1, chunkID: 500},
		{extentID: 2, chunkID: 700},
	} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", seeded.extentID, seeded.chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put extent=%d chunk=%d: %v", seeded.extentID, seeded.chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].CandidateCount != 1 || results[0].DeletedCount != 1 {
		t.Fatalf("results=%+v want one targeted deletion", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)); err != nil {
		t.Fatalf("Get extent1 chunk500 err=%v", err)
	} else if found {
		t.Fatalf("targeted extent chunk was not deleted")
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 2, 700)); err != nil || !found {
		t.Fatalf("Get extent2 chunk700 found=%v err=%v", found, err)
	}
	op, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(payload-gc): %v", err)
	}
	if op.State != metadata.MutationOperationCommitted {
		t.Fatalf("payload-gc state=%q want=%q", op.State, metadata.MutationOperationCommitted)
	}
	if len(op.AffectedExtentIDs) != 1 || op.AffectedExtentIDs[0] != 1 {
		t.Fatalf("payload-gc affected extents=%v want=[1]", op.AffectedExtentIDs)
	}
}

func TestLocalPayloadGarbageCollectorScopesSweepToAffectedPages(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, mapping := range []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 12},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-2", Revision: 12},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}
	for _, page := range []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindZero},
			},
		},
	} {
		if err := repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             metadata.PayloadGCMutationOperationID("00a1b2c3"),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc",
		State:                   metadata.MutationOperationPending,
		AffectedPageNos:         []uint64{0},
		RetiredPhysicalChunkIDs: []uint64{500},
	}); err != nil {
		t.Fatalf("PutMutationOperation(payload-gc): %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, seeded := range []struct {
		extentID uint64
		chunkID  uint64
	}{
		{extentID: 1, chunkID: 500},
		{extentID: 2, chunkID: 700},
	} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", seeded.extentID, seeded.chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put extent=%d chunk=%d: %v", seeded.extentID, seeded.chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].CandidateCount != 1 || results[0].DeletedCount != 1 {
		t.Fatalf("results=%+v want one targeted deletion", results)
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)); err != nil {
		t.Fatalf("Get extent1 chunk500 err=%v", err)
	} else if found {
		t.Fatalf("page-targeted chunk was not deleted")
	}
	if _, found, err := storeA.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 2, 700)); err != nil || !found {
		t.Fatalf("Get extent2 chunk700 found=%v err=%v", found, err)
	}
	op, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(payload-gc): %v", err)
	}
	if len(op.AffectedPageNos) != 1 || op.AffectedPageNos[0] != 0 {
		t.Fatalf("payload-gc affected pages=%v want=[0]", op.AffectedPageNos)
	}
}

func TestLocalPayloadGarbageCollectorCreatesChunkBatchOperations(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             metadata.PayloadGCMutationOperationID("00a1b2c3"),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc",
		State:                   metadata.MutationOperationPending,
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{500, 501},
	}); err != nil {
		t.Fatalf("PutMutationOperation(payload-gc): %v", err)
	}

	storeA := store.NewMemoryStore()
	for _, chunkID := range []uint64{500, 501} {
		if err := storeA.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": storeA})
	collector.chunkBatchSize = 1
	if _, err := collector.SweepVolume(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("SweepVolume: %v", err)
	}
	for batchIndex, wantChunkID := range []uint64{500, 501} {
		op, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCBatchMutationOperationID("00a1b2c3", batchIndex))
		if err != nil {
			t.Fatalf("GetMutationOperation(batch %d): %v", batchIndex, err)
		}
		if op.Kind != "payload_gc_batch" || op.State != metadata.MutationOperationCommitted {
			t.Fatalf("batch operation=%+v", op)
		}
		if len(op.RetiredPhysicalChunkIDs) != 1 || op.RetiredPhysicalChunkIDs[0] != wantChunkID {
			t.Fatalf("batch retired chunk ids=%v want=[%d]", op.RetiredPhysicalChunkIDs, wantChunkID)
		}
	}
}

func TestLocalPayloadGarbageCollectorRetriesOnlyFailedChunkBatch(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 12,
		Status:   metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:             metadata.PayloadGCMutationOperationID("00a1b2c3"),
		VolumeID:                "00a1b2c3",
		Kind:                    "payload_gc",
		State:                   metadata.MutationOperationPending,
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{500, 501},
	}); err != nil {
		t.Fatalf("PutMutationOperation(payload-gc): %v", err)
	}

	baseStore := store.NewMemoryStore()
	for _, chunkID := range []uint64{500, 501} {
		if err := baseStore.Put(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID), []byte("payload")); err != nil {
			t.Fatalf("Put chunk %d: %v", chunkID, err)
		}
	}
	failKey := localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 501)
	flakyStore := newFlakyDeleteStore(baseStore, failKey, 1)

	collector := NewLocalPayloadGarbageCollector(repo, map[string]store.ObjectStore{"rep-a": flakyStore})
	collector.chunkBatchSize = 1

	if _, err := collector.SweepVolume(ctx, "00a1b2c3"); err == nil {
		t.Fatalf("first SweepVolume unexpectedly succeeded")
	}

	parentOp, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(parent after first run): %v", err)
	}
	if parentOp.State != metadata.MutationOperationFailed {
		t.Fatalf("parent state after first run=%q want=%q", parentOp.State, metadata.MutationOperationFailed)
	}
	batch0ID := metadata.PayloadGCBatchMutationOperationID("00a1b2c3", 0)
	batch1ID := metadata.PayloadGCBatchMutationOperationID("00a1b2c3", 1)
	batch0, err := repo.GetMutationOperation(ctx, "00a1b2c3", batch0ID)
	if err != nil {
		t.Fatalf("GetMutationOperation(batch0): %v", err)
	}
	batch1, err := repo.GetMutationOperation(ctx, "00a1b2c3", batch1ID)
	if err != nil {
		t.Fatalf("GetMutationOperation(batch1): %v", err)
	}
	if batch0.State != metadata.MutationOperationCommitted {
		t.Fatalf("batch0 state=%q want=%q", batch0.State, metadata.MutationOperationCommitted)
	}
	if batch1.State != metadata.MutationOperationFailed {
		t.Fatalf("batch1 state=%q want=%q", batch1.State, metadata.MutationOperationFailed)
	}
	batch0StartedAt := batch0.StartedAtUnix
	batch1StartedAt := batch1.StartedAtUnix
	if batch0StartedAt == 0 || batch1StartedAt == 0 {
		t.Fatalf("expected non-zero batch start times: batch0=%d batch1=%d", batch0StartedAt, batch1StartedAt)
	}
	if flakyStore.deleteHits[localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)] != 1 {
		t.Fatalf("chunk 500 delete hits=%d want=1", flakyStore.deleteHits[localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)])
	}
	if flakyStore.deleteHits[failKey] != 1 {
		t.Fatalf("chunk 501 delete hits=%d want=1", flakyStore.deleteHits[failKey])
	}

	results, err := collector.SweepVolume(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("second SweepVolume: %v", err)
	}
	if len(results) != 1 || results[0].CandidateCount != 1 || results[0].DeletedCount != 1 {
		t.Fatalf("results=%+v want one retried candidate deletion", results)
	}

	parentOp, err = repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(parent after retry): %v", err)
	}
	if parentOp.State != metadata.MutationOperationCommitted {
		t.Fatalf("parent state after retry=%q want=%q", parentOp.State, metadata.MutationOperationCommitted)
	}
	batch0After, err := repo.GetMutationOperation(ctx, "00a1b2c3", batch0ID)
	if err != nil {
		t.Fatalf("GetMutationOperation(batch0 after retry): %v", err)
	}
	batch1After, err := repo.GetMutationOperation(ctx, "00a1b2c3", batch1ID)
	if err != nil {
		t.Fatalf("GetMutationOperation(batch1 after retry): %v", err)
	}
	if batch0After.State != metadata.MutationOperationCommitted {
		t.Fatalf("batch0 state after retry=%q want=%q", batch0After.State, metadata.MutationOperationCommitted)
	}
	if batch1After.State != metadata.MutationOperationCommitted {
		t.Fatalf("batch1 state after retry=%q want=%q", batch1After.State, metadata.MutationOperationCommitted)
	}
	if batch0After.StartedAtUnix != batch0StartedAt {
		t.Fatalf("batch0 should not be recreated: started_at=%d want=%d", batch0After.StartedAtUnix, batch0StartedAt)
	}
	if batch1After.StartedAtUnix != batch1StartedAt {
		t.Fatalf("batch1 should retain original started_at on retry: started_at=%d want=%d", batch1After.StartedAtUnix, batch1StartedAt)
	}
	if flakyStore.deleteHits[localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)] != 1 {
		t.Fatalf("chunk 500 delete hits after retry=%d want=1", flakyStore.deleteHits[localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 500)])
	}
	if flakyStore.deleteHits[failKey] != 2 {
		t.Fatalf("chunk 501 delete hits after retry=%d want=2", flakyStore.deleteHits[failKey])
	}
	for _, chunkID := range []uint64{500, 501} {
		if _, found, err := baseStore.Get(ctx, localReplicaPayloadKey("rep-a", "00a1b2c3", 1, chunkID)); err != nil {
			t.Fatalf("Get chunk %d err=%v", chunkID, err)
		} else if found {
			t.Fatalf("chunk %d should have been deleted", chunkID)
		}
	}
}
