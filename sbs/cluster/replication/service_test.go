package replication

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/payload"
)

type fakeReplicaWriter struct {
	results map[uint64]*ReplicaWriteResult
	errs    map[uint64]error
	reqs    map[uint64]ReplicaWriteRequest
}

func (f fakeReplicaWriter) WriteExtent(_ context.Context, plan ExtentWritePlan, req ReplicaWriteRequest) (*ReplicaWriteResult, error) {
	if f.reqs != nil {
		f.reqs[plan.Extent.ExtentID] = req
	}
	if err := f.errs[plan.Extent.ExtentID]; err != nil {
		return nil, err
	}
	if result, ok := f.results[plan.Extent.ExtentID]; ok {
		return result, nil
	}
	return &ReplicaWriteResult{}, nil
}

type failOnWriteReplicaWriter struct {
	t *testing.T
}

func (f failOnWriteReplicaWriter) WriteExtent(_ context.Context, plan ExtentWritePlan, _ ReplicaWriteRequest) (*ReplicaWriteResult, error) {
	f.t.Fatalf("WriteExtent called for zero no-op extent=%d", plan.Extent.ExtentID)
	return nil, nil
}

func TestWriteServiceWritesQuorumCommitsAndAcks(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
				{
					Extent:           zeroExtent(2),
					PlacementRef:     "pl-2",
					ReplicaSetID:     "rs-2",
					Primary:          ReplicaTarget{ReplicaID: "rep-x"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-x"}, {ReplicaID: "rep-y"}, {ReplicaID: "rep-z"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
			2: {AckedReplicaIDs: []string{"rep-x", "rep-y"}},
		},
		errs: map[uint64]error{},
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    8192,
		Data:           []byte("payload"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked {
		t.Fatalf("response=%+v", resp)
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
	if record.Revision != 12 {
		t.Fatalf("record revision=%d want=12", record.Revision)
	}
}

func TestWriteServicePromotedZeroNoopSkipsIntentReplicaAndMetadata(t *testing.T) {
	store := newFakeIntentStore()
	page := metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       11,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, ChunkID: 0, PlacementRef: "pl-1"},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
					ChunkSizeBytes:   4096,
					AllocationPages: []metadata.ResolvedAllocationPage{
						{Page: page, RangeStartChunk: 0, RangeEndChunk: 1, CoversWholePage: true},
					},
				},
			},
		},
	})
	svc := NewWriteService(executor, failOnWriteReplicaWriter{t: t})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-zero-noop",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-zero-noop",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           make([]byte, 4096),
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		ZeroSemantic:   true,
		AllowZeroNoop:  true,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked || resp.Revision != store.volumeState.Revision {
		t.Fatalf("response=%+v want committed zero no-op at current revision", resp)
	}
	record, ok := store.records["idem-zero-noop"]
	if !ok || record.ResultState != metadata.IdempotencyCommitted || record.Revision != store.volumeState.Revision {
		t.Fatalf("zero no-op idempotency record=%+v found=%v", record, ok)
	}
	if store.intentPutCalls != 0 || len(store.mutationOps) != 0 {
		t.Fatalf("zero no-op should not write mutation intent records: intent_put=%d mutations=%+v", store.intentPutCalls, store.mutationOps)
	}
	if store.commitCalls != 0 || store.pageScopedCalls != 0 || store.rangeLocalCalls != 0 || store.appendOnlyCalls != 0 || store.effectsApplyCalls != 0 {
		t.Fatalf("zero no-op should not commit metadata: commit=%d page=%d range=%d append=%d effects=%d", store.commitCalls, store.pageScopedCalls, store.rangeLocalCalls, store.appendOnlyCalls, store.effectsApplyCalls)
	}
}

func TestWriteServiceUnsafeZeroNoopCanSkipIdempotencyRecord(t *testing.T) {
	store := newFakeIntentStore()
	page := metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       11,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, ChunkID: 0, PlacementRef: "pl-1"},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
					ChunkSizeBytes:   4096,
					AllocationPages: []metadata.ResolvedAllocationPage{
						{Page: page, RangeStartChunk: 0, RangeEndChunk: 1, CoversWholePage: true},
					},
				},
			},
		},
	})
	svc := NewWriteService(executor, failOnWriteReplicaWriter{t: t})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:                      "00a1b2c3",
		RequestID:                     "req-zero-noop-unsafe",
		AttachmentID:                  "att-1",
		Generation:                    9,
		IdempotencyKey:                "idem-zero-noop-unsafe",
		OffsetBytes:                   0,
		LengthBytes:                   4096,
		Data:                          make([]byte, 4096),
		PageBytes:                     4096,
		ChunkSizeBytes:                4096,
		ZeroSemantic:                  true,
		AllowZeroNoop:                 true,
		UnsafeZeroNoopSkipIdempotency: true,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked || resp.Revision != store.volumeState.Revision {
		t.Fatalf("response=%+v want committed unsafe zero no-op at current revision", resp)
	}
	if _, ok := store.records["idem-zero-noop-unsafe"]; ok {
		t.Fatalf("unsafe zero no-op should not write idempotency record")
	}
	if store.intentPutCalls != 0 || len(store.mutationOps) != 0 {
		t.Fatalf("unsafe zero no-op should not write mutation intent records: intent_put=%d mutations=%+v", store.intentPutCalls, store.mutationOps)
	}
	if store.commitCalls != 0 || store.pageScopedCalls != 0 || store.rangeLocalCalls != 0 || store.appendOnlyCalls != 0 || store.effectsApplyCalls != 0 {
		t.Fatalf("unsafe zero no-op should not commit metadata: commit=%d page=%d range=%d append=%d effects=%d", store.commitCalls, store.pageScopedCalls, store.rangeLocalCalls, store.appendOnlyCalls, store.effectsApplyCalls)
	}
}

func TestWriteServiceOmitsReplicaPayloadForFullChunkZeroSemanticCommit(t *testing.T) {
	store := newFakeIntentStore()
	page := metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       11,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}
	store.allocationPages[0] = page
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, ChunkID: 0, PlacementRef: "pl-1"},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
					ChunkSizeBytes:   4096,
					AllocationPages: []metadata.ResolvedAllocationPage{
						{Page: page, RangeStartChunk: 0, RangeEndChunk: 1, CoversWholePage: true},
					},
				},
			},
		},
	})
	reqs := make(map[uint64]ReplicaWriteRequest)
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
		reqs: reqs,
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-zero-payload-omitted",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-zero-payload-omitted",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		ZeroSemantic:   true,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked {
		t.Fatalf("response=%+v", resp)
	}
	gotReq := reqs[1]
	if gotReq.Data != nil {
		t.Fatalf("replica data len=%d want nil omitted payload", len(gotReq.Data))
	}
	record := store.records["idem-zero-payload-omitted"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
	committed := store.allocationPages[0]
	if len(committed.Extents) != 1 || committed.Extents[0].Kind != metadata.AllocationKindZero {
		t.Fatalf("committed allocation page=%+v want zero extent", committed)
	}
}

func TestWriteServiceMaterializesReplicaPayloadWhenZeroSemanticNeedsMerge(t *testing.T) {
	store := newFakeIntentStore()
	page := metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       11,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}
	store.allocationPages[0] = page
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, ChunkID: 0, PlacementRef: "pl-1"},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
					ChunkSizeBytes:   4096,
					AllocationPages: []metadata.ResolvedAllocationPage{
						{Page: page, RangeStartChunk: 0, RangeEndChunk: 1, CoversWholePage: true},
					},
				},
			},
		},
	})
	reqs := make(map[uint64]ReplicaWriteRequest)
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
		reqs: reqs,
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-zero-payload-merge",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-zero-payload-merge",
		OffsetBytes:    128,
		LengthBytes:    512,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		ZeroSemantic:   true,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked {
		t.Fatalf("response=%+v", resp)
	}
	gotReq := reqs[1]
	if !bytes.Equal(gotReq.Data, make([]byte, 512)) {
		t.Fatalf("replica data len=%d want 512 zero bytes", len(gotReq.Data))
	}
}

func TestWriteServiceWriteCloneCommitsDeltaWithoutVolumeIntent(t *testing.T) {
	store := newFakeIntentStore()
	cloneCommitter := &fakeCloneDeltaCommitter{}
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					ChunkSizeBytes:   4096,
					MetadataRevision: 11,
					AllocationPages: []metadata.ResolvedAllocationPage{
						{
							Page: metadata.AllocationPageRecord{
								VolumeID:       "00a1b2c3",
								PageNo:         0,
								PageBytes:      4096,
								ChunkSizeBytes: 4096,
								Revision:       11,
								Extents: []metadata.AllocationExtentRecord{
									{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
								},
							},
							RangeStartChunk: 0,
							RangeEndChunk:   1,
							CoversWholePage: true,
						},
					},
				},
			},
		},
	}).WithCloneDeltaMetadataCommitter(cloneCommitter)
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
	})

	resp, err := svc.WriteClone(context.Background(), "clone-1", WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-clone-write-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-clone-write-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("payload"),
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("WriteClone: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked || resp.CloneID != "clone-1" {
		t.Fatalf("response=%+v", resp)
	}
	if cloneCommitter.cloneID != "clone-1" || len(cloneCommitter.pages) != 1 {
		t.Fatalf("clone delta commit=%q %+v", cloneCommitter.cloneID, cloneCommitter.pages)
	}
	page := cloneCommitter.pages[0]
	if len(page.Extents) != 1 || page.Extents[0].Kind != metadata.AllocationKindData || page.Extents[0].PhysicalChunkStart != 1 {
		t.Fatalf("clone delta page=%+v", page)
	}
	if len(store.records) != 0 || len(store.mutationOps) != 0 {
		t.Fatalf("clone write should not create volume intent records: records=%+v mutations=%+v", store.records, store.mutationOps)
	}
	if store.commitCalls != 0 || store.stateCommitCalls != 0 || store.effectsApplyCalls != 0 {
		t.Fatalf("volume commit path should not run: commit=%d state=%d effects=%d", store.commitCalls, store.stateCommitCalls, store.effectsApplyCalls)
	}
}

func TestWriteServiceReturnsReplayForCommittedIntent(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
		Revision:       15,
	}
	executor := NewExecutor(store, fakePlanner{})
	svc := NewWriteService(executor, fakeReplicaWriter{})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("Write replay: %v", err)
	}
	if !resp.Replay || !resp.Committed || resp.Revision != 15 {
		t.Fatalf("response=%+v", resp)
	}
}

func TestWriteServiceMarksFailedOnReplicaWriterError(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{},
		errs:    map[uint64]error{1: errors.New("replica unavailable")},
	})

	_, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err == nil {
		t.Fatal("Write expected error, got nil")
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyFailed {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyFailed)
	}
}

func TestWriteServiceFailsWhenExtentDoesNotReachQuorum(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {
				AckedReplicaIDs: []string{"rep-b"},
				FailureMessages: []string{
					"replica \"rep-a\" on node \"u01\": issue key access lease: unavailable",
				},
			},
		},
		errs: map[uint64]error{},
	})

	_, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err == nil {
		t.Fatal("Write expected error, got nil")
	}
	for _, want := range []string{
		"acked=1",
		"required=2",
		"primary=rep-a",
		"primary_acked=false",
		"acked_replica_ids=rep-b",
		"issue key access lease",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("quorum error=%q missing %q", err.Error(), want)
		}
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyFailed {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyFailed)
	}
}

func TestWriteServiceCommitsGuardedNonPrimaryQuorumResult(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {
				AckedReplicaIDs:       []string{"rep-b", "rep-c"},
				AllowNonPrimaryQuorum: true,
				Stats: ReplicaWriteStats{
					QuorumEarlyReturn:  true,
					PrimaryAckRequired: false,
					PrimaryAcked:       false,
					PendingReplicas:    1,
				},
			},
		},
		errs: map[uint64]error{},
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked {
		t.Fatalf("response=%+v", resp)
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
}

func TestWriteServiceFailsOnMetadataCASConflict(t *testing.T) {
	store := newFakeIntentStore()
	store.forceCAS = true
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
	})

	_, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-cas-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-cas-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("abcd"),
	})
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("error=%v want=%v", err, metadata.ErrCASConflict)
	}
	record := store.records["idem-cas-1"]
	if record.ResultState != metadata.IdempotencyFailed {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyFailed)
	}
}

func TestWriteServiceRetriesTransientMetadataCASConflict(t *testing.T) {
	store := newFakeIntentStore()
	store.casFailures = 2
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:       zeroExtent(1),
					PlacementRef: "pl-1",
					ReplicaSetID: "rs-1",
					Primary:      ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks: 2,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-cas-retry-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-cas-retry-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("abcd"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.Revision != 12 {
		t.Fatalf("response=%+v", resp)
	}
	if store.commitCalls != 3 {
		t.Fatalf("commitCalls=%d want=3", store.commitCalls)
	}
	record := store.records["idem-cas-retry-1"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
}

func TestWriteServiceRetriesBurstMetadataCASConflict(t *testing.T) {
	store := newFakeIntentStore()
	store.casFailures = 15
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-cas-burst-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-cas-burst-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("abcd"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.Revision != 12 {
		t.Fatalf("response=%+v", resp)
	}
	if store.commitCalls != 16 {
		t.Fatalf("commitCalls=%d want=16", store.commitCalls)
	}
	record := store.records["idem-cas-burst-1"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
}

func TestMetadataCommitLockSetSerializesSameVolume(t *testing.T) {
	locks := newMetadataCommitLockSet()
	first, err := locks.acquire(context.Background(), "vol-a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	acquired := make(chan *metadataCommitGuard, 1)
	errCh := make(chan error, 1)
	go func() {
		second, err := locks.acquire(context.Background(), "vol-a")
		if err != nil {
			errCh <- err
			return
		}
		acquired <- second
	}()

	select {
	case second := <-acquired:
		second.release()
		t.Fatal("second acquire completed before first release")
	case err := <-errCh:
		t.Fatalf("second acquire: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	first.release()

	var second *metadataCommitGuard
	select {
	case second = <-acquired:
	case err := <-errCh:
		t.Fatalf("second acquire after release: %v", err)
	case <-time.After(time.Second):
		t.Fatal("second acquire did not complete after first release")
	}
	second.release()
}

func TestMetadataCommitLockSetAllowsDifferentVolumes(t *testing.T) {
	locks := newMetadataCommitLockSet()
	first, err := locks.acquire(context.Background(), "vol-a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.release()

	second, err := locks.acquire(context.Background(), "vol-b")
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	second.release()
}

func TestNewWriteServiceSharesDefaultMetadataCommitLockSet(t *testing.T) {
	first := NewWriteService(nil, fakeReplicaWriter{})
	second := NewWriteService(nil, fakeReplicaWriter{})
	if first.commitLocks == nil {
		t.Fatal("first WriteService commitLocks is nil")
	}
	if first.commitLocks != second.commitLocks {
		t.Fatal("NewWriteService must share metadata commit locks across request-scoped services")
	}
}

func TestShouldSerializeMetadataCommitMode(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{mode: "volume_scoped", want: true},
		{mode: "volume_scoped_async_effects", want: true},
		{mode: "page_scoped", want: false},
		{mode: "range_local", want: false},
		{mode: "range_local_async_effects", want: false},
		{mode: "append_only_service_ordered_effects", want: false},
		{mode: "clone_delta", want: false},
	}
	for _, tc := range cases {
		if got := shouldSerializeMetadataCommitMode(tc.mode); got != tc.want {
			t.Fatalf("shouldSerializeMetadataCommitMode(%q)=%t want %t", tc.mode, got, tc.want)
		}
	}
}

func TestWriteServiceEmitsStructuredCommitLog(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:           zeroExtent(1),
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
		errs: map[uint64]error{},
	})

	var buf bytes.Buffer
	restore := structuredlog.SetOutput(&buf)
	defer restore()

	_, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("abcd"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	logs := buf.String()
	for _, want := range []string{
		`"component":"sbs.replication"`,
		`"event":"write_committed"`,
		`"request_id":"req-1"`,
		`"volume_id":"00a1b2c3"`,
		`"idempotency_key":"idem-1"`,
		`"revision":12`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q: %s", want, logs)
		}
	}
}

func TestWriteAndReadWithPebbleReplicaStores(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))
	payloadBytes := make([]byte, 4096)
	copy(payloadBytes, []byte("payload-through-pebble"))

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-pebble-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-pebble-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           payloadBytes,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked {
		t.Fatalf("writeResp=%+v", writeResp)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != writeResp.Revision {
		t.Fatalf("allocation page revision=%d want=%d", page.Revision, writeResp.Revision)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != metadata.AllocationKindData || page.Extents[0].PhysicalChunkStart != 11 {
		t.Fatalf("allocation page extents=%+v", page.Extents)
	}
	operation, err := repo.GetMutationOperation(ctx, "00a1b2c3", writeMutationOperationID("idem-pebble-1"))
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if operation.State != metadata.MutationOperationCommitted || operation.AllocationRevision != writeResp.Revision {
		t.Fatalf("mutation operation=%+v", operation)
	}
	if _, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3")); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("payload-gc operation err=%v want not found", err)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != writeResp.Revision {
		t.Fatalf("extent mapping=%+v write revision=%d", mapping, writeResp.Revision)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(readResp.Data[:len("payload-through-pebble")]) != "payload-through-pebble" {
		t.Fatalf("payload prefix=%q", readResp.Data[:len("payload-through-pebble")])
	}
	if len(readResp.ReplicaReads) != 1 || readResp.ReplicaReads[0] != "rep-a" {
		t.Fatalf("replicaReads=%v want [rep-a]", readResp.ReplicaReads)
	}
}

func TestWriteCloneWithRepositoryCommitsCloneDeltaReadView(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        10,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4096,
		SourceSizeBytes:          4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       10,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, metadata.CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator).WithCloneDeltaMetadataCommitter(resolver)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))
	payloadBytes := make([]byte, 4096)
	copy(payloadBytes, []byte("clone-delta-payload"))

	writeResp, err := writeSvc.WriteClone(ctx, clone.CloneID, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-clone-pebble-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-clone-pebble-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           payloadBytes,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("WriteClone: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked || writeResp.CloneID != clone.CloneID {
		t.Fatalf("writeResp=%+v", writeResp)
	}
	clonePages, err := resolver.ResolveCloneAllocationPages(ctx, clone.CloneID, 0, 4096, 4096, 4096)
	if err != nil {
		t.Fatalf("ResolveCloneAllocationPages: %v", err)
	}
	if len(clonePages) != 1 || len(clonePages[0].Page.Extents) != 1 || clonePages[0].Page.Extents[0].Kind != metadata.AllocationKindData || clonePages[0].Page.Extents[0].PhysicalChunkStart != 1 {
		t.Fatalf("clone read-view pages=%+v", clonePages)
	}
	snapshotPages, err := resolver.ResolveSnapshotAllocationPages(ctx, snapshotID, 0, 4096, 4096, 4096)
	if err != nil {
		t.Fatalf("ResolveSnapshotAllocationPages: %v", err)
	}
	if len(snapshotPages) != 1 || len(snapshotPages[0].Page.Extents) != 1 || snapshotPages[0].Page.Extents[0].Kind != metadata.AllocationKindZero {
		t.Fatalf("snapshot base should remain zero: %+v", snapshotPages)
	}
	snapshotRead, err := readSvc.ReadSnapshot(ctx, snapshotID, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-snapshot-pebble-read-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if snapshotRead.SnapshotID != snapshotID || !bytes.Equal(snapshotRead.Data, make([]byte, 4096)) {
		t.Fatalf("snapshotRead=%+v prefix=%q", snapshotRead, snapshotRead.Data[:len("clone-delta-payload")])
	}
	cloneRead, err := readSvc.ReadClone(ctx, clone.CloneID, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-clone-pebble-read-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("ReadClone: %v", err)
	}
	if cloneRead.CloneID != clone.CloneID || string(cloneRead.Data[:len("clone-delta-payload")]) != "clone-delta-payload" {
		t.Fatalf("cloneRead=%+v prefix=%q", cloneRead, cloneRead.Data[:len("clone-delta-payload")])
	}
	if len(cloneRead.ReplicaReads) != 1 || cloneRead.ReplicaReads[0] != "rep-a" {
		t.Fatalf("cloneRead replicaReads=%v want [rep-a]", cloneRead.ReplicaReads)
	}
	sourceRead, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-pebble-read-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Read source volume: %v", err)
	}
	if !bytes.Equal(sourceRead.Data, make([]byte, 4096)) {
		t.Fatalf("source volume read should remain zero-backed, prefix=%q", sourceRead.Data[:len("clone-delta-payload")])
	}
	if _, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("source volume allocation page err=%v want not found", err)
	}
}

func TestWriteCloneAllocatesCowChunkWhenSnapshotReferencesSourceData(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator).WithCloneDeltaMetadataCommitter(resolver)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	sourcePayload := make([]byte, 4096)
	copy(sourcePayload, []byte("source-base-payload"))
	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-source-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           sourcePayload,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	}); err != nil {
		t.Fatalf("Write source: %v", err)
	}
	sourcePage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	sourcePhysical := sourcePage.Extents[0].PhysicalChunkStart

	snapshotID := "snap-00a1b2c3-20260521T121000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        11,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4096,
		SourceSizeBytes:          4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{sourcePage}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, metadata.CloneRecord{
		CloneID:          "clone-data-base",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}

	clonePayload := make([]byte, 4096)
	copy(clonePayload, []byte("clone-delta-payload"))
	if _, err := writeSvc.WriteClone(ctx, clone.CloneID, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-clone-cow-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-clone-cow-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           clonePayload,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	}); err != nil {
		t.Fatalf("WriteClone: %v", err)
	}

	clonePages, err := resolver.ResolveCloneAllocationPages(ctx, clone.CloneID, 0, 4096, 4096, 4096)
	if err != nil {
		t.Fatalf("ResolveCloneAllocationPages: %v", err)
	}
	if got := clonePages[0].Page.Extents[0].PhysicalChunkStart; got == sourcePhysical {
		t.Fatalf("clone reused source physical chunk id=%d", got)
	}
	sourceRead, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-read-after-clone-cow-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Read source: %v", err)
	}
	if !bytes.Equal(sourceRead.Data, sourcePayload) {
		t.Fatalf("source changed after clone write: prefix=%q", sourceRead.Data[:len("source-base-payload")])
	}
}

func TestWriteServiceSourceWriteAfterSnapshotAllocatesCowChunk(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	beforePayload := make([]byte, 4096)
	copy(beforePayload, []byte("source-before-snapshot"))
	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-before-snapshot",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-source-before-snapshot",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           beforePayload,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	}); err != nil {
		t.Fatalf("Write source before snapshot: %v", err)
	}
	sourcePage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage before snapshot: %v", err)
	}
	sourcePhysical := sourcePage.Extents[0].PhysicalChunkStart

	snapshotID := "snap-00a1b2c3-20260521T122000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        11,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4096,
		SourceSizeBytes:          4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{sourcePage}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	afterPayload := make([]byte, 4096)
	copy(afterPayload, []byte("source-after-snapshot"))
	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-after-snapshot",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-source-after-snapshot",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           afterPayload,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	}); err != nil {
		t.Fatalf("Write source after snapshot: %v", err)
	}
	updatedPage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage after snapshot: %v", err)
	}
	if got := updatedPage.Extents[0].PhysicalChunkStart; got == sourcePhysical {
		t.Fatalf("source overwrite reused snapshot physical chunk id=%d", got)
	}

	sourceRead, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-read-after-snapshot-overwrite",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Read source after snapshot overwrite: %v", err)
	}
	if !bytes.Equal(sourceRead.Data, afterPayload) {
		t.Fatalf("source read mismatch after snapshot overwrite: prefix=%q", sourceRead.Data[:len("source-after-snapshot")])
	}

	snapshotRead, err := readSvc.ReadSnapshot(ctx, snapshotID, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-snapshot-read-after-source-overwrite",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("ReadSnapshot after source overwrite: %v", err)
	}
	if !bytes.Equal(snapshotRead.Data, beforePayload) {
		t.Fatalf("snapshot changed after source overwrite: prefix=%q", snapshotRead.Data[:len("source-before-snapshot")])
	}
}

func TestWriteServiceSourcePartialWriteAfterSnapshotAllocatesCowChunk(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   64 * 1024,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	beforePayload := make([]byte, 4096)
	copy(beforePayload, []byte("source-partial-before-snapshot"))
	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-partial-before-snapshot",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-source-partial-before-snapshot",
		OffsetBytes:    0,
		LengthBytes:    uint64(len(beforePayload)),
		Data:           beforePayload,
		PageBytes:      4 * 1024 * 1024,
		ChunkSizeBytes: 64 * 1024,
	}); err != nil {
		t.Fatalf("Write source before snapshot: %v", err)
	}
	sourcePage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage before snapshot: %v", err)
	}
	sourcePhysical := sourcePage.Extents[0].PhysicalChunkStart

	snapshotID := "snap-00a1b2c3-20260521T123000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        11,
		AllocationChunkSizeBytes: 64 * 1024,
		AllocationPageSizeBytes:  4 * 1024 * 1024,
		SourceSizeBytes:          64 * 1024,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{sourcePage}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	afterPayload := make([]byte, 4096)
	copy(afterPayload, []byte("source-partial-after-snapshot"))
	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-partial-after-snapshot",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-source-partial-after-snapshot",
		OffsetBytes:    0,
		LengthBytes:    uint64(len(afterPayload)),
		Data:           afterPayload,
		PageBytes:      4 * 1024 * 1024,
		ChunkSizeBytes: 64 * 1024,
	}); err != nil {
		t.Fatalf("Write source after snapshot: %v", err)
	}
	updatedPage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage after snapshot: %v", err)
	}
	if got := updatedPage.Extents[0].PhysicalChunkStart; got == sourcePhysical {
		t.Fatalf("source partial overwrite reused snapshot physical chunk id=%d", got)
	}

	sourceRead, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-source-read-after-partial-snapshot-overwrite",
		OffsetBytes:    0,
		LengthBytes:    uint64(len(afterPayload)),
		PageBytes:      4 * 1024 * 1024,
		ChunkSizeBytes: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("Read source after snapshot overwrite: %v", err)
	}
	if !bytes.Equal(sourceRead.Data, afterPayload) {
		t.Fatalf("source read mismatch after partial snapshot overwrite: prefix=%q", sourceRead.Data[:len("source-partial-after-snapshot")])
	}

	snapshotRead, err := readSvc.ReadSnapshot(ctx, snapshotID, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-snapshot-read-after-source-partial-overwrite",
		OffsetBytes:    0,
		LengthBytes:    uint64(len(beforePayload)),
		PageBytes:      4 * 1024 * 1024,
		ChunkSizeBytes: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("ReadSnapshot after source overwrite: %v", err)
	}
	if !bytes.Equal(snapshotRead.Data, beforePayload) {
		t.Fatalf("snapshot changed after source partial overwrite: prefix=%q", snapshotRead.Data[:len("source-partial-before-snapshot")])
	}
}

func TestWriteAndReadWithPebbleReplicaStoresAllocatesChunksForZeroBackedFirstWrite(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))
	payloadBytes := make([]byte, 4096)
	copy(payloadBytes, []byte("payload-zero-backed-first-write"))

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-zero-backed-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-zero-backed-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           payloadBytes,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked {
		t.Fatalf("writeResp=%+v", writeResp)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != metadata.AllocationKindData || page.Extents[0].PhysicalChunkStart == 0 {
		t.Fatalf("allocation page extents=%+v", page.Extents)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(readResp.Data[:len("payload-zero-backed-first-write")]) != "payload-zero-backed-first-write" {
		t.Fatalf("payload prefix=%q", readResp.Data[:len("payload-zero-backed-first-write")])
	}
}

func TestWriteAndReadWithPebbleReplicaStoresAllocatesChunksForZeroBackedMultiChunkWrite(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-zero-backed-write-2",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-zero-backed-2",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("ABCDEFGH"),
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked {
		t.Fatalf("writeResp=%+v", writeResp)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != metadata.AllocationKindData || page.Extents[0].ChunkCount != 2 || page.Extents[0].PhysicalChunkStart == 0 {
		t.Fatalf("allocation page extents=%+v", page.Extents)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte("ABCDEFGH")) {
		t.Fatalf("read payload=%q want=%q", readResp.Data, "ABCDEFGH")
	}
}

func TestWriteServiceBootstrapsLegacyAllocationMapOnFirstWrite(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, mapping := range []metadata.ExtentMappingRecord{
		{ExtentID: 1, VolumeID: "00a1b2c3", LogicalOffset: 0, LengthBytes: 4, ChunkID: 11, PlacementRef: "pl-1", Revision: 10},
		{ExtentID: 2, VolumeID: "00a1b2c3", LogicalOffset: 4, LengthBytes: 4, ChunkID: 21, PlacementRef: "pl-2", Revision: 10},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}
	for _, replicaSet := range []metadata.ReplicaSetState{
		{
			ReplicaSetID: "pl-1",
			VolumeID:     "00a1b2c3",
			PlacementRef: "pl-1",
			Epoch:        5,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
			},
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			FailureDomains:   []string{"host-a", "host-b", "host-c"},
		},
		{
			ReplicaSetID: "pl-2",
			VolumeID:     "00a1b2c3",
			PlacementRef: "pl-2",
			Epoch:        5,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
			},
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			FailureDomains:   []string{"host-a", "host-b", "host-c"},
		},
	} {
		if err := repo.PutReplicaSet(ctx, replicaSet); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", replicaSet.ReplicaSetID, err)
		}
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()
	for replicaID, objects := range replicaStores.ObjectStores() {
		if err := objects.Put(ctx, localReplicaPayloadKey(replicaID, "00a1b2c3", 1, 11), []byte("ABCD")); err != nil {
			t.Fatalf("seed page0 %s: %v", replicaID, err)
		}
		if err := objects.Put(ctx, localReplicaPayloadKey(replicaID, "00a1b2c3", 2, 21), []byte("WXYZ")); err != nil {
			t.Fatalf("seed page1 %s: %v", replicaID, err)
		}
	}

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-bootstrap-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-bootstrap-1",
		OffsetBytes:    0,
		LengthBytes:    4,
		Data:           []byte("EFGH"),
		PageBytes:      4,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !writeResp.Committed {
		t.Fatalf("writeResp=%+v", writeResp)
	}

	pages, err := repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("allocation pages=%d want=2", len(pages))
	}
	for _, extentID := range []uint64{1, 2} {
		mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", extentID)
		if err != nil {
			t.Fatalf("GetExtentMapping(%d): %v", extentID, err)
		}
		if mapping.ChunkID != 0 {
			t.Fatalf("extent mapping %d=%+v", extentID, mapping)
		}
	}

	page1Resp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    4,
		LengthBytes:    4,
		PageBytes:      4,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read page1: %v", err)
	}
	if !bytes.Equal(page1Resp.Data, []byte("WXYZ")) {
		t.Fatalf("page1 payload=%q want=%q", page1Resp.Data, []byte("WXYZ"))
	}
}

func TestWriteAndReadWithPebbleReplicaStoresUsesAllocationPages(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
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
		Revision:       3,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-write-1",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-1",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("ABCDEFGH"),
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked {
		t.Fatalf("writeResp=%+v", writeResp)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte("ABCDEFGH")) {
		t.Fatalf("read payload=%q want=%q", readResp.Data, "ABCDEFGH")
	}

	writeResp, err = writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-write-2",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-2",
		OffsetBytes:    2,
		LengthBytes:    4,
		Data:           []byte("wxyz"),
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("partial Write: %v", err)
	}
	if !writeResp.Committed || writeResp.State != WriteStateAcked {
		t.Fatalf("partial writeResp=%+v", writeResp)
	}

	readResp, err = readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read after partial write: %v", err)
	}
	want := []byte{'A', 'B', 'w', 'x', 'y', 'z', 'G', 'H'}
	if !bytes.Equal(readResp.Data, want) {
		t.Fatalf("read payload after partial write=%q want=%q", readResp.Data, want)
	}
}

func TestWriteServicePreservesSamePageParallelAllocationChunksAfterBoundaryWrite(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, mapping := range []metadata.ExtentMappingRecord{
		{ExtentID: 1, VolumeID: "00a1b2c3", LogicalOffset: 0, LengthBytes: 64, ChunkID: 1000, PlacementRef: "pl-1", Revision: 10},
		{ExtentID: 2, VolumeID: "00a1b2c3", LogicalOffset: 64, LengthBytes: 64, ChunkID: 2000, PlacementRef: "pl-2", Revision: 10},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}
	replicas := []metadata.ReplicaDescriptor{
		{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
		{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
		{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
	}
	for _, placementRef := range []string{"pl-1", "pl-2"} {
		if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
			ReplicaSetID:     placementRef,
			VolumeID:         "00a1b2c3",
			PlacementRef:     placementRef,
			Epoch:            5,
			Replicas:         replicas,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			FailureDomains:   []string{"host-a", "host-b", "host-c"},
		}); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", placementRef, err)
		}
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-boundary",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-boundary",
		OffsetBytes:    60,
		LengthBytes:    8,
		Data:           []byte("BBBBBBBB"),
		PageBytes:      64,
		ChunkSizeBytes: 8,
	}); err != nil {
		t.Fatalf("boundary Write: %v", err)
	}

	payloads := [][]byte{
		[]byte("0000000011111111"),
		[]byte("2222222233333333"),
		[]byte("4444444455555555"),
		[]byte("6666666677777777"),
	}
	errCh := make(chan error, len(payloads))
	var wg sync.WaitGroup
	for i, payload := range payloads {
		i, payload := i, payload
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := writeSvc.Write(ctx, WriteRequest{
				VolumeID:       "00a1b2c3",
				RequestID:      "req-parallel-" + string(rune('0'+i)),
				AttachmentID:   "att-1",
				Generation:     7,
				IdempotencyKey: "idem-parallel-" + string(rune('0'+i)),
				OffsetBytes:    64 + uint64(i*16),
				LengthBytes:    uint64(len(payload)),
				Data:           payload,
				PageBytes:      64,
				ChunkSizeBytes: 8,
			})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("parallel Write: %v", err)
		}
	}

	for i, want := range payloads {
		resp, err := readSvc.Read(ctx, ReadRequest{
			VolumeID:       "00a1b2c3",
			OffsetBytes:    64 + uint64(i*16),
			LengthBytes:    uint64(len(want)),
			PageBytes:      64,
			ChunkSizeBytes: 8,
		})
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if !bytes.Equal(resp.Data, want) {
			t.Fatalf("parallel payload %d mismatch: got=%q want=%q", i, resp.Data, want)
		}
	}
}

func TestWriteServiceZeroSemanticPromotesFullChunkToAllocationZero(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
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
		Revision:       3,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-zero-seed",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-zero-seed",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("ABCDEFGH"),
		PageBytes:      8,
		ChunkSizeBytes: 4,
	}); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, metadata.SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    metadata.SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        11,
		AllocationChunkSizeBytes: 4,
		AllocationPageSizeBytes:  8,
		SourceSizeBytes:          8,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	sourcePage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage before zero: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []metadata.AllocationPageRecord{sourcePage}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	writeResp, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-zero-full",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-zero-full",
		OffsetBytes:    0,
		LengthBytes:    4,
		Data:           []byte{0, 0, 0, 0},
		PageBytes:      8,
		ChunkSizeBytes: 4,
		ZeroSemantic:   true,
	})
	if err != nil {
		t.Fatalf("zero Write: %v", err)
	}
	if !writeResp.Committed || writeResp.Revision != 12 {
		t.Fatalf("writeResp=%+v", writeResp)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 12 {
		t.Fatalf("allocation page revision=%d want=12", page.Revision)
	}
	if len(page.Extents) != 2 {
		t.Fatalf("allocation page extents=%+v", page.Extents)
	}
	if page.Extents[0].Kind != metadata.AllocationKindZero || page.Extents[0].LogicalChunkStart != 0 || page.Extents[0].ChunkCount != 1 {
		t.Fatalf("first extent=%+v want zero chunk 0", page.Extents[0])
	}
	if page.Extents[1].Kind != metadata.AllocationKindData || page.Extents[1].LogicalChunkStart != 1 || page.Extents[1].PhysicalChunkStart != 501 {
		t.Fatalf("second extent=%+v want data chunk 1 -> 501", page.Extents[1])
	}
	operation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "write-6964656d2d616c6c6f632d7a65726f2d66756c6c")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if len(operation.RetiredPhysicalChunkIDs) != 1 || operation.RetiredPhysicalChunkIDs[0] != 500 {
		t.Fatalf("retired physical chunk ids=%v want=[500]", operation.RetiredPhysicalChunkIDs)
	}
	payloadGCOp, err := repo.GetMutationOperation(ctx, "00a1b2c3", metadata.PayloadGCMutationOperationID("00a1b2c3"))
	if err != nil {
		t.Fatalf("GetMutationOperation(payload-gc): %v", err)
	}
	if payloadGCOp.State != metadata.MutationOperationPending {
		t.Fatalf("payload-gc state=%q want=%q", payloadGCOp.State, metadata.MutationOperationPending)
	}
	if len(payloadGCOp.AffectedExtentIDs) != 1 || payloadGCOp.AffectedExtentIDs[0] != 1 {
		t.Fatalf("payload-gc affected extents=%v want=[1]", payloadGCOp.AffectedExtentIDs)
	}
	if len(payloadGCOp.AffectedPageNos) != 1 || payloadGCOp.AffectedPageNos[0] != 0 {
		t.Fatalf("payload-gc affected pages=%v want=[0]", payloadGCOp.AffectedPageNos)
	}
	if len(payloadGCOp.RetiredPhysicalChunkIDs) != 1 || payloadGCOp.RetiredPhysicalChunkIDs[0] != 500 {
		t.Fatalf("payload-gc retired chunks=%v want=[500]", payloadGCOp.RetiredPhysicalChunkIDs)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read after zero: %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte{0, 0, 0, 0, 'E', 'F', 'G', 'H'}) {
		t.Fatalf("read payload after zero=%v", readResp.Data)
	}

	snapshotRead, err := readSvc.ReadSnapshot(ctx, snapshotID, ReadRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-zero-snapshot-read",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("ReadSnapshot after zero-semantic discard: %v", err)
	}
	if !bytes.Equal(snapshotRead.Data, []byte("ABCDEFGH")) {
		t.Fatalf("snapshot payload after discard=%q want ABCDEFGH", snapshotRead.Data)
	}
}

func TestWriteServiceZeroSemanticKeepsPartialChunkAsDataAllocation(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	if err := repo.PutVolumeState(ctx, metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             5,
		Revision:          10,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, metadata.ExtentMappingRecord{
		ExtentID:      1,
		VolumeID:      "00a1b2c3",
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
		Revision:       3,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, metadata.ReplicaSetState{
		ReplicaSetID: "pl-1",
		VolumeID:     "00a1b2c3",
		PlacementRef: "pl-1",
		Epoch:        5,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	replicaStores, err := payload.OpenReplicaStores(filepath.Join(t.TempDir(), "payload"), []string{"rep-a", "rep-b", "rep-c"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicaStores.Close()

	resolver := metadata.NewService(repo)
	coordinator := NewCoordinator(resolver, resolver)
	executor := NewExecutor(repo, coordinator)
	writeSvc := NewWriteService(executor, NewLocalReplicaWriter(replicaStores.ObjectStores()))
	readSvc := NewReadService(coordinator, NewLocalReplicaReader(replicaStores.ObjectStores()))

	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-zero-partial-seed",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-zero-partial-seed",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("ABCDEFGH"),
		PageBytes:      8,
		ChunkSizeBytes: 4,
	}); err != nil {
		t.Fatalf("seed Write: %v", err)
	}

	if _, err := writeSvc.Write(ctx, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-alloc-zero-partial",
		AttachmentID:   "att-1",
		Generation:     7,
		IdempotencyKey: "idem-alloc-zero-partial",
		OffsetBytes:    1,
		LengthBytes:    2,
		Data:           []byte{0, 0},
		PageBytes:      8,
		ChunkSizeBytes: 4,
		ZeroSemantic:   true,
	}); err != nil {
		t.Fatalf("partial zero Write: %v", err)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != metadata.AllocationKindData || page.Extents[0].ChunkCount != 2 {
		t.Fatalf("allocation page extents=%+v want data extent preserved", page.Extents)
	}

	readResp, err := readSvc.Read(ctx, ReadRequest{
		VolumeID:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    8,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("Read after partial zero: %v", err)
	}
	if !bytes.Equal(readResp.Data, []byte{'A', 0, 0, 'D', 'E', 'F', 'G', 'H'}) {
		t.Fatalf("read payload after partial zero=%v", readResp.Data)
	}
}

func TestWriteServiceRecoversMatchingPendingIntent(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:       zeroExtent(1),
					PlacementRef: "pl-1",
					ReplicaSetID: "rs-1",
					Primary:      ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks: 2,
				},
			},
		},
	})
	svc := NewWriteService(executor, fakeReplicaWriter{
		results: map[uint64]*ReplicaWriteResult{
			1: {AckedReplicaIDs: []string{"rep-a", "rep-b"}},
		},
	})

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-pending-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
		Data:           []byte("abcd"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if resp == nil || !resp.Committed {
		t.Fatalf("response=%+v", resp)
	}
}

func TestWriteServiceWithLocalReplicaWriterPersistsPayload(t *testing.T) {
	storeMeta := newFakeIntentStore()
	executor := NewExecutor(storeMeta, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent: metadata.ExtentMappingRecord{
						VolumeID:      "00a1b2c3",
						ExtentID:      1,
						LogicalOffset: 0,
						LengthBytes:   8,
						ChunkID:       101,
						PlacementRef:  "pl-1",
						Revision:      11,
					},
					PlacementRef:     "pl-1",
					ReplicaSetID:     "rs-1",
					Primary:          ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets:     []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks:     2,
					MetadataRevision: 11,
				},
			},
		},
	})
	localA := store.NewMemoryStore()
	localB := store.NewMemoryStore()
	writer := NewLocalReplicaWriter(map[string]store.ObjectStore{
		"rep-a": localA,
		"rep-b": localB,
	})
	svc := NewWriteService(executor, writer)

	resp, err := svc.Write(context.Background(), WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-local-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-local-1",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("abcdefgh"),
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !resp.Committed || resp.State != WriteStateAcked {
		t.Fatalf("response=%+v", resp)
	}

	key := localReplicaPayloadKey("rep-a", "00a1b2c3", 1, 101)
	got, found, err := localA.Get(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("localA Get err=%v found=%v", err, found)
	}
	if string(got) != "abcdefgh" {
		t.Fatalf("localA payload=%q want=%q", got, "abcdefgh")
	}
}
