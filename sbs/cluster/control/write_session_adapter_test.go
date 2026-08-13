package control

import (
	"context"
	"errors"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakeWriteSessionInternalService struct {
	state         metadata.VolumeState
	idem          metadata.IdempotencyRecord
	operation     metadata.MutationOperationRecord
	commitReq     metadata.CommitWriteStateRequest
	pageCommitReq metadata.CommitWriteMetadataRequest
	cloneID       string
	clonePages    []metadata.AllocationPageRecord
	called        string
	err           error
	commitErr     error
}

func (s *fakeWriteSessionInternalService) GetVolumeState(context.Context, string) (metadata.VolumeState, error) {
	s.called = "get_volume_state"
	return s.state, s.err
}

func (s *fakeWriteSessionInternalService) PutVolumeState(_ context.Context, state metadata.VolumeState) error {
	s.called = "put_volume_state"
	s.state = state
	return s.err
}

func (s *fakeWriteSessionInternalService) GetIdempotencyRecord(context.Context, string, string) (metadata.IdempotencyRecord, error) {
	s.called = "get_idempotency"
	return s.idem, s.err
}

func (s *fakeWriteSessionInternalService) PutIdempotencyRecord(_ context.Context, rec metadata.IdempotencyRecord) error {
	s.called = "put_idempotency"
	s.idem = rec
	return s.err
}

func (s *fakeWriteSessionInternalService) GetMutationOperation(context.Context, string, string) (metadata.MutationOperationRecord, error) {
	s.called = "get_mutation"
	return s.operation, s.err
}

func (s *fakeWriteSessionInternalService) PutMutationOperation(_ context.Context, rec metadata.MutationOperationRecord) error {
	s.called = "put_mutation"
	s.operation = rec
	return s.err
}

func (s *fakeWriteSessionInternalService) PutWriteIntent(_ context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	s.called = "put_write_intent"
	s.idem = record
	s.operation = operation
	return s.err
}

func (s *fakeWriteSessionInternalService) CommitWriteState(_ context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	s.called = "commit_write_state"
	s.commitReq = req
	return s.state, s.idem, s.commitErr
}

func (s *fakeWriteSessionInternalService) CommitPageScopedWriteMetadata(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	s.called = "commit_page_scoped_write_metadata"
	s.pageCommitReq = req
	return s.state, s.idem, s.commitErr
}

func (s *fakeWriteSessionInternalService) CommitRangeLocalWriteState(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	s.called = "commit_range_local_write_state"
	s.pageCommitReq = req
	return s.state, s.idem, s.commitErr
}

func (s *fakeWriteSessionInternalService) CommitAppendOnlyWriteStateAndQueueEffects(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	s.called = "commit_append_only_write_state_and_queue_effects"
	s.pageCommitReq = req
	return s.state, s.idem, s.commitErr
}

func (s *fakeWriteSessionInternalService) CommitCloneDeltaAllocationPages(_ context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	s.called = "commit_clone_delta_allocation_pages"
	s.cloneID = cloneID
	s.clonePages = append([]metadata.AllocationPageRecord(nil), pages...)
	return s.commitErr
}

func TestRepositoryBackedWriteSessionInternalServiceCommitsWriteState(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-1",
		ResultState:    metadata.IdempotencyPending,
		Revision:       7,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	service := NewRepositoryBackedWriteSessionInternalService(repo)
	state, idem, err := service.CommitWriteState(ctx, metadata.CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
	})
	if err != nil {
		t.Fatalf("CommitWriteState: %v", err)
	}
	if state.Revision != 8 {
		t.Fatalf("state revision=%d want 8", state.Revision)
	}
	if idem.ResultState != metadata.IdempotencyCommitted || idem.Revision != 8 {
		t.Fatalf("unexpected idempotency record: %+v", idem)
	}
}

func TestRepositoryBackedWriteSessionInternalServicePutsWriteIntent(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	service := NewRepositoryBackedWriteSessionInternalService(repo)
	record := metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-intent",
		ResultState:    metadata.IdempotencyPending,
		Revision:       7,
	}
	operation := metadata.MutationOperationRecord{
		VolumeID:           "00a1b2c3",
		OperationID:        "write-idem-intent",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		IdempotencyKey:     "idem-intent",
		PlacementRevision:  7,
		AllocationRevision: 8,
	}
	if err := service.PutWriteIntent(ctx, record, operation); err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	gotRecord, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-intent")
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	gotOperation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "write-idem-intent")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if gotRecord.ResultState != metadata.IdempotencyPending || gotOperation.State != metadata.MutationOperationRunning {
		t.Fatalf("unexpected intent state: record=%+v operation=%+v", gotRecord, gotOperation)
	}
}

func TestRepositoryBackedWriteSessionInternalServiceCommitsPageScopedWriteMetadata(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       3,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-page",
		ResultState:    metadata.IdempotencyPending,
		Revision:       7,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	service := NewRepositoryBackedWriteSessionInternalService(repo)
	state, idem, err := service.CommitPageScopedWriteMetadata(ctx, metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-page",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
				Extents: []metadata.AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata: %v", err)
	}
	if state.Revision != 7 || idem.ResultState != metadata.IdempotencyCommitted || idem.Revision != 4 {
		t.Fatalf("unexpected commit result: state=%+v idem=%+v", state, idem)
	}
}

func TestRepositoryBackedWriteSessionInternalServiceInlineAppendOnlyEffectsPersistsAllocation(t *testing.T) {
	repo := metadata.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()
	if err := repo.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-append-inline",
		ResultState:    metadata.IdempotencyPending,
		Revision:       7,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:        "write-idem-append-inline",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 7,
		IdempotencyKey:     "idem-append-inline",
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	header := &metadata.PayloadEncryptionHeader{
		HeaderVersion:   metadata.PayloadEncryptionHeaderVersion,
		CipherSuite:     "aes_256_gcm",
		ObjectID:        "replicated:00a1b2c3:101",
		BackendType:     metadata.PhysicalObjectBackendReplicated,
		PlaintextLength: 4096,
	}

	service := NewRepositoryBackedWriteSessionInternalServiceWithInlineEffects(repo)
	_, idem, err := service.CommitAppendOnlyWriteStateAndQueueEffects(ctx, metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-append-inline",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
		MutationOperationID:      "write-idem-append-inline",
		ExpectedMutationState:    metadata.MutationOperationRunning,
		AffectedPageNos:          []uint64{0},
		AllocationPages: []metadata.AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Extents: []metadata.AllocationExtentRecord{{
				LogicalChunkStart:  0,
				ChunkCount:         1,
				Kind:               metadata.AllocationKindData,
				PhysicalChunkStart: 101,
				Encryption:         header,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteStateAndQueueEffects: %v", err)
	}
	if idem.ResultState != metadata.IdempotencyCommitted || idem.Revision == 0 {
		t.Fatalf("idempotency=%+v want committed append-only revision", idem)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Encryption == nil || page.Extents[0].Encryption.ObjectID != header.ObjectID {
		t.Fatalf("allocation page did not preserve inline append-only effects header: %+v", page.Extents)
	}
}

func TestServiceBackedWriteSessionAdapterDelegatesRecords(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	adapter := NewServiceBackedWriteSessionAdapter(service)
	ctx := context.Background()

	if err := adapter.PutVolumeState(ctx, metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 1}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if service.called != "put_volume_state" || service.state.VolumeID != "00a1b2c3" {
		t.Fatalf("unexpected volume state delegation: called=%q state=%+v", service.called, service.state)
	}

	if err := adapter.PutIdempotencyRecord(ctx, metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1"}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	if service.called != "put_idempotency" || service.idem.IdempotencyKey != "idem-1" {
		t.Fatalf("unexpected idempotency delegation: called=%q idem=%+v", service.called, service.idem)
	}

	if err := adapter.PutMutationOperation(ctx, metadata.MutationOperationRecord{VolumeID: "00a1b2c3", OperationID: "op-1"}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	if service.called != "put_mutation" || service.operation.OperationID != "op-1" {
		t.Fatalf("unexpected mutation delegation: called=%q operation=%+v", service.called, service.operation)
	}

	if err := adapter.PutWriteIntent(ctx,
		metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-intent"},
		metadata.MutationOperationRecord{VolumeID: "00a1b2c3", OperationID: "write-idem-intent"},
	); err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	if service.called != "put_write_intent" || service.idem.IdempotencyKey != "idem-intent" || service.operation.OperationID != "write-idem-intent" {
		t.Fatalf("unexpected write intent delegation: called=%q idem=%+v operation=%+v", service.called, service.idem, service.operation)
	}
}

func TestServiceBackedWriteSessionAdapterDelegatesCommitWriteState(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 12},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-commit", ResultState: metadata.IdempotencyCommitted},
	}
	adapter := NewServiceBackedWriteSessionAdapter(service)

	state, idem, err := adapter.CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{
		VolumeID:          "00a1b2c3",
		IdempotencyKey:    "idem-commit",
		CommittedRevision: 12,
	})
	if err != nil {
		t.Fatalf("CommitWriteState: %v", err)
	}
	if service.called != "commit_write_state" || service.commitReq.IdempotencyKey != "idem-commit" {
		t.Fatalf("unexpected commit delegation: called=%q req=%+v", service.called, service.commitReq)
	}
	if state.Revision != 12 || idem.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("unexpected commit response: state=%+v idem=%+v", state, idem)
	}
}

func TestServiceBackedWriteSessionAdapterDelegatesPageScopedCommit(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-page", ResultState: metadata.IdempotencyCommitted, Revision: 4},
	}
	adapter := NewServiceBackedWriteSessionAdapter(service)

	state, idem, err := adapter.CommitPageScopedWriteMetadata(context.Background(), metadata.CommitWriteMetadataRequest{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-page",
		AllocationPages: []metadata.AllocationPageRecord{
			{VolumeID: "00a1b2c3", PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 3},
		},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata: %v", err)
	}
	if service.called != "commit_page_scoped_write_metadata" || service.pageCommitReq.IdempotencyKey != "idem-page" {
		t.Fatalf("unexpected page commit delegation: called=%q req=%+v", service.called, service.pageCommitReq)
	}
	if state.Revision != 7 || idem.Revision != 4 {
		t.Fatalf("unexpected commit response: state=%+v idem=%+v", state, idem)
	}
}

func TestServiceBackedWriteSessionAdapterDelegatesRangeLocalCommit(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-range", ResultState: metadata.IdempotencyCommitted, Revision: 4},
	}
	adapter := NewServiceBackedWriteSessionAdapter(service)

	state, idem, err := adapter.CommitRangeLocalWriteState(context.Background(), metadata.CommitWriteMetadataRequest{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-range",
		AllocationPages: []metadata.AllocationPageRecord{
			{VolumeID: "00a1b2c3", PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 3},
		},
	})
	if err != nil {
		t.Fatalf("CommitRangeLocalWriteState: %v", err)
	}
	if service.called != "commit_range_local_write_state" || service.pageCommitReq.IdempotencyKey != "idem-range" {
		t.Fatalf("unexpected range-local commit delegation: called=%q req=%+v", service.called, service.pageCommitReq)
	}
	if state.Revision != 7 || idem.Revision != 4 {
		t.Fatalf("unexpected commit response: state=%+v idem=%+v", state, idem)
	}
}

func TestServiceBackedWriteSessionAdapterPropagatesError(t *testing.T) {
	expected := errors.New("write session unavailable")
	adapter := NewServiceBackedWriteSessionAdapter(&fakeWriteSessionInternalService{err: expected})

	_, err := adapter.GetVolumeState(context.Background(), "00a1b2c3")
	if !errors.Is(err, expected) {
		t.Fatalf("error=%v want %v", err, expected)
	}
}

func TestServiceBackedWriteSessionAdapterRequiresInternalService(t *testing.T) {
	adapter := NewServiceBackedWriteSessionAdapter(nil)

	_, err := adapter.GetVolumeState(context.Background(), "00a1b2c3")
	if err == nil {
		t.Fatal("expected error")
	}
	if err := adapter.PutVolumeState(context.Background(), metadata.VolumeState{VolumeID: "00a1b2c3"}); err == nil {
		t.Fatal("expected PutVolumeState error")
	}
	_, _, err = adapter.CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{VolumeID: "00a1b2c3"})
	if err == nil {
		t.Fatal("expected CommitWriteState error")
	}
	_, _, err = adapter.CommitPageScopedWriteMetadata(context.Background(), metadata.CommitWriteMetadataRequest{VolumeID: "00a1b2c3"})
	if err == nil {
		t.Fatal("expected CommitPageScopedWriteMetadata error")
	}
	_, _, err = adapter.CommitRangeLocalWriteState(context.Background(), metadata.CommitWriteMetadataRequest{VolumeID: "00a1b2c3"})
	if err == nil {
		t.Fatal("expected CommitRangeLocalWriteState error")
	}
}
