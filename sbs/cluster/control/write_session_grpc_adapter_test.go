package control

import (
	"context"
	"errors"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeWriteSessionGRPCClient struct {
	req        *internalv1.CommitWriteStateRequest
	pageReq    *internalv1.CommitPageScopedWriteMetadataRequest
	rangeReq   *internalv1.CommitRangeLocalWriteStateRequest
	appendReq  *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest
	cloneReq   *internalv1.CommitCloneDeltaAllocationPagesRequest
	intentReq  *internalv1.PutWriteIntentRequest
	resp       *internalv1.CommitWriteStateResponse
	pageResp   *internalv1.CommitPageScopedWriteMetadataResponse
	rangeResp  *internalv1.CommitRangeLocalWriteStateResponse
	appendResp *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse
	cloneResp  *internalv1.CommitCloneDeltaAllocationPagesResponse
	err        error
}

func (c *fakeWriteSessionGRPCClient) CommitWriteState(ctx context.Context, req *internalv1.CommitWriteStateRequest, opts ...grpc.CallOption) (*internalv1.CommitWriteStateResponse, error) {
	c.req = req
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}

func (c *fakeWriteSessionGRPCClient) CommitPageScopedWriteMetadata(ctx context.Context, req *internalv1.CommitPageScopedWriteMetadataRequest, opts ...grpc.CallOption) (*internalv1.CommitPageScopedWriteMetadataResponse, error) {
	c.pageReq = req
	if c.err != nil {
		return nil, c.err
	}
	return c.pageResp, nil
}

func (c *fakeWriteSessionGRPCClient) CommitRangeLocalWriteState(ctx context.Context, req *internalv1.CommitRangeLocalWriteStateRequest, opts ...grpc.CallOption) (*internalv1.CommitRangeLocalWriteStateResponse, error) {
	c.rangeReq = req
	if c.err != nil {
		return nil, c.err
	}
	return c.rangeResp, nil
}

func (c *fakeWriteSessionGRPCClient) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest, opts ...grpc.CallOption) (*internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse, error) {
	c.appendReq = req
	if c.err != nil {
		return nil, c.err
	}
	return c.appendResp, nil
}

func (c *fakeWriteSessionGRPCClient) CommitCloneDeltaAllocationPages(ctx context.Context, req *internalv1.CommitCloneDeltaAllocationPagesRequest, opts ...grpc.CallOption) (*internalv1.CommitCloneDeltaAllocationPagesResponse, error) {
	c.cloneReq = req
	if c.err != nil {
		return nil, c.err
	}
	if c.cloneResp != nil {
		return c.cloneResp, nil
	}
	return &internalv1.CommitCloneDeltaAllocationPagesResponse{}, nil
}

func (c *fakeWriteSessionGRPCClient) GetVolumeState(context.Context, *internalv1.GetVolumeStateRequest, ...grpc.CallOption) (*internalv1.GetVolumeStateResponse, error) {
	return nil, c.err
}

func (c *fakeWriteSessionGRPCClient) PutVolumeState(context.Context, *internalv1.PutVolumeStateRequest, ...grpc.CallOption) (*internalv1.PutVolumeStateResponse, error) {
	return &internalv1.PutVolumeStateResponse{}, c.err
}

func (c *fakeWriteSessionGRPCClient) GetIdempotencyRecord(context.Context, *internalv1.GetIdempotencyRecordRequest, ...grpc.CallOption) (*internalv1.GetIdempotencyRecordResponse, error) {
	return nil, c.err
}

func (c *fakeWriteSessionGRPCClient) PutIdempotencyRecord(context.Context, *internalv1.PutIdempotencyRecordRequest, ...grpc.CallOption) (*internalv1.PutIdempotencyRecordResponse, error) {
	return &internalv1.PutIdempotencyRecordResponse{}, c.err
}

func (c *fakeWriteSessionGRPCClient) GetMutationOperation(context.Context, *internalv1.GetMutationOperationRequest, ...grpc.CallOption) (*internalv1.GetMutationOperationResponse, error) {
	return nil, c.err
}

func (c *fakeWriteSessionGRPCClient) PutMutationOperation(context.Context, *internalv1.PutMutationOperationRequest, ...grpc.CallOption) (*internalv1.PutMutationOperationResponse, error) {
	return &internalv1.PutMutationOperationResponse{}, c.err
}

func (c *fakeWriteSessionGRPCClient) PutWriteIntent(_ context.Context, req *internalv1.PutWriteIntentRequest, _ ...grpc.CallOption) (*internalv1.PutWriteIntentResponse, error) {
	c.intentReq = req
	return &internalv1.PutWriteIntentResponse{}, c.err
}

func TestGRPCWriteSessionAdapterDelegatesCommitWriteState(t *testing.T) {
	client := &fakeWriteSessionGRPCClient{
		resp: CommitWriteStateResponseToProto(
			metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 8},
			metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1", ResultState: metadata.IdempotencyCommitted, Revision: 8},
		),
	}
	adapter := NewGRPCWriteSessionAdapter(client)

	state, record, err := adapter.CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{
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
	if client.req.GetVolumeId() != "00a1b2c3" || client.req.GetCommittedRevision() != 8 {
		t.Fatalf("unexpected request: %+v", client.req)
	}
	if state.Revision != 8 || record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("unexpected response: state=%+v record=%+v", state, record)
	}
}

func TestGRPCWriteSessionAdapterDelegatesPutWriteIntent(t *testing.T) {
	client := &fakeWriteSessionGRPCClient{}
	adapter := NewGRPCWriteSessionAdapter(client)

	err := adapter.PutWriteIntent(context.Background(),
		metadata.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-intent",
			ResultState:    metadata.IdempotencyPending,
		},
		metadata.MutationOperationRecord{
			VolumeID:       "00a1b2c3",
			OperationID:    "write-idem-intent",
			State:          metadata.MutationOperationRunning,
			IdempotencyKey: "idem-intent",
		},
	)
	if err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	if client.intentReq == nil ||
		client.intentReq.GetIdempotencyRecord().GetIdempotencyKey() != "idem-intent" ||
		client.intentReq.GetMutationOperation().GetOperationId() != "write-idem-intent" {
		t.Fatalf("unexpected intent request: %+v", client.intentReq)
	}
}

func TestGRPCWriteSessionAdapterDelegatesPageScopedCommit(t *testing.T) {
	client := &fakeWriteSessionGRPCClient{
		pageResp: CommitPageScopedWriteMetadataResponseToProto(
			metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7},
			metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-page", ResultState: metadata.IdempotencyCommitted, Revision: 4},
		),
	}
	adapter := NewGRPCWriteSessionAdapter(client)

	state, record, err := adapter.CommitPageScopedWriteMetadata(context.Background(), metadata.CommitWriteMetadataRequest{
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
		MutationOperationID:   "write-idem-page",
		ExpectedMutationState: metadata.MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata: %v", err)
	}
	if client.pageReq.GetVolumeId() != "00a1b2c3" || client.pageReq.GetAllocationPages()[0].GetRevision() != 3 {
		t.Fatalf("unexpected request: %+v", client.pageReq)
	}
	if state.Revision != 7 || record.Revision != 4 {
		t.Fatalf("unexpected response: state=%+v record=%+v", state, record)
	}
}

func TestGRPCWriteSessionAdapterDelegatesRangeLocalCommit(t *testing.T) {
	client := &fakeWriteSessionGRPCClient{
		rangeResp: CommitRangeLocalWriteStateResponseToProto(
			metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7},
			metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-range", ResultState: metadata.IdempotencyCommitted, Revision: 4},
		),
	}
	adapter := NewGRPCWriteSessionAdapter(client)

	state, record, err := adapter.CommitRangeLocalWriteState(context.Background(), metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-range",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
			},
		},
		MutationOperationID:   "write-idem-range",
		ExpectedMutationState: metadata.MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
	})
	if err != nil {
		t.Fatalf("CommitRangeLocalWriteState: %v", err)
	}
	if client.rangeReq.GetVolumeId() != "00a1b2c3" || client.rangeReq.GetAllocationPages()[0].GetRevision() != 3 {
		t.Fatalf("unexpected request: %+v", client.rangeReq)
	}
	if state.Revision != 7 || record.Revision != 4 {
		t.Fatalf("unexpected response: state=%+v record=%+v", state, record)
	}
}

func TestGRPCWriteSessionAdapterRequiresClient(t *testing.T) {
	_, _, err := NewGRPCWriteSessionAdapter(nil).CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	_, _, err = NewGRPCWriteSessionAdapter(nil).CommitPageScopedWriteMetadata(context.Background(), metadata.CommitWriteMetadataRequest{})
	if err == nil {
		t.Fatal("expected page-scoped error")
	}
	_, _, err = NewGRPCWriteSessionAdapter(nil).CommitRangeLocalWriteState(context.Background(), metadata.CommitWriteMetadataRequest{})
	if err == nil {
		t.Fatal("expected range-local error")
	}
	if err := NewGRPCWriteSessionAdapter(nil).CommitCloneDeltaAllocationPages(context.Background(), "clone-1", nil); err == nil {
		t.Fatal("expected clone delta error")
	}
}

func TestGRPCWriteSessionAdapterDelegatesCloneDeltaCommit(t *testing.T) {
	client := &fakeWriteSessionGRPCClient{}
	adapter := NewGRPCWriteSessionAdapter(client)

	err := adapter.CommitCloneDeltaAllocationPages(context.Background(), "clone-1", []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 41},
		},
	}})
	if err != nil {
		t.Fatalf("CommitCloneDeltaAllocationPages: %v", err)
	}
	if client.cloneReq.GetCloneId() != "clone-1" || len(client.cloneReq.GetAllocationPages()) != 1 || client.cloneReq.GetAllocationPages()[0].GetExtents()[0].GetPhysicalChunkStart() != 41 {
		t.Fatalf("unexpected clone request: %+v", client.cloneReq)
	}
}

func TestGRPCWriteSessionAdapterPropagatesError(t *testing.T) {
	expected := errors.New("transport unavailable")
	_, _, err := NewGRPCWriteSessionAdapter(&fakeWriteSessionGRPCClient{err: expected}).CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{
		ExpectedIdempotencyState: metadata.IdempotencyPending,
	})
	if !errors.Is(err, expected) {
		t.Fatalf("error=%v want %v", err, expected)
	}
}

func TestGRPCWriteSessionAdapterTranslatesNotFound(t *testing.T) {
	adapter := NewGRPCWriteSessionAdapter(&fakeWriteSessionGRPCClient{err: status.Error(codes.NotFound, metadata.ErrNotFound.Error())})

	if _, err := adapter.GetIdempotencyRecord(context.Background(), "00a1b2c3", "idem-1"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetIdempotencyRecord error=%v want %v", err, metadata.ErrNotFound)
	}
	if _, err := adapter.GetMutationOperation(context.Background(), "00a1b2c3", "op-1"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetMutationOperation error=%v want %v", err, metadata.ErrNotFound)
	}
	if _, err := adapter.GetVolumeState(context.Background(), "00a1b2c3"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetVolumeState error=%v want %v", err, metadata.ErrNotFound)
	}
}

func TestGRPCWriteSessionAdapterTranslatesCASConflict(t *testing.T) {
	adapter := NewGRPCWriteSessionAdapter(&fakeWriteSessionGRPCClient{err: status.Error(codes.Aborted, metadata.ErrCASConflict.Error())})

	_, _, err := adapter.CommitWriteState(context.Background(), metadata.CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
	})
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("CommitWriteState error=%v want %v", err, metadata.ErrCASConflict)
	}
	_, _, err = adapter.CommitPageScopedWriteMetadata(context.Background(), metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
	})
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("CommitPageScopedWriteMetadata error=%v want %v", err, metadata.ErrCASConflict)
	}
}
