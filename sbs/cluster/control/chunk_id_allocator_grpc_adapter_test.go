package control

import (
	"context"
	"errors"
	"testing"

	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
)

type fakeChunkIDAllocatorServiceClient struct {
	req   *internalv1.AllocateChunkIDsRequest
	resp  *internalv1.AllocateChunkIDsResponse
	err   error
	calls int
}

func (c *fakeChunkIDAllocatorServiceClient) AllocateChunkIDs(_ context.Context, req *internalv1.AllocateChunkIDsRequest, _ ...grpc.CallOption) (*internalv1.AllocateChunkIDsResponse, error) {
	c.req = req
	c.calls++
	return c.resp, c.err
}

func TestGRPCChunkIDAllocatorAdapterAllocatesChunkIDs(t *testing.T) {
	client := &fakeChunkIDAllocatorServiceClient{
		resp: &internalv1.AllocateChunkIDsResponse{
			VolumeId:     "00a1b2c3",
			Count:        3,
			StartChunkId: 11,
		},
	}
	adapter := NewGRPCChunkIDAllocatorAdapter(client)
	startID, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 3)
	if err != nil {
		t.Fatalf("AllocateChunkIDs: %v", err)
	}
	if startID != 11 {
		t.Fatalf("start_id=%d want 11", startID)
	}
	if client.calls != 1 {
		t.Fatalf("calls=%d want 1", client.calls)
	}
	if client.req.GetVolumeId() != "00a1b2c3" || client.req.GetCount() != 3 {
		t.Fatalf("unexpected request: %+v", client.req)
	}
}

func TestGRPCChunkIDAllocatorAdapterPropagatesTransportError(t *testing.T) {
	expected := errors.New("allocator unavailable")
	adapter := NewGRPCChunkIDAllocatorAdapter(&fakeChunkIDAllocatorServiceClient{err: expected})
	_, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 1)
	if !errors.Is(err, expected) {
		t.Fatalf("AllocateChunkIDs error=%v want %v", err, expected)
	}
}

func TestGRPCChunkIDAllocatorAdapterRejectsMismatchedResponse(t *testing.T) {
	adapter := NewGRPCChunkIDAllocatorAdapter(&fakeChunkIDAllocatorServiceClient{
		resp: &internalv1.AllocateChunkIDsResponse{
			VolumeId:     "00a1b2c3",
			Count:        2,
			StartChunkId: 11,
		},
	})
	_, err := adapter.AllocateChunkIDs(context.Background(), "00a1b2c3", 1)
	if !errors.Is(err, ErrInvalidChunkIDAllocatorRequest) {
		t.Fatalf("AllocateChunkIDs error=%v want invalid request", err)
	}
}
