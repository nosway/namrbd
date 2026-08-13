package control

import (
	"context"
	"errors"
	"testing"
	"time"

	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServeAllocateChunkIDsDelegatesToInternalService(t *testing.T) {
	service := &fakeChunkIDAllocatorInternalService{start: 17}
	var records []string
	resp, err := ServeAllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "00a1b2c3",
		Count:    4,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeAllocateChunkIDs: %v", err)
	}
	if service.calls != 1 {
		t.Fatalf("calls=%d want 1", service.calls)
	}
	if service.req.VolumeID != "00a1b2c3" || service.req.Count != 4 {
		t.Fatalf("unexpected request: %+v", service.req)
	}
	if resp.GetVolumeId() != "00a1b2c3" || resp.GetCount() != 4 || resp.GetStartChunkId() != 17 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeAllocateChunkIDsMapsInvalidRequest(t *testing.T) {
	service := &fakeChunkIDAllocatorInternalService{start: 17}
	var records []string
	_, err := ServeAllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "not-a-volume",
		Count:    1,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if service.calls != 0 {
		t.Fatalf("calls=%d want 0", service.calls)
	}
	if len(records) != 1 || records[0] != string(ChunkIDAllocatorErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeAllocateChunkIDsMapsInternalServiceError(t *testing.T) {
	expected := errors.New("allocator unavailable")
	_, err := ServeAllocateChunkIDs(context.Background(), &internalv1.AllocateChunkIDsRequest{
		VolumeId: "00a1b2c3",
		Count:    1,
	}, &fakeChunkIDAllocatorInternalService{err: expected}, nil)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status=%v err=%v want internal", status.Code(err), err)
	}
}
