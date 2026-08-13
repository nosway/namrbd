package control

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakePlacementApplyGRPCClient struct {
	req  *internalv1.ApplyPlacementChangesRequest
	resp *internalv1.ApplyPlacementChangesResponse
	err  error
}

func validPlacementApplyRequest() metadata.PlacementApplyRequest {
	return metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 7,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []metadata.AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 4, Kind: metadata.AllocationKindData, PhysicalChunkStart: 99},
				},
			},
		},
		NormalizeExtentIDs: []uint64{1},
	}
}

func (c *fakePlacementApplyGRPCClient) ApplyPlacementChanges(_ context.Context, req *internalv1.ApplyPlacementChangesRequest, _ ...grpc.CallOption) (*internalv1.ApplyPlacementChangesResponse, error) {
	c.req = req
	if c.err != nil {
		return nil, c.err
	}
	if c.resp != nil {
		return c.resp, nil
	}
	return &internalv1.ApplyPlacementChangesResponse{
		VolumeId:          req.GetVolumeId(),
		CommittedRevision: req.GetCommittedRevision(),
	}, nil
}

func TestGRPCPlacementApplyAdapterDelegatesToClient(t *testing.T) {
	client := &fakePlacementApplyGRPCClient{}
	adapter := NewGRPCPlacementApplyAdapter(client)

	req := validPlacementApplyRequest()
	if err := adapter.ApplyPlacementChanges(context.Background(), req); err != nil {
		t.Fatalf("ApplyPlacementChanges: %v", err)
	}
	if client.req == nil {
		t.Fatal("grpc client was not called")
	}
	if client.req.GetVolumeId() != req.VolumeID || client.req.GetCommittedRevision() != req.CommittedRevision {
		t.Fatalf("request identity=(%q,%d) want=(%q,%d)", client.req.GetVolumeId(), client.req.GetCommittedRevision(), req.VolumeID, req.CommittedRevision)
	}
	if len(client.req.GetAllocationPages()) != 1 || client.req.GetAllocationPages()[0].GetPageNo() != 0 {
		t.Fatalf("allocation pages=%#v", client.req.GetAllocationPages())
	}
}

func TestGRPCPlacementApplyAdapterRequiresClient(t *testing.T) {
	err := NewGRPCPlacementApplyAdapter(nil).ApplyPlacementChanges(context.Background(), validPlacementApplyRequest())
	if err == nil {
		t.Fatal("ApplyPlacementChanges succeeded with nil client")
	}
}

func TestGRPCPlacementApplyAdapterValidatesBeforeCallingClient(t *testing.T) {
	client := &fakePlacementApplyGRPCClient{}
	err := NewGRPCPlacementApplyAdapter(client).ApplyPlacementChanges(context.Background(), metadata.PlacementApplyRequest{
		VolumeID:          "not-a-volume",
		CommittedRevision: 1,
	})
	if !errors.Is(err, metadata.ErrInvalidPlacementApplyRequest) {
		t.Fatalf("ApplyPlacementChanges error=%v want ErrInvalidPlacementApplyRequest", err)
	}
	if client.req != nil {
		t.Fatalf("grpc client was called with invalid request: %#v", client.req)
	}
}

func TestGRPCPlacementApplyAdapterPropagatesStatusError(t *testing.T) {
	var logs bytes.Buffer
	restore := structuredlog.SetOutput(&logs)
	defer restore()

	adapter := NewGRPCPlacementApplyAdapter(&fakePlacementApplyGRPCClient{
		err: status.Error(codes.Aborted, "conflict"),
	})
	err := adapter.ApplyPlacementChanges(context.Background(), validPlacementApplyRequest())
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("ApplyPlacementChanges error=%v want %v", err, metadata.ErrCASConflict)
	}
	for _, want := range []string{
		`"event":"placement_apply_grpc_failed"`,
		`"error_class":"conflict"`,
		`"volume_id":"00a1b2c3"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log missing %q in %s", want, logs.String())
		}
	}
}

func TestGRPCPlacementApplyAdapterTranslatesNotFound(t *testing.T) {
	adapter := NewGRPCPlacementApplyAdapter(&fakePlacementApplyGRPCClient{
		err: status.Error(codes.NotFound, metadata.ErrNotFound.Error()),
	})
	err := adapter.ApplyPlacementChanges(context.Background(), validPlacementApplyRequest())
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("ApplyPlacementChanges error=%v want %v", err, metadata.ErrNotFound)
	}
}

func TestGRPCPlacementApplyAdapterLogsDeadlineAsUnavailable(t *testing.T) {
	var logs bytes.Buffer
	restore := structuredlog.SetOutput(&logs)
	defer restore()

	adapter := NewGRPCPlacementApplyAdapter(&fakePlacementApplyGRPCClient{
		err: status.Error(codes.DeadlineExceeded, "deadline"),
	})
	err := adapter.ApplyPlacementChanges(context.Background(), validPlacementApplyRequest())
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("status.Code=%v want=%v err=%v", got, codes.DeadlineExceeded, err)
	}
	for _, want := range []string{
		`"event":"placement_apply_grpc_failed"`,
		`"error_class":"unavailable"`,
		`"volume_id":"00a1b2c3"`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log missing %q in %s", want, logs.String())
		}
	}
}

func TestGRPCPlacementApplyAdapterRejectsResponseIdentityMismatch(t *testing.T) {
	adapter := NewGRPCPlacementApplyAdapter(&fakePlacementApplyGRPCClient{
		resp: &internalv1.ApplyPlacementChangesResponse{
			VolumeId:          "00a1b2c3",
			CommittedRevision: 999,
		},
	})
	err := adapter.ApplyPlacementChanges(context.Background(), validPlacementApplyRequest())
	if !errors.Is(err, metadata.ErrInvalidPlacementApplyRequest) {
		t.Fatalf("ApplyPlacementChanges error=%v want ErrInvalidPlacementApplyRequest", err)
	}
}
