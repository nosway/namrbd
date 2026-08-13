package control

import (
	"strings"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func TestChunkIDAllocationRequestProtoRoundTrip(t *testing.T) {
	req := metadata.ChunkIDAllocationRequest{VolumeID: "00a1b2c3", Count: 4}
	got, err := ChunkIDAllocationRequestFromProto(ChunkIDAllocationRequestToProto(req))
	if err != nil {
		t.Fatalf("ChunkIDAllocationRequestFromProto: %v", err)
	}
	if got != req {
		t.Fatalf("request=%+v want %+v", got, req)
	}
}

func TestChunkIDAllocationResponseProtoRoundTrip(t *testing.T) {
	req := metadata.ChunkIDAllocationRequest{VolumeID: "00a1b2c3", Count: 4}
	got, err := ChunkIDAllocationResponseFromProto(ChunkIDAllocationResponseToProto(req, 9), req)
	if err != nil {
		t.Fatalf("ChunkIDAllocationResponseFromProto: %v", err)
	}
	if got != 9 {
		t.Fatalf("start_id=%d want 9", got)
	}
}

func TestChunkIDAllocationResponseRejectsIdentityMismatch(t *testing.T) {
	req := metadata.ChunkIDAllocationRequest{VolumeID: "00a1b2c3", Count: 4}
	_, err := ChunkIDAllocationResponseFromProto(&internalv1.AllocateChunkIDsResponse{
		VolumeId:     "00a1b2c3",
		Count:        3,
		StartChunkId: 9,
	}, req)
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("error=%v want identity mismatch", err)
	}
}
