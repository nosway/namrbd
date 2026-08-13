package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func ChunkIDAllocationRequestToProto(req metadata.ChunkIDAllocationRequest) *internalv1.AllocateChunkIDsRequest {
	return &internalv1.AllocateChunkIDsRequest{
		VolumeId: req.VolumeID,
		Count:    req.Count,
	}
}

func ChunkIDAllocationRequestFromProto(req *internalv1.AllocateChunkIDsRequest) (metadata.ChunkIDAllocationRequest, error) {
	if req == nil {
		return metadata.ChunkIDAllocationRequest{}, InvalidChunkIDAllocatorRequestError("chunk id allocation proto request is required")
	}
	out := metadata.ChunkIDAllocationRequest{
		VolumeID: req.GetVolumeId(),
		Count:    req.GetCount(),
	}
	if err := out.Validate(); err != nil {
		return metadata.ChunkIDAllocationRequest{}, InvalidChunkIDAllocatorRequestError("%v", err)
	}
	return out, nil
}

func ChunkIDAllocationResponseToProto(req metadata.ChunkIDAllocationRequest, startID uint64) *internalv1.AllocateChunkIDsResponse {
	return &internalv1.AllocateChunkIDsResponse{
		VolumeId:     req.VolumeID,
		Count:        req.Count,
		StartChunkId: startID,
	}
}

func ChunkIDAllocationResponseFromProto(resp *internalv1.AllocateChunkIDsResponse, req metadata.ChunkIDAllocationRequest) (uint64, error) {
	if resp == nil {
		return 0, InvalidChunkIDAllocatorRequestError("chunk id allocation proto response is required")
	}
	if resp.GetVolumeId() != req.VolumeID || resp.GetCount() != req.Count {
		return 0, InvalidChunkIDAllocatorRequestError(
			"chunk id allocation response identity mismatch: got volume_id=%q count=%d want volume_id=%q count=%d",
			resp.GetVolumeId(),
			resp.GetCount(),
			req.VolumeID,
			req.Count,
		)
	}
	startID := resp.GetStartChunkId()
	if req.Count > 0 && startID == 0 {
		return 0, InvalidChunkIDAllocatorRequestError("chunk id allocation response start_chunk_id is required")
	}
	if req.Count == 0 && startID != 0 {
		return 0, InvalidChunkIDAllocatorRequestError("chunk id allocation zero-count response start_chunk_id=%d want 0", startID)
	}
	return startID, nil
}

func newChunkIDAllocationRequest(volumeID string, count uint32) (metadata.ChunkIDAllocationRequest, error) {
	req := metadata.ChunkIDAllocationRequest{VolumeID: volumeID, Count: count}
	if err := req.Validate(); err != nil {
		return metadata.ChunkIDAllocationRequest{}, fmt.Errorf("%w: %v", ErrInvalidChunkIDAllocatorRequest, err)
	}
	return req, nil
}
