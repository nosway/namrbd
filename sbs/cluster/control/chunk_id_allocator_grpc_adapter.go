package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

type GRPCChunkIDAllocatorAdapter struct {
	client internalv1.ChunkIDAllocatorServiceClient
}

func NewGRPCChunkIDAllocatorAdapter(client internalv1.ChunkIDAllocatorServiceClient) *GRPCChunkIDAllocatorAdapter {
	return &GRPCChunkIDAllocatorAdapter{client: client}
}

func (a *GRPCChunkIDAllocatorAdapter) AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error) {
	start := time.Now()
	req, err := newChunkIDAllocationRequest(volumeID, count)
	if err != nil {
		logChunkIDAllocatorGRPCFailure(err, ClassifyChunkIDAllocatorError(err), time.Since(start), req)
		return 0, err
	}
	if a.client == nil {
		err := fmt.Errorf("chunk id allocator gRPC client is required")
		logChunkIDAllocatorGRPCFailure(err, ChunkIDAllocatorErrorInternal, time.Since(start), req)
		return 0, err
	}
	resp, err := a.client.AllocateChunkIDs(ctx, ChunkIDAllocationRequestToProto(req))
	if err != nil {
		logChunkIDAllocatorGRPCFailure(err, ClassifyChunkIDAllocatorTransportError(err), time.Since(start), req)
		return 0, err
	}
	startID, err := ChunkIDAllocationResponseFromProto(resp, req)
	if err != nil {
		logChunkIDAllocatorGRPCFailure(err, ChunkIDAllocatorErrorInvalidArgument, time.Since(start), req)
		return 0, err
	}
	return startID, nil
}

func logChunkIDAllocatorGRPCFailure(err error, class ChunkIDAllocatorErrorClass, duration time.Duration, req metadata.ChunkIDAllocationRequest) {
	structuredlog.Error("sbs.cluster.control", "chunk_id_allocator_grpc_failed", err,
		structuredlog.F("error_class", string(class)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("count", req.Count),
	)
}
