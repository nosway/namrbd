package control

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

const chunkIDAllocatorServiceOutcomeOK = "ok"

// ChunkIDAllocatorOutcomeRecorder records classified internal gRPC service
// outcomes. It lets cmd/sbs-service keep owning metrics storage while the
// façade behavior moves into this package.
type ChunkIDAllocatorOutcomeRecorder func(class string, duration time.Duration)

// ServeAllocateChunkIDs handles the server side of the internal chunk ID
// allocation gRPC façade.
func ServeAllocateChunkIDs(ctx context.Context, req *internalv1.AllocateChunkIDsRequest, service ChunkIDAllocatorInternalService, record ChunkIDAllocatorOutcomeRecorder) (*internalv1.AllocateChunkIDsResponse, error) {
	start := time.Now()
	allocationReq, err := ChunkIDAllocationRequestFromProto(req)
	if err != nil {
		class := ClassifyChunkIDAllocatorError(err)
		duration := time.Since(start)
		recordChunkIDAllocatorOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "chunk_id_allocator_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", req.GetVolumeId()),
			structuredlog.F("count", req.GetCount()),
		)
		return nil, ChunkIDAllocatorErrorToGRPCStatus(err)
	}
	if service == nil {
		err := fmt.Errorf("chunk id allocator internal service is required")
		class := ChunkIDAllocatorErrorInternal
		duration := time.Since(start)
		recordChunkIDAllocatorOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "chunk_id_allocator_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", allocationReq.VolumeID),
			structuredlog.F("count", allocationReq.Count),
		)
		return nil, ChunkIDAllocatorErrorToGRPCStatus(err)
	}
	startID, err := service.AllocateChunkIDs(ctx, allocationReq)
	if err != nil {
		class := ClassifyChunkIDAllocatorError(err)
		duration := time.Since(start)
		recordChunkIDAllocatorOutcome(record, string(class), duration)
		structuredlog.Error("sbs.service", "chunk_id_allocator_failed", err,
			structuredlog.F("error_class", string(class)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
			structuredlog.F("volume_id", allocationReq.VolumeID),
			structuredlog.F("count", allocationReq.Count),
		)
		return nil, ChunkIDAllocatorErrorToGRPCStatus(err)
	}
	duration := time.Since(start)
	recordChunkIDAllocatorOutcome(record, chunkIDAllocatorServiceOutcomeOK, duration)
	structuredlog.Info("sbs.service", "chunk_id_allocator_allocated",
		structuredlog.F("duration_ms", duration.Milliseconds()),
		structuredlog.F("volume_id", allocationReq.VolumeID),
		structuredlog.F("count", allocationReq.Count),
		structuredlog.F("start_chunk_id", startID),
	)
	return ChunkIDAllocationResponseToProto(allocationReq, startID), nil
}

func recordChunkIDAllocatorOutcome(record ChunkIDAllocatorOutcomeRecorder, class string, duration time.Duration) {
	if record != nil {
		record(class, duration)
	}
}
