package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

var ErrInvalidChunkIDAllocatorRequest = errors.New("invalid chunk id allocator request")

type ChunkIDAllocatorErrorClass string

const (
	ChunkIDAllocatorErrorInvalidArgument ChunkIDAllocatorErrorClass = "invalid_argument"
	ChunkIDAllocatorErrorConflict        ChunkIDAllocatorErrorClass = "conflict"
	ChunkIDAllocatorErrorNotFound        ChunkIDAllocatorErrorClass = "not_found"
	ChunkIDAllocatorErrorUnavailable     ChunkIDAllocatorErrorClass = "unavailable"
	ChunkIDAllocatorErrorInternal        ChunkIDAllocatorErrorClass = "internal"
)

func InvalidChunkIDAllocatorRequestError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidChunkIDAllocatorRequest, fmt.Sprintf(format, args...))
}

func ClassifyChunkIDAllocatorError(err error) ChunkIDAllocatorErrorClass {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidChunkIDAllocatorRequest):
		return ChunkIDAllocatorErrorInvalidArgument
	case errors.Is(err, metadata.ErrCASConflict):
		return ChunkIDAllocatorErrorConflict
	case errors.Is(err, metadata.ErrNotFound):
		return ChunkIDAllocatorErrorNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return ChunkIDAllocatorErrorUnavailable
	default:
		return ChunkIDAllocatorErrorInternal
	}
}
