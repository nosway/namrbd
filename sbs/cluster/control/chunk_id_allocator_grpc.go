package control

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ChunkIDAllocatorErrorClassGRPCCode(class ChunkIDAllocatorErrorClass) codes.Code {
	switch class {
	case ChunkIDAllocatorErrorInvalidArgument:
		return codes.InvalidArgument
	case ChunkIDAllocatorErrorConflict:
		return codes.Aborted
	case ChunkIDAllocatorErrorNotFound:
		return codes.NotFound
	case ChunkIDAllocatorErrorUnavailable:
		return codes.Unavailable
	case ChunkIDAllocatorErrorInternal:
		return codes.Internal
	default:
		return codes.Internal
	}
}

func ChunkIDAllocatorGRPCCodeErrorClass(code codes.Code) ChunkIDAllocatorErrorClass {
	switch code {
	case codes.OK:
		return ""
	case codes.InvalidArgument:
		return ChunkIDAllocatorErrorInvalidArgument
	case codes.Aborted:
		return ChunkIDAllocatorErrorConflict
	case codes.NotFound:
		return ChunkIDAllocatorErrorNotFound
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return ChunkIDAllocatorErrorUnavailable
	default:
		return ChunkIDAllocatorErrorInternal
	}
}

func ClassifyChunkIDAllocatorTransportError(err error) ChunkIDAllocatorErrorClass {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		return ChunkIDAllocatorGRPCCodeErrorClass(st.Code())
	}
	return ClassifyChunkIDAllocatorError(err)
}

func ChunkIDAllocatorErrorToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(ChunkIDAllocatorErrorClassGRPCCode(ClassifyChunkIDAllocatorError(err)), err.Error())
}
