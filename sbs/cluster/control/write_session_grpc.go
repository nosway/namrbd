package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func WriteSessionErrorClassGRPCCode(class WriteSessionErrorClass) codes.Code {
	switch class {
	case WriteSessionErrorInvalidArgument:
		return codes.InvalidArgument
	case WriteSessionErrorConflict:
		return codes.Aborted
	case WriteSessionErrorNotFound:
		return codes.NotFound
	case WriteSessionErrorUnavailable:
		return codes.Unavailable
	case WriteSessionErrorInternal:
		return codes.Internal
	default:
		return codes.Internal
	}
}

func WriteSessionGRPCCodeErrorClass(code codes.Code) WriteSessionErrorClass {
	switch code {
	case codes.OK:
		return ""
	case codes.InvalidArgument:
		return WriteSessionErrorInvalidArgument
	case codes.Aborted:
		return WriteSessionErrorConflict
	case codes.NotFound:
		return WriteSessionErrorNotFound
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return WriteSessionErrorUnavailable
	default:
		return WriteSessionErrorInternal
	}
}

func ClassifyWriteSessionTransportError(err error) WriteSessionErrorClass {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		return WriteSessionGRPCCodeErrorClass(st.Code())
	}
	return ClassifyWriteSessionError(err)
}

func WriteSessionTransportErrorToMetadataError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("%w: %s", metadata.ErrNotFound, st.Message())
	case codes.Aborted:
		return fmt.Errorf("%w: %s", metadata.ErrCASConflict, st.Message())
	default:
		return err
	}
}

func WriteSessionErrorToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(WriteSessionErrorClassGRPCCode(ClassifyWriteSessionError(err)), err.Error())
}
