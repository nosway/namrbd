package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func PlacementApplyErrorClassGRPCCode(class PlacementApplyErrorClass) codes.Code {
	switch class {
	case PlacementApplyErrorInvalidArgument:
		return codes.InvalidArgument
	case PlacementApplyErrorConflict:
		return codes.Aborted
	case PlacementApplyErrorNotFound:
		return codes.NotFound
	case PlacementApplyErrorUnavailable:
		return codes.Unavailable
	case PlacementApplyErrorInternal:
		return codes.Internal
	default:
		return codes.Internal
	}
}

func PlacementApplyGRPCCodeErrorClass(code codes.Code) PlacementApplyErrorClass {
	switch code {
	case codes.OK:
		return ""
	case codes.InvalidArgument:
		return PlacementApplyErrorInvalidArgument
	case codes.Aborted:
		return PlacementApplyErrorConflict
	case codes.NotFound:
		return PlacementApplyErrorNotFound
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return PlacementApplyErrorUnavailable
	default:
		return PlacementApplyErrorInternal
	}
}

func ClassifyPlacementApplyTransportError(err error) PlacementApplyErrorClass {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		return PlacementApplyGRPCCodeErrorClass(st.Code())
	}
	return ClassifyPlacementApplyError(err)
}

func PlacementApplyTransportErrorToMetadataError(err error) error {
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

func PlacementApplyErrorToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(PlacementApplyErrorClassGRPCCode(ClassifyPlacementApplyError(err)), err.Error())
}
