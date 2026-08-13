package control

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func PlacementResolverErrorClassGRPCCode(class PlacementResolverErrorClass) codes.Code {
	switch class {
	case PlacementResolverErrorInvalidArgument:
		return codes.InvalidArgument
	case PlacementResolverErrorNotFound:
		return codes.NotFound
	case PlacementResolverErrorUnavailable:
		return codes.Unavailable
	case PlacementResolverErrorInternal:
		return codes.Internal
	default:
		return codes.Internal
	}
}

func PlacementResolverGRPCCodeErrorClass(code codes.Code) PlacementResolverErrorClass {
	switch code {
	case codes.OK:
		return ""
	case codes.InvalidArgument:
		return PlacementResolverErrorInvalidArgument
	case codes.NotFound:
		return PlacementResolverErrorNotFound
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return PlacementResolverErrorUnavailable
	default:
		return PlacementResolverErrorInternal
	}
}

func ClassifyPlacementResolverTransportError(err error) PlacementResolverErrorClass {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		return PlacementResolverGRPCCodeErrorClass(st.Code())
	}
	return ClassifyPlacementResolverError(err)
}

func PlacementResolverErrorToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(PlacementResolverErrorClassGRPCCode(ClassifyPlacementResolverError(err)), err.Error())
}
