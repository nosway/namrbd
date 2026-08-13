package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

var ErrInvalidPlacementResolverRequest = errors.New("invalid placement resolver request")

type PlacementResolverErrorClass string

const (
	PlacementResolverErrorInvalidArgument PlacementResolverErrorClass = "invalid_argument"
	PlacementResolverErrorNotFound        PlacementResolverErrorClass = "not_found"
	PlacementResolverErrorUnavailable     PlacementResolverErrorClass = "unavailable"
	PlacementResolverErrorInternal        PlacementResolverErrorClass = "internal"
)

func InvalidPlacementResolverRequestError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPlacementResolverRequest, fmt.Sprintf(format, args...))
}

func ClassifyPlacementResolverError(err error) PlacementResolverErrorClass {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidPlacementResolverRequest):
		return PlacementResolverErrorInvalidArgument
	case errors.Is(err, metadata.ErrNotFound):
		return PlacementResolverErrorNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return PlacementResolverErrorUnavailable
	default:
		return PlacementResolverErrorInternal
	}
}
