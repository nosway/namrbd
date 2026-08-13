package control

import (
	"context"
	"errors"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type PlacementApplyErrorClass string

const (
	PlacementApplyErrorInvalidArgument PlacementApplyErrorClass = "invalid_argument"
	PlacementApplyErrorConflict        PlacementApplyErrorClass = "conflict"
	PlacementApplyErrorNotFound        PlacementApplyErrorClass = "not_found"
	PlacementApplyErrorUnavailable     PlacementApplyErrorClass = "unavailable"
	PlacementApplyErrorInternal        PlacementApplyErrorClass = "internal"
)

func ClassifyPlacementApplyError(err error) PlacementApplyErrorClass {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, metadata.ErrInvalidPlacementApplyRequest):
		return PlacementApplyErrorInvalidArgument
	case errors.Is(err, metadata.ErrCASConflict):
		return PlacementApplyErrorConflict
	case errors.Is(err, metadata.ErrNotFound):
		return PlacementApplyErrorNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return PlacementApplyErrorUnavailable
	default:
		return PlacementApplyErrorInternal
	}
}
