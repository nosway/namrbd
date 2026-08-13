package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

var ErrInvalidWriteSessionRequest = errors.New("invalid write session request")

type WriteSessionErrorClass string

const (
	WriteSessionErrorInvalidArgument WriteSessionErrorClass = "invalid_argument"
	WriteSessionErrorConflict        WriteSessionErrorClass = "conflict"
	WriteSessionErrorNotFound        WriteSessionErrorClass = "not_found"
	WriteSessionErrorUnavailable     WriteSessionErrorClass = "unavailable"
	WriteSessionErrorInternal        WriteSessionErrorClass = "internal"
)

func InvalidWriteSessionRequestError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidWriteSessionRequest, fmt.Sprintf(format, args...))
}

func ClassifyWriteSessionError(err error) WriteSessionErrorClass {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrInvalidWriteSessionRequest):
		return WriteSessionErrorInvalidArgument
	case errors.Is(err, metadata.ErrCASConflict):
		return WriteSessionErrorConflict
	case errors.Is(err, metadata.ErrNotFound):
		return WriteSessionErrorNotFound
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return WriteSessionErrorUnavailable
	default:
		return WriteSessionErrorInternal
	}
}
