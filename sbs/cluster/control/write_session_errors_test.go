package control

import (
	"context"
	"errors"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestClassifyWriteSessionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want WriteSessionErrorClass
	}{
		{name: "nil", err: nil, want: ""},
		{name: "invalid", err: InvalidWriteSessionRequestError("bad"), want: WriteSessionErrorInvalidArgument},
		{name: "conflict", err: metadata.ErrCASConflict, want: WriteSessionErrorConflict},
		{name: "not found", err: metadata.ErrNotFound, want: WriteSessionErrorNotFound},
		{name: "deadline", err: context.DeadlineExceeded, want: WriteSessionErrorUnavailable},
		{name: "internal", err: errors.New("boom"), want: WriteSessionErrorInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyWriteSessionError(tt.err); got != tt.want {
				t.Fatalf("ClassifyWriteSessionError=%q want %q", got, tt.want)
			}
		})
	}
}
