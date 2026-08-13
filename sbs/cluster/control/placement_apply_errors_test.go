package control

import (
	"context"
	"fmt"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestClassifyPlacementApplyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want PlacementApplyErrorClass
	}{
		{name: "nil", err: nil, want: ""},
		{name: "invalid", err: fmt.Errorf("wrap: %w", metadata.ErrInvalidPlacementApplyRequest), want: PlacementApplyErrorInvalidArgument},
		{name: "conflict", err: metadata.ErrCASConflict, want: PlacementApplyErrorConflict},
		{name: "not found", err: metadata.ErrNotFound, want: PlacementApplyErrorNotFound},
		{name: "deadline", err: context.DeadlineExceeded, want: PlacementApplyErrorUnavailable},
		{name: "canceled", err: context.Canceled, want: PlacementApplyErrorUnavailable},
		{name: "internal", err: fmt.Errorf("boom"), want: PlacementApplyErrorInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyPlacementApplyError(tt.err); got != tt.want {
				t.Fatalf("ClassifyPlacementApplyError(%v)=%q want=%q", tt.err, got, tt.want)
			}
		})
	}
}
