package control

import (
	"context"
	"errors"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestClassifyPlacementResolverError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want PlacementResolverErrorClass
	}{
		{name: "nil", err: nil, want: ""},
		{name: "invalid", err: InvalidPlacementResolverRequestError("bad"), want: PlacementResolverErrorInvalidArgument},
		{name: "not found", err: metadata.ErrNotFound, want: PlacementResolverErrorNotFound},
		{name: "deadline", err: context.DeadlineExceeded, want: PlacementResolverErrorUnavailable},
		{name: "internal", err: errors.New("boom"), want: PlacementResolverErrorInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyPlacementResolverError(tt.err); got != tt.want {
				t.Fatalf("ClassifyPlacementResolverError=%q want %q", got, tt.want)
			}
		})
	}
}

func TestValidatePlacementResolverRequest(t *testing.T) {
	if err := ValidatePlacementResolverRange("not-a-volume", 0, 1); !errors.Is(err, ErrInvalidPlacementResolverRequest) {
		t.Fatalf("invalid volume error=%v want invalid request", err)
	}
	if err := ValidatePlacementResolverGeometry(4096, 3000); !errors.Is(err, ErrInvalidPlacementResolverRequest) {
		t.Fatalf("invalid geometry error=%v want invalid request", err)
	}
}
