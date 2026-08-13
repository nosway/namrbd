package control

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPlacementResolverErrorClassGRPCCode(t *testing.T) {
	tests := []struct {
		class PlacementResolverErrorClass
		want  codes.Code
	}{
		{PlacementResolverErrorInvalidArgument, codes.InvalidArgument},
		{PlacementResolverErrorNotFound, codes.NotFound},
		{PlacementResolverErrorUnavailable, codes.Unavailable},
		{PlacementResolverErrorInternal, codes.Internal},
		{"unknown", codes.Internal},
	}
	for _, tt := range tests {
		if got := PlacementResolverErrorClassGRPCCode(tt.class); got != tt.want {
			t.Fatalf("PlacementResolverErrorClassGRPCCode(%q)=%v want %v", tt.class, got, tt.want)
		}
	}
}

func TestClassifyPlacementResolverTransportError(t *testing.T) {
	if got := ClassifyPlacementResolverTransportError(status.Error(codes.NotFound, "missing")); got != PlacementResolverErrorNotFound {
		t.Fatalf("transport class=%q want not_found", got)
	}
	if got := ClassifyPlacementResolverTransportError(status.Error(codes.DeadlineExceeded, "slow")); got != PlacementResolverErrorUnavailable {
		t.Fatalf("transport class=%q want unavailable", got)
	}
}
