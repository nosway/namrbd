package control

import (
	"context"
	"fmt"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPlacementApplyErrorClassGRPCCode(t *testing.T) {
	tests := []struct {
		name  string
		class PlacementApplyErrorClass
		want  codes.Code
	}{
		{name: "invalid", class: PlacementApplyErrorInvalidArgument, want: codes.InvalidArgument},
		{name: "conflict", class: PlacementApplyErrorConflict, want: codes.Aborted},
		{name: "not found", class: PlacementApplyErrorNotFound, want: codes.NotFound},
		{name: "unavailable", class: PlacementApplyErrorUnavailable, want: codes.Unavailable},
		{name: "internal", class: PlacementApplyErrorInternal, want: codes.Internal},
		{name: "unknown", class: PlacementApplyErrorClass("future"), want: codes.Internal},
		{name: "nil class", class: "", want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PlacementApplyErrorClassGRPCCode(tt.class); got != tt.want {
				t.Fatalf("PlacementApplyErrorClassGRPCCode(%q)=%v want=%v", tt.class, got, tt.want)
			}
		})
	}
}

func TestPlacementApplyGRPCCodeErrorClass(t *testing.T) {
	tests := []struct {
		code codes.Code
		want PlacementApplyErrorClass
	}{
		{code: codes.OK, want: ""},
		{code: codes.InvalidArgument, want: PlacementApplyErrorInvalidArgument},
		{code: codes.Aborted, want: PlacementApplyErrorConflict},
		{code: codes.NotFound, want: PlacementApplyErrorNotFound},
		{code: codes.Unavailable, want: PlacementApplyErrorUnavailable},
		{code: codes.DeadlineExceeded, want: PlacementApplyErrorUnavailable},
		{code: codes.Canceled, want: PlacementApplyErrorUnavailable},
		{code: codes.Internal, want: PlacementApplyErrorInternal},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			if got := PlacementApplyGRPCCodeErrorClass(tt.code); got != tt.want {
				t.Fatalf("PlacementApplyGRPCCodeErrorClass(%v)=%v want=%v", tt.code, got, tt.want)
			}
		})
	}
}

func TestClassifyPlacementApplyTransportErrorUsesGRPCStatus(t *testing.T) {
	err := status.Error(codes.Aborted, "conflict")
	if got := ClassifyPlacementApplyTransportError(err); got != PlacementApplyErrorConflict {
		t.Fatalf("ClassifyPlacementApplyTransportError=%v want=%v", got, PlacementApplyErrorConflict)
	}
}

func TestPlacementApplyErrorToGRPCStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "invalid", err: fmt.Errorf("wrap: %w", metadata.ErrInvalidPlacementApplyRequest), want: codes.InvalidArgument},
		{name: "conflict", err: metadata.ErrCASConflict, want: codes.Aborted},
		{name: "not found", err: metadata.ErrNotFound, want: codes.NotFound},
		{name: "deadline", err: context.DeadlineExceeded, want: codes.Unavailable},
		{name: "internal", err: fmt.Errorf("boom"), want: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := PlacementApplyErrorToGRPCStatus(tt.err)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("status.Code(%v)=%v want=%v", err, got, tt.want)
			}
		})
	}
}

func TestPlacementApplyErrorToGRPCStatusNil(t *testing.T) {
	if err := PlacementApplyErrorToGRPCStatus(nil); err != nil {
		t.Fatalf("PlacementApplyErrorToGRPCStatus(nil)=%v want nil", err)
	}
}
