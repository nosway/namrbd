package control

import (
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifyWriteSessionTransportErrorUsesGRPCStatus(t *testing.T) {
	err := status.Error(codes.Aborted, "conflict")
	if got := ClassifyWriteSessionTransportError(err); got != WriteSessionErrorConflict {
		t.Fatalf("ClassifyWriteSessionTransportError=%v want %v", got, WriteSessionErrorConflict)
	}
}

func TestWriteSessionErrorToGRPCStatus(t *testing.T) {
	tests := []struct {
		err  error
		want codes.Code
	}{
		{err: InvalidWriteSessionRequestError("bad"), want: codes.InvalidArgument},
		{err: metadata.ErrCASConflict, want: codes.Aborted},
		{err: metadata.ErrNotFound, want: codes.NotFound},
	}
	for _, tt := range tests {
		err := WriteSessionErrorToGRPCStatus(tt.err)
		if got := status.Code(err); got != tt.want {
			t.Fatalf("status.Code=%v want=%v err=%v", got, tt.want, err)
		}
	}
}

func TestWriteSessionErrorToGRPCStatusNil(t *testing.T) {
	if err := WriteSessionErrorToGRPCStatus(nil); err != nil {
		t.Fatalf("WriteSessionErrorToGRPCStatus(nil)=%v want nil", err)
	}
}
