package control

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServeApplyPlacementChangesDelegatesToInternalService(t *testing.T) {
	service := &fakePlacementApplyInternalService{}
	var records []string
	resp, err := ServeApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 11,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeApplyPlacementChanges: %v", err)
	}
	if !service.called {
		t.Fatal("service was not called")
	}
	if service.req.VolumeID != "00a1b2c3" || service.req.CommittedRevision != 11 {
		t.Fatalf("unexpected request: %+v", service.req)
	}
	if resp.GetVolumeId() != "00a1b2c3" || resp.GetCommittedRevision() != 11 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeApplyPlacementChangesMapsValidationError(t *testing.T) {
	service := &fakePlacementApplyInternalService{}
	var records []string
	_, err := ServeApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "invalid",
		CommittedRevision: 1,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if service.called {
		t.Fatal("service was called for invalid request")
	}
	if len(records) != 1 || records[0] != string(PlacementApplyErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeApplyPlacementChangesMapsInternalServiceError(t *testing.T) {
	_, err := ServeApplyPlacementChanges(context.Background(), &internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
	}, &fakePlacementApplyInternalService{err: metadata.ErrCASConflict}, nil)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
}
