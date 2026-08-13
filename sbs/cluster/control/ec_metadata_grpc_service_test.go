package control

import (
	"context"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func TestServeCommitECFullStripeWriteUsesVolumeCommitLock(t *testing.T) {
	service := &fakeECMetadataInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 8},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-ec", ResultState: metadata.IdempotencyCommitted, Revision: 8},
	}
	locked := false
	lockedVolume := ""
	locker := func(volumeID string) func() {
		locked = true
		lockedVolume = volumeID
		return func() {
			locked = false
		}
	}
	service.onCommitFullStripe = func() {
		if !locked {
			t.Fatal("EC full-stripe commit reached service without volume commit lock")
		}
	}

	resp, err := ServeCommitECFullStripeWrite(context.Background(), &internalv1.CommitECFullStripeWriteRequest{
		VolumeId:              "00a1b2c3",
		ExpectedEpoch:         1,
		ExpectedRevision:      7,
		IdempotencyKey:        "idem-ec",
		CommittedRevision:     8,
		PhysicalObject:        PhysicalObjectRecordToProto(ecMetadataTestPhysicalObject()),
		EcStripe:              ECStripeRecordToProto(ecMetadataTestStripe()),
		AllocationPages:       []*internalv1.AllocationPage{ecMetadataTestAllocationPage()},
		MutationOperationId:   "ec-write-0",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, service, locker, nil)
	if err != nil {
		t.Fatalf("ServeCommitECFullStripeWrite: %v", err)
	}
	if resp.GetVolumeState().GetRevision() != 8 {
		t.Fatalf("revision=%d want 8", resp.GetVolumeState().GetRevision())
	}
	if locked {
		t.Fatal("volume commit lock was not released")
	}
	if lockedVolume != "00a1b2c3" {
		t.Fatalf("locked volume=%q want 00a1b2c3", lockedVolume)
	}
}

func TestServeCommitECDiscardUsesVolumeCommitLock(t *testing.T) {
	service := &fakeECMetadataInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 9},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-ec-discard", ResultState: metadata.IdempotencyCommitted, Revision: 9},
	}
	locked := false
	locker := func(volumeID string) func() {
		if volumeID != "00a1b2c3" {
			t.Fatalf("locked volume=%q want 00a1b2c3", volumeID)
		}
		locked = true
		return func() {
			locked = false
		}
	}
	service.onCommitDiscard = func() {
		if !locked {
			t.Fatal("EC discard commit reached service without volume commit lock")
		}
	}

	resp, err := ServeCommitECDiscard(context.Background(), &internalv1.CommitECDiscardRequest{
		VolumeId:              "00a1b2c3",
		ExpectedEpoch:         1,
		ExpectedRevision:      8,
		IdempotencyKey:        "idem-ec-discard",
		CommittedRevision:     9,
		AllocationPages:       []*internalv1.AllocationPage{ecMetadataTestAllocationPage()},
		MutationOperationId:   "ec-discard-0",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, service, locker, nil)
	if err != nil {
		t.Fatalf("ServeCommitECDiscard: %v", err)
	}
	if resp.GetVolumeState().GetRevision() != 9 {
		t.Fatalf("revision=%d want 9", resp.GetVolumeState().GetRevision())
	}
	if locked {
		t.Fatal("volume commit lock was not released")
	}
}

func ecMetadataTestAllocationPage() *internalv1.AllocationPage {
	return &internalv1.AllocationPage{
		VolumeId:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       7,
		Extents: []*internalv1.AllocationExtent{{
			LogicalChunkStart: 0,
			ChunkCount:        1,
			Kind:              internalv1.AllocationKind_ALLOCATION_KIND_DATA,
			BackingRef:        "ec-object-0",
		}},
	}
}

type fakeECMetadataInternalService struct {
	state metadata.VolumeState
	idem  metadata.IdempotencyRecord

	onCommitFullStripe func()
	onCommitDiscard    func()
}

func (s *fakeECMetadataInternalService) GetPhysicalObject(context.Context, string, string) (metadata.PhysicalObjectRecord, error) {
	return metadata.PhysicalObjectRecord{}, metadata.ErrNotFound
}

func (s *fakeECMetadataInternalService) PutPhysicalObject(context.Context, metadata.PhysicalObjectRecord) error {
	return nil
}

func (s *fakeECMetadataInternalService) GetECStripe(context.Context, string, string, uint64) (metadata.ECStripeRecord, error) {
	return metadata.ECStripeRecord{}, metadata.ErrNotFound
}

func (s *fakeECMetadataInternalService) PutECStripe(context.Context, metadata.ECStripeRecord) error {
	return nil
}

func (s *fakeECMetadataInternalService) CommitECFullStripeWrite(context.Context, metadata.CommitECFullStripeWriteRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.onCommitFullStripe != nil {
		s.onCommitFullStripe()
	}
	return s.state, s.idem, nil
}

func (s *fakeECMetadataInternalService) CommitECDiscard(context.Context, metadata.CommitECDiscardRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if s.onCommitDiscard != nil {
		s.onCommitDiscard()
	}
	return s.state, s.idem, nil
}

var _ ECMetadataInternalService = (*fakeECMetadataInternalService)(nil)
