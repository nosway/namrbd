package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
)

func TestReadWriteAndKeyFormat(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{
		{ID: 101, Prefix: "devA", SizeBytes: 4096 * 8},
	})

	data := make([]byte, 4096*2)
	data[0] = 0xAA
	data[4096] = 0xBB
	if err := svc.Write(context.Background(), 101, 0, uint64(len(data)), data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	chunk, ok, err := mem.Get(context.Background(), store.BuildChunkKey("devA", 1))
	if err != nil || !ok || len(chunk) != DefaultAllocationChunkSize || chunk[0] != 0xAA || chunk[4096] != 0xBB {
		t.Fatalf("unexpected chunk payload: ok=%v err=%v len=%d first=%x second=%x", ok, err, len(chunk), first(chunk), byteAt(chunk, 4096))
	}

	got, err := svc.Read(context.Background(), 101, 0, 8192)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if got[0] != 0xAA || got[4096] != 0xBB {
		t.Fatalf("unexpected read data")
	}

	meta := svc.metadata.(*inMemoryMetadataRepository)
	pages, err := meta.ListExtentPages(context.Background(), 101)
	if err != nil {
		t.Fatalf("ListExtentPages failed: %v", err)
	}
	if len(pages) != 1 || len(pages[0].Extents) != 1 || pages[0].Extents[0].Kind != AllocationChunkKindData {
		t.Fatalf("unexpected extent pages: %+v", pages)
	}
}

func TestReadMissReturnsZeros(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{ID: 1, Prefix: "d", SizeBytes: 4096 * 2}})
	got, err := svc.Read(context.Background(), 1, 0, 4096)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	for i := range got {
		if got[i] != 0 {
			t.Fatalf("expected zero-filled block")
		}
	}
}

type refreshingMetadataRepository struct {
	MetadataRepository
	freshSpec VolumeSpec
}

func (r *refreshingMetadataRepository) RefreshVolume(ctx context.Context, volumeID uint64) (VolumeSpec, error) {
	if uint64(r.freshSpec.ID) != volumeID {
		return VolumeSpec{}, ErrVolumeNotFound
	}
	if err := r.MetadataRepository.EnsureVolume(ctx, r.freshSpec); err != nil {
		return VolumeSpec{}, err
	}
	return r.freshSpec, nil
}

type capturingReloadRepository struct {
	unsupportedDataRepository
	reloaded VolumeSpec
}

func (r *capturingReloadRepository) ReloadAttachment(_ context.Context, volume VolumeSpec) error {
	r.reloaded = volume
	return nil
}

func TestReloadVolumeDataPathUsesFreshVolumeSpec(t *testing.T) {
	ctx := context.Background()
	oldSpec := NormalizeVolumeSpec(VolumeSpec{
		ID:        101,
		Prefix:    "devA",
		SizeBytes: 128 << 20,
		BlockSize: 4096,
	})
	newSpec := oldSpec
	newSpec.SizeBytes = 192 << 20
	base := NewInMemoryMetadataRepository([]VolumeSpec{oldSpec})
	if _, err := base.Attach(ctx, AttachRequest{VolumeID: uint64(oldSpec.ID), HostID: "host-a", DeviceID: 7}); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	meta := &refreshingMetadataRepository{MetadataRepository: base, freshSpec: newSpec}
	data := &capturingReloadRepository{}
	svc := NewWithRepositoryOptions(meta, data, "gw-a")

	st, err := svc.ReloadVolumeDataPath(ctx, uint64(oldSpec.ID))
	if err != nil {
		t.Fatalf("ReloadVolumeDataPath failed: %v", err)
	}
	if data.reloaded.SizeBytes != newSpec.SizeBytes {
		t.Fatalf("reloaded SizeBytes=%d want=%d", data.reloaded.SizeBytes, newSpec.SizeBytes)
	}
	if st.SizeBytes != newSpec.SizeBytes {
		t.Fatalf("state SizeBytes=%d want=%d", st.SizeBytes, newSpec.SizeBytes)
	}
}

type cloneRoundTripDataRepository struct {
	payloads map[string][]byte
}

func (r *cloneRoundTripDataRepository) ReadAt(context.Context, VolumeSpec, uint64, uint64) ([]byte, error) {
	return nil, ErrNotSupported
}

func (r *cloneRoundTripDataRepository) WriteAt(context.Context, VolumeSpec, uint64, uint64, []byte) error {
	return ErrNotSupported
}

func (r *cloneRoundTripDataRepository) ReadCloneAt(_ context.Context, _ VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	key := fmt.Sprintf("%s:%d", cloneID, offsetBytes)
	data := append([]byte(nil), r.payloads[key]...)
	if uint64(len(data)) != lengthBytes {
		return nil, ErrOutOfRange
	}
	return data, nil
}

func (r *cloneRoundTripDataRepository) WriteCloneAt(_ context.Context, _ VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64, data []byte) error {
	if uint64(len(data)) != lengthBytes {
		return ErrBadDataLength
	}
	if r.payloads == nil {
		r.payloads = map[string][]byte{}
	}
	key := fmt.Sprintf("%s:%d", cloneID, offsetBytes)
	r.payloads[key] = append([]byte(nil), data...)
	return nil
}

func TestServiceCloneReadWriteDelegatesToCloneDataRepository(t *testing.T) {
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	data := &cloneRoundTripDataRepository{}
	svc := NewWithRepositoryOptions(NewInMemoryMetadataRepository([]VolumeSpec{spec}), data, "gw-a")

	payload := make([]byte, 4096)
	payload[0] = 0x7a
	payload[1] = 0x51
	if err := svc.WriteClone(context.Background(), 101, "clone-1", 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("WriteClone failed: %v", err)
	}
	got, err := svc.ReadClone(context.Background(), 101, "clone-1", 0, uint64(len(payload)))
	if err != nil {
		t.Fatalf("ReadClone failed: %v", err)
	}
	if len(got) != len(payload) || got[0] != payload[0] || got[1] != payload[1] {
		t.Fatalf("unexpected clone payload")
	}
	if _, err := svc.ReadClone(context.Background(), 101, "", 0, 4096); err == nil {
		t.Fatalf("ReadClone should reject empty clone id")
	}
}

func TestRejectUnalignedWrite(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{ID: 1, Prefix: "d", SizeBytes: 4096 * 2}})
	err := svc.Write(context.Background(), 1, 1, 4096, make([]byte, 4096))
	if err == nil {
		t.Fatalf("expected alignment error")
	}
}

func TestAttachDetachGenerationAndOwnership(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{ID: 101, Prefix: "devA", SizeBytes: 4096 * 8}})

	st, err := svc.VolumeState(101)
	if err != nil {
		t.Fatalf("VolumeState failed: %v", err)
	}
	if st.Generation != 1 || st.AttachedHostID != "" || st.AttachmentID != "" || st.AttachedDeviceID != 0 {
		t.Fatalf("unexpected initial state: %+v", st)
	}

	st, err = svc.Attach(101, "host-a", 7)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if st.AttachedHostID != "host-a" || st.Generation != 1 || st.AttachmentID != "att-00000065-0001" || st.AttachedDeviceID != 7 {
		t.Fatalf("unexpected attach state: %+v", st)
	}

	st2, err := svc.Attach(101, "host-a", 7)
	if err != nil {
		t.Fatalf("idempotent attach failed: %v", err)
	}
	if st2.AttachmentID != st.AttachmentID {
		t.Fatalf("attachment id changed across idempotent attach: before=%q after=%q", st.AttachmentID, st2.AttachmentID)
	}

	_, err = svc.Attach(101, "host-b", 7)
	if err != ErrAttachConflict {
		t.Fatalf("expected attach conflict, got %v", err)
	}

	_, err = svc.Detach(101, "host-a", "wrong")
	if err != ErrDetachConflict {
		t.Fatalf("expected detach conflict for wrong attachment id, got %v", err)
	}

	st, err = svc.Detach(101, "host-a", "att-00000065-0001")
	if err != nil {
		t.Fatalf("Detach failed: %v", err)
	}
	if st.AttachedHostID != "" || st.Generation != 2 || st.AttachmentID != "" || st.AttachedDeviceID != 0 {
		t.Fatalf("unexpected detach state: %+v", st)
	}
}

type localCloseDataRepository struct {
	volumeID     uint64
	hostID       string
	attachmentID string
	calls        int
}

func (r *localCloseDataRepository) ReadAt(context.Context, VolumeSpec, uint64, uint64) ([]byte, error) {
	return nil, ErrNotSupported
}

func (r *localCloseDataRepository) WriteAt(context.Context, VolumeSpec, uint64, uint64, []byte) error {
	return ErrNotSupported
}

func (r *localCloseDataRepository) CloseLocalAttachment(_ context.Context, volumeID uint64, hostID, attachmentID string) error {
	r.calls++
	r.volumeID = volumeID
	r.hostID = hostID
	r.attachmentID = attachmentID
	return nil
}

func TestDetachClosesLocalAttachmentAfterPeerClearedMetadata(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	data := &localCloseDataRepository{}
	svc := NewWithRepositories(meta, data)

	st, err := svc.Attach(101, "host-a", 7)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if _, err := meta.Detach(context.Background(), DetachRequest{
		VolumeID:     101,
		HostID:       "host-a",
		AttachmentID: st.AttachmentID,
	}); err != nil {
		t.Fatalf("peer metadata Detach failed: %v", err)
	}

	st, err = svc.DetachContext(context.Background(), 101, "host-a", st.AttachmentID)
	if err != nil {
		t.Fatalf("DetachContext failed: %v", err)
	}
	if st.AttachedHostID != "" || st.AttachmentID != "" || st.Generation != 2 {
		t.Fatalf("unexpected detached state: %+v", st)
	}
	if data.calls != 1 || data.volumeID != 101 || data.hostID != "host-a" || data.attachmentID != "att-00000065-0001" {
		t.Fatalf("local close was not called with the cleared attachment: %+v", data)
	}
}

type closeDeadlineDataRepository struct{}

func (closeDeadlineDataRepository) ReadAt(context.Context, VolumeSpec, uint64, uint64) ([]byte, error) {
	return nil, ErrNotSupported
}

func (closeDeadlineDataRepository) WriteAt(context.Context, VolumeSpec, uint64, uint64, []byte) error {
	return ErrNotSupported
}

func (closeDeadlineDataRepository) CloseAttachment(context.Context, uint64, AttachmentRecord) error {
	return context.DeadlineExceeded
}

func TestDetachContinuesWhenAttachmentCloseTimesOut(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	svc := NewWithRepositories(meta, closeDeadlineDataRepository{})
	st, err := svc.Attach(101, "host-a", 7)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}

	st, err = svc.DetachContext(context.Background(), 101, "host-a", st.AttachmentID)
	if err != nil {
		t.Fatalf("DetachContext failed: %v", err)
	}
	if st.AttachedHostID != "" || st.AttachmentID != "" || st.Generation != 2 {
		t.Fatalf("unexpected detach state after deferred close: %+v", st)
	}
}

func TestAttachRejectsDisabledVolume(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{ID: 101, Prefix: "devA", SizeBytes: 4096 * 8}})
	meta := svc.metadata.(*inMemoryMetadataRepository)
	if _, err := meta.SetVolumeState(context.Background(), 101, VolumeStateDisabled); err != nil {
		t.Fatalf("SetVolumeState failed: %v", err)
	}

	if _, err := svc.Attach(101, "host-a", 1); err != ErrVolumeDisabled {
		t.Fatalf("expected ErrVolumeDisabled, got %v", err)
	}
}

func TestAttachRejectsWriterFencedGateway(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	svc := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-a")
	if err := meta.PutVolumeStatus(context.Background(), VolumeStatusRecord{
		VolumeID:                HexVolumeID(101),
		InUse:                   true,
		CurrentGatewayID:        "gw-a",
		HandoffRequired:         true,
		HandoffReason:           "current_gateway_not_desired",
		HandoffTargetGatewaySet: []string{"gw-b"},
		WriterFencingEpoch:      3,
	}); err != nil {
		t.Fatalf("PutVolumeStatus failed: %v", err)
	}

	if _, err := svc.Attach(101, "host-a", 1); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("expected ErrWriterFenced, got %v", err)
	}
}

type trackingDataRepository struct {
	lastVolume VolumeSpec
	writes     int
	payload    []byte
}

func (r *trackingDataRepository) ReadAt(_ context.Context, _ VolumeSpec, _ uint64, lengthBytes uint64) ([]byte, error) {
	return make([]byte, lengthBytes), nil
}

func (r *trackingDataRepository) WriteAt(_ context.Context, volume VolumeSpec, _ uint64, _ uint64, data []byte) error {
	r.lastVolume = volume
	r.writes++
	r.payload = append(r.payload[:0], data...)
	return nil
}

type zeroHintDataRepository struct{}

func (zeroHintDataRepository) ReadAt(context.Context, VolumeSpec, uint64, uint64) ([]byte, error) {
	return nil, errors.New("ReadAt should not be called when ReadAtResult is available")
}

func (zeroHintDataRepository) ReadAtResult(context.Context, VolumeSpec, uint64, uint64) (ReadResult, error) {
	return ReadResult{ZeroData: true}, nil
}

func (zeroHintDataRepository) WriteAt(context.Context, VolumeSpec, uint64, uint64, []byte) error {
	return nil
}

func TestReadResultZeroHintPreservesReadCompatibility(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "zero-hint",
		Prefix:    "zero-hint-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	svc := NewWithRepositoryOptions(meta, zeroHintDataRepository{}, "test-gw")

	result, err := svc.ReadResult(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("ReadResult failed: %v", err)
	}
	if !result.ZeroData || result.Data != nil {
		t.Fatalf("unexpected ReadResult: %+v", result)
	}
	data, err := svc.Read(context.Background(), 101, 0, 4096)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(data) != 4096 || !bytes.Equal(data, make([]byte, 4096)) {
		t.Fatalf("Read materialized payload incorrectly")
	}
}

func TestWriteRejectsSealedProtectedTargetAndPreservesOrdinarySourceWrite(t *testing.T) {
	ctx := context.Background()
	meta := NewInMemoryMetadataRepository([]VolumeSpec{
		NormalizeVolumeSpec(VolumeSpec{
			ID:        HexVolumeID(101),
			Name:      "source",
			Prefix:    "source-00000065",
			SizeBytes: 4096 * 8,
			BlockSize: 4096,
		}),
		NormalizeVolumeSpec(VolumeSpec{
			ID:        HexVolumeID(102),
			Name:      "sealed",
			Prefix:    "sealed-00000066",
			SizeBytes: 4096 * 8,
			BlockSize: 4096,
			ProtectedState: &VolumeProtectedState{
				State:           VolumeProtectedStateSealed,
				ReasonCode:      ProtectedWriteReasonSealedReadOnly,
				SealedObjectID:  "phase-t-sealed-image-fixture",
				SealOperationID: "phase-t-seal-operation-fixture",
				LifecycleState:  "materialized",
				SourceVolumeID:  "00000065",
			},
		}),
	})
	data := &trackingDataRepository{}
	svc := NewWithRepositoryOptions(meta, data, "gw-a")

	payload := bytes.Repeat([]byte{0x5a}, 4096)
	if err := svc.Write(ctx, 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("ordinary source write failed: %v", err)
	}
	if data.writes != 1 || uint64(data.lastVolume.ID) != 101 {
		t.Fatalf("ordinary source write did not reach data path: writes=%d volume=%s", data.writes, CanonicalVolumeID(uint64(data.lastVolume.ID)))
	}

	err := svc.Write(ctx, 102, 0, uint64(len(payload)), payload)
	if !errors.Is(err, ErrProtectedWriteRejected) {
		t.Fatalf("sealed write err=%v want ErrProtectedWriteRejected", err)
	}
	rejection, ok := ProtectedWriteRejectionFromError(err)
	if !ok {
		t.Fatalf("sealed write error did not expose rejection details: %v", err)
	}
	if rejection.VolumeID != "00000066" ||
		rejection.ProtectedState != string(VolumeProtectedStateSealed) ||
		rejection.ReasonCode != ProtectedWriteReasonSealedReadOnly ||
		rejection.SealedObjectID != "phase-t-sealed-image-fixture" ||
		rejection.SealOperationID != "phase-t-seal-operation-fixture" ||
		rejection.LifecycleState != "materialized" {
		t.Fatalf("unexpected rejection details: %+v", rejection)
	}
	if !strings.Contains(err.Error(), ProtectedWriteReasonSealedReadOnly) {
		t.Fatalf("error did not include stable reason code: %v", err)
	}
	if data.writes != 1 {
		t.Fatalf("sealed write reached data path, writes=%d", data.writes)
	}
}

func TestAttachByDesiredGatewayKeepsHandoffPendingUntilConverged(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	if _, err := meta.Attach(context.Background(), AttachRequest{
		VolumeID:  101,
		HostID:    "host-a",
		DeviceID:  1,
		GatewayID: "gw-a",
	}); err != nil {
		t.Fatalf("seed Attach failed: %v", err)
	}
	status, err := meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus failed: %v", err)
	}
	status.HandoffRequired = true
	status.HandoffReason = "current_gateway_not_desired"
	status.HandoffTargetGatewaySet = []string{"gw-b"}
	status.WriterFencingEpoch = 7
	status.PathPlanRevision = 9
	if err := meta.PutVolumeStatus(context.Background(), status); err != nil {
		t.Fatalf("PutVolumeStatus failed: %v", err)
	}

	svc := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-b")
	st, err := svc.Attach(101, "host-a", 1)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if st.AttachmentID != "att-00000065-0002" || st.Generation != 2 {
		t.Fatalf("unexpected attachment state: %+v", st)
	}

	status, err = meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus after attach failed: %v", err)
	}
	if !status.HandoffRequired || status.HandoffReason != "current_gateway_not_desired" || len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-b" {
		t.Fatalf("expected handoff to remain pending until convergence: %+v", status)
	}
	if status.HandoffStage != "acknowledged_pending_convergence" {
		t.Fatalf("expected pending convergence handoff stage: %+v", status)
	}
	if status.CurrentGatewayID != "gw-b" {
		t.Fatalf("expected current gateway to switch to gw-b: %+v", status)
	}
	if status.AttachmentGeneration != 2 || status.CurrentAttachmentID != "att-00000065-0002" {
		t.Fatalf("expected attachment generation/id to rotate during handoff: %+v", status)
	}
	if status.HandoffAckedAtUnix == 0 || status.HandoffAckedGeneration != 2 {
		t.Fatalf("expected desired gateway attach to acknowledge handoff: %+v", status)
	}
	if status.WriterFencingEpoch != 7 {
		t.Fatalf("expected fencing epoch to be preserved, got %+v", status)
	}
}

func TestAttachByDesiredGatewayClearsHandoffWhenAlreadyConverged(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	if _, err := meta.Attach(context.Background(), AttachRequest{
		VolumeID:  101,
		HostID:    "host-a",
		DeviceID:  1,
		GatewayID: "gw-a",
	}); err != nil {
		t.Fatalf("seed Attach failed: %v", err)
	}
	status, err := meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus failed: %v", err)
	}
	status.HandoffRequired = true
	status.HandoffReason = "current_gateway_not_desired"
	status.HandoffTargetGatewaySet = []string{"gw-b"}
	status.WriterFencingEpoch = 7
	status.PathPlanRevision = 9
	status.RuntimeAppliedPathPlanRevision = 9
	if err := meta.PutVolumeStatus(context.Background(), status); err != nil {
		t.Fatalf("PutVolumeStatus failed: %v", err)
	}

	svc := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-b")
	st, err := svc.Attach(101, "host-a", 1)
	if err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	if st.Generation != 2 || st.AttachmentID != "att-00000065-0002" {
		t.Fatalf("expected converged handoff attach to rotate generation: %+v", st)
	}
	status, err = meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus after attach failed: %v", err)
	}
	if !status.HandoffRequired || status.HandoffStage != "ready_to_complete" || status.HandoffReason != "current_gateway_not_desired" || len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-b" {
		t.Fatalf("expected converged handoff to remain pending until completion hold expires: %+v", status)
	}
	if status.HandoffAckedAtUnix == 0 || status.HandoffAckedGeneration != 2 || status.HandoffCompletionEligibleAtUnix <= status.HandoffAckedAtUnix {
		t.Fatalf("expected completion hold metadata after converged attach: %+v", status)
	}
	if status.ControllerReconcileRequestedAtUnix != 0 || status.ControllerReconcileReason != "" {
		t.Fatalf("did not expect immediate reconcile request while completion hold is active: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix != status.HandoffCompletionEligibleAtUnix || status.ControllerReconcileScheduledReason != "handoff_completion_ready" {
		t.Fatalf("expected scheduled reconcile for handoff completion hold: %+v", status)
	}
}

func TestAttachRejectsStaleGatewayAfterDesiredGatewayReattach(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	if _, err := meta.Attach(context.Background(), AttachRequest{
		VolumeID:  101,
		HostID:    "host-a",
		DeviceID:  1,
		GatewayID: "gw-a",
	}); err != nil {
		t.Fatalf("seed Attach failed: %v", err)
	}
	status, err := meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus failed: %v", err)
	}
	status.HandoffRequired = true
	status.HandoffReason = "current_gateway_not_desired"
	status.HandoffTargetGatewaySet = []string{"gw-b"}
	status.WriterFencingEpoch = 7
	status.PathPlanRevision = 9
	if err := meta.PutVolumeStatus(context.Background(), status); err != nil {
		t.Fatalf("PutVolumeStatus failed: %v", err)
	}

	if _, err := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-b").Attach(101, "host-a", 1); err != nil {
		t.Fatalf("desired gateway attach failed: %v", err)
	}
	if _, err := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-a").Attach(101, "host-a", 1); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("expected stale gw-a to stay fenced, got %v", err)
	}
}

func TestAttachRejectsAlternateTargetGatewayAfterGenerationRotation(t *testing.T) {
	meta := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	if _, err := meta.Attach(context.Background(), AttachRequest{
		VolumeID:  101,
		HostID:    "host-a",
		DeviceID:  1,
		GatewayID: "gw-a",
	}); err != nil {
		t.Fatalf("seed Attach failed: %v", err)
	}
	if _, err := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-b").Attach(101, "host-a", 1); err != nil {
		t.Fatalf("desired gateway attach failed: %v", err)
	}
	status, err := meta.GetVolumeStatus(context.Background(), 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus failed: %v", err)
	}
	status.HandoffRequired = true
	status.HandoffStage = "acknowledged_pending_convergence"
	status.HandoffReason = "current_gateway_not_desired"
	status.HandoffTargetGatewaySet = []string{"gw-b", "gw-c"}
	status.CurrentGatewayID = "gw-b"
	status.HandoffAckedAtUnix = 1
	status.HandoffAckedGeneration = status.AttachmentGeneration
	status.WriterFencingEpoch = 8
	if err := meta.PutVolumeStatus(context.Background(), status); err != nil {
		t.Fatalf("PutVolumeStatus failed: %v", err)
	}

	if _, err := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-c").Attach(101, "host-a", 1); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("expected alternate target gw-c to be fenced after generation rotation, got %v", err)
	}
	if _, err := NewWithRepositoryOptions(meta, unsupportedDataRepository{}, "gw-b").Attach(101, "host-a", 1); err != nil {
		t.Fatalf("expected current target gw-b reattach to remain allowed, got %v", err)
	}
}

func TestServiceMetricsSnapshot(t *testing.T) {
	mem := store.NewMemoryStore()
	svc := New(mem, []store.Volume{{ID: 101, Prefix: "devA", SizeBytes: 4096 * 8}})

	if _, err := svc.Attach(101, "host-a", 7); err != nil {
		t.Fatalf("Attach failed: %v", err)
	}
	payload := make([]byte, 4096)
	if err := svc.Write(context.Background(), 101, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if _, err := svc.Read(context.Background(), 101, 0, uint64(len(payload))); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	_ = svc.Write(context.Background(), 101, 1, uint64(len(payload)), payload)

	metrics := svc.MetricsSnapshot()
	if metrics.ByOperation["attach"].Count != 1 {
		t.Fatalf("unexpected attach metrics: %+v", metrics.ByOperation["attach"])
	}
	if metrics.ByOperation["write"].Count != 2 || metrics.ByOperation["write"].Errors != 1 || metrics.ByOperation["write"].Bytes != uint64(len(payload))*2 {
		t.Fatalf("unexpected write metrics: %+v", metrics.ByOperation["write"])
	}
	if metrics.ByOperation["read"].Count != 1 || metrics.ByOperation["read"].Bytes != uint64(len(payload)) {
		t.Fatalf("unexpected read metrics: %+v", metrics.ByOperation["read"])
	}
	if len(metrics.Retry) != 0 {
		t.Fatalf("unexpected retry metrics: %+v", metrics.Retry)
	}
	if metrics.RetrySummary.TotalRetries != 0 || metrics.RetrySummary.OpenUnavailableRetries != 0 || metrics.RetrySummary.ReopenRetries != 0 {
		t.Fatalf("unexpected retry summary: %+v", metrics.RetrySummary)
	}
}

func first(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

func byteAt(b []byte, idx int) byte {
	if idx < 0 || idx >= len(b) {
		return 0
	}
	return b[idx]
}
