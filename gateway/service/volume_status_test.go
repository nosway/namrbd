package service

import (
	"context"
	"testing"
)

func TestInMemoryMetadataRepositoryAttachDetachPreservesPathPlanStatusFields(t *testing.T) {
	repo := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})
	mem := repo.(*inMemoryMetadataRepository)
	mem.status[101] = VolumeStatusRecord{
		VolumeID:                      HexVolumeID(101),
		GatewayConnectionState:        GatewayStateUnknown,
		DesiredActiveGatewaySet:       []string{"gw-a", "gw-b"},
		ObservedActiveGatewaySet:      []string{"gw-a"},
		PathPlanRevision:              7,
		WriterFencingEpoch:            11,
		RuntimePathNeedsAttention:     true,
		RuntimePathAttentionReasons:   []string{"lane_unavailable"},
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
		HandoffRequired:               true,
		HandoffReason:                 "current_gateway_not_desired",
		HandoffTargetGatewaySet:       []string{"gw-a"},
	}

	ctx := context.Background()
	attachment, err := repo.Attach(ctx, AttachRequest{VolumeID: 101, HostID: "host-a", DeviceID: 1, GatewayID: "gw-a"})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	status, err := repo.GetVolumeStatus(ctx, 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus after attach: %v", err)
	}
	if status.PathPlanRevision != 7 || status.WriterFencingEpoch != 11 {
		t.Fatalf("attach should preserve path-plan/fencing fields: %+v", status)
	}
	if !status.HandoffRequired || status.HandoffReason != "current_gateway_not_desired" || len(status.HandoffTargetGatewaySet) != 1 {
		t.Fatalf("attach should preserve handoff fields: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 2 || len(status.ObservedActiveGatewaySet) != 1 {
		t.Fatalf("attach should preserve desired/observed gateway sets: %+v", status)
	}
	if !status.RuntimePathNeedsAttention || len(status.RuntimePathRecommendedActions) != 2 {
		t.Fatalf("attach should preserve runtime feedback fields: %+v", status)
	}
	if status.AttachmentGeneration != attachment.Generation {
		t.Fatalf("unexpected attachment generation: got=%d want=%d", status.AttachmentGeneration, attachment.Generation)
	}

	if _, err := repo.Detach(ctx, DetachRequest{VolumeID: 101, HostID: "host-a", AttachmentID: attachment.AttachmentID}); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	status, err = repo.GetVolumeStatus(ctx, 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus after detach: %v", err)
	}
	if status.PathPlanRevision != 7 || status.WriterFencingEpoch != 11 {
		t.Fatalf("detach should preserve path-plan/fencing fields: %+v", status)
	}
	if !status.HandoffRequired || status.HandoffReason != "current_gateway_not_desired" || len(status.HandoffTargetGatewaySet) != 1 {
		t.Fatalf("detach should preserve handoff fields: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 2 || len(status.ObservedActiveGatewaySet) != 1 {
		t.Fatalf("detach should preserve desired/observed gateway sets: %+v", status)
	}
	if !status.RuntimePathNeedsAttention || len(status.RuntimePathRecommendedActions) != 2 {
		t.Fatalf("detach should preserve runtime feedback fields: %+v", status)
	}
	if status.AttachmentGeneration != attachment.Generation+1 {
		t.Fatalf("unexpected attachment generation after detach: got=%d want=%d", status.AttachmentGeneration, attachment.Generation+1)
	}
}

func TestInMemoryMetadataRepositoryAttachBumpsGenerationWhenGatewayChanges(t *testing.T) {
	repo := NewInMemoryMetadataRepository([]VolumeSpec{NormalizeVolumeSpec(VolumeSpec{
		ID:        HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})})

	ctx := context.Background()
	first, err := repo.Attach(ctx, AttachRequest{VolumeID: 101, HostID: "host-a", DeviceID: 1, GatewayID: "gw-a"})
	if err != nil {
		t.Fatalf("Attach gw-a: %v", err)
	}
	second, err := repo.Attach(ctx, AttachRequest{VolumeID: 101, HostID: "host-a", DeviceID: 1, GatewayID: "gw-b"})
	if err != nil {
		t.Fatalf("Attach gw-b: %v", err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("expected generation bump on gateway change: first=%+v second=%+v", first, second)
	}
	if second.AttachmentID == first.AttachmentID {
		t.Fatalf("expected new attachment id on gateway change: first=%+v second=%+v", first, second)
	}
	status, err := repo.GetVolumeStatus(ctx, 101)
	if err != nil {
		t.Fatalf("GetVolumeStatus: %v", err)
	}
	if status.CurrentGatewayID != "gw-b" || status.AttachmentGeneration != second.Generation || status.CurrentAttachmentID != second.AttachmentID {
		t.Fatalf("unexpected status after gateway change attach: %+v", status)
	}
}
