package cluster

import (
	"testing"

	"github.com/nosway/namrbd/gateway/service"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestPublishedVolumeSpecFromSummaryPreservesECGeometry(t *testing.T) {
	spec := publishedVolumeSpecFromSummary(0x6a18abcf, &adminv1.VolumeSummary{
		VolumeId:             "6a18abcf",
		SizeBytes:            64 << 20,
		BlockSize:            4096,
		ChunkSizeBytes:       128 << 10,
		ExtentPageBytes:      4 << 20,
		TopologyMode:         "strict",
		RedundancyBackend:    service.RedundancyBackendEC,
		EcProfileId:          "ec-6-3",
		EcCodecId:            metadata.ECCodecRSVandGF8,
		EcDataShards:         6,
		EcParityShards:       3,
		EcStripeUnitBytes:    128 << 10,
		EcFailureDomain:      metadata.ECFailureDomainZone,
		WeakPlacementAllowed: true,
	})

	if spec.RedundancyBackend != service.RedundancyBackendEC {
		t.Fatalf("redundancy_backend=%q want ec", spec.RedundancyBackend)
	}
	if spec.ECProfileID != "ec-6-3" || spec.ECCodecID != metadata.ECCodecRSVandGF8 {
		t.Fatalf("unexpected EC profile fields: %+v", spec)
	}
	if spec.ECDataShards != 6 || spec.ECParityShards != 3 || spec.ECStripeUnitBytes != 128<<10 {
		t.Fatalf("unexpected EC geometry: %+v", spec)
	}
	if spec.ECFailureDomain != metadata.ECFailureDomainZone || spec.TopologyMode != "strict" || !spec.WeakPlacementAllowed {
		t.Fatalf("unexpected EC topology fields: %+v", spec)
	}
}

func TestPublishedVolumeSpecFromSummaryDefaultsReplicatedBackend(t *testing.T) {
	spec := publishedVolumeSpecFromSummary(0x00a1b2c3, &adminv1.VolumeSummary{
		VolumeId:  "00a1b2c3",
		SizeBytes: 1 << 20,
		BlockSize: 4096,
	})
	if spec.RedundancyBackend != service.RedundancyBackendReplicated {
		t.Fatalf("redundancy_backend=%q want replicated", spec.RedundancyBackend)
	}
	if spec.ChunkSizeBytes != service.DefaultAllocationChunkSize || spec.ExtentPageBytes != service.DefaultAllocationPageSize {
		t.Fatalf("geometry defaults not applied: %+v", spec)
	}
}

func TestPublishedVolumeSpecFromSummaryPreservesProtectedState(t *testing.T) {
	spec := publishedVolumeSpecFromSummary(0x75, &adminv1.VolumeSummary{
		VolumeId:  "00000075",
		SizeBytes: 1 << 20,
		BlockSize: 4096,
		ProtectedState: &adminv1.VolumeProtectedState{
			State:            " sealed ",
			ReasonCode:       " worm_sealed_read_only ",
			SealedObjectId:   " sealed-image-001 ",
			SealOperationId:  " seal-op-001 ",
			PolicySnapshotId: " policy-snap-001 ",
			LifecycleState:   " materialized ",
			SourceVolumeId:   " 00000065 ",
		},
	})
	if spec.ProtectedState == nil {
		t.Fatalf("ProtectedState is nil")
	}
	if spec.ProtectedState.State != service.VolumeProtectedStateSealed ||
		spec.ProtectedState.ReasonCode != service.ProtectedWriteReasonSealedReadOnly ||
		spec.ProtectedState.SourceVolumeID != "00000065" {
		t.Fatalf("unexpected protected state: %+v", spec.ProtectedState)
	}
}
