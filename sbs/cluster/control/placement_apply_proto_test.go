package control

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func TestPlacementApplyRequestProtoRoundTrip(t *testing.T) {
	req := metadata.PlacementApplyRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 42,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         7,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Revision:       42,
				Extents: []metadata.AllocationExtentRecord{
					{
						LogicalChunkStart:  0,
						ChunkCount:         2,
						Kind:               metadata.AllocationKindData,
						PhysicalChunkStart: 100,
						BackingRef:         "store-a",
						Generation:         3,
						Checksum:           "sum",
						Encryption:         ecMetadataTestPayloadEncryptionHeader("replicated:00a1b2c3:100", metadata.PhysicalObjectBackendReplicated, 2*1024),
					},
					{
						LogicalChunkStart: 2,
						ChunkCount:        2,
						Kind:              metadata.AllocationKindZero,
					},
					{
						LogicalChunkStart: 4,
						ChunkCount:        1,
						Kind:              metadata.AllocationKindShared,
						BackingRef:        "shared-a",
					},
				},
			},
		},
		NormalizeExtentIDs:      []uint64{9, 10},
		RetiredPhysicalChunkIDs: []uint64{99, 100},
	}

	got, err := PlacementApplyRequestFromProto(PlacementApplyRequestToProto(req))
	if err != nil {
		t.Fatalf("PlacementApplyRequestFromProto: %v", err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("round trip mismatch\ngot:  %#v\nwant: %#v", got, req)
	}
}

func TestPlacementApplyRequestFromProtoRejectsNil(t *testing.T) {
	_, err := PlacementApplyRequestFromProto(nil)
	if !errors.Is(err, metadata.ErrInvalidPlacementApplyRequest) {
		t.Fatalf("PlacementApplyRequestFromProto(nil) error=%v want ErrInvalidPlacementApplyRequest", err)
	}
}

func TestPlacementApplyRequestFromProtoRejectsInvalidKind(t *testing.T) {
	_, err := PlacementApplyRequestFromProto(&internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "00a1b2c3",
		CommittedRevision: 1,
		AllocationPages: []*internalv1.AllocationPage{
			{
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Extents: []*internalv1.AllocationExtent{
					{
						LogicalChunkStart: 0,
						ChunkCount:        1,
						Kind:              internalv1.AllocationKind_ALLOCATION_KIND_UNSPECIFIED,
					},
				},
			},
		},
	})
	if !errors.Is(err, metadata.ErrInvalidPlacementApplyRequest) {
		t.Fatalf("PlacementApplyRequestFromProto invalid kind error=%v want ErrInvalidPlacementApplyRequest", err)
	}
}

func TestPlacementApplyRequestFromProtoValidatesRequest(t *testing.T) {
	_, err := PlacementApplyRequestFromProto(&internalv1.ApplyPlacementChangesRequest{
		VolumeId:          "not-a-volume",
		CommittedRevision: 1,
	})
	if !errors.Is(err, metadata.ErrInvalidPlacementApplyRequest) {
		t.Fatalf("PlacementApplyRequestFromProto invalid request error=%v want ErrInvalidPlacementApplyRequest", err)
	}
}
