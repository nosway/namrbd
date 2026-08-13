package control

import (
	"reflect"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestResolvedExtentPlacementProtoRoundTrip(t *testing.T) {
	rec := metadata.ResolvedExtentPlacement{
		ExtentMapping: metadata.ExtentMappingRecord{
			VolumeID:      "00a1b2c3",
			ExtentID:      1,
			LogicalOffset: 4096,
			LengthBytes:   8192,
			PlacementRef:  "pl-1",
			Revision:      7,
		},
		ReplicaSet: metadata.ReplicaSetState{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            2,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary, FailureDomain: "az-a"},
			},
		},
		Nodes: map[string]metadata.NodeMembershipRecord{
			"node-a": {
				NodeID:         "node-a",
				ReplicaID:      "rep-a",
				LifecycleState: metadata.NodeLifecycleActive,
				HealthState:    metadata.NodeHealthHealthy,
				SBSEndpoints:   []metadata.SBSEndpoint{{Address: "127.0.0.1", Port: 9700}},
			},
		},
	}
	got, err := ResolvedExtentPlacementFromProto(ResolvedExtentPlacementToProto(rec))
	if err != nil {
		t.Fatalf("ResolvedExtentPlacementFromProto: %v", err)
	}
	if got.ExtentMapping != rec.ExtentMapping {
		t.Fatalf("extent mapping=%+v want %+v", got.ExtentMapping, rec.ExtentMapping)
	}
	if got.ReplicaSet.ReplicaSetID != "rs-1" || got.ReplicaSet.Replicas[0].Role != metadata.ReplicaRolePrimary {
		t.Fatalf("replica set=%+v", got.ReplicaSet)
	}
	if got.Nodes["node-a"].HealthState != metadata.NodeHealthHealthy {
		t.Fatalf("nodes=%+v", got.Nodes)
	}
}

func TestResolvedAllocationPageProtoRoundTrip(t *testing.T) {
	header := ecMetadataTestPayloadEncryptionHeader("replicated:00a1b2c3:200", metadata.PhysicalObjectBackendReplicated, 2*1024)
	header.LogicalOffset = 6 * 1024
	header.AuthTagHex = "0123456789abcdef0123456789abcdef"
	rec := metadata.ResolvedAllocationPage{
		Page: metadata.AllocationPageRecord{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Revision:       7,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 100},
				{LogicalChunkStart: 6, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 200, Encryption: header},
			},
		},
		RangeStartChunk: 4,
		RangeEndChunk:   8,
		CoversWholePage: false,
	}
	proto := ResolvedAllocationPageToProto(rec)
	if proto.GetPage().GetExtents()[0].GetEncryption() != nil {
		t.Fatalf("unencrypted extent unexpectedly gained encryption header")
	}
	if proto.GetPage().GetExtents()[1].GetEncryption() == nil {
		t.Fatalf("encrypted extent lost proto encryption header")
	}
	got, err := ResolvedAllocationPageFromProto(proto)
	if err != nil {
		t.Fatalf("ResolvedAllocationPageFromProto: %v", err)
	}
	if got.Page.VolumeID != rec.Page.VolumeID || got.Page.Extents[0].Kind != metadata.AllocationKindData {
		t.Fatalf("page=%+v want %+v", got.Page, rec.Page)
	}
	if got.Page.Extents[0].Encryption != nil {
		t.Fatalf("unencrypted extent round trip gained encryption header: %+v", got.Page.Extents[0].Encryption)
	}
	if !reflect.DeepEqual(got.Page.Extents[1].Encryption, header) {
		t.Fatalf("encrypted extent header=%+v want %+v", got.Page.Extents[1].Encryption, header)
	}
	if got.RangeStartChunk != 4 || got.RangeEndChunk != 8 || got.CoversWholePage {
		t.Fatalf("range=(%d,%d) covers=%t", got.RangeStartChunk, got.RangeEndChunk, got.CoversWholePage)
	}
}
