package control

import (
	"reflect"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestECMetadataProtoRoundTrip(t *testing.T) {
	object := ecMetadataTestPhysicalObject()
	gotObject, err := PhysicalObjectRecordFromProto(PhysicalObjectRecordToProto(object))
	if err != nil {
		t.Fatalf("PhysicalObjectRecordFromProto: %v", err)
	}
	if !reflect.DeepEqual(gotObject, object) {
		t.Fatalf("physical object round trip mismatch:\ngot  %+v\nwant %+v", gotObject, object)
	}

	stripe := ecMetadataTestStripe()
	gotStripe, err := ECStripeRecordFromProto(ECStripeRecordToProto(stripe))
	if err != nil {
		t.Fatalf("ECStripeRecordFromProto: %v", err)
	}
	if !reflect.DeepEqual(gotStripe, stripe) {
		t.Fatalf("ec stripe round trip mismatch:\ngot  %+v\nwant %+v", gotStripe, stripe)
	}

	req := metadata.CommitECFullStripeWriteRequest{
		VolumeID:                "00a1b2c3",
		ExpectedEpoch:           1,
		ExpectedRevision:        2,
		IdempotencyKey:          "idem-ec",
		CommittedRevision:       3,
		PhysicalObject:          object,
		ECStripe:                stripe,
		AllocationPages:         []metadata.AllocationPageRecord{{VolumeID: "00a1b2c3", PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 2, Extents: []metadata.AllocationExtentRecord{}}},
		MutationOperationID:     "ec-write-0",
		ExpectedMutationState:   metadata.MutationOperationRunning,
		AffectedPageNos:         []uint64{0},
		AffectedExtentIDs:       []uint64{1},
		RetiredPhysicalChunkIDs: []uint64{9},
		RetiredECObjects:        []metadata.RetiredECObjectRef{{ObjectID: "old-ec-object-0", StripeID: "0", StripeGeneration: 1}},
		MutationOperation: metadata.MutationOperationRecord{
			OperationID:             "ec-write-0",
			VolumeID:                "00a1b2c3",
			Kind:                    "ec-write-full-stripe",
			State:                   metadata.MutationOperationRunning,
			WriterFencingEpoch:      1,
			IdempotencyKey:          "idem-ec",
			AffectedExtentIDs:       []uint64{},
			AffectedPageNos:         []uint64{},
			CompletedPageNos:        []uint64{},
			RetryPageWindows:        []metadata.MutationPageWindowRecord{},
			RetiredPhysicalChunkIDs: []uint64{},
			StartedAtUnix:           100,
			LastUpdatedAtUnix:       100,
		},
	}
	gotReq, err := CommitECFullStripeWriteRequestFromProto(CommitECFullStripeWriteRequestToProto(req))
	if err != nil {
		t.Fatalf("CommitECFullStripeWriteRequestFromProto: %v", err)
	}
	normalizeECCommitRequestForTest(&gotReq)
	normalizeECCommitRequestForTest(&req)
	if !reflect.DeepEqual(gotReq, req) {
		t.Fatalf("commit request round trip mismatch:\ngot  %+v\nwant %+v", gotReq, req)
	}

	discardReq := metadata.CommitECDiscardRequest{
		VolumeID:                "00a1b2c3",
		ExpectedEpoch:           1,
		ExpectedRevision:        3,
		IdempotencyKey:          "idem-ec-discard",
		CommittedRevision:       4,
		AllocationPages:         []metadata.AllocationPageRecord{{VolumeID: "00a1b2c3", PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 3, Extents: []metadata.AllocationExtentRecord{}}},
		MutationOperationID:     "ec-discard-0",
		ExpectedMutationState:   metadata.MutationOperationRunning,
		AffectedPageNos:         []uint64{0},
		AffectedExtentIDs:       []uint64{2},
		RetiredPhysicalChunkIDs: []uint64{10},
		RetiredECObjects:        []metadata.RetiredECObjectRef{{ObjectID: "old-ec-object-0", StripeID: "0", StripeGeneration: 1}},
	}
	gotDiscardReq, err := CommitECDiscardRequestFromProto(CommitECDiscardRequestToProto(discardReq))
	if err != nil {
		t.Fatalf("CommitECDiscardRequestFromProto: %v", err)
	}
	if !reflect.DeepEqual(gotDiscardReq, discardReq) {
		t.Fatalf("discard request round trip mismatch:\ngot  %+v\nwant %+v", gotDiscardReq, discardReq)
	}
}

func normalizeECCommitRequestForTest(req *metadata.CommitECFullStripeWriteRequest) {
	normalizeMutationOperationRecordForTest(&req.MutationOperation)
}

func normalizeMutationOperationRecordForTest(record *metadata.MutationOperationRecord) {
	if len(record.AffectedExtentIDs) == 0 {
		record.AffectedExtentIDs = nil
	}
	if len(record.AffectedPageNos) == 0 {
		record.AffectedPageNos = nil
	}
	if len(record.CompletedPageNos) == 0 {
		record.CompletedPageNos = nil
	}
	if len(record.RetryPageWindows) == 0 {
		record.RetryPageWindows = nil
	}
	if len(record.RetiredPhysicalChunkIDs) == 0 {
		record.RetiredPhysicalChunkIDs = nil
	}
}

func ecMetadataTestPhysicalObject() metadata.PhysicalObjectRecord {
	return metadata.PhysicalObjectRecord{
		VolumeID:      "00a1b2c3",
		ObjectID:      "ec-object-0",
		BackendType:   metadata.PhysicalObjectBackendEC,
		PlacementRef:  "ec/ec-6-3/0/1",
		LogicalLength: 786432,
		Generation:    1,
		Checksum:      "payload-checksum",
		Encryption:    ecMetadataTestPayloadEncryptionHeader("ec-object-0", metadata.PhysicalObjectBackendEC, 786432),
		State:         metadata.PhysicalObjectStatePreparing,
		EC: &metadata.ECPhysicalObjectDescriptor{
			ProfileID:        "ec-6-3",
			StripeID:         "0",
			StripeGeneration: 1,
			StripeUnitBytes:  128 << 10,
			DataShards:       2,
			CodingShards:     1,
			StripeOffset:     0,
			DataShardRefs:    []string{"data-0", "data-1"},
			CodeShardRefs:    []string{"coding-0"},
		},
		CreatedAtUnix: 11,
		UpdatedAtUnix: 12,
	}
}

func ecMetadataTestStripe() metadata.ECStripeRecord {
	return metadata.ECStripeRecord{
		VolumeID:         "00a1b2c3",
		ObjectID:         "ec-object-0",
		ProfileID:        "ec-6-3",
		StripeID:         "0",
		StripeGeneration: 1,
		StripeUnitBytes:  128 << 10,
		DataShards:       2,
		CodingShards:     1,
		TopologyRevision: 1,
		State:            metadata.ECStripeStatePreparing,
		Shards: []metadata.ECShardRecord{
			{ShardID: 0, Role: metadata.ECShardRoleData, RoleIndex: 0, Zone: "z1", NodeID: "n1", StoreID: "n1/default", ShardObjectID: "obj-0", Checksum: "sum-0", SizeBytes: 128 << 10, Encryption: ecMetadataTestShardEncryptionHeader("obj-0", "0", 0, 128<<10)},
			{ShardID: 1, Role: metadata.ECShardRoleData, RoleIndex: 1, Zone: "z2", NodeID: "n2", StoreID: "n2/default", ShardObjectID: "obj-1", Checksum: "sum-1", SizeBytes: 128 << 10, Encryption: ecMetadataTestShardEncryptionHeader("obj-1", "0", 1, 128<<10)},
			{ShardID: 2, Role: metadata.ECShardRoleCoding, RoleIndex: 0, Zone: "z3", NodeID: "n3", StoreID: "n3/default", ShardObjectID: "obj-2", Checksum: "sum-2", SizeBytes: 128 << 10, Encryption: ecMetadataTestShardEncryptionHeader("obj-2", "0", 2, 128<<10)},
		},
		CreatedAtUnix: 11,
		UpdatedAtUnix: 12,
	}
}

func ecMetadataTestPayloadEncryptionHeader(objectID string, backend metadata.PhysicalObjectBackendType, plaintextLength uint64) *metadata.PayloadEncryptionHeader {
	return &metadata.PayloadEncryptionHeader{
		HeaderVersion:    metadata.PayloadEncryptionHeaderVersion,
		CipherSuite:      metadata.PayloadCipherSuiteAES256GCM,
		EncryptionScope:  "volume",
		SecurityPolicyID: "phase-p-security-policy-fixture",
		PolicyGeneration: 1,
		KeyProviderID:    "phase-p-local-fixture",
		DataKeyID:        "phase-p-data-key-fixture",
		KeyID:            "phase-p-provider-key-fixture",
		KeyVersion:       1,
		KeyGeneration:    1,
		ObjectID:         objectID,
		BackendType:      backend,
		NonceHex:         "00112233445566778899aabb",
		NonceSource:      "fixture_deterministic_unique_object",
		AADDigest:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		PlaintextLength:  plaintextLength,
		CiphertextLength: plaintextLength + 16,
		AuthTagBytes:     16,
	}
}

func ecMetadataTestShardEncryptionHeader(objectID, stripeID string, shardID uint32, sizeBytes uint32) *metadata.PayloadEncryptionHeader {
	header := ecMetadataTestPayloadEncryptionHeader(objectID, metadata.PhysicalObjectBackendEC, uint64(sizeBytes))
	header.StripeID = stripeID
	header.ShardID = shardID
	header.ShardIDPresent = true
	return header
}
