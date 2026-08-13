package metadata

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPhysicalObjectRecordAndECStripeRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-k-test")

	object := testECPhysicalObjectRecord()
	stripe := testECStripeRecord()
	if err := repo.PutPhysicalObject(ctx, object); err != nil {
		t.Fatalf("PutPhysicalObject: %v", err)
	}
	if err := repo.PutECStripe(ctx, stripe); err != nil {
		t.Fatalf("PutECStripe: %v", err)
	}

	gotObject, err := repo.GetPhysicalObject(ctx, "00A1B2C3", object.ObjectID)
	if err != nil {
		t.Fatalf("GetPhysicalObject: %v", err)
	}
	if gotObject.VolumeID != "00a1b2c3" {
		t.Fatalf("object volume_id=%q want canonical", gotObject.VolumeID)
	}
	ref := gotObject.Ref()
	if ref.BackendType != PhysicalObjectBackendEC || ref.EC == nil {
		t.Fatalf("object ref=%+v", ref)
	}
	if ref.EC.ProfileID != "ec-6-3" || ref.EC.StripeGeneration != 1 {
		t.Fatalf("ec descriptor=%+v", ref.EC)
	}

	gotStripe, err := repo.GetECStripe(ctx, "00a1b2c3", "stripe-000001", 1)
	if err != nil {
		t.Fatalf("GetECStripe: %v", err)
	}
	if gotStripe.ObjectID != object.ObjectID {
		t.Fatalf("stripe object_id=%q want %q", gotStripe.ObjectID, object.ObjectID)
	}
	if len(gotStripe.Shards) != int(gotStripe.DataShards+gotStripe.CodingShards) {
		t.Fatalf("stripe shards=%d", len(gotStripe.Shards))
	}

	objects, err := repo.ListPhysicalObjects(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListPhysicalObjects: %v", err)
	}
	if len(objects) != 1 || objects[0].ObjectID != object.ObjectID {
		t.Fatalf("objects=%+v", objects)
	}
	stripes, err := repo.ListECStripes(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListECStripes: %v", err)
	}
	if len(stripes) != 1 || stripes[0].StripeID != "stripe-000001" {
		t.Fatalf("stripes=%+v", stripes)
	}
}

func TestPhysicalObjectValidationRejectsIncompleteECDescriptor(t *testing.T) {
	rec := testECPhysicalObjectRecord()
	rec.EC.StripeUnitBytes = 0
	err := ValidatePhysicalObjectRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "stripe_unit_bytes") {
		t.Fatalf("ValidatePhysicalObjectRecord error=%v want stripe_unit_bytes", err)
	}
}

func TestPhysicalObjectRecordRoundTripPreservesEncryptionHeader(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-p-test")
	rec := PhysicalObjectRecord{
		VolumeID:      "00a1b2c3",
		ObjectID:      "replicated:00a1b2c3:100",
		BackendType:   PhysicalObjectBackendReplicated,
		LogicalLength: 4096,
		Generation:    7,
		State:         PhysicalObjectStateCommitted,
		Replicated: &ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: 100,
			ChunkCount:         1,
		},
		Encryption: testPayloadEncryptionHeader("replicated:00a1b2c3:100", PhysicalObjectBackendReplicated, 4096),
	}
	if err := repo.PutPhysicalObject(ctx, rec); err != nil {
		t.Fatalf("PutPhysicalObject encrypted record: %v", err)
	}
	got, err := repo.GetPhysicalObject(ctx, "00a1b2c3", rec.ObjectID)
	if err != nil {
		t.Fatalf("GetPhysicalObject encrypted record: %v", err)
	}
	if got.Encryption == nil || got.Encryption.ObjectID != rec.ObjectID {
		t.Fatalf("encryption header not preserved: %+v", got.Encryption)
	}
	ref := got.Ref()
	if ref.Encryption == nil || ref.Encryption.ObjectID != rec.ObjectID {
		t.Fatalf("ref encryption header not preserved: %+v", ref.Encryption)
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("encrypted physical object ref validate: %v", err)
	}
	ref.Encryption.ObjectID = "mutated"
	if got.Encryption.ObjectID != rec.ObjectID {
		t.Fatalf("Ref leaked encryption header pointer alias into record")
	}
}

func TestPhysicalObjectValidationRejectsMismatchedEncryptionHeader(t *testing.T) {
	rec := PhysicalObjectRecord{
		VolumeID:      "00a1b2c3",
		ObjectID:      "replicated:00a1b2c3:100",
		BackendType:   PhysicalObjectBackendReplicated,
		LogicalLength: 4096,
		State:         PhysicalObjectStateCommitted,
		Replicated: &ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: 100,
			ChunkCount:         1,
		},
		Encryption: testPayloadEncryptionHeader("replicated:00a1b2c3:101", PhysicalObjectBackendReplicated, 4096),
	}
	err := ValidatePhysicalObjectRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "object_id") {
		t.Fatalf("ValidatePhysicalObjectRecord error=%v want object_id mismatch", err)
	}

	rec.Encryption = testPayloadEncryptionHeader(rec.ObjectID, PhysicalObjectBackendEC, 4096)
	err = ValidatePhysicalObjectRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "backend_type") {
		t.Fatalf("ValidatePhysicalObjectRecord error=%v want backend_type mismatch", err)
	}
}

func TestECStripeValidationPreservesShardEncryptionHeaders(t *testing.T) {
	rec := testECStripeRecord()
	for i := range rec.Shards {
		rec.Shards[i].SizeBytes = 128 << 10
		rec.Shards[i].Encryption = testPayloadEncryptionShardHeader(rec.Shards[i].ShardObjectID, rec.StripeID, rec.Shards[i].ShardID, rec.Shards[i].SizeBytes)
	}
	if err := ValidateECStripeRecord(rec); err != nil {
		t.Fatalf("ValidateECStripeRecord encrypted shards: %v", err)
	}

	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-p-test")
	if err := repo.PutECStripe(ctx, rec); err != nil {
		t.Fatalf("PutECStripe encrypted shards: %v", err)
	}
	got, err := repo.GetECStripe(ctx, "00a1b2c3", rec.StripeID, rec.StripeGeneration)
	if err != nil {
		t.Fatalf("GetECStripe encrypted shards: %v", err)
	}
	if got.Shards[0].Encryption == nil || got.Shards[0].Encryption.ShardID != got.Shards[0].ShardID {
		t.Fatalf("shard encryption header not preserved: %+v", got.Shards[0].Encryption)
	}
}

func TestECStripeValidationRejectsMismatchedShardEncryptionHeader(t *testing.T) {
	rec := testECStripeRecord()
	rec.Shards[0].SizeBytes = 128 << 10
	rec.Shards[0].Encryption = testPayloadEncryptionShardHeader("other-shard", rec.StripeID, rec.Shards[0].ShardID, rec.Shards[0].SizeBytes)
	err := ValidateECStripeRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "shard_object_id") {
		t.Fatalf("ValidateECStripeRecord error=%v want shard object mismatch", err)
	}

	rec = testECStripeRecord()
	rec.Shards[0].SizeBytes = 128 << 10
	rec.Shards[0].Encryption = testPayloadEncryptionShardHeader(rec.Shards[0].ShardObjectID, rec.StripeID, rec.Shards[0].ShardID+1, rec.Shards[0].SizeBytes)
	err = ValidateECStripeRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "shard_id") {
		t.Fatalf("ValidateECStripeRecord error=%v want shard id mismatch", err)
	}
}

func TestECStripeValidationRejectsIncompleteShardSet(t *testing.T) {
	rec := testECStripeRecord()
	rec.Shards = rec.Shards[:len(rec.Shards)-1]
	err := ValidateECStripeRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "shard count") {
		t.Fatalf("ValidateECStripeRecord error=%v want shard count", err)
	}
}

func TestECStripeValidationRejectsDuplicateRoleIndex(t *testing.T) {
	rec := testECStripeRecord()
	rec.Shards[1].RoleIndex = rec.Shards[0].RoleIndex
	err := ValidateECStripeRecord(rec)
	if err == nil || !strings.Contains(err.Error(), "duplicate role_index") {
		t.Fatalf("ValidateECStripeRecord error=%v want duplicate role_index", err)
	}
}

func TestResolveAllocationEntriesFromPageHandlesReplicatedAndECBackends(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "")
	object := testECPhysicalObjectRecord()
	stripe := testECStripeRecord()
	if err := repo.PutPhysicalObject(ctx, object); err != nil {
		t.Fatalf("PutPhysicalObject: %v", err)
	}
	if err := repo.PutECStripe(ctx, stripe); err != nil {
		t.Fatalf("PutECStripe: %v", err)
	}

	page := AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         1,
		PageBytes:      16 << 10,
		ChunkSizeBytes: 4 << 10,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 100},
			{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindData, BackingRef: object.ObjectID},
		},
	}
	entries, err := ResolveAllocationEntriesFromPage(ctx, repo, page)
	if err != nil {
		t.Fatalf("ResolveAllocationEntriesFromPage: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries=%d want=3", len(entries))
	}
	if entries[1].Entry.PhysicalObjectRef.BackendType != PhysicalObjectBackendReplicated {
		t.Fatalf("entry[1] backend=%q", entries[1].Entry.PhysicalObjectRef.BackendType)
	}
	ecEntry := entries[2]
	if ecEntry.Entry.PhysicalObjectRef.BackendType != PhysicalObjectBackendEC {
		t.Fatalf("entry[2] backend=%q", ecEntry.Entry.PhysicalObjectRef.BackendType)
	}
	if ecEntry.Entry.PhysicalObjectRef.EC.StripeID != "stripe-000001" {
		t.Fatalf("ec descriptor=%+v", ecEntry.Entry.PhysicalObjectRef.EC)
	}
	if ecEntry.PhysicalObject == nil || ecEntry.PhysicalObject.ObjectID != object.ObjectID {
		t.Fatalf("physical object=%+v", ecEntry.PhysicalObject)
	}
	if ecEntry.ECStripe == nil || ecEntry.ECStripe.ObjectID != object.ObjectID {
		t.Fatalf("ec stripe=%+v", ecEntry.ECStripe)
	}
}

func TestResolveSnapshotAllocationEntriesPreservesECBackingRefs(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "")
	object := testECPhysicalObjectRecord()
	stripe := testECStripeRecord()
	if err := repo.PutPhysicalObject(ctx, object); err != nil {
		t.Fatalf("PutPhysicalObject: %v", err)
	}
	if err := repo.PutECStripe(ctx, stripe); err != nil {
		t.Fatalf("PutECStripe: %v", err)
	}

	snapshotID := "snap-00a1b2c3-20260601T000000.000000000Z"
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      16 << 10,
		ChunkSizeBytes: 4 << 10,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, BackingRef: object.ObjectID},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	page, err := repo.GetSnapshotAllocationPage(ctx, snapshotID, 0)
	if err != nil {
		t.Fatalf("GetSnapshotAllocationPage: %v", err)
	}
	entries, err := ResolveAllocationEntriesFromPage(ctx, repo, page)
	if err != nil {
		t.Fatalf("ResolveAllocationEntriesFromPage: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries=%d want=1", len(entries))
	}
	entry := entries[0]
	if entry.Entry.PhysicalObjectRef == nil || entry.Entry.PhysicalObjectRef.BackendType != PhysicalObjectBackendEC {
		t.Fatalf("physical ref=%+v", entry.Entry.PhysicalObjectRef)
	}
	if entry.PhysicalObject == nil || entry.PhysicalObject.ObjectID != object.ObjectID {
		t.Fatalf("physical object=%+v", entry.PhysicalObject)
	}
	if entry.ECStripe == nil || entry.ECStripe.StripeGeneration != 1 || entry.ECStripe.ObjectID != object.ObjectID {
		t.Fatalf("ec stripe=%+v", entry.ECStripe)
	}
}

func TestResolveAllocationEntriesFromPageRejectsMissingECBacking(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	_, err := ResolveAllocationEntriesFromPage(context.Background(), repo, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         1,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, BackingRef: "missing-object"},
		},
	})
	if err == nil || !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveAllocationEntriesFromPage error=%v want ErrNotFound", err)
	}
}

func TestResolveAllocationEntriesFromPageRejectsMixedBackingRefAndPhysicalChunk(t *testing.T) {
	repo := NewRepository(newFakeKV(), "")
	_, err := ResolveAllocationEntriesFromPage(context.Background(), repo, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         1,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 100, BackingRef: "ec-object"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "must not mix backing_ref") {
		t.Fatalf("ResolveAllocationEntriesFromPage error=%v want mixed backing error", err)
	}
}

func testECPhysicalObjectRecord() PhysicalObjectRecord {
	return PhysicalObjectRecord{
		VolumeID:      "00a1b2c3",
		ObjectID:      "ec-object-000001",
		BackendType:   PhysicalObjectBackendEC,
		LogicalLength: 6 * 128 << 10,
		Generation:    1,
		State:         PhysicalObjectStateCommitted,
		EC: &ECPhysicalObjectDescriptor{
			ProfileID:        "ec-6-3",
			StripeID:         "stripe-000001",
			StripeGeneration: 1,
			StripeUnitBytes:  128 << 10,
			DataShards:       6,
			CodingShards:     3,
		},
	}
}

func testECStripeRecord() ECStripeRecord {
	shards := make([]ECShardRecord, 0, 9)
	for i := uint32(0); i < 6; i++ {
		shards = append(shards, ECShardRecord{
			ShardID:       i,
			Role:          ECShardRoleData,
			RoleIndex:     i,
			Zone:          "zone-a",
			NodeID:        "node-a-" + oneDigit(i),
			StoreID:       "store-a-" + oneDigit(i),
			ShardObjectID: "ec-object-000001-data-" + oneDigit(i),
		})
	}
	for i := uint32(0); i < 3; i++ {
		shardID := uint32(6) + i
		shards = append(shards, ECShardRecord{
			ShardID:       shardID,
			Role:          ECShardRoleCoding,
			RoleIndex:     i,
			Zone:          "zone-b",
			NodeID:        "node-b-" + oneDigit(i),
			StoreID:       "store-b-" + oneDigit(i),
			ShardObjectID: "ec-object-000001-coding-" + oneDigit(i),
		})
	}
	return ECStripeRecord{
		VolumeID:         "00a1b2c3",
		ObjectID:         "ec-object-000001",
		ProfileID:        "ec-6-3",
		StripeID:         "stripe-000001",
		StripeGeneration: 1,
		StripeUnitBytes:  128 << 10,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		State:            ECStripeStateCommitted,
		Shards:           shards,
	}
}

func testPayloadEncryptionHeader(objectID string, backend PhysicalObjectBackendType, plaintextLength uint64) *PayloadEncryptionHeader {
	return &PayloadEncryptionHeader{
		HeaderVersion:    PayloadEncryptionHeaderVersion,
		CipherSuite:      PayloadCipherSuiteAES256GCM,
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

func testPayloadEncryptionShardHeader(objectID, stripeID string, shardID uint32, sizeBytes uint32) *PayloadEncryptionHeader {
	header := testPayloadEncryptionHeader(objectID, PhysicalObjectBackendEC, uint64(sizeBytes))
	header.StripeID = stripeID
	header.ShardID = shardID
	header.ShardIDPresent = true
	return header
}

func oneDigit(n uint32) string {
	return string(rune('0' + n))
}
