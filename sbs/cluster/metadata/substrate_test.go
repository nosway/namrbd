package metadata

import "testing"

func TestAllocationEntriesFromPageMapsCurrentRecordsToSubstrateEntries(t *testing.T) {
	page := AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         7,
		PageBytes:      4 * 65536,
		ChunkSizeBytes: 65536,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
			{LogicalChunkStart: 1, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 100, Generation: 3, Checksum: "abc", Encryption: testPayloadEncryptionHeader("replicated:00a1b2c3:100", PhysicalObjectBackendReplicated, 2*65536)},
			{LogicalChunkStart: 3, ChunkCount: 1, Kind: AllocationKindShared, PhysicalChunkStart: 200, Generation: 4},
		},
	}

	entries, err := AllocationEntriesFromPage(page)
	if err != nil {
		t.Fatalf("AllocationEntriesFromPage: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries=%d want=3", len(entries))
	}
	if entries[0].State != AllocationEntryStateZero || entries[0].PhysicalObjectRef != nil {
		t.Fatalf("zero entry=%+v", entries[0])
	}
	data := entries[1]
	if data.State != AllocationEntryStateAllocated {
		t.Fatalf("data state=%q", data.State)
	}
	if data.PhysicalObjectRef == nil {
		t.Fatal("data entry missing physical object ref")
	}
	if data.PhysicalObjectRef.BackendType != PhysicalObjectBackendReplicated {
		t.Fatalf("backend=%q", data.PhysicalObjectRef.BackendType)
	}
	if data.PhysicalObjectRef.ObjectID != "replicated:00a1b2c3:100" {
		t.Fatalf("object_id=%q", data.PhysicalObjectRef.ObjectID)
	}
	if data.PhysicalObjectRef.LogicalLength != 2*65536 {
		t.Fatalf("logical_length=%d", data.PhysicalObjectRef.LogicalLength)
	}
	if data.PhysicalObjectRef.Replicated == nil || data.PhysicalObjectRef.Replicated.PhysicalChunkStart != 100 {
		t.Fatalf("replicated descriptor=%+v", data.PhysicalObjectRef.Replicated)
	}
	if data.PhysicalObjectRef.Encryption == nil {
		t.Fatal("data entry missing encryption header")
	}
	if data.PhysicalObjectRef.Encryption.ObjectID != "replicated:00a1b2c3:100" {
		t.Fatalf("encryption object_id=%q", data.PhysicalObjectRef.Encryption.ObjectID)
	}
	if entries[2].State != AllocationEntryStateShared {
		t.Fatalf("shared state=%q", entries[2].State)
	}
	for idx, entry := range entries {
		if err := entry.ValidateCommittedReadViewEntry(); err != nil {
			t.Fatalf("entry[%d] failed committed read-view validation: %v", idx, err)
		}
	}
}

func TestAllocationEntriesFromPageRejectsInvalidCurrentRecords(t *testing.T) {
	tests := []struct {
		name   string
		extent AllocationExtentRecord
	}{
		{
			name:   "zero chunk count",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, Kind: AllocationKindZero},
		},
		{
			name:   "zero extent with physical object",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero, PhysicalChunkStart: 99},
		},
		{
			name:   "zero extent with encryption header",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero, Encryption: testPayloadEncryptionHeader("replicated:00a1b2c3:99", PhysicalObjectBackendReplicated, 65536)},
		},
		{
			name:   "data extent without physical object",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData},
		},
		{
			name:   "data extent with mismatched encryption object",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 99, Encryption: testPayloadEncryptionHeader("replicated:00a1b2c3:98", PhysicalObjectBackendReplicated, 65536)},
		},
		{
			name:   "unsupported kind",
			extent: AllocationExtentRecord{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKind("mystery")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := AllocationEntriesFromPage(AllocationPageRecord{
				VolumeID:       "00a1b2c3",
				PageBytes:      65536,
				ChunkSizeBytes: 65536,
				Extents:        []AllocationExtentRecord{tc.extent},
			})
			if err == nil {
				t.Fatal("AllocationEntriesFromPage succeeded")
			}
		})
	}
}

func TestAllocationEntryCommittedReadViewValidation(t *testing.T) {
	validReplicatedRef := &PhysicalObjectRef{
		BackendType:   PhysicalObjectBackendReplicated,
		ObjectID:      "replicated:00a1b2c3:10",
		LogicalLength: 65536,
		Replicated: &ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: 10,
			ChunkCount:         1,
		},
	}
	validECRef := &PhysicalObjectRef{
		BackendType:   PhysicalObjectBackendEC,
		ObjectID:      "ec:00a1b2c3:stripe-7",
		LogicalLength: 65536,
		EC: &ECPhysicalObjectDescriptor{
			ProfileID:        "ec-4-2",
			StripeID:         "stripe-7",
			StripeGeneration: 1,
			StripeUnitBytes:  128 << 10,
			DataShards:       4,
			CodingShards:     2,
		},
	}

	tests := []struct {
		name    string
		entry   AllocationEntry
		wantErr bool
	}{
		{
			name: "zero",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateZero,
			},
		},
		{
			name: "replicated allocated",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateAllocated,
				PhysicalObjectRef: validReplicatedRef,
			},
		},
		{
			name: "ec allocated",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateAllocated,
				PhysicalObjectRef: validECRef,
			},
		},
		{
			name: "deleted not visible",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateDeleted,
			},
			wantErr: true,
		},
		{
			name: "pending not visible",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStatePending,
			},
			wantErr: true,
		},
		{
			name: "allocated missing ref",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateAllocated,
			},
			wantErr: true,
		},
		{
			name: "zero with ref",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateZero,
				PhysicalObjectRef: validReplicatedRef,
			},
			wantErr: true,
		},
		{
			name: "ec missing descriptor",
			entry: AllocationEntry{
				LogicalChunkStart: 0,
				ChunkCount:        1,
				State:             AllocationEntryStateAllocated,
				PhysicalObjectRef: &PhysicalObjectRef{
					BackendType:   PhysicalObjectBackendEC,
					ObjectID:      "ec:00a1b2c3:stripe-8",
					LogicalLength: 65536,
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.entry.ValidateCommittedReadViewEntry()
			if tc.wantErr && err == nil {
				t.Fatal("ValidateCommittedReadViewEntry succeeded")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateCommittedReadViewEntry: %v", err)
			}
		})
	}
}
