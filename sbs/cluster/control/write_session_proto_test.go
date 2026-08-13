package control

import (
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func TestCommitWriteStateRequestProtoRoundTrip(t *testing.T) {
	in := metadata.CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            2,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
	}

	out, err := CommitWriteStateRequestFromProto(CommitWriteStateRequestToProto(in))
	if err != nil {
		t.Fatalf("CommitWriteStateRequestFromProto: %v", err)
	}
	if out != in {
		t.Fatalf("round trip=%+v want %+v", out, in)
	}
}

func TestCommitWriteStateResponseProtoRoundTrip(t *testing.T) {
	inState := metadata.VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             2,
		Revision:          8,
		PlacementPolicyID: "policy-a",
		ProtectionPolicy:  "rf3",
		Status:            metadata.VolumeStatusHealthy,
	}
	inRecord := metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-1",
		AttachmentID:   "att-1",
		Generation:     3,
		Epoch:          2,
		Revision:       8,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
	}

	outState, outRecord, err := CommitWriteStateResponseFromProto(CommitWriteStateResponseToProto(inState, inRecord))
	if err != nil {
		t.Fatalf("CommitWriteStateResponseFromProto: %v", err)
	}
	if outState != inState {
		t.Fatalf("state round trip=%+v want %+v", outState, inState)
	}
	if outRecord != inRecord {
		t.Fatalf("record round trip=%+v want %+v", outRecord, inRecord)
	}
}

func TestCommitPageScopedWriteMetadataRequestProtoRoundTrip(t *testing.T) {
	in := metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            2,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-page",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
		AllocationPages: []metadata.AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         1,
				PageBytes:      4096,
				ChunkSizeBytes: 1024,
				Revision:       3,
				Extents: []metadata.AllocationExtentRecord{
					{LogicalChunkStart: 4, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101, Encryption: ecMetadataTestPayloadEncryptionHeader("replicated:00a1b2c3:101", metadata.PhysicalObjectBackendReplicated, 2*1024)},
					{LogicalChunkStart: 6, ChunkCount: 2, Kind: metadata.AllocationKindZero},
				},
			},
		},
		NormalizeExtentMappings: []uint64{11, 12},
		MutationOperationID:     "write-idem-page",
		ExpectedMutationState:   metadata.MutationOperationRunning,
		AffectedExtentIDs:       []uint64{21},
		AffectedPageNos:         []uint64{1},
		AffectedPageChunkRanges: []metadata.AllocationPageChunkRangeRecord{{PageNo: 1, StartChunk: 1, EndChunk: 3}},
		RetiredPhysicalChunkIDs: []uint64{99},
	}

	out, err := CommitPageScopedWriteMetadataRequestFromProto(CommitPageScopedWriteMetadataRequestToProto(in))
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadataRequestFromProto: %v", err)
	}
	if out.VolumeID != in.VolumeID ||
		out.ExpectedEpoch != in.ExpectedEpoch ||
		out.ExpectedRevision != in.ExpectedRevision ||
		out.IdempotencyKey != in.IdempotencyKey ||
		out.ExpectedIdempotencyState != in.ExpectedIdempotencyState ||
		out.CommittedRevision != in.CommittedRevision ||
		out.MutationOperationID != in.MutationOperationID ||
		out.ExpectedMutationState != in.ExpectedMutationState {
		t.Fatalf("round trip scalar=%+v want %+v", out, in)
	}
	if len(out.AllocationPages) != 1 || len(out.AllocationPages[0].Extents) != 2 || out.AllocationPages[0].Revision != 3 {
		t.Fatalf("round trip allocation pages=%+v", out.AllocationPages)
	}
	gotEncryption := out.AllocationPages[0].Extents[0].Encryption
	if gotEncryption == nil || gotEncryption.ObjectID != "replicated:00a1b2c3:101" || gotEncryption.BackendType != metadata.PhysicalObjectBackendReplicated {
		t.Fatalf("round trip allocation encryption=%+v", gotEncryption)
	}
	if len(out.NormalizeExtentMappings) != 2 || out.NormalizeExtentMappings[0] != 11 || out.NormalizeExtentMappings[1] != 12 {
		t.Fatalf("round trip normalize extents=%v", out.NormalizeExtentMappings)
	}
	if len(out.AffectedExtentIDs) != 1 || out.AffectedExtentIDs[0] != 21 {
		t.Fatalf("round trip affected extents=%v", out.AffectedExtentIDs)
	}
	if len(out.AffectedPageNos) != 1 || out.AffectedPageNos[0] != 1 {
		t.Fatalf("round trip affected pages=%v", out.AffectedPageNos)
	}
	if len(out.AffectedPageChunkRanges) != 1 || out.AffectedPageChunkRanges[0] != in.AffectedPageChunkRanges[0] {
		t.Fatalf("round trip affected page chunk ranges=%v", out.AffectedPageChunkRanges)
	}
	if len(out.RetiredPhysicalChunkIDs) != 1 || out.RetiredPhysicalChunkIDs[0] != 99 {
		t.Fatalf("round trip retired chunks=%v", out.RetiredPhysicalChunkIDs)
	}
}

func TestCommitAppendOnlyWriteStateAndQueueEffectsRequestProtoRoundTripCarriesMutationSnapshot(t *testing.T) {
	in := metadata.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            2,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-append",
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        8,
		AttachmentID:             "att-1",
		Generation:               9,
		AllowMissingWriteIntent:  true,
		AllocationPages: []metadata.AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Revision:       3,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
			},
		}},
		NormalizeExtentMappings: []uint64{11},
		MutationOperationID:     "write-idem-append",
		ExpectedMutationState:   metadata.MutationOperationRunning,
		AffectedExtentIDs:       []uint64{21},
		AffectedPageNos:         []uint64{1},
		AffectedPageChunkRanges: []metadata.AllocationPageChunkRangeRecord{{PageNo: 1, StartChunk: 0, EndChunk: 1}},
		RetiredPhysicalChunkIDs: []uint64{99},
		MutationOperation: metadata.MutationOperationRecord{
			OperationID:             "write-idem-append",
			VolumeID:                "00a1b2c3",
			Kind:                    "write",
			State:                   metadata.MutationOperationRunning,
			AllocationRevision:      7,
			WriterFencingEpoch:      2,
			IdempotencyKey:          "idem-append",
			AffectedExtentIDs:       []uint64{21},
			AffectedPageNos:         []uint64{1},
			RetiredPhysicalChunkIDs: []uint64{99},
			StartedAtUnix:           100,
			LastUpdatedAtUnix:       100,
		},
	}

	out, err := CommitAppendOnlyWriteStateAndQueueEffectsRequestFromProto(CommitAppendOnlyWriteStateAndQueueEffectsRequestToProto(in))
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteStateAndQueueEffectsRequestFromProto: %v", err)
	}
	if out.MutationOperation.OperationID != in.MutationOperation.OperationID ||
		out.MutationOperation.State != in.MutationOperation.State ||
		out.MutationOperation.StartedAtUnix != in.MutationOperation.StartedAtUnix ||
		out.MutationOperation.IdempotencyKey != in.MutationOperation.IdempotencyKey {
		t.Fatalf("mutation snapshot round trip=%+v want %+v", out.MutationOperation, in.MutationOperation)
	}
	if len(out.MutationOperation.RetiredPhysicalChunkIDs) != 1 || out.MutationOperation.RetiredPhysicalChunkIDs[0] != 99 {
		t.Fatalf("mutation snapshot retired chunks=%v", out.MutationOperation.RetiredPhysicalChunkIDs)
	}
	if len(out.AllocationPages) != 1 || out.AllocationPages[0].Extents[0].PhysicalChunkStart != 101 {
		t.Fatalf("allocation pages=%+v", out.AllocationPages)
	}
	if len(out.AffectedPageChunkRanges) != 1 || out.AffectedPageChunkRanges[0] != in.AffectedPageChunkRanges[0] {
		t.Fatalf("affected page chunk ranges=%+v", out.AffectedPageChunkRanges)
	}
	if out.AttachmentID != in.AttachmentID || out.Generation != in.Generation || !out.AllowMissingWriteIntent {
		t.Fatalf("missing-intent fields=%q/%d/%t", out.AttachmentID, out.Generation, out.AllowMissingWriteIntent)
	}
}

func TestCommitCloneDeltaAllocationPagesRequestProtoRoundTrip(t *testing.T) {
	inPages := []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         3,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 12, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 77, Encryption: ecMetadataTestPayloadEncryptionHeader("replicated:00a1b2c3:77", metadata.PhysicalObjectBackendReplicated, 2*1024)},
		},
	}}

	cloneID, outPages, err := CommitCloneDeltaAllocationPagesRequestFromProto(CommitCloneDeltaAllocationPagesRequestToProto("clone-1", inPages))
	if err != nil {
		t.Fatalf("CommitCloneDeltaAllocationPagesRequestFromProto: %v", err)
	}
	if cloneID != "clone-1" {
		t.Fatalf("cloneID=%q want clone-1", cloneID)
	}
	if len(outPages) != 1 || outPages[0].PageNo != 3 || outPages[0].Extents[0].PhysicalChunkStart != 77 {
		t.Fatalf("allocation pages=%+v", outPages)
	}
	gotEncryption := outPages[0].Extents[0].Encryption
	if gotEncryption == nil || gotEncryption.ObjectID != "replicated:00a1b2c3:77" || gotEncryption.BackendType != metadata.PhysicalObjectBackendReplicated {
		t.Fatalf("clone delta allocation encryption=%+v", gotEncryption)
	}
}

func TestCommitWriteStateRequestFromProtoRejectsInvalidIdempotencyState(t *testing.T) {
	_, err := CommitWriteStateRequestFromProto(&internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED,
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCommitWriteStateResponseFromProtoRequiresIdempotencyRecord(t *testing.T) {
	_, _, err := CommitWriteStateResponseFromProto(&internalv1.CommitWriteStateResponse{
		VolumeState: &internalv1.VolumeState{VolumeId: "00a1b2c3"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIdempotencyRecordProtoRoundTripFailedState(t *testing.T) {
	in := metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-failed",
		ResultState:    metadata.IdempotencyFailed,
		Revision:       9,
	}

	out, err := IdempotencyRecordFromProto(IdempotencyRecordToProto(in))
	if err != nil {
		t.Fatalf("IdempotencyRecordFromProto: %v", err)
	}
	if out != in {
		t.Fatalf("round trip=%+v want %+v", out, in)
	}
}

func TestMutationOperationRecordProtoRoundTrip(t *testing.T) {
	in := metadata.MutationOperationRecord{
		OperationID:             "write-1",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   metadata.MutationOperationCommitted,
		PlacementRevision:       7,
		AllocationRevision:      8,
		WriterFencingEpoch:      2,
		IdempotencyKey:          "idem-1",
		AffectedExtentIDs:       []uint64{1, 2},
		AffectedPageNos:         []uint64{3, 4},
		CompletedPageNos:        []uint64{5},
		RetryPageWindows:        []metadata.MutationPageWindowRecord{{ExtentID: 9, StartPageNo: 10, EndPageNo: 11, DataBytes: 12, DataChunks: 13}},
		RetiredPhysicalChunkIDs: []uint64{14, 15},
		StartedAtUnix:           16,
		LastUpdatedAtUnix:       17,
		ErrorMessage:            "done",
	}

	out, err := MutationOperationRecordFromProto(MutationOperationRecordToProto(in))
	if err != nil {
		t.Fatalf("MutationOperationRecordFromProto: %v", err)
	}
	if out.OperationID != in.OperationID || out.State != in.State || out.AllocationRevision != in.AllocationRevision {
		t.Fatalf("round trip=%+v want %+v", out, in)
	}
	if len(out.RetryPageWindows) != 1 || out.RetryPageWindows[0] != in.RetryPageWindows[0] {
		t.Fatalf("retry windows=%+v want %+v", out.RetryPageWindows, in.RetryPageWindows)
	}
	if len(out.RetiredPhysicalChunkIDs) != 2 || out.RetiredPhysicalChunkIDs[1] != 15 {
		t.Fatalf("retired chunks=%+v", out.RetiredPhysicalChunkIDs)
	}
}
