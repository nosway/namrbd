package control

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServeCommitWriteStateDelegatesToInternalService(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 8},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1", ResultState: metadata.IdempotencyCommitted, Revision: 8},
	}
	var records []string
	resp, err := ServeCommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
	}, service, nil, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitWriteState: %v", err)
	}
	if service.called != "commit_write_state" {
		t.Fatalf("called=%q want commit_write_state", service.called)
	}
	if service.commitReq.VolumeID != "00a1b2c3" || service.commitReq.CommittedRevision != 8 {
		t.Fatalf("unexpected request: %+v", service.commitReq)
	}
	if resp.GetVolumeState().GetRevision() != 8 || resp.GetIdempotencyRecord().GetResultState() != internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_COMMITTED {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeWriteSessionMetadataCRUDDelegatesToInternalService(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 7},
		idem: metadata.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-1",
			ResultState:    metadata.IdempotencyCommitted,
			Revision:       7,
		},
		operation: metadata.MutationOperationRecord{
			VolumeID:    "00a1b2c3",
			OperationID: "op-1",
			State:       metadata.MutationOperationCommitted,
		},
	}

	stateResp, err := ServeGetVolumeState(context.Background(), &internalv1.GetVolumeStateRequest{VolumeId: "00a1b2c3"}, service)
	if err != nil {
		t.Fatalf("ServeGetVolumeState: %v", err)
	}
	if service.called != "get_volume_state" || stateResp.GetVolumeState().GetRevision() != 7 {
		t.Fatalf("unexpected get volume result called=%q resp=%+v", service.called, stateResp)
	}

	if _, err := ServePutVolumeState(context.Background(), &internalv1.PutVolumeStateRequest{
		VolumeState: VolumeStateToProto(metadata.VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 8}),
	}, service); err != nil {
		t.Fatalf("ServePutVolumeState: %v", err)
	}
	if service.called != "put_volume_state" || service.state.Revision != 8 {
		t.Fatalf("unexpected put volume result called=%q state=%+v", service.called, service.state)
	}

	idemResp, err := ServeGetIdempotencyRecord(context.Background(), &internalv1.GetIdempotencyRecordRequest{VolumeId: "00a1b2c3", IdempotencyKey: "idem-1"}, service)
	if err != nil {
		t.Fatalf("ServeGetIdempotencyRecord: %v", err)
	}
	if service.called != "get_idempotency" || idemResp.GetIdempotencyRecord().GetResultState() != internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_COMMITTED {
		t.Fatalf("unexpected get idempotency result called=%q resp=%+v", service.called, idemResp)
	}

	if _, err := ServePutIdempotencyRecord(context.Background(), &internalv1.PutIdempotencyRecordRequest{
		IdempotencyRecord: IdempotencyRecordToProto(metadata.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-2",
			ResultState:    metadata.IdempotencyPending,
			Revision:       8,
		}),
	}, service); err != nil {
		t.Fatalf("ServePutIdempotencyRecord: %v", err)
	}
	if service.called != "put_idempotency" || service.idem.IdempotencyKey != "idem-2" {
		t.Fatalf("unexpected put idempotency result called=%q idem=%+v", service.called, service.idem)
	}

	opResp, err := ServeGetMutationOperation(context.Background(), &internalv1.GetMutationOperationRequest{VolumeId: "00a1b2c3", OperationId: "op-1"}, service)
	if err != nil {
		t.Fatalf("ServeGetMutationOperation: %v", err)
	}
	if service.called != "get_mutation" || opResp.GetMutationOperation().GetState() != internalv1.MutationOperationState_MUTATION_OPERATION_STATE_COMMITTED {
		t.Fatalf("unexpected get mutation result called=%q resp=%+v", service.called, opResp)
	}

	if _, err := ServePutMutationOperation(context.Background(), &internalv1.PutMutationOperationRequest{
		MutationOperation: MutationOperationRecordToProto(metadata.MutationOperationRecord{
			VolumeID:    "00a1b2c3",
			OperationID: "op-2",
			State:       metadata.MutationOperationRunning,
		}),
	}, service); err != nil {
		t.Fatalf("ServePutMutationOperation: %v", err)
	}
	if service.called != "put_mutation" || service.operation.OperationID != "op-2" {
		t.Fatalf("unexpected put mutation result called=%q operation=%+v", service.called, service.operation)
	}

	if _, err := ServePutWriteIntent(context.Background(), &internalv1.PutWriteIntentRequest{
		IdempotencyRecord: IdempotencyRecordToProto(metadata.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-intent",
			ResultState:    metadata.IdempotencyPending,
		}),
		MutationOperation: MutationOperationRecordToProto(metadata.MutationOperationRecord{
			VolumeID:       "00a1b2c3",
			OperationID:    "write-idem-intent",
			State:          metadata.MutationOperationRunning,
			IdempotencyKey: "idem-intent",
		}),
	}, service); err != nil {
		t.Fatalf("ServePutWriteIntent: %v", err)
	}
	if service.called != "put_write_intent" || service.idem.IdempotencyKey != "idem-intent" || service.operation.OperationID != "write-idem-intent" {
		t.Fatalf("unexpected put write intent result called=%q idem=%+v operation=%+v", service.called, service.idem, service.operation)
	}
}

func TestServeWriteSessionMetadataCRUDMapsErrors(t *testing.T) {
	service := &fakeWriteSessionInternalService{err: metadata.ErrNotFound}
	_, err := ServeGetVolumeState(context.Background(), &internalv1.GetVolumeStateRequest{VolumeId: "00a1b2c3"}, service)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status=%v err=%v want not found", status.Code(err), err)
	}

	_, err = ServePutIdempotencyRecord(context.Background(), &internalv1.PutIdempotencyRecordRequest{}, &fakeWriteSessionInternalService{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}

	_, err = ServePutMutationOperation(context.Background(), &internalv1.PutMutationOperationRequest{}, &fakeWriteSessionInternalService{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
}

func TestServeCommitWriteStateMapsInvalidRequest(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	var records []string
	_, err := ServeCommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED,
	}, service, nil, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if service.called != "" {
		t.Fatalf("called=%q want no service call", service.called)
	}
	if len(records) != 1 || records[0] != string(WriteSessionErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeCommitWriteStateMapsInternalServiceError(t *testing.T) {
	_, err := ServeCommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
	}, &fakeWriteSessionInternalService{commitErr: metadata.ErrCASConflict}, nil, nil)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
}

func TestServeCommitWriteStateUsesVolumeLocker(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 8},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-1", ResultState: metadata.IdempotencyCommitted, Revision: 8},
	}
	locked := false
	unlocked := false
	_, err := ServeCommitWriteState(context.Background(), &internalv1.CommitWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		IdempotencyKey:           "idem-1",
		CommittedRevision:        8,
	}, service, func(volumeID string) func() {
		if volumeID != "00a1b2c3" {
			t.Fatalf("lock volume_id=%q want 00a1b2c3", volumeID)
		}
		locked = true
		return func() { unlocked = true }
	}, nil)
	if err != nil {
		t.Fatalf("ServeCommitWriteState: %v", err)
	}
	if !locked || !unlocked {
		t.Fatalf("locked=%v unlocked=%v want both true", locked, unlocked)
	}
}

func TestServeCommitPageScopedWriteMetadataDelegatesToInternalService(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-page", ResultState: metadata.IdempotencyCommitted, Revision: 4},
	}
	var records []string
	resp, err := ServeCommitPageScopedWriteMetadata(context.Background(), &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-page",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{
			{
				VolumeId:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
				Extents: []*internalv1.AllocationExtent{
					{
						LogicalChunkStart:  0,
						ChunkCount:         1,
						Kind:               internalv1.AllocationKind_ALLOCATION_KIND_DATA,
						PhysicalChunkStart: 101,
					},
				},
			},
		},
		MutationOperationId:   "write-idem-page",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		AffectedPageNos:       []uint64{0},
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitPageScopedWriteMetadata: %v", err)
	}
	if service.called != "commit_page_scoped_write_metadata" {
		t.Fatalf("called=%q want commit_page_scoped_write_metadata", service.called)
	}
	if service.pageCommitReq.VolumeID != "00a1b2c3" || service.pageCommitReq.IdempotencyKey != "idem-page" {
		t.Fatalf("unexpected request: %+v", service.pageCommitReq)
	}
	if len(service.pageCommitReq.AllocationPages) != 1 || service.pageCommitReq.AllocationPages[0].Revision != 3 {
		t.Fatalf("unexpected allocation pages: %+v", service.pageCommitReq.AllocationPages)
	}
	if resp.GetVolumeState().GetRevision() != 7 || resp.GetIdempotencyRecord().GetRevision() != 4 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeCommitPageScopedWriteMetadataMapsInvalidRequest(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	var records []string
	_, err := ServeCommitPageScopedWriteMetadata(context.Background(), &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if service.called != "" {
		t.Fatalf("called=%q want no service call", service.called)
	}
	if len(records) != 1 || records[0] != string(WriteSessionErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeCommitPageScopedWriteMetadataMapsInternalServiceError(t *testing.T) {
	_, err := ServeCommitPageScopedWriteMetadata(context.Background(), &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		MutationOperationId:      "write-idem-page",
		ExpectedMutationState:    internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, &fakeWriteSessionInternalService{commitErr: metadata.ErrCASConflict}, nil)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
}

func TestServeCommitRangeLocalWriteStateDelegatesToInternalService(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-range", ResultState: metadata.IdempotencyCommitted, Revision: 4},
	}
	var records []string
	resp, err := ServeCommitRangeLocalWriteState(context.Background(), &internalv1.CommitRangeLocalWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-range",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{
			{
				VolumeId:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
			},
		},
		MutationOperationId:   "write-idem-range",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		AffectedPageNos:       []uint64{0},
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitRangeLocalWriteState: %v", err)
	}
	if service.called != "commit_range_local_write_state" {
		t.Fatalf("called=%q want commit_range_local_write_state", service.called)
	}
	if service.pageCommitReq.VolumeID != "00a1b2c3" || service.pageCommitReq.IdempotencyKey != "idem-range" {
		t.Fatalf("unexpected request: %+v", service.pageCommitReq)
	}
	if len(service.pageCommitReq.AllocationPages) != 1 || service.pageCommitReq.AllocationPages[0].Revision != 3 {
		t.Fatalf("unexpected allocation pages: %+v", service.pageCommitReq.AllocationPages)
	}
	if resp.GetVolumeState().GetRevision() != 7 || resp.GetIdempotencyRecord().GetRevision() != 4 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeCommitRangeLocalWriteStateMapsInvalidRequest(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	var records []string
	_, err := ServeCommitRangeLocalWriteState(context.Background(), &internalv1.CommitRangeLocalWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED,
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if service.called != "" {
		t.Fatalf("called=%q want no service call", service.called)
	}
	if len(records) != 1 || records[0] != string(WriteSessionErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeCommitRangeLocalWriteStateMapsInternalServiceError(t *testing.T) {
	_, err := ServeCommitRangeLocalWriteState(context.Background(), &internalv1.CommitRangeLocalWriteStateRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		MutationOperationId:      "write-idem-range",
		ExpectedMutationState:    internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, &fakeWriteSessionInternalService{commitErr: metadata.ErrCASConflict}, nil)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
}

func TestServeCommitAppendOnlyWriteStateAndQueueEffectsDelegatesAndEnqueues(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-append", ResultState: metadata.IdempotencyCommitted, Revision: 9},
	}
	queue := &fakeWriteSessionEffectsQueue{stats: WriteSessionEffectsQueueStats{Depth: 2}}
	var records []string
	resp, err := ServeCommitAppendOnlyWriteStateAndQueueEffects(context.Background(), &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-append",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{
			{
				VolumeId:       "00a1b2c3",
				PageNo:         5,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       3,
			},
		},
		MutationOperationId:     "write-idem-append",
		ExpectedMutationState:   internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		NormalizeExtentIds:      []uint64{12},
		AffectedPageNos:         []uint64{5},
		AffectedExtentIds:       []uint64{12},
		RetiredPhysicalChunkIds: []uint64{99},
	}, service, queue, true, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitAppendOnlyWriteStateAndQueueEffects: %v", err)
	}
	if service.called != "commit_append_only_write_state_and_queue_effects" {
		t.Fatalf("called=%q want commit_append_only_write_state_and_queue_effects", service.called)
	}
	if service.pageCommitReq.VolumeID != "00a1b2c3" || service.pageCommitReq.IdempotencyKey != "idem-append" {
		t.Fatalf("unexpected request: %+v", service.pageCommitReq)
	}
	if queue.calls != 1 {
		t.Fatalf("queue calls=%d want 1", queue.calls)
	}
	if queue.req.CommittedRevision != 9 {
		t.Fatalf("queued committed revision=%d want 9", queue.req.CommittedRevision)
	}
	if len(queue.req.AffectedPageNos) != 1 || queue.req.AffectedPageNos[0] != 5 {
		t.Fatalf("unexpected queued affected pages: %+v", queue.req.AffectedPageNos)
	}
	if resp.GetVolumeState().GetRevision() != 7 || resp.GetIdempotencyRecord().GetRevision() != 9 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeCommitAppendOnlyWriteStateAndQueueEffectsUsesCombinedQueue(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	queue := &fakeAppendOnlyCommitQueue{
		fakeWriteSessionEffectsQueue: fakeWriteSessionEffectsQueue{stats: WriteSessionEffectsQueueStats{Depth: 3}},
		state:                        metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		record: metadata.IdempotencyRecord{
			VolumeID:       "00a1b2c3",
			IdempotencyKey: "idem-append-combined",
			ResultState:    metadata.IdempotencyCommitted,
			Revision:       99,
		},
	}
	var records []string
	resp, err := ServeCommitAppendOnlyWriteStateAndQueueEffects(context.Background(), &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedEpoch:            1,
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-append-combined",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		CommittedRevision:        8,
		AllocationPages: []*internalv1.AllocationPage{{
			VolumeId:       "00a1b2c3",
			PageNo:         5,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
		}},
		MutationOperationId:   "write-idem-append-combined",
		ExpectedMutationState: internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
		AffectedPageNos:       []uint64{5},
	}, service, queue, true, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitAppendOnlyWriteStateAndQueueEffects: %v", err)
	}
	if service.called != "" {
		t.Fatalf("service called=%q want no separate append-only state commit", service.called)
	}
	if queue.commitCalls != 1 || queue.effectsCalls != 0 {
		t.Fatalf("commit calls=%d effects calls=%d want 1/0", queue.commitCalls, queue.effectsCalls)
	}
	if queue.commitReq.IdempotencyKey != "idem-append-combined" || len(queue.commitReq.AllocationPages) != 1 {
		t.Fatalf("combined queue request=%+v", queue.commitReq)
	}
	if resp.GetIdempotencyRecord().GetRevision() != 99 {
		t.Fatalf("response record revision=%d want 99", resp.GetIdempotencyRecord().GetRevision())
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeCommitAppendOnlyWriteStateAndQueueEffectsRequiresQueue(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	var records []string
	_, err := ServeCommitAppendOnlyWriteStateAndQueueEffects(context.Background(), &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		MutationOperationId:      "write-idem-append",
		ExpectedMutationState:    internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, service, nil, false, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status=%v err=%v want failed precondition", status.Code(err), err)
	}
	if service.called != "" {
		t.Fatalf("called=%q want no service call", service.called)
	}
	if len(records) != 1 || records[0] != "failed_precondition" {
		t.Fatalf("records=%v want failed_precondition", records)
	}
}

func TestServeCommitAppendOnlyWriteStateAndQueueEffectsMapsQueueError(t *testing.T) {
	service := &fakeWriteSessionInternalService{
		state: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		idem:  metadata.IdempotencyRecord{VolumeID: "00a1b2c3", IdempotencyKey: "idem-append", ResultState: metadata.IdempotencyCommitted, Revision: 9},
	}
	queue := &fakeWriteSessionEffectsQueue{err: metadata.ErrCASConflict}
	_, err := ServeCommitAppendOnlyWriteStateAndQueueEffects(context.Background(), &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 "00a1b2c3",
		ExpectedIdempotencyState: internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING,
		MutationOperationId:      "write-idem-append",
		ExpectedMutationState:    internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING,
	}, service, queue, false, nil)
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status=%v err=%v want aborted", status.Code(err), err)
	}
	if queue.calls != 1 {
		t.Fatalf("queue calls=%d want 1", queue.calls)
	}
}

func TestServeCommitCloneDeltaAllocationPagesDelegatesToInternalService(t *testing.T) {
	service := &fakeWriteSessionInternalService{}
	var records []string
	resp, err := ServeCommitCloneDeltaAllocationPages(context.Background(), &internalv1.CommitCloneDeltaAllocationPagesRequest{
		CloneId: "clone-1",
		AllocationPages: []*internalv1.AllocationPage{{
			VolumeId:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Revision:       12,
			Extents: []*internalv1.AllocationExtent{{
				LogicalChunkStart:  0,
				ChunkCount:         1,
				Kind:               internalv1.AllocationKind_ALLOCATION_KIND_DATA,
				PhysicalChunkStart: 31,
			}},
		}},
	}, service, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeCommitCloneDeltaAllocationPages: %v", err)
	}
	if resp == nil {
		t.Fatal("response is nil")
	}
	if service.called != "commit_clone_delta_allocation_pages" || service.cloneID != "clone-1" {
		t.Fatalf("called=%q cloneID=%q", service.called, service.cloneID)
	}
	if len(service.clonePages) != 1 || service.clonePages[0].Extents[0].PhysicalChunkStart != 31 {
		t.Fatalf("clone pages=%+v", service.clonePages)
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

type fakeWriteSessionEffectsQueue struct {
	req   metadata.ApplyCommittedWriteEffectsRequest
	stats WriteSessionEffectsQueueStats
	err   error
	calls int
}

func (q *fakeWriteSessionEffectsQueue) EnqueueAndWait(_ context.Context, req metadata.ApplyCommittedWriteEffectsRequest) (WriteSessionEffectsQueueStats, error) {
	q.calls++
	q.req = req
	return q.stats, q.err
}

type fakeAppendOnlyCommitQueue struct {
	fakeWriteSessionEffectsQueue
	commitReq    metadata.CommitWriteMetadataRequest
	state        metadata.VolumeState
	record       metadata.IdempotencyRecord
	commitErr    error
	commitCalls  int
	effectsCalls int
}

func (q *fakeAppendOnlyCommitQueue) EnqueueAndWait(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) (WriteSessionEffectsQueueStats, error) {
	q.effectsCalls++
	return q.fakeWriteSessionEffectsQueue.EnqueueAndWait(ctx, req)
}

func (q *fakeAppendOnlyCommitQueue) EnqueueAppendOnlyCommitAndWait(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, WriteSessionEffectsQueueStats, error) {
	q.commitCalls++
	q.commitReq = req
	return q.state, q.record, q.stats, q.commitErr
}
