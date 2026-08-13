package control

import (
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func CommitWriteStateRequestToProto(req metadata.CommitWriteStateRequest) *internalv1.CommitWriteStateRequest {
	return &internalv1.CommitWriteStateRequest{
		VolumeId:                 req.VolumeID,
		ExpectedEpoch:            req.ExpectedEpoch,
		ExpectedRevision:         req.ExpectedRevision,
		IdempotencyKey:           req.IdempotencyKey,
		ExpectedIdempotencyState: idempotencyResultStateToProto(req.ExpectedIdempotencyState),
		CommittedRevision:        req.CommittedRevision,
	}
}

func CommitWriteStateRequestFromProto(req *internalv1.CommitWriteStateRequest) (metadata.CommitWriteStateRequest, error) {
	if req == nil {
		return metadata.CommitWriteStateRequest{}, InvalidWriteSessionRequestError("commit write state proto request is required")
	}
	state, err := idempotencyResultStateFromProto(req.GetExpectedIdempotencyState())
	if err != nil {
		return metadata.CommitWriteStateRequest{}, err
	}
	return metadata.CommitWriteStateRequest{
		VolumeID:                 req.GetVolumeId(),
		ExpectedEpoch:            req.GetExpectedEpoch(),
		ExpectedRevision:         req.GetExpectedRevision(),
		IdempotencyKey:           req.GetIdempotencyKey(),
		ExpectedIdempotencyState: state,
		CommittedRevision:        req.GetCommittedRevision(),
	}, nil
}

func CommitWriteStateResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitWriteStateResponse {
	return &internalv1.CommitWriteStateResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitPageScopedWriteMetadataResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitPageScopedWriteMetadataResponse {
	return &internalv1.CommitPageScopedWriteMetadataResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitRangeLocalWriteStateResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitRangeLocalWriteStateResponse {
	return &internalv1.CommitRangeLocalWriteStateResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitAppendOnlyWriteStateAndQueueEffectsResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse {
	return &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitCloneDeltaAllocationPagesRequestToProto(cloneID string, pages []metadata.AllocationPageRecord) *internalv1.CommitCloneDeltaAllocationPagesRequest {
	return &internalv1.CommitCloneDeltaAllocationPagesRequest{
		CloneId:         cloneID,
		AllocationPages: allocationPagesToProto(pages),
	}
}

func CommitCloneDeltaAllocationPagesRequestFromProto(req *internalv1.CommitCloneDeltaAllocationPagesRequest) (string, []metadata.AllocationPageRecord, error) {
	if req == nil {
		return "", nil, InvalidWriteSessionRequestError("clone delta allocation pages proto request is required")
	}
	pages, err := allocationPagesFromProto(req.GetAllocationPages())
	if err != nil {
		return "", nil, err
	}
	return req.GetCloneId(), pages, nil
}

func CommitWriteStateResponseFromProto(resp *internalv1.CommitWriteStateResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, InvalidWriteSessionRequestError("commit write state proto response is required")
	}
	return commitWriteStateResponseFieldsFromProto(resp.GetVolumeState(), resp.GetIdempotencyRecord())
}

func CommitPageScopedWriteMetadataResponseFromProto(resp *internalv1.CommitPageScopedWriteMetadataResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, InvalidWriteSessionRequestError("page-scoped write metadata proto response is required")
	}
	return commitWriteStateResponseFieldsFromProto(resp.GetVolumeState(), resp.GetIdempotencyRecord())
}

func CommitRangeLocalWriteStateResponseFromProto(resp *internalv1.CommitRangeLocalWriteStateResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, InvalidWriteSessionRequestError("range-local write state proto response is required")
	}
	return commitWriteStateResponseFieldsFromProto(resp.GetVolumeState(), resp.GetIdempotencyRecord())
}

func CommitAppendOnlyWriteStateAndQueueEffectsResponseFromProto(resp *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, InvalidWriteSessionRequestError("append-only write state effects proto response is required")
	}
	return commitWriteStateResponseFieldsFromProto(resp.GetVolumeState(), resp.GetIdempotencyRecord())
}

func commitWriteStateResponseFieldsFromProto(protoState *internalv1.VolumeState, protoRecord *internalv1.IdempotencyRecord) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	state := VolumeStateFromProto(protoState)
	record, err := IdempotencyRecordFromProto(protoRecord)
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func CommitPageScopedWriteMetadataRequestToProto(req metadata.CommitWriteMetadataRequest) *internalv1.CommitPageScopedWriteMetadataRequest {
	protoReq := commitWriteMetadataRequestToProtoFields(req)
	return &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 protoReq.VolumeId,
		ExpectedEpoch:            protoReq.ExpectedEpoch,
		ExpectedRevision:         protoReq.ExpectedRevision,
		IdempotencyKey:           protoReq.IdempotencyKey,
		ExpectedIdempotencyState: protoReq.ExpectedIdempotencyState,
		CommittedRevision:        protoReq.CommittedRevision,
		AllocationPages:          protoReq.AllocationPages,
		NormalizeExtentIds:       protoReq.NormalizeExtentIds,
		MutationOperationId:      protoReq.MutationOperationId,
		ExpectedMutationState:    protoReq.ExpectedMutationState,
		AffectedExtentIds:        protoReq.AffectedExtentIds,
		AffectedPageNos:          protoReq.AffectedPageNos,
		RetiredPhysicalChunkIds:  protoReq.RetiredPhysicalChunkIds,
		AffectedPageChunkRanges:  protoReq.AffectedPageChunkRanges,
	}
}

func CommitRangeLocalWriteStateRequestToProto(req metadata.CommitWriteMetadataRequest) *internalv1.CommitRangeLocalWriteStateRequest {
	protoReq := commitWriteMetadataRequestToProtoFields(req)
	return &internalv1.CommitRangeLocalWriteStateRequest{
		VolumeId:                 protoReq.VolumeId,
		ExpectedEpoch:            protoReq.ExpectedEpoch,
		ExpectedRevision:         protoReq.ExpectedRevision,
		IdempotencyKey:           protoReq.IdempotencyKey,
		ExpectedIdempotencyState: protoReq.ExpectedIdempotencyState,
		CommittedRevision:        protoReq.CommittedRevision,
		AllocationPages:          protoReq.AllocationPages,
		NormalizeExtentIds:       protoReq.NormalizeExtentIds,
		MutationOperationId:      protoReq.MutationOperationId,
		ExpectedMutationState:    protoReq.ExpectedMutationState,
		AffectedExtentIds:        protoReq.AffectedExtentIds,
		AffectedPageNos:          protoReq.AffectedPageNos,
		RetiredPhysicalChunkIds:  protoReq.RetiredPhysicalChunkIds,
		AffectedPageChunkRanges:  protoReq.AffectedPageChunkRanges,
	}
}

func CommitAppendOnlyWriteStateAndQueueEffectsRequestToProto(req metadata.CommitWriteMetadataRequest) *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest {
	protoReq := commitWriteMetadataRequestToProtoFields(req)
	out := &internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest{
		VolumeId:                 protoReq.VolumeId,
		ExpectedEpoch:            protoReq.ExpectedEpoch,
		ExpectedRevision:         protoReq.ExpectedRevision,
		IdempotencyKey:           protoReq.IdempotencyKey,
		ExpectedIdempotencyState: protoReq.ExpectedIdempotencyState,
		CommittedRevision:        protoReq.CommittedRevision,
		AllocationPages:          protoReq.AllocationPages,
		NormalizeExtentIds:       protoReq.NormalizeExtentIds,
		MutationOperationId:      protoReq.MutationOperationId,
		ExpectedMutationState:    protoReq.ExpectedMutationState,
		AffectedExtentIds:        protoReq.AffectedExtentIds,
		AffectedPageNos:          protoReq.AffectedPageNos,
		RetiredPhysicalChunkIds:  protoReq.RetiredPhysicalChunkIds,
		AffectedPageChunkRanges:  protoReq.AffectedPageChunkRanges,
		AttachmentId:             req.AttachmentID,
		Generation:               req.Generation,
		AllowMissingWriteIntent:  req.AllowMissingWriteIntent,
	}
	if req.MutationOperation.OperationID != "" {
		out.MutationOperation = MutationOperationRecordToProto(req.MutationOperation)
	}
	return out
}

func commitWriteMetadataRequestToProtoFields(req metadata.CommitWriteMetadataRequest) *internalv1.CommitPageScopedWriteMetadataRequest {
	return &internalv1.CommitPageScopedWriteMetadataRequest{
		VolumeId:                 req.VolumeID,
		ExpectedEpoch:            req.ExpectedEpoch,
		ExpectedRevision:         req.ExpectedRevision,
		IdempotencyKey:           req.IdempotencyKey,
		ExpectedIdempotencyState: idempotencyResultStateToProto(req.ExpectedIdempotencyState),
		CommittedRevision:        req.CommittedRevision,
		AllocationPages:          allocationPagesToProto(req.AllocationPages),
		NormalizeExtentIds:       append([]uint64(nil), req.NormalizeExtentMappings...),
		MutationOperationId:      req.MutationOperationID,
		ExpectedMutationState:    mutationOperationStateToProto(req.ExpectedMutationState),
		AffectedExtentIds:        append([]uint64(nil), req.AffectedExtentIDs...),
		AffectedPageNos:          append([]uint64(nil), req.AffectedPageNos...),
		RetiredPhysicalChunkIds:  append([]uint64(nil), req.RetiredPhysicalChunkIDs...),
		AffectedPageChunkRanges:  allocationPageChunkRangesToProto(req.AffectedPageChunkRanges),
	}
}

func allocationPagesToProto(in []metadata.AllocationPageRecord) []*internalv1.AllocationPage {
	pages := make([]*internalv1.AllocationPage, 0, len(in))
	for _, page := range in {
		extents := make([]*internalv1.AllocationExtent, 0, len(page.Extents))
		for _, extent := range page.Extents {
			extents = append(extents, &internalv1.AllocationExtent{
				LogicalChunkStart:  extent.LogicalChunkStart,
				ChunkCount:         extent.ChunkCount,
				Kind:               placementApplyKindToProto(extent.Kind),
				PhysicalChunkStart: extent.PhysicalChunkStart,
				BackingRef:         extent.BackingRef,
				Generation:         extent.Generation,
				Checksum:           extent.Checksum,
				Encryption:         payloadEncryptionHeaderToProto(extent.Encryption),
			})
		}
		pages = append(pages, &internalv1.AllocationPage{
			VolumeId:       page.VolumeID,
			PageNo:         page.PageNo,
			PageBytes:      page.PageBytes,
			ChunkSizeBytes: page.ChunkSizeBytes,
			Revision:       page.Revision,
			Extents:        extents,
		})
	}
	return pages
}

func CommitPageScopedWriteMetadataRequestFromProto(req *internalv1.CommitPageScopedWriteMetadataRequest) (metadata.CommitWriteMetadataRequest, error) {
	if req == nil {
		return metadata.CommitWriteMetadataRequest{}, InvalidWriteSessionRequestError("page-scoped write metadata proto request is required")
	}
	return commitWriteMetadataRequestFromProtoFields(
		req.GetVolumeId(),
		req.GetExpectedEpoch(),
		req.GetExpectedRevision(),
		req.GetIdempotencyKey(),
		req.GetExpectedIdempotencyState(),
		req.GetCommittedRevision(),
		req.GetAllocationPages(),
		req.GetNormalizeExtentIds(),
		req.GetMutationOperationId(),
		req.GetExpectedMutationState(),
		req.GetAffectedExtentIds(),
		req.GetAffectedPageNos(),
		req.GetRetiredPhysicalChunkIds(),
		req.GetAffectedPageChunkRanges(),
	)
}

func CommitRangeLocalWriteStateRequestFromProto(req *internalv1.CommitRangeLocalWriteStateRequest) (metadata.CommitWriteMetadataRequest, error) {
	if req == nil {
		return metadata.CommitWriteMetadataRequest{}, InvalidWriteSessionRequestError("range-local write state proto request is required")
	}
	return commitWriteMetadataRequestFromProtoFields(
		req.GetVolumeId(),
		req.GetExpectedEpoch(),
		req.GetExpectedRevision(),
		req.GetIdempotencyKey(),
		req.GetExpectedIdempotencyState(),
		req.GetCommittedRevision(),
		req.GetAllocationPages(),
		req.GetNormalizeExtentIds(),
		req.GetMutationOperationId(),
		req.GetExpectedMutationState(),
		req.GetAffectedExtentIds(),
		req.GetAffectedPageNos(),
		req.GetRetiredPhysicalChunkIds(),
		req.GetAffectedPageChunkRanges(),
	)
}

func CommitAppendOnlyWriteStateAndQueueEffectsRequestFromProto(req *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest) (metadata.CommitWriteMetadataRequest, error) {
	if req == nil {
		return metadata.CommitWriteMetadataRequest{}, InvalidWriteSessionRequestError("append-only write state effects proto request is required")
	}
	out, err := commitWriteMetadataRequestFromProtoFields(
		req.GetVolumeId(),
		req.GetExpectedEpoch(),
		req.GetExpectedRevision(),
		req.GetIdempotencyKey(),
		req.GetExpectedIdempotencyState(),
		req.GetCommittedRevision(),
		req.GetAllocationPages(),
		req.GetNormalizeExtentIds(),
		req.GetMutationOperationId(),
		req.GetExpectedMutationState(),
		req.GetAffectedExtentIds(),
		req.GetAffectedPageNos(),
		req.GetRetiredPhysicalChunkIds(),
		req.GetAffectedPageChunkRanges(),
	)
	if err != nil {
		return metadata.CommitWriteMetadataRequest{}, err
	}
	if req.GetMutationOperation() != nil {
		operation, err := MutationOperationRecordFromProto(req.GetMutationOperation())
		if err != nil {
			return metadata.CommitWriteMetadataRequest{}, err
		}
		out.MutationOperation = operation
	}
	out.AttachmentID = req.GetAttachmentId()
	out.Generation = req.GetGeneration()
	out.AllowMissingWriteIntent = req.GetAllowMissingWriteIntent()
	return out, nil
}

func commitWriteMetadataRequestFromProtoFields(
	volumeID string,
	expectedEpoch uint64,
	expectedRevision uint64,
	idempotencyKey string,
	expectedIdempotencyState internalv1.IdempotencyResultState,
	committedRevision uint64,
	allocationPages []*internalv1.AllocationPage,
	normalizeExtentIDs []uint64,
	mutationOperationID string,
	expectedMutationState internalv1.MutationOperationState,
	affectedExtentIDs []uint64,
	affectedPageNos []uint64,
	retiredPhysicalChunkIDs []uint64,
	affectedPageChunkRanges []*internalv1.AllocationPageChunkRange,
) (metadata.CommitWriteMetadataRequest, error) {
	idemState, err := idempotencyResultStateFromProto(expectedIdempotencyState)
	if err != nil {
		return metadata.CommitWriteMetadataRequest{}, err
	}
	mutationState, err := mutationOperationStateFromProto(expectedMutationState)
	if err != nil {
		return metadata.CommitWriteMetadataRequest{}, err
	}
	pages, err := allocationPagesFromProto(allocationPages)
	if err != nil {
		return metadata.CommitWriteMetadataRequest{}, err
	}
	chunkRanges, err := allocationPageChunkRangesFromProto(affectedPageChunkRanges)
	if err != nil {
		return metadata.CommitWriteMetadataRequest{}, err
	}
	return metadata.CommitWriteMetadataRequest{
		VolumeID:                 volumeID,
		ExpectedEpoch:            expectedEpoch,
		ExpectedRevision:         expectedRevision,
		IdempotencyKey:           idempotencyKey,
		ExpectedIdempotencyState: idemState,
		CommittedRevision:        committedRevision,
		AllocationPages:          pages,
		NormalizeExtentMappings:  append([]uint64(nil), normalizeExtentIDs...),
		MutationOperationID:      mutationOperationID,
		ExpectedMutationState:    mutationState,
		AffectedExtentIDs:        append([]uint64(nil), affectedExtentIDs...),
		AffectedPageNos:          append([]uint64(nil), affectedPageNos...),
		AffectedPageChunkRanges:  chunkRanges,
		RetiredPhysicalChunkIDs:  append([]uint64(nil), retiredPhysicalChunkIDs...),
	}, nil
}

func allocationPageChunkRangesToProto(in []metadata.AllocationPageChunkRangeRecord) []*internalv1.AllocationPageChunkRange {
	out := make([]*internalv1.AllocationPageChunkRange, 0, len(in))
	for _, rng := range in {
		out = append(out, &internalv1.AllocationPageChunkRange{
			PageNo:     rng.PageNo,
			StartChunk: rng.StartChunk,
			EndChunk:   rng.EndChunk,
		})
	}
	return out
}

func allocationPageChunkRangesFromProto(in []*internalv1.AllocationPageChunkRange) ([]metadata.AllocationPageChunkRangeRecord, error) {
	out := make([]metadata.AllocationPageChunkRangeRecord, 0, len(in))
	for _, rng := range in {
		if rng == nil {
			return nil, InvalidWriteSessionRequestError("affected page chunk range is required")
		}
		out = append(out, metadata.AllocationPageChunkRangeRecord{
			PageNo:     rng.GetPageNo(),
			StartChunk: rng.GetStartChunk(),
			EndChunk:   rng.GetEndChunk(),
		})
	}
	return out, nil
}

func allocationPagesFromProto(allocationPages []*internalv1.AllocationPage) ([]metadata.AllocationPageRecord, error) {
	pages := make([]metadata.AllocationPageRecord, 0, len(allocationPages))
	for _, page := range allocationPages {
		if page == nil {
			return nil, InvalidWriteSessionRequestError("write allocation page is required")
		}
		extents := make([]metadata.AllocationExtentRecord, 0, len(page.GetExtents()))
		for _, extent := range page.GetExtents() {
			if extent == nil {
				return nil, InvalidWriteSessionRequestError("write allocation extent is required")
			}
			kind, err := placementApplyKindFromProto(extent.GetKind())
			if err != nil {
				return nil, InvalidWriteSessionRequestError("%s", err.Error())
			}
			extents = append(extents, metadata.AllocationExtentRecord{
				LogicalChunkStart:  extent.GetLogicalChunkStart(),
				ChunkCount:         extent.GetChunkCount(),
				Kind:               kind,
				PhysicalChunkStart: extent.GetPhysicalChunkStart(),
				BackingRef:         extent.GetBackingRef(),
				Generation:         extent.GetGeneration(),
				Checksum:           extent.GetChecksum(),
				Encryption:         payloadEncryptionHeaderFromProto(extent.GetEncryption()),
			})
		}
		pages = append(pages, metadata.AllocationPageRecord{
			VolumeID:       page.GetVolumeId(),
			PageNo:         page.GetPageNo(),
			PageBytes:      page.GetPageBytes(),
			ChunkSizeBytes: page.GetChunkSizeBytes(),
			Revision:       page.GetRevision(),
			Extents:        extents,
		})
	}
	return pages, nil
}

func VolumeStateToProto(state metadata.VolumeState) *internalv1.VolumeState {
	return &internalv1.VolumeState{
		VolumeId:          state.VolumeID,
		Epoch:             state.Epoch,
		Revision:          state.Revision,
		PlacementPolicyId: state.PlacementPolicyID,
		ProtectionPolicy:  state.ProtectionPolicy,
		Status:            string(state.Status),
	}
}

func VolumeStateFromProto(state *internalv1.VolumeState) metadata.VolumeState {
	if state == nil {
		return metadata.VolumeState{}
	}
	return metadata.VolumeState{
		VolumeID:          state.GetVolumeId(),
		Epoch:             state.GetEpoch(),
		Revision:          state.GetRevision(),
		PlacementPolicyID: state.GetPlacementPolicyId(),
		ProtectionPolicy:  state.GetProtectionPolicy(),
		Status:            metadata.VolumeStatus(state.GetStatus()),
	}
}

func IdempotencyRecordToProto(record metadata.IdempotencyRecord) *internalv1.IdempotencyRecord {
	return &internalv1.IdempotencyRecord{
		VolumeId:       record.VolumeID,
		IdempotencyKey: record.IdempotencyKey,
		AttachmentId:   record.AttachmentID,
		Generation:     record.Generation,
		Epoch:          record.Epoch,
		Revision:       record.Revision,
		Operation:      record.Operation,
		ResultState:    idempotencyResultStateToProto(record.ResultState),
	}
}

func IdempotencyRecordFromProto(record *internalv1.IdempotencyRecord) (metadata.IdempotencyRecord, error) {
	if record == nil {
		return metadata.IdempotencyRecord{}, InvalidWriteSessionRequestError("idempotency proto record is required")
	}
	state, err := idempotencyResultStateFromProto(record.GetResultState())
	if err != nil {
		return metadata.IdempotencyRecord{}, err
	}
	return metadata.IdempotencyRecord{
		VolumeID:       record.GetVolumeId(),
		IdempotencyKey: record.GetIdempotencyKey(),
		AttachmentID:   record.GetAttachmentId(),
		Generation:     record.GetGeneration(),
		Epoch:          record.GetEpoch(),
		Revision:       record.GetRevision(),
		Operation:      record.GetOperation(),
		ResultState:    state,
	}, nil
}

func idempotencyResultStateToProto(state metadata.IdempotencyResultState) internalv1.IdempotencyResultState {
	switch state {
	case metadata.IdempotencyPending:
		return internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING
	case metadata.IdempotencyCommitted:
		return internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_COMMITTED
	case metadata.IdempotencyFailed:
		return internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_FAILED
	default:
		return internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_UNSPECIFIED
	}
}

func idempotencyResultStateFromProto(state internalv1.IdempotencyResultState) (metadata.IdempotencyResultState, error) {
	switch state {
	case internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_PENDING:
		return metadata.IdempotencyPending, nil
	case internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_COMMITTED:
		return metadata.IdempotencyCommitted, nil
	case internalv1.IdempotencyResultState_IDEMPOTENCY_RESULT_STATE_FAILED:
		return metadata.IdempotencyFailed, nil
	default:
		return "", InvalidWriteSessionRequestError("invalid idempotency result state %q", state.String())
	}
}

func MutationOperationRecordToProto(record metadata.MutationOperationRecord) *internalv1.MutationOperationRecord {
	windows := make([]*internalv1.MutationPageWindow, 0, len(record.RetryPageWindows))
	for _, window := range record.RetryPageWindows {
		windows = append(windows, &internalv1.MutationPageWindow{
			ExtentId:    window.ExtentID,
			StartPageNo: window.StartPageNo,
			EndPageNo:   window.EndPageNo,
			DataBytes:   window.DataBytes,
			DataChunks:  window.DataChunks,
		})
	}
	return &internalv1.MutationOperationRecord{
		OperationId:             record.OperationID,
		VolumeId:                record.VolumeID,
		Kind:                    record.Kind,
		State:                   mutationOperationStateToProto(record.State),
		PlacementRevision:       record.PlacementRevision,
		AllocationRevision:      record.AllocationRevision,
		WriterFencingEpoch:      record.WriterFencingEpoch,
		IdempotencyKey:          record.IdempotencyKey,
		AffectedExtentIds:       append([]uint64(nil), record.AffectedExtentIDs...),
		AffectedPageNos:         append([]uint64(nil), record.AffectedPageNos...),
		CompletedPageNos:        append([]uint64(nil), record.CompletedPageNos...),
		RetryPageWindows:        windows,
		RetiredPhysicalChunkIds: append([]uint64(nil), record.RetiredPhysicalChunkIDs...),
		StartedAtUnix:           record.StartedAtUnix,
		LastUpdatedAtUnix:       record.LastUpdatedAtUnix,
		ErrorMessage:            record.ErrorMessage,
	}
}

func MutationOperationRecordFromProto(record *internalv1.MutationOperationRecord) (metadata.MutationOperationRecord, error) {
	if record == nil {
		return metadata.MutationOperationRecord{}, InvalidWriteSessionRequestError("mutation operation proto record is required")
	}
	state, err := mutationOperationStateFromProto(record.GetState())
	if err != nil {
		return metadata.MutationOperationRecord{}, err
	}
	windows := make([]metadata.MutationPageWindowRecord, 0, len(record.GetRetryPageWindows()))
	for _, window := range record.GetRetryPageWindows() {
		if window == nil {
			return metadata.MutationOperationRecord{}, InvalidWriteSessionRequestError("mutation retry page window is required")
		}
		windows = append(windows, metadata.MutationPageWindowRecord{
			ExtentID:    window.GetExtentId(),
			StartPageNo: window.GetStartPageNo(),
			EndPageNo:   window.GetEndPageNo(),
			DataBytes:   window.GetDataBytes(),
			DataChunks:  window.GetDataChunks(),
		})
	}
	return metadata.MutationOperationRecord{
		OperationID:             record.GetOperationId(),
		VolumeID:                record.GetVolumeId(),
		Kind:                    record.GetKind(),
		State:                   state,
		PlacementRevision:       record.GetPlacementRevision(),
		AllocationRevision:      record.GetAllocationRevision(),
		WriterFencingEpoch:      record.GetWriterFencingEpoch(),
		IdempotencyKey:          record.GetIdempotencyKey(),
		AffectedExtentIDs:       append([]uint64(nil), record.GetAffectedExtentIds()...),
		AffectedPageNos:         append([]uint64(nil), record.GetAffectedPageNos()...),
		CompletedPageNos:        append([]uint64(nil), record.GetCompletedPageNos()...),
		RetryPageWindows:        windows,
		RetiredPhysicalChunkIDs: append([]uint64(nil), record.GetRetiredPhysicalChunkIds()...),
		StartedAtUnix:           record.GetStartedAtUnix(),
		LastUpdatedAtUnix:       record.GetLastUpdatedAtUnix(),
		ErrorMessage:            record.GetErrorMessage(),
	}, nil
}

func mutationOperationStateToProto(state metadata.MutationOperationState) internalv1.MutationOperationState {
	switch state {
	case metadata.MutationOperationPending:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_PENDING
	case metadata.MutationOperationRunning:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING
	case metadata.MutationOperationCommitted:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_COMMITTED
	case metadata.MutationOperationFailed:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_FAILED
	case metadata.MutationOperationRolledBack:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_ROLLED_BACK
	default:
		return internalv1.MutationOperationState_MUTATION_OPERATION_STATE_UNSPECIFIED
	}
}

func mutationOperationStateFromProto(state internalv1.MutationOperationState) (metadata.MutationOperationState, error) {
	switch state {
	case internalv1.MutationOperationState_MUTATION_OPERATION_STATE_PENDING:
		return metadata.MutationOperationPending, nil
	case internalv1.MutationOperationState_MUTATION_OPERATION_STATE_RUNNING:
		return metadata.MutationOperationRunning, nil
	case internalv1.MutationOperationState_MUTATION_OPERATION_STATE_COMMITTED:
		return metadata.MutationOperationCommitted, nil
	case internalv1.MutationOperationState_MUTATION_OPERATION_STATE_FAILED:
		return metadata.MutationOperationFailed, nil
	case internalv1.MutationOperationState_MUTATION_OPERATION_STATE_ROLLED_BACK:
		return metadata.MutationOperationRolledBack, nil
	default:
		return "", InvalidWriteSessionRequestError("invalid mutation operation state %q", state.String())
	}
}
