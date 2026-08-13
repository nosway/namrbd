package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func PhysicalObjectRecordToProto(rec metadata.PhysicalObjectRecord) *internalv1.PhysicalObject {
	out := &internalv1.PhysicalObject{
		VolumeId:      rec.VolumeID,
		ObjectId:      rec.ObjectID,
		BackendType:   string(rec.BackendType),
		PlacementRef:  rec.PlacementRef,
		LogicalLength: rec.LogicalLength,
		Generation:    rec.Generation,
		Checksum:      rec.Checksum,
		State:         string(rec.State),
		Encryption:    payloadEncryptionHeaderToProto(rec.Encryption),
		CreatedAtUnix: rec.CreatedAtUnix,
		UpdatedAtUnix: rec.UpdatedAtUnix,
	}
	if rec.Replicated != nil {
		out.Replicated = &internalv1.ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: rec.Replicated.PhysicalChunkStart,
			ChunkCount:         rec.Replicated.ChunkCount,
		}
	}
	if rec.EC != nil {
		out.Ec = &internalv1.ECPhysicalObjectDescriptor{
			ProfileId:        rec.EC.ProfileID,
			StripeId:         rec.EC.StripeID,
			StripeGeneration: rec.EC.StripeGeneration,
			StripeUnitBytes:  rec.EC.StripeUnitBytes,
			DataShards:       rec.EC.DataShards,
			CodingShards:     rec.EC.CodingShards,
			StripeOffset:     rec.EC.StripeOffset,
			DataShardRefs:    append([]string(nil), rec.EC.DataShardRefs...),
			CodeShardRefs:    append([]string(nil), rec.EC.CodeShardRefs...),
		}
	}
	return out
}

func PhysicalObjectRecordFromProto(rec *internalv1.PhysicalObject) (metadata.PhysicalObjectRecord, error) {
	if rec == nil {
		return metadata.PhysicalObjectRecord{}, InvalidWriteSessionRequestError("physical object is required")
	}
	out := metadata.PhysicalObjectRecord{
		VolumeID:      rec.GetVolumeId(),
		ObjectID:      rec.GetObjectId(),
		BackendType:   metadata.PhysicalObjectBackendType(rec.GetBackendType()),
		PlacementRef:  rec.GetPlacementRef(),
		LogicalLength: rec.GetLogicalLength(),
		Generation:    rec.GetGeneration(),
		Checksum:      rec.GetChecksum(),
		State:         metadata.PhysicalObjectState(rec.GetState()),
		Encryption:    payloadEncryptionHeaderFromProto(rec.GetEncryption()),
		CreatedAtUnix: rec.GetCreatedAtUnix(),
		UpdatedAtUnix: rec.GetUpdatedAtUnix(),
	}
	if replicated := rec.GetReplicated(); replicated != nil {
		out.Replicated = &metadata.ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: replicated.GetPhysicalChunkStart(),
			ChunkCount:         replicated.GetChunkCount(),
		}
	}
	if ec := rec.GetEc(); ec != nil {
		out.EC = &metadata.ECPhysicalObjectDescriptor{
			ProfileID:        ec.GetProfileId(),
			StripeID:         ec.GetStripeId(),
			StripeGeneration: ec.GetStripeGeneration(),
			StripeUnitBytes:  ec.GetStripeUnitBytes(),
			DataShards:       ec.GetDataShards(),
			CodingShards:     ec.GetCodingShards(),
			StripeOffset:     ec.GetStripeOffset(),
			DataShardRefs:    append([]string(nil), ec.GetDataShardRefs()...),
			CodeShardRefs:    append([]string(nil), ec.GetCodeShardRefs()...),
		}
	}
	return metadata.NormalizePhysicalObjectRecord(out), nil
}

func ECStripeRecordToProto(rec metadata.ECStripeRecord) *internalv1.ECStripe {
	shards := make([]*internalv1.ECShard, 0, len(rec.Shards))
	for _, shard := range rec.Shards {
		shards = append(shards, &internalv1.ECShard{
			ShardId:       shard.ShardID,
			Role:          string(shard.Role),
			RoleIndex:     shard.RoleIndex,
			Zone:          shard.Zone,
			NodeId:        shard.NodeID,
			StoreId:       shard.StoreID,
			ShardObjectId: shard.ShardObjectID,
			Checksum:      shard.Checksum,
			SizeBytes:     shard.SizeBytes,
			Encryption:    payloadEncryptionHeaderToProto(shard.Encryption),
		})
	}
	return &internalv1.ECStripe{
		VolumeId:         rec.VolumeID,
		ObjectId:         rec.ObjectID,
		ProfileId:        rec.ProfileID,
		StripeId:         rec.StripeID,
		StripeGeneration: rec.StripeGeneration,
		StripeUnitBytes:  rec.StripeUnitBytes,
		DataShards:       rec.DataShards,
		CodingShards:     rec.CodingShards,
		TopologyRevision: rec.TopologyRevision,
		State:            string(rec.State),
		Shards:           shards,
		CreatedAtUnix:    rec.CreatedAtUnix,
		UpdatedAtUnix:    rec.UpdatedAtUnix,
	}
}

func ECStripeRecordFromProto(rec *internalv1.ECStripe) (metadata.ECStripeRecord, error) {
	if rec == nil {
		return metadata.ECStripeRecord{}, InvalidWriteSessionRequestError("ec stripe is required")
	}
	shards := make([]metadata.ECShardRecord, 0, len(rec.GetShards()))
	for _, shard := range rec.GetShards() {
		if shard == nil {
			return metadata.ECStripeRecord{}, InvalidWriteSessionRequestError("ec shard is required")
		}
		shards = append(shards, metadata.ECShardRecord{
			ShardID:       shard.GetShardId(),
			Role:          metadata.ECShardRole(shard.GetRole()),
			RoleIndex:     shard.GetRoleIndex(),
			Zone:          shard.GetZone(),
			NodeID:        shard.GetNodeId(),
			StoreID:       shard.GetStoreId(),
			ShardObjectID: shard.GetShardObjectId(),
			Checksum:      shard.GetChecksum(),
			SizeBytes:     shard.GetSizeBytes(),
			Encryption:    payloadEncryptionHeaderFromProto(shard.GetEncryption()),
		})
	}
	return metadata.NormalizeECStripeRecord(metadata.ECStripeRecord{
		VolumeID:         rec.GetVolumeId(),
		ObjectID:         rec.GetObjectId(),
		ProfileID:        rec.GetProfileId(),
		StripeID:         rec.GetStripeId(),
		StripeGeneration: rec.GetStripeGeneration(),
		StripeUnitBytes:  rec.GetStripeUnitBytes(),
		DataShards:       rec.GetDataShards(),
		CodingShards:     rec.GetCodingShards(),
		TopologyRevision: rec.GetTopologyRevision(),
		State:            metadata.ECStripeState(rec.GetState()),
		Shards:           shards,
		CreatedAtUnix:    rec.GetCreatedAtUnix(),
		UpdatedAtUnix:    rec.GetUpdatedAtUnix(),
	}), nil
}

func payloadEncryptionHeaderToProto(header *metadata.PayloadEncryptionHeader) *internalv1.PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	out := &internalv1.PayloadEncryptionHeader{
		HeaderVersion:    int32(header.HeaderVersion),
		CipherSuite:      header.CipherSuite,
		EncryptionScope:  header.EncryptionScope,
		SecurityPolicyId: header.SecurityPolicyID,
		PolicyGeneration: header.PolicyGeneration,
		KeyProviderId:    header.KeyProviderID,
		DataKeyId:        header.DataKeyID,
		KeyId:            header.KeyID,
		KeyVersion:       header.KeyVersion,
		KeyGeneration:    header.KeyGeneration,
		ObjectId:         header.ObjectID,
		BackendType:      string(header.BackendType),
		NonceHex:         header.NonceHex,
		NonceSource:      header.NonceSource,
		AadDigest:        header.AADDigest,
		LogicalOffset:    header.LogicalOffset,
		StripeId:         header.StripeID,
		PlaintextLength:  header.PlaintextLength,
		CiphertextLength: header.CiphertextLength,
		AuthTagBytes:     uint32(header.AuthTagBytes),
		AuthTagHex:       header.AuthTagHex,
	}
	if header.ShardIDPresent {
		out.ShardId = &header.ShardID
	}
	return out
}

func payloadEncryptionHeaderFromProto(header *internalv1.PayloadEncryptionHeader) *metadata.PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	out := &metadata.PayloadEncryptionHeader{
		HeaderVersion:    int(header.GetHeaderVersion()),
		CipherSuite:      header.GetCipherSuite(),
		EncryptionScope:  header.GetEncryptionScope(),
		SecurityPolicyID: header.GetSecurityPolicyId(),
		PolicyGeneration: header.GetPolicyGeneration(),
		KeyProviderID:    header.GetKeyProviderId(),
		DataKeyID:        header.GetDataKeyId(),
		KeyID:            header.GetKeyId(),
		KeyVersion:       header.GetKeyVersion(),
		KeyGeneration:    header.GetKeyGeneration(),
		ObjectID:         header.GetObjectId(),
		BackendType:      metadata.PhysicalObjectBackendType(header.GetBackendType()),
		NonceHex:         header.GetNonceHex(),
		NonceSource:      header.GetNonceSource(),
		AADDigest:        header.GetAadDigest(),
		LogicalOffset:    header.GetLogicalOffset(),
		StripeID:         header.GetStripeId(),
		PlaintextLength:  header.GetPlaintextLength(),
		CiphertextLength: header.GetCiphertextLength(),
		AuthTagBytes:     int(header.GetAuthTagBytes()),
		AuthTagHex:       header.GetAuthTagHex(),
	}
	if header.ShardId != nil {
		out.ShardID = header.GetShardId()
		out.ShardIDPresent = true
	}
	return out
}

func CommitECFullStripeWriteRequestToProto(req metadata.CommitECFullStripeWriteRequest) *internalv1.CommitECFullStripeWriteRequest {
	out := &internalv1.CommitECFullStripeWriteRequest{
		VolumeId:                req.VolumeID,
		ExpectedEpoch:           req.ExpectedEpoch,
		ExpectedRevision:        req.ExpectedRevision,
		IdempotencyKey:          req.IdempotencyKey,
		CommittedRevision:       req.CommittedRevision,
		PhysicalObject:          PhysicalObjectRecordToProto(req.PhysicalObject),
		EcStripe:                ECStripeRecordToProto(req.ECStripe),
		AllocationPages:         allocationPagesToProto(req.AllocationPages),
		MutationOperationId:     req.MutationOperationID,
		ExpectedMutationState:   mutationOperationStateToProto(req.ExpectedMutationState),
		AffectedPageNos:         append([]uint64(nil), req.AffectedPageNos...),
		AffectedExtentIds:       append([]uint64(nil), req.AffectedExtentIDs...),
		RetiredPhysicalChunkIds: append([]uint64(nil), req.RetiredPhysicalChunkIDs...),
		RetiredEcObjects:        retiredECObjectsToProto(req.RetiredECObjects),
	}
	if req.MutationOperation.OperationID != "" {
		out.MutationOperation = MutationOperationRecordToProto(req.MutationOperation)
	}
	return out
}

func CommitECFullStripeWriteRequestFromProto(req *internalv1.CommitECFullStripeWriteRequest) (metadata.CommitECFullStripeWriteRequest, error) {
	if req == nil {
		return metadata.CommitECFullStripeWriteRequest{}, InvalidWriteSessionRequestError("commit ec full-stripe write request is required")
	}
	object, err := PhysicalObjectRecordFromProto(req.GetPhysicalObject())
	if err != nil {
		return metadata.CommitECFullStripeWriteRequest{}, err
	}
	stripe, err := ECStripeRecordFromProto(req.GetEcStripe())
	if err != nil {
		return metadata.CommitECFullStripeWriteRequest{}, err
	}
	pages, err := allocationPagesFromProto(req.GetAllocationPages())
	if err != nil {
		return metadata.CommitECFullStripeWriteRequest{}, err
	}
	expectedMutationState, err := mutationOperationStateFromProto(req.GetExpectedMutationState())
	if err != nil {
		return metadata.CommitECFullStripeWriteRequest{}, err
	}
	var mutationOperation metadata.MutationOperationRecord
	if req.GetMutationOperation() != nil {
		mutationOperation, err = MutationOperationRecordFromProto(req.GetMutationOperation())
		if err != nil {
			return metadata.CommitECFullStripeWriteRequest{}, err
		}
	}
	return metadata.CommitECFullStripeWriteRequest{
		VolumeID:                req.GetVolumeId(),
		ExpectedEpoch:           req.GetExpectedEpoch(),
		ExpectedRevision:        req.GetExpectedRevision(),
		IdempotencyKey:          req.GetIdempotencyKey(),
		CommittedRevision:       req.GetCommittedRevision(),
		PhysicalObject:          object,
		ECStripe:                stripe,
		AllocationPages:         pages,
		MutationOperationID:     req.GetMutationOperationId(),
		ExpectedMutationState:   expectedMutationState,
		AffectedPageNos:         append([]uint64(nil), req.GetAffectedPageNos()...),
		AffectedExtentIDs:       append([]uint64(nil), req.GetAffectedExtentIds()...),
		RetiredPhysicalChunkIDs: append([]uint64(nil), req.GetRetiredPhysicalChunkIds()...),
		RetiredECObjects:        retiredECObjectsFromProto(req.GetRetiredEcObjects()),
		MutationOperation:       mutationOperation,
	}, nil
}

func CommitECDiscardRequestToProto(req metadata.CommitECDiscardRequest) *internalv1.CommitECDiscardRequest {
	return &internalv1.CommitECDiscardRequest{
		VolumeId:                req.VolumeID,
		ExpectedEpoch:           req.ExpectedEpoch,
		ExpectedRevision:        req.ExpectedRevision,
		IdempotencyKey:          req.IdempotencyKey,
		CommittedRevision:       req.CommittedRevision,
		AllocationPages:         allocationPagesToProto(req.AllocationPages),
		MutationOperationId:     req.MutationOperationID,
		ExpectedMutationState:   mutationOperationStateToProto(req.ExpectedMutationState),
		AffectedPageNos:         append([]uint64(nil), req.AffectedPageNos...),
		AffectedExtentIds:       append([]uint64(nil), req.AffectedExtentIDs...),
		RetiredPhysicalChunkIds: append([]uint64(nil), req.RetiredPhysicalChunkIDs...),
		RetiredEcObjects:        retiredECObjectsToProto(req.RetiredECObjects),
	}
}

func CommitECDiscardRequestFromProto(req *internalv1.CommitECDiscardRequest) (metadata.CommitECDiscardRequest, error) {
	if req == nil {
		return metadata.CommitECDiscardRequest{}, InvalidWriteSessionRequestError("commit ec discard request is required")
	}
	pages, err := allocationPagesFromProto(req.GetAllocationPages())
	if err != nil {
		return metadata.CommitECDiscardRequest{}, err
	}
	expectedMutationState, err := mutationOperationStateFromProto(req.GetExpectedMutationState())
	if err != nil {
		return metadata.CommitECDiscardRequest{}, err
	}
	return metadata.CommitECDiscardRequest{
		VolumeID:                req.GetVolumeId(),
		ExpectedEpoch:           req.GetExpectedEpoch(),
		ExpectedRevision:        req.GetExpectedRevision(),
		IdempotencyKey:          req.GetIdempotencyKey(),
		CommittedRevision:       req.GetCommittedRevision(),
		AllocationPages:         pages,
		MutationOperationID:     req.GetMutationOperationId(),
		ExpectedMutationState:   expectedMutationState,
		AffectedPageNos:         append([]uint64(nil), req.GetAffectedPageNos()...),
		AffectedExtentIDs:       append([]uint64(nil), req.GetAffectedExtentIds()...),
		RetiredPhysicalChunkIDs: append([]uint64(nil), req.GetRetiredPhysicalChunkIds()...),
		RetiredECObjects:        retiredECObjectsFromProto(req.GetRetiredEcObjects()),
	}, nil
}

func retiredECObjectsToProto(refs []metadata.RetiredECObjectRef) []*internalv1.RetiredECObjectRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]*internalv1.RetiredECObjectRef, 0, len(refs))
	for _, ref := range refs {
		out = append(out, &internalv1.RetiredECObjectRef{
			ObjectId:         ref.ObjectID,
			StripeId:         ref.StripeID,
			StripeGeneration: ref.StripeGeneration,
		})
	}
	return out
}

func retiredECObjectsFromProto(refs []*internalv1.RetiredECObjectRef) []metadata.RetiredECObjectRef {
	if len(refs) == 0 {
		return nil
	}
	out := make([]metadata.RetiredECObjectRef, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		out = append(out, metadata.RetiredECObjectRef{
			ObjectID:         ref.GetObjectId(),
			StripeID:         ref.GetStripeId(),
			StripeGeneration: ref.GetStripeGeneration(),
		})
	}
	return out
}

func CommitECFullStripeWriteResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitECFullStripeWriteResponse {
	return &internalv1.CommitECFullStripeWriteResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitECFullStripeWriteResponseFromProto(resp *internalv1.CommitECFullStripeWriteResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("commit ec full-stripe write response is required")
	}
	record, err := IdempotencyRecordFromProto(resp.GetIdempotencyRecord())
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return VolumeStateFromProto(resp.GetVolumeState()), record, nil
}

func CommitECDiscardResponseToProto(state metadata.VolumeState, record metadata.IdempotencyRecord) *internalv1.CommitECDiscardResponse {
	return &internalv1.CommitECDiscardResponse{
		VolumeState:       VolumeStateToProto(state),
		IdempotencyRecord: IdempotencyRecordToProto(record),
	}
}

func CommitECDiscardResponseFromProto(resp *internalv1.CommitECDiscardResponse) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	if resp == nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, fmt.Errorf("commit ec discard response is required")
	}
	record, err := IdempotencyRecordFromProto(resp.GetIdempotencyRecord())
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return VolumeStateFromProto(resp.GetVolumeState()), record, nil
}
