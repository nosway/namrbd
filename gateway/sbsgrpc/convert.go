package sbsgrpc

import (
	"github.com/nosway/namrbd/gateway/service"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
)

func toProtoRequestContext(ctx service.SBSRequestContext) *sbsv1.RequestContext {
	return &sbsv1.RequestContext{
		RequestId:      ctx.RequestID,
		GatewayId:      ctx.GatewayID,
		HostId:         ctx.HostID,
		SessionId:      ctx.SessionID,
		AttachmentId:   ctx.AttachmentID,
		Generation:     ctx.Generation,
		IdempotencyKey: ctx.IdempotencyKey,
		DeadlineUnixMs: ctx.DeadlineUnixMS,
		TraceId:        ctx.TraceID,
	}
}

func fromProtoRequestContext(ctx *sbsv1.RequestContext) service.SBSRequestContext {
	if ctx == nil {
		return service.SBSRequestContext{}
	}
	return service.SBSRequestContext{
		RequestID:      ctx.RequestId,
		GatewayID:      ctx.GatewayId,
		HostID:         ctx.HostId,
		SessionID:      ctx.SessionId,
		AttachmentID:   ctx.AttachmentId,
		Generation:     ctx.Generation,
		IdempotencyKey: ctx.IdempotencyKey,
		DeadlineUnixMS: ctx.DeadlineUnixMs,
		TraceID:        ctx.TraceId,
	}
}

func toProtoVolumeProfile(p service.SBSVolumeProfile) *sbsv1.VolumeProfile {
	return &sbsv1.VolumeProfile{
		SizeBytes:       p.SizeBytes,
		BlockSize:       p.BlockSize,
		MaxIoSize:       p.MaxIOSize,
		SupportsFlush:   p.SupportsFlush,
		SupportsDiscard: p.SupportsDiscard,
		SupportsZero:    p.SupportsZero,
		ConsistencyMode: p.ConsistencyMode,
	}
}

func fromProtoVolumeProfile(p *sbsv1.VolumeProfile) service.SBSVolumeProfile {
	if p == nil {
		return service.SBSVolumeProfile{}
	}
	return service.SBSVolumeProfile{
		SizeBytes:       p.SizeBytes,
		BlockSize:       p.BlockSize,
		MaxIOSize:       p.MaxIoSize,
		SupportsFlush:   p.SupportsFlush,
		SupportsDiscard: p.SupportsDiscard,
		SupportsZero:    p.SupportsZero,
		ConsistencyMode: p.ConsistencyMode,
	}
}

func toProtoAccessMode(m service.SBSAccessMode) sbsv1.AccessMode {
	switch m {
	case service.SBSAccessModeExclusiveWriter:
		return sbsv1.AccessMode_ACCESS_MODE_EXCLUSIVE_WRITER
	default:
		return sbsv1.AccessMode_ACCESS_MODE_UNSPECIFIED
	}
}

func fromProtoAccessMode(m sbsv1.AccessMode) service.SBSAccessMode {
	switch m {
	case sbsv1.AccessMode_ACCESS_MODE_EXCLUSIVE_WRITER:
		return service.SBSAccessModeExclusiveWriter
	default:
		return ""
	}
}

func toProtoVolumeState(s service.SBSVolumeState) sbsv1.VolumeState {
	switch s {
	case service.SBSVolumeStateReady:
		return sbsv1.VolumeState_VOLUME_STATE_READY
	case service.SBSVolumeStateDegraded:
		return sbsv1.VolumeState_VOLUME_STATE_DEGRADED
	case service.SBSVolumeStateRecovering:
		return sbsv1.VolumeState_VOLUME_STATE_RECOVERING
	case service.SBSVolumeStateUnavailable:
		return sbsv1.VolumeState_VOLUME_STATE_UNAVAILABLE
	default:
		return sbsv1.VolumeState_VOLUME_STATE_UNSPECIFIED
	}
}

func fromProtoVolumeState(s sbsv1.VolumeState) service.SBSVolumeState {
	switch s {
	case sbsv1.VolumeState_VOLUME_STATE_READY:
		return service.SBSVolumeStateReady
	case sbsv1.VolumeState_VOLUME_STATE_DEGRADED:
		return service.SBSVolumeStateDegraded
	case sbsv1.VolumeState_VOLUME_STATE_RECOVERING:
		return service.SBSVolumeStateRecovering
	case sbsv1.VolumeState_VOLUME_STATE_UNAVAILABLE:
		return service.SBSVolumeStateUnavailable
	default:
		return ""
	}
}

func toProtoErrorCode(c service.SBSErrorCode) sbsv1.ErrorCode {
	switch c {
	case service.SBSErrorCodeNotFound:
		return sbsv1.ErrorCode_ERROR_CODE_NOT_FOUND
	case service.SBSErrorCodeBadRequest:
		return sbsv1.ErrorCode_ERROR_CODE_BAD_REQUEST
	case service.SBSErrorCodeStaleGeneration:
		return sbsv1.ErrorCode_ERROR_CODE_STALE_GENERATION
	case service.SBSErrorCodeAttachmentMismatch:
		return sbsv1.ErrorCode_ERROR_CODE_ATTACHMENT_MISMATCH
	case service.SBSErrorCodeIdempotencyConflict:
		return sbsv1.ErrorCode_ERROR_CODE_IDEMPOTENCY_CONFLICT
	case service.SBSErrorCodeUnavailable:
		return sbsv1.ErrorCode_ERROR_CODE_UNAVAILABLE
	case service.SBSErrorCodeTimeout:
		return sbsv1.ErrorCode_ERROR_CODE_TIMEOUT
	default:
		return sbsv1.ErrorCode_ERROR_CODE_INTERNAL
	}
}

func fromProtoErrorCode(c sbsv1.ErrorCode) service.SBSErrorCode {
	switch c {
	case sbsv1.ErrorCode_ERROR_CODE_NOT_FOUND:
		return service.SBSErrorCodeNotFound
	case sbsv1.ErrorCode_ERROR_CODE_BAD_REQUEST:
		return service.SBSErrorCodeBadRequest
	case sbsv1.ErrorCode_ERROR_CODE_STALE_GENERATION:
		return service.SBSErrorCodeStaleGeneration
	case sbsv1.ErrorCode_ERROR_CODE_ATTACHMENT_MISMATCH:
		return service.SBSErrorCodeAttachmentMismatch
	case sbsv1.ErrorCode_ERROR_CODE_IDEMPOTENCY_CONFLICT:
		return service.SBSErrorCodeIdempotencyConflict
	case sbsv1.ErrorCode_ERROR_CODE_UNAVAILABLE:
		return service.SBSErrorCodeUnavailable
	case sbsv1.ErrorCode_ERROR_CODE_TIMEOUT:
		return service.SBSErrorCodeTimeout
	default:
		return service.SBSErrorCodeInternal
	}
}

func toProtoOpenVolumeRequest(req *service.OpenVolumeRequest) *sbsv1.OpenVolumeRequest {
	return &sbsv1.OpenVolumeRequest{
		VolumeId:   req.VolumeID,
		AccessMode: toProtoAccessMode(req.AccessMode),
		Context:    toProtoRequestContext(req.Context),
	}
}

func fromProtoOpenVolumeRequest(req *sbsv1.OpenVolumeRequest) *service.OpenVolumeRequest {
	return &service.OpenVolumeRequest{
		VolumeID:   req.GetVolumeId(),
		AccessMode: fromProtoAccessMode(req.GetAccessMode()),
		Context:    fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoOpenVolumeResponse(resp *service.OpenVolumeResponse) *sbsv1.OpenVolumeResponse {
	return &sbsv1.OpenVolumeResponse{
		Status:         resp.Status,
		VolumeHandle:   resp.VolumeHandle,
		VolumeId:       resp.VolumeID,
		VolumeRevision: resp.VolumeRevision,
		Profile:        toProtoVolumeProfile(resp.Profile),
		ServerVersion:  resp.ServerVersion,
	}
}

func fromProtoOpenVolumeResponse(resp *sbsv1.OpenVolumeResponse) *service.OpenVolumeResponse {
	return &service.OpenVolumeResponse{
		Status:         resp.GetStatus(),
		VolumeHandle:   resp.GetVolumeHandle(),
		VolumeID:       resp.GetVolumeId(),
		VolumeRevision: resp.GetVolumeRevision(),
		Profile:        fromProtoVolumeProfile(resp.GetProfile()),
		ServerVersion:  resp.GetServerVersion(),
	}
}

func toProtoCloseVolumeRequest(req *service.CloseVolumeRequest) *sbsv1.CloseVolumeRequest {
	return &sbsv1.CloseVolumeRequest{
		VolumeId:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		Context:      toProtoRequestContext(req.Context),
	}
}

func fromProtoCloseVolumeRequest(req *sbsv1.CloseVolumeRequest) *service.CloseVolumeRequest {
	return &service.CloseVolumeRequest{
		VolumeID:     req.GetVolumeId(),
		VolumeHandle: req.GetVolumeHandle(),
		Context:      fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoCloseVolumeResponse(resp *service.CloseVolumeResponse) *sbsv1.CloseVolumeResponse {
	return &sbsv1.CloseVolumeResponse{Status: resp.Status}
}

func fromProtoCloseVolumeResponse(resp *sbsv1.CloseVolumeResponse) *service.CloseVolumeResponse {
	return &service.CloseVolumeResponse{Status: resp.GetStatus()}
}

func toProtoGetVolumeProfileRequest(req *service.GetVolumeProfileRequest) *sbsv1.GetVolumeProfileRequest {
	return &sbsv1.GetVolumeProfileRequest{
		VolumeId: req.VolumeID,
		Context:  toProtoRequestContext(req.Context),
	}
}

func fromProtoGetVolumeProfileRequest(req *sbsv1.GetVolumeProfileRequest) *service.GetVolumeProfileRequest {
	return &service.GetVolumeProfileRequest{
		VolumeID: req.GetVolumeId(),
		Context:  fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoGetVolumeProfileResponse(resp *service.GetVolumeProfileResponse) *sbsv1.GetVolumeProfileResponse {
	return &sbsv1.GetVolumeProfileResponse{
		VolumeId: resp.VolumeID,
		Profile:  toProtoVolumeProfile(resp.Profile),
	}
}

func fromProtoGetVolumeProfileResponse(resp *sbsv1.GetVolumeProfileResponse) *service.GetVolumeProfileResponse {
	return &service.GetVolumeProfileResponse{
		VolumeID: resp.GetVolumeId(),
		Profile:  fromProtoVolumeProfile(resp.GetProfile()),
	}
}

func toProtoGetVolumeStatusRequest(req *service.GetVolumeStatusRequest) *sbsv1.GetVolumeStatusRequest {
	return &sbsv1.GetVolumeStatusRequest{
		VolumeId: req.VolumeID,
		Context:  toProtoRequestContext(req.Context),
	}
}

func fromProtoGetVolumeStatusRequest(req *sbsv1.GetVolumeStatusRequest) *service.GetVolumeStatusRequest {
	return &service.GetVolumeStatusRequest{
		VolumeID: req.GetVolumeId(),
		Context:  fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoGetVolumeStatusResponse(resp *service.GetVolumeStatusResponse) *sbsv1.GetVolumeStatusResponse {
	return &sbsv1.GetVolumeStatusResponse{
		VolumeId:       resp.VolumeID,
		State:          toProtoVolumeState(resp.State),
		Readable:       resp.Readable,
		Writable:       resp.Writable,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoGetVolumeStatusResponse(resp *sbsv1.GetVolumeStatusResponse) *service.GetVolumeStatusResponse {
	return &service.GetVolumeStatusResponse{
		VolumeID:       resp.GetVolumeId(),
		State:          fromProtoVolumeState(resp.GetState()),
		Readable:       resp.GetReadable(),
		Writable:       resp.GetWritable(),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}

func toProtoReadRequest(req *service.ReadRequest) *sbsv1.ReadRequest {
	return &sbsv1.ReadRequest{
		VolumeId:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		OffsetBytes:  req.OffsetBytes,
		LengthBytes:  req.LengthBytes,
		Context:      toProtoRequestContext(req.Context),
	}
}

func fromProtoReadRequest(req *sbsv1.ReadRequest) *service.ReadRequest {
	return &service.ReadRequest{
		VolumeID:     req.GetVolumeId(),
		VolumeHandle: req.GetVolumeHandle(),
		OffsetBytes:  req.GetOffsetBytes(),
		LengthBytes:  req.GetLengthBytes(),
		Context:      fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoReadResponse(resp *service.ReadResponse) *sbsv1.ReadResponse {
	return &sbsv1.ReadResponse{
		VolumeId:       resp.VolumeID,
		OffsetBytes:    resp.OffsetBytes,
		LengthBytes:    resp.LengthBytes,
		Data:           resp.Data,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoReadResponse(resp *sbsv1.ReadResponse) *service.ReadResponse {
	return &service.ReadResponse{
		VolumeID:       resp.GetVolumeId(),
		OffsetBytes:    resp.GetOffsetBytes(),
		LengthBytes:    resp.GetLengthBytes(),
		Data:           append([]byte(nil), resp.GetData()...),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}

func toProtoWriteRequest(req *service.WriteRequest) *sbsv1.WriteRequest {
	return &sbsv1.WriteRequest{
		VolumeId:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		OffsetBytes:  req.OffsetBytes,
		LengthBytes:  req.LengthBytes,
		Data:         req.Data,
		Context:      toProtoRequestContext(req.Context),
	}
}

func fromProtoWriteRequest(req *sbsv1.WriteRequest) *service.WriteRequest {
	return &service.WriteRequest{
		VolumeID:     req.GetVolumeId(),
		VolumeHandle: req.GetVolumeHandle(),
		OffsetBytes:  req.GetOffsetBytes(),
		LengthBytes:  req.GetLengthBytes(),
		Data:         append([]byte(nil), req.GetData()...),
		Context:      fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoWriteResponse(resp *service.WriteResponse) *sbsv1.WriteResponse {
	return &sbsv1.WriteResponse{
		Status:         resp.Status,
		VolumeId:       resp.VolumeID,
		OffsetBytes:    resp.OffsetBytes,
		LengthBytes:    resp.LengthBytes,
		CommitId:       resp.CommitID,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoWriteResponse(resp *sbsv1.WriteResponse) *service.WriteResponse {
	return &service.WriteResponse{
		Status:         resp.GetStatus(),
		VolumeID:       resp.GetVolumeId(),
		OffsetBytes:    resp.GetOffsetBytes(),
		LengthBytes:    resp.GetLengthBytes(),
		CommitID:       resp.GetCommitId(),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}

func toProtoReadPhysicalChunkRequest(req *service.ReadPhysicalChunkRequest) *sbsv1.ReadPhysicalChunkRequest {
	return &sbsv1.ReadPhysicalChunkRequest{
		VolumeId:         req.VolumeID,
		VolumeHandle:     req.VolumeHandle,
		PhysicalChunkId:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		Context:          toProtoRequestContext(req.Context),
	}
}

func fromProtoReadPhysicalChunkRequest(req *sbsv1.ReadPhysicalChunkRequest) *service.ReadPhysicalChunkRequest {
	return &service.ReadPhysicalChunkRequest{
		VolumeID:         req.GetVolumeId(),
		VolumeHandle:     req.GetVolumeHandle(),
		PhysicalChunkID:  req.GetPhysicalChunkId(),
		ChunkOffsetBytes: req.GetChunkOffsetBytes(),
		LengthBytes:      req.GetLengthBytes(),
		Context:          fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoReadPhysicalChunkResponse(resp *service.ReadPhysicalChunkResponse) *sbsv1.ReadPhysicalChunkResponse {
	return &sbsv1.ReadPhysicalChunkResponse{
		VolumeId:         resp.VolumeID,
		PhysicalChunkId:  resp.PhysicalChunkID,
		ChunkOffsetBytes: resp.ChunkOffsetBytes,
		LengthBytes:      resp.LengthBytes,
		Data:             resp.Data,
		VolumeRevision:   resp.VolumeRevision,
	}
}

func fromProtoReadPhysicalChunkResponse(resp *sbsv1.ReadPhysicalChunkResponse) *service.ReadPhysicalChunkResponse {
	return &service.ReadPhysicalChunkResponse{
		VolumeID:         resp.GetVolumeId(),
		PhysicalChunkID:  resp.GetPhysicalChunkId(),
		ChunkOffsetBytes: resp.GetChunkOffsetBytes(),
		LengthBytes:      resp.GetLengthBytes(),
		Data:             append([]byte(nil), resp.GetData()...),
		VolumeRevision:   resp.GetVolumeRevision(),
	}
}

func toProtoWritePhysicalChunkRequest(req *service.WritePhysicalChunkRequest) *sbsv1.WritePhysicalChunkRequest {
	return &sbsv1.WritePhysicalChunkRequest{
		VolumeId:         req.VolumeID,
		VolumeHandle:     req.VolumeHandle,
		PhysicalChunkId:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		Data:             req.Data,
		Context:          toProtoRequestContext(req.Context),
	}
}

func fromProtoWritePhysicalChunkRequest(req *sbsv1.WritePhysicalChunkRequest) *service.WritePhysicalChunkRequest {
	return &service.WritePhysicalChunkRequest{
		VolumeID:         req.GetVolumeId(),
		VolumeHandle:     req.GetVolumeHandle(),
		PhysicalChunkID:  req.GetPhysicalChunkId(),
		ChunkOffsetBytes: req.GetChunkOffsetBytes(),
		LengthBytes:      req.GetLengthBytes(),
		Data:             append([]byte(nil), req.GetData()...),
		Context:          fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoWritePhysicalChunkResponse(resp *service.WritePhysicalChunkResponse) *sbsv1.WritePhysicalChunkResponse {
	return &sbsv1.WritePhysicalChunkResponse{
		Status:           resp.Status,
		VolumeId:         resp.VolumeID,
		PhysicalChunkId:  resp.PhysicalChunkID,
		ChunkOffsetBytes: resp.ChunkOffsetBytes,
		LengthBytes:      resp.LengthBytes,
		CommitId:         resp.CommitID,
		VolumeRevision:   resp.VolumeRevision,
	}
}

func fromProtoWritePhysicalChunkResponse(resp *sbsv1.WritePhysicalChunkResponse) *service.WritePhysicalChunkResponse {
	return &service.WritePhysicalChunkResponse{
		Status:           resp.GetStatus(),
		VolumeID:         resp.GetVolumeId(),
		PhysicalChunkID:  resp.GetPhysicalChunkId(),
		ChunkOffsetBytes: resp.GetChunkOffsetBytes(),
		LengthBytes:      resp.GetLengthBytes(),
		CommitID:         resp.GetCommitId(),
		VolumeRevision:   resp.GetVolumeRevision(),
	}
}

func toProtoWriteECShardRequest(req *service.WriteECShardRequest) *sbsv1.WriteECShardRequest {
	return &sbsv1.WriteECShardRequest{
		VolumeId:         req.VolumeID,
		VolumeHandle:     req.VolumeHandle,
		ObjectId:         req.ObjectID,
		StripeId:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardId:          req.ShardID,
		Role:             req.Role,
		RoleIndex:        req.RoleIndex,
		StoreId:          req.StoreID,
		Data:             req.Data,
		Checksum:         req.Checksum,
		Context:          toProtoRequestContext(req.Context),
	}
}

func fromProtoWriteECShardRequest(req *sbsv1.WriteECShardRequest) *service.WriteECShardRequest {
	return &service.WriteECShardRequest{
		VolumeID:         req.GetVolumeId(),
		VolumeHandle:     req.GetVolumeHandle(),
		ObjectID:         req.GetObjectId(),
		StripeID:         req.GetStripeId(),
		StripeGeneration: req.GetStripeGeneration(),
		ShardID:          req.GetShardId(),
		Role:             req.GetRole(),
		RoleIndex:        req.GetRoleIndex(),
		StoreID:          req.GetStoreId(),
		Data:             append([]byte(nil), req.GetData()...),
		Checksum:         req.GetChecksum(),
		Context:          fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoWriteECShardResponse(resp *service.WriteECShardResponse) *sbsv1.WriteECShardResponse {
	return &sbsv1.WriteECShardResponse{
		Status:           resp.Status,
		VolumeId:         resp.VolumeID,
		ObjectId:         resp.ObjectID,
		StripeId:         resp.StripeID,
		StripeGeneration: resp.StripeGeneration,
		ShardId:          resp.ShardID,
		Role:             resp.Role,
		RoleIndex:        resp.RoleIndex,
		StoreId:          resp.StoreID,
		LengthBytes:      resp.LengthBytes,
		Checksum:         resp.Checksum,
	}
}

func fromProtoWriteECShardResponse(resp *sbsv1.WriteECShardResponse) *service.WriteECShardResponse {
	return &service.WriteECShardResponse{
		Status:           resp.GetStatus(),
		VolumeID:         resp.GetVolumeId(),
		ObjectID:         resp.GetObjectId(),
		StripeID:         resp.GetStripeId(),
		StripeGeneration: resp.GetStripeGeneration(),
		ShardID:          resp.GetShardId(),
		Role:             resp.GetRole(),
		RoleIndex:        resp.GetRoleIndex(),
		StoreID:          resp.GetStoreId(),
		LengthBytes:      resp.GetLengthBytes(),
		Checksum:         resp.GetChecksum(),
	}
}

func toProtoReadECShardRequest(req *service.ReadECShardRequest) *sbsv1.ReadECShardRequest {
	return &sbsv1.ReadECShardRequest{
		VolumeId:         req.VolumeID,
		VolumeHandle:     req.VolumeHandle,
		ObjectId:         req.ObjectID,
		StripeId:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardId:          req.ShardID,
		StoreId:          req.StoreID,
		OffsetBytes:      req.OffsetBytes,
		LengthBytes:      req.LengthBytes,
		Context:          toProtoRequestContext(req.Context),
	}
}

func fromProtoReadECShardRequest(req *sbsv1.ReadECShardRequest) *service.ReadECShardRequest {
	return &service.ReadECShardRequest{
		VolumeID:         req.GetVolumeId(),
		VolumeHandle:     req.GetVolumeHandle(),
		ObjectID:         req.GetObjectId(),
		StripeID:         req.GetStripeId(),
		StripeGeneration: req.GetStripeGeneration(),
		ShardID:          req.GetShardId(),
		StoreID:          req.GetStoreId(),
		OffsetBytes:      req.GetOffsetBytes(),
		LengthBytes:      req.GetLengthBytes(),
		Context:          fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoReadECShardResponse(resp *service.ReadECShardResponse) *sbsv1.ReadECShardResponse {
	return &sbsv1.ReadECShardResponse{
		VolumeId:         resp.VolumeID,
		ObjectId:         resp.ObjectID,
		StripeId:         resp.StripeID,
		StripeGeneration: resp.StripeGeneration,
		ShardId:          resp.ShardID,
		StoreId:          resp.StoreID,
		OffsetBytes:      resp.OffsetBytes,
		LengthBytes:      resp.LengthBytes,
		Data:             resp.Data,
		Checksum:         resp.Checksum,
	}
}

func fromProtoReadECShardResponse(resp *sbsv1.ReadECShardResponse) *service.ReadECShardResponse {
	return &service.ReadECShardResponse{
		VolumeID:         resp.GetVolumeId(),
		ObjectID:         resp.GetObjectId(),
		StripeID:         resp.GetStripeId(),
		StripeGeneration: resp.GetStripeGeneration(),
		ShardID:          resp.GetShardId(),
		StoreID:          resp.GetStoreId(),
		OffsetBytes:      resp.GetOffsetBytes(),
		LengthBytes:      resp.GetLengthBytes(),
		Data:             append([]byte(nil), resp.GetData()...),
		Checksum:         resp.GetChecksum(),
	}
}

func toProtoDeleteECShardRequest(req *service.DeleteECShardRequest) *sbsv1.DeleteECShardRequest {
	return &sbsv1.DeleteECShardRequest{
		VolumeId:         req.VolumeID,
		VolumeHandle:     req.VolumeHandle,
		ObjectId:         req.ObjectID,
		StripeId:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardId:          req.ShardID,
		StoreId:          req.StoreID,
		Context:          toProtoRequestContext(req.Context),
	}
}

func fromProtoDeleteECShardRequest(req *sbsv1.DeleteECShardRequest) *service.DeleteECShardRequest {
	return &service.DeleteECShardRequest{
		VolumeID:         req.GetVolumeId(),
		VolumeHandle:     req.GetVolumeHandle(),
		ObjectID:         req.GetObjectId(),
		StripeID:         req.GetStripeId(),
		StripeGeneration: req.GetStripeGeneration(),
		ShardID:          req.GetShardId(),
		StoreID:          req.GetStoreId(),
		Context:          fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoDeleteECShardResponse(resp *service.DeleteECShardResponse) *sbsv1.DeleteECShardResponse {
	return &sbsv1.DeleteECShardResponse{
		Status:           resp.Status,
		VolumeId:         resp.VolumeID,
		ObjectId:         resp.ObjectID,
		StripeId:         resp.StripeID,
		StripeGeneration: resp.StripeGeneration,
		ShardId:          resp.ShardID,
		StoreId:          resp.StoreID,
	}
}

func fromProtoDeleteECShardResponse(resp *sbsv1.DeleteECShardResponse) *service.DeleteECShardResponse {
	return &service.DeleteECShardResponse{
		Status:           resp.GetStatus(),
		VolumeID:         resp.GetVolumeId(),
		ObjectID:         resp.GetObjectId(),
		StripeID:         resp.GetStripeId(),
		StripeGeneration: resp.GetStripeGeneration(),
		ShardID:          resp.GetShardId(),
		StoreID:          resp.GetStoreId(),
	}
}

func toProtoFlushRequest(req *service.FlushRequest) *sbsv1.FlushRequest {
	return &sbsv1.FlushRequest{
		VolumeId:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		Context:      toProtoRequestContext(req.Context),
	}
}

func fromProtoFlushRequest(req *sbsv1.FlushRequest) *service.FlushRequest {
	return &service.FlushRequest{
		VolumeID:     req.GetVolumeId(),
		VolumeHandle: req.GetVolumeHandle(),
		Context:      fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoFlushResponse(resp *service.FlushResponse) *sbsv1.FlushResponse {
	return &sbsv1.FlushResponse{
		Status:         resp.Status,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoFlushResponse(resp *sbsv1.FlushResponse) *service.FlushResponse {
	return &service.FlushResponse{
		Status:         resp.GetStatus(),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}

func toProtoDiscardRequest(req *service.DiscardRequest) *sbsv1.DiscardRequest {
	return &sbsv1.DiscardRequest{
		VolumeId:    req.VolumeID,
		OffsetBytes: req.OffsetBytes,
		LengthBytes: req.LengthBytes,
		Context:     toProtoRequestContext(req.Context),
	}
}

func fromProtoDiscardRequest(req *sbsv1.DiscardRequest) *service.DiscardRequest {
	return &service.DiscardRequest{
		VolumeID:    req.GetVolumeId(),
		OffsetBytes: req.GetOffsetBytes(),
		LengthBytes: req.GetLengthBytes(),
		Context:     fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoDiscardResponse(resp *service.DiscardResponse) *sbsv1.DiscardResponse {
	return &sbsv1.DiscardResponse{
		Status:         resp.Status,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoDiscardResponse(resp *sbsv1.DiscardResponse) *service.DiscardResponse {
	return &service.DiscardResponse{
		Status:         resp.GetStatus(),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}

func toProtoZeroRequest(req *service.ZeroRequest) *sbsv1.ZeroRequest {
	return &sbsv1.ZeroRequest{
		VolumeId:    req.VolumeID,
		OffsetBytes: req.OffsetBytes,
		LengthBytes: req.LengthBytes,
		Context:     toProtoRequestContext(req.Context),
	}
}

func fromProtoZeroRequest(req *sbsv1.ZeroRequest) *service.ZeroRequest {
	return &service.ZeroRequest{
		VolumeID:    req.GetVolumeId(),
		OffsetBytes: req.GetOffsetBytes(),
		LengthBytes: req.GetLengthBytes(),
		Context:     fromProtoRequestContext(req.GetContext()),
	}
}

func toProtoZeroResponse(resp *service.ZeroResponse) *sbsv1.ZeroResponse {
	return &sbsv1.ZeroResponse{
		Status:         resp.Status,
		VolumeRevision: resp.VolumeRevision,
	}
}

func fromProtoZeroResponse(resp *sbsv1.ZeroResponse) *service.ZeroResponse {
	return &service.ZeroResponse{
		Status:         resp.GetStatus(),
		VolumeRevision: resp.GetVolumeRevision(),
	}
}
