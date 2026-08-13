package sbsgrpc

import (
	"context"
	"errors"

	"github.com/nosway/namrbd/gateway/service"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	sbsv1.UnimplementedVolumeServiceServer
	next service.SBSClient
}

func NewServer(next service.SBSClient) *Server {
	return &Server{next: next}
}

func (s *Server) OpenVolume(ctx context.Context, req *sbsv1.OpenVolumeRequest) (*sbsv1.OpenVolumeResponse, error) {
	resp, err := s.next.OpenVolume(ctx, fromProtoOpenVolumeRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoOpenVolumeResponse(resp), nil
}

func (s *Server) CloseVolume(ctx context.Context, req *sbsv1.CloseVolumeRequest) (*sbsv1.CloseVolumeResponse, error) {
	resp, err := s.next.CloseVolume(ctx, fromProtoCloseVolumeRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoCloseVolumeResponse(resp), nil
}

func (s *Server) GetVolumeProfile(ctx context.Context, req *sbsv1.GetVolumeProfileRequest) (*sbsv1.GetVolumeProfileResponse, error) {
	resp, err := s.next.GetVolumeProfile(ctx, fromProtoGetVolumeProfileRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoGetVolumeProfileResponse(resp), nil
}

func (s *Server) GetVolumeStatus(ctx context.Context, req *sbsv1.GetVolumeStatusRequest) (*sbsv1.GetVolumeStatusResponse, error) {
	resp, err := s.next.GetVolumeStatus(ctx, fromProtoGetVolumeStatusRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoGetVolumeStatusResponse(resp), nil
}

func (s *Server) Read(ctx context.Context, req *sbsv1.ReadRequest) (*sbsv1.ReadResponse, error) {
	resp, err := s.next.Read(ctx, fromProtoReadRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoReadResponse(resp), nil
}

func (s *Server) Write(ctx context.Context, req *sbsv1.WriteRequest) (*sbsv1.WriteResponse, error) {
	resp, err := s.next.Write(ctx, fromProtoWriteRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoWriteResponse(resp), nil
}

func (s *Server) ReadPhysicalChunk(ctx context.Context, req *sbsv1.ReadPhysicalChunkRequest) (*sbsv1.ReadPhysicalChunkResponse, error) {
	next, ok := s.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, status.Error(codes.Unimplemented, service.ErrNotSupported.Error())
	}
	resp, err := next.ReadPhysicalChunk(ctx, fromProtoReadPhysicalChunkRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoReadPhysicalChunkResponse(resp), nil
}

func (s *Server) WritePhysicalChunk(ctx context.Context, req *sbsv1.WritePhysicalChunkRequest) (*sbsv1.WritePhysicalChunkResponse, error) {
	next, ok := s.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, status.Error(codes.Unimplemented, service.ErrNotSupported.Error())
	}
	resp, err := next.WritePhysicalChunk(ctx, fromProtoWritePhysicalChunkRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoWritePhysicalChunkResponse(resp), nil
}

func (s *Server) WriteECShard(ctx context.Context, req *sbsv1.WriteECShardRequest) (*sbsv1.WriteECShardResponse, error) {
	next, ok := s.next.(service.ECShardSBSClient)
	if !ok {
		return nil, status.Error(codes.Unimplemented, service.ErrNotSupported.Error())
	}
	resp, err := next.WriteECShard(ctx, fromProtoWriteECShardRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoWriteECShardResponse(resp), nil
}

func (s *Server) ReadECShard(ctx context.Context, req *sbsv1.ReadECShardRequest) (*sbsv1.ReadECShardResponse, error) {
	next, ok := s.next.(service.ECShardSBSClient)
	if !ok {
		return nil, status.Error(codes.Unimplemented, service.ErrNotSupported.Error())
	}
	resp, err := next.ReadECShard(ctx, fromProtoReadECShardRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoReadECShardResponse(resp), nil
}

func (s *Server) DeleteECShard(ctx context.Context, req *sbsv1.DeleteECShardRequest) (*sbsv1.DeleteECShardResponse, error) {
	next, ok := s.next.(service.ECShardSBSClient)
	if !ok {
		return nil, status.Error(codes.Unimplemented, service.ErrNotSupported.Error())
	}
	resp, err := next.DeleteECShard(ctx, fromProtoDeleteECShardRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoDeleteECShardResponse(resp), nil
}

func (s *Server) Flush(ctx context.Context, req *sbsv1.FlushRequest) (*sbsv1.FlushResponse, error) {
	resp, err := s.next.Flush(ctx, fromProtoFlushRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoFlushResponse(resp), nil
}

func (s *Server) Discard(ctx context.Context, req *sbsv1.DiscardRequest) (*sbsv1.DiscardResponse, error) {
	resp, err := s.next.Discard(ctx, fromProtoDiscardRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoDiscardResponse(resp), nil
}

func (s *Server) Zero(ctx context.Context, req *sbsv1.ZeroRequest) (*sbsv1.ZeroResponse, error) {
	resp, err := s.next.Zero(ctx, fromProtoZeroRequest(req))
	if err != nil {
		return nil, toGRPCError(err)
	}
	return toProtoZeroResponse(resp), nil
}

func toGRPCError(err error) error {
	var sbsErr *service.SBSError
	if !asSBSError(err, &sbsErr) {
		return status.Error(codes.Internal, err.Error())
	}
	detail := &sbsv1.ErrorDetail{
		Code:      toProtoErrorCode(sbsErr.Code),
		Message:   sbsErr.Message,
		Retryable: sbsErr.Retryable,
	}
	st := status.New(toGRPCCode(sbsErr.Code), sbsErr.Error())
	withDetail, detailErr := st.WithDetails(detail)
	if detailErr != nil {
		return st.Err()
	}
	return withDetail.Err()
}

func asSBSError(err error, target **service.SBSError) bool {
	if err == nil {
		return false
	}
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) {
		return false
	}
	*target = sbsErr
	return true
}

func toGRPCCode(code service.SBSErrorCode) codes.Code {
	switch code {
	case service.SBSErrorCodeNotFound:
		return codes.NotFound
	case service.SBSErrorCodeBadRequest:
		return codes.InvalidArgument
	case service.SBSErrorCodeStaleGeneration, service.SBSErrorCodeAttachmentMismatch, service.SBSErrorCodeIdempotencyConflict:
		return codes.FailedPrecondition
	case service.SBSErrorCodeUnavailable:
		return codes.Unavailable
	case service.SBSErrorCodeTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}
