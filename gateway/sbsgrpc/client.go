package sbsgrpc

import (
	"context"

	"github.com/nosway/namrbd/gateway/service"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	next sbsv1.VolumeServiceClient
}

func NewClient(next sbsv1.VolumeServiceClient) *Client {
	return &Client{next: next}
}

func (c *Client) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	resp, err := c.next.OpenVolume(ctx, toProtoOpenVolumeRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoOpenVolumeResponse(resp), nil
}

func (c *Client) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	resp, err := c.next.CloseVolume(ctx, toProtoCloseVolumeRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoCloseVolumeResponse(resp), nil
}

func (c *Client) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	resp, err := c.next.GetVolumeProfile(ctx, toProtoGetVolumeProfileRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoGetVolumeProfileResponse(resp), nil
}

func (c *Client) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	resp, err := c.next.GetVolumeStatus(ctx, toProtoGetVolumeStatusRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoGetVolumeStatusResponse(resp), nil
}

func (c *Client) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	resp, err := c.next.Read(ctx, toProtoReadRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoReadResponse(resp), nil
}

func (c *Client) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	resp, err := c.next.Write(ctx, toProtoWriteRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoWriteResponse(resp), nil
}

func (c *Client) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	resp, err := c.next.ReadPhysicalChunk(ctx, toProtoReadPhysicalChunkRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoReadPhysicalChunkResponse(resp), nil
}

func (c *Client) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	resp, err := c.next.WritePhysicalChunk(ctx, toProtoWritePhysicalChunkRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoWritePhysicalChunkResponse(resp), nil
}

func (c *Client) WriteECShard(ctx context.Context, req *service.WriteECShardRequest) (*service.WriteECShardResponse, error) {
	resp, err := c.next.WriteECShard(ctx, toProtoWriteECShardRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoWriteECShardResponse(resp), nil
}

func (c *Client) ReadECShard(ctx context.Context, req *service.ReadECShardRequest) (*service.ReadECShardResponse, error) {
	resp, err := c.next.ReadECShard(ctx, toProtoReadECShardRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoReadECShardResponse(resp), nil
}

func (c *Client) DeleteECShard(ctx context.Context, req *service.DeleteECShardRequest) (*service.DeleteECShardResponse, error) {
	resp, err := c.next.DeleteECShard(ctx, toProtoDeleteECShardRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoDeleteECShardResponse(resp), nil
}

func (c *Client) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	resp, err := c.next.Flush(ctx, toProtoFlushRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoFlushResponse(resp), nil
}

func (c *Client) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	resp, err := c.next.Discard(ctx, toProtoDiscardRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoDiscardResponse(resp), nil
}

func (c *Client) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	resp, err := c.next.Zero(ctx, toProtoZeroRequest(req))
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return fromProtoZeroResponse(resp), nil
}

func fromGRPCError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	for _, detail := range st.Details() {
		if ed, ok := detail.(*sbsv1.ErrorDetail); ok {
			return &service.SBSError{
				Code:      fromProtoErrorCode(ed.Code),
				Message:   ed.Message,
				Retryable: ed.Retryable,
			}
		}
	}
	if code, retryable, ok := serviceCodeFromGRPCCode(st.Code()); ok {
		return &service.SBSError{
			Code:      code,
			Message:   st.Message(),
			Retryable: retryable,
		}
	}
	return &service.SBSError{
		Code:      service.SBSErrorCodeInternal,
		Message:   st.Message(),
		Retryable: false,
	}
}

func serviceCodeFromGRPCCode(code codes.Code) (service.SBSErrorCode, bool, bool) {
	switch code {
	case codes.NotFound:
		return service.SBSErrorCodeNotFound, false, true
	case codes.InvalidArgument:
		return service.SBSErrorCodeBadRequest, false, true
	case codes.FailedPrecondition:
		return service.SBSErrorCodeStaleGeneration, false, true
	case codes.Canceled, codes.Unavailable:
		return service.SBSErrorCodeUnavailable, true, true
	case codes.DeadlineExceeded:
		return service.SBSErrorCodeTimeout, true, true
	default:
		return service.SBSErrorCodeInternal, false, false
	}
}
