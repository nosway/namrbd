package sbsgrpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCRoundTripWithInMemorySBSClient(t *testing.T) {
	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	impl := service.NewInMemorySBSClient([]service.VolumeSpec{spec})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcServer, NewServer(impl))
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()

	client := NewClient(sbsv1.NewVolumeServiceClient(conn))

	openResp, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   "00000065",
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: service.SBSRequestContext{
			RequestID:    "req-open-1",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "sess-1",
			AttachmentID: "att-00000065-0001",
			Generation:   7,
		},
	})
	if err != nil {
		t.Fatalf("OpenVolume failed: %v", err)
	}

	writeReq := &service.WriteRequest{
		VolumeID:     "00000065",
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Data:         make([]byte, 4096),
		Context: service.SBSRequestContext{
			RequestID:      "req-write-1",
			GatewayID:      "gw-a",
			HostID:         "host-a",
			SessionID:      "sess-1",
			AttachmentID:   "att-00000065-0001",
			Generation:     7,
			IdempotencyKey: "idem-write-1",
		},
	}
	writeReq.Data[0] = 0xAA
	if _, err := client.Write(context.Background(), writeReq); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readResp, err := client.Read(context.Background(), &service.ReadRequest{
		VolumeID:     "00000065",
		VolumeHandle: openResp.VolumeHandle,
		OffsetBytes:  0,
		LengthBytes:  4096,
		Context: service.SBSRequestContext{
			RequestID:    "req-read-1",
			GatewayID:    "gw-a",
			HostID:       "host-a",
			SessionID:    "sess-1",
			AttachmentID: "att-00000065-0001",
			Generation:   7,
		},
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(readResp.Data) != 4096 || readResp.Data[0] != 0xAA {
		t.Fatalf("unexpected read response")
	}
}

func TestMaterializeVolumeGRPCRoundTrip(t *testing.T) {
	impl := &recordingMaterializeSBSClient{SBSClient: service.NewInMemorySBSClient(nil)}
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcServer, NewServer(impl))
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	req := &service.MaterializeVolumeRequest{
		Spec: service.VolumeSpec{
			ID:              service.HexVolumeID(0x00a1b2c3),
			Name:            "sbs-00a1b2c3",
			Prefix:          "sbs-00a1b2c3",
			SizeBytes:       1 << 20,
			BlockSize:       4096,
			ChunkSizeBytes:  65536,
			ExtentPageBytes: 4 << 20,
		},
		Context: service.SBSRequestContext{RequestID: "materialize-1", GatewayID: "gw-a"},
	}
	resp, err := NewClient(sbsv1.NewVolumeServiceClient(conn)).MaterializeVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("MaterializeVolume: %v", err)
	}
	if impl.request == nil || impl.request.Context != req.Context || impl.request.Spec.Prefix != req.Spec.Prefix {
		t.Fatalf("request did not round trip: got=%+v want=%+v", impl.request, req)
	}
	if resp.Status != "ok" || resp.Spec.ID != req.Spec.ID || resp.Spec.SizeBytes != req.Spec.SizeBytes ||
		resp.Spec.BlockSize != req.Spec.BlockSize || resp.Spec.ChunkSizeBytes != req.Spec.ChunkSizeBytes ||
		resp.Spec.ExtentPageBytes != req.Spec.ExtentPageBytes {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

type recordingMaterializeSBSClient struct {
	service.SBSClient
	request *service.MaterializeVolumeRequest
}

func (c *recordingMaterializeSBSClient) MaterializeVolume(_ context.Context, req *service.MaterializeVolumeRequest) (*service.MaterializeVolumeResponse, error) {
	copyReq := *req
	c.request = &copyReq
	return &service.MaterializeVolumeResponse{Status: "ok", Spec: req.Spec}, nil
}

func TestISCSIWriterFenceGRPCRoundTrip(t *testing.T) {
	impl := &recordingFenceSBSClient{SBSClient: service.NewInMemorySBSClient(nil)}
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcServer, NewServer(impl))
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	fence := service.ISCSIWriterFence{
		VolumeID: "00000065", ExportID: "export-a", ExportLeaseID: "lease-b",
		ExportEpoch: 8, ActiveGatewayID: "gw-b", RegistryRevision: 42,
	}
	resp, err := NewClient(sbsv1.NewVolumeServiceClient(conn)).ApplyISCSIWriterFence(context.Background(), &service.ApplyISCSIWriterFenceRequest{Fence: fence})
	if err != nil {
		t.Fatalf("ApplyISCSIWriterFence: %v", err)
	}
	if impl.fence != fence || resp.Fence != fence || !resp.Applied || resp.StaleWriterRejectedCount != 3 {
		t.Fatalf("fence roundtrip impl=%+v resp=%+v", impl.fence, resp)
	}
}

type recordingFenceSBSClient struct {
	service.SBSClient
	fence service.ISCSIWriterFence
}

func TestCompressionPolicyGRPCRoundTrip(t *testing.T) {
	impl := &recordingCompressionSBSClient{SBSClient: service.NewInMemorySBSClient(nil)}
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcServer, NewServer(impl))
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	policy := service.CompressionPolicy{
		VolumeID: "00000065", PolicyID: "zstd-a", PolicyRevision: 4,
		Codec: "ZSTD", MinimumInputBytes: 4096, ChecksumEnabled: true, Enabled: true,
	}
	client := NewClient(sbsv1.NewVolumeServiceClient(conn))
	apply, err := client.ApplyCompressionPolicy(context.Background(), &service.ApplyCompressionPolicyRequest{Policy: policy})
	if err != nil {
		t.Fatalf("ApplyCompressionPolicy: %v", err)
	}
	if impl.policy != policy || !apply.Applied || apply.Runtime.PolicyRevision != 4 {
		t.Fatalf("compression apply roundtrip policy=%+v resp=%+v", impl.policy, apply)
	}
	get, err := client.GetCompressionRuntimeStatus(context.Background(), &service.GetCompressionRuntimeStatusRequest{VolumeID: policy.VolumeID})
	if err != nil || get.Runtime.CompressedBytes != 1234 {
		t.Fatalf("compression status roundtrip resp=%+v err=%v", get, err)
	}
}

type recordingCompressionSBSClient struct {
	service.SBSClient
	policy service.CompressionPolicy
}

func (c *recordingCompressionSBSClient) ApplyCompressionPolicy(_ context.Context, req *service.ApplyCompressionPolicyRequest) (*service.ApplyCompressionPolicyResponse, error) {
	c.policy = req.Policy
	return &service.ApplyCompressionPolicyResponse{
		Status: "ok", Applied: true,
		Runtime: service.CompressionRuntimeStatus{VolumeID: req.Policy.VolumeID, PolicyID: req.Policy.PolicyID, PolicyRevision: req.Policy.PolicyRevision, Applied: true},
	}, nil
}

func (c *recordingCompressionSBSClient) GetCompressionRuntimeStatus(_ context.Context, req *service.GetCompressionRuntimeStatusRequest) (*service.GetCompressionRuntimeStatusResponse, error) {
	return &service.GetCompressionRuntimeStatusResponse{Runtime: service.CompressionRuntimeStatus{
		VolumeID: req.VolumeID, PolicyID: c.policy.PolicyID, PolicyRevision: c.policy.PolicyRevision,
		Applied: true, CompressedBytes: 1234,
	}}, nil
}

func (c *recordingFenceSBSClient) ApplyISCSIWriterFence(_ context.Context, req *service.ApplyISCSIWriterFenceRequest) (*service.ApplyISCSIWriterFenceResponse, error) {
	c.fence = req.Fence
	return &service.ApplyISCSIWriterFenceResponse{
		Status: "ok", Applied: true, Fence: req.Fence, StaleWriterRejectedCount: 3,
	}, nil
}

func TestFromGRPCErrorPreservesStatusWithoutDetail(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantCode      service.SBSErrorCode
		wantRetryable bool
	}{
		{
			name:          "unavailable",
			err:           status.Error(codes.Unavailable, "metadata unavailable"),
			wantCode:      service.SBSErrorCodeUnavailable,
			wantRetryable: true,
		},
		{
			name:          "deadline",
			err:           status.Error(codes.DeadlineExceeded, "metadata deadline"),
			wantCode:      service.SBSErrorCodeTimeout,
			wantRetryable: true,
		},
		{
			name:          "not found",
			err:           status.Error(codes.NotFound, "missing"),
			wantCode:      service.SBSErrorCodeNotFound,
			wantRetryable: false,
		},
		{
			name:          "internal",
			err:           status.Error(codes.Internal, "boom"),
			wantCode:      service.SBSErrorCodeInternal,
			wantRetryable: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fromGRPCError(tt.err)
			sbsErr, ok := err.(*service.SBSError)
			if !ok {
				t.Fatalf("error type=%T want *SBSError", err)
			}
			if sbsErr.Code != tt.wantCode || sbsErr.Retryable != tt.wantRetryable {
				t.Fatalf("error=(%s retryable=%v) want (%s retryable=%v)",
					sbsErr.Code, sbsErr.Retryable, tt.wantCode, tt.wantRetryable)
			}
		})
	}
}

func TestToGRPCErrorPreservesWrappedSBSErrorDetail(t *testing.T) {
	err := toGRPCError(fmt.Errorf("wrapped: %w", &service.SBSError{
		Code:      service.SBSErrorCodeUnavailable,
		Message:   "temporary unavailable",
		Retryable: true,
	}))
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError failed for %T", err)
	}
	if st.Code() != codes.Unavailable {
		t.Fatalf("code=%v want %v", st.Code(), codes.Unavailable)
	}
	roundTrip := fromGRPCError(err)
	sbsErr, ok := roundTrip.(*service.SBSError)
	if !ok {
		t.Fatalf("roundtrip error type=%T want *SBSError", roundTrip)
	}
	if sbsErr.Code != service.SBSErrorCodeUnavailable || !sbsErr.Retryable {
		t.Fatalf("roundtrip error=(%s retryable=%v) want unavailable retryable", sbsErr.Code, sbsErr.Retryable)
	}
}
