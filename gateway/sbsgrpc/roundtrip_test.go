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
