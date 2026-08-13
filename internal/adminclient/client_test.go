package adminclient

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCTargetNormalizesPlainHostPort(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "ip host port",
			endpoint: "10.169.207.212:9443",
			want:     "passthrough:///10.169.207.212:9443",
		},
		{
			name:     "dns host port",
			endpoint: "sbs-service.namrbd-system.svc:9897",
			want:     "passthrough:///sbs-service.namrbd-system.svc:9897",
		},
		{
			name:     "explicit dns target",
			endpoint: "dns:///sbs-service.namrbd-system.svc:9897",
			want:     "dns:///sbs-service.namrbd-system.svc:9897",
		},
		{
			name:     "explicit passthrough target",
			endpoint: "passthrough:///bufnet",
			want:     "passthrough:///bufnet",
		},
		{
			name:     "explicit unix target",
			endpoint: "unix:///tmp/namrbd-admin.sock",
			want:     "unix:///tmp/namrbd-admin.sock",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := grpcTarget(tt.endpoint); got != tt.want {
				t.Fatalf("grpcTarget(%q)=%q want %q", tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestParseEndpointSpecs(t *testing.T) {
	got := ParseEndpointSpecs("u01:9443", "svc-u04=u04:9443,svc-u05=passthrough:///u05:9443 u01:9443")
	want := []EndpointSpec{
		{Endpoint: "u01:9443"},
		{NodeID: "svc-u04", Endpoint: "u04:9443"},
		{NodeID: "svc-u05", Endpoint: "passthrough:///u05:9443"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseEndpointSpecs()=%+v want %+v", got, want)
	}
}

func TestLeaderHintFromError(t *testing.T) {
	leaderID, ok := LeaderHintFromError(status.Error(codes.Unavailable, "local node is not leader; current leader=svc-u04"))
	if !ok || leaderID != "svc-u04" {
		t.Fatalf("LeaderHintFromError()=(%q,%v) want (svc-u04,true)", leaderID, ok)
	}
	if _, ok := LeaderHintFromError(status.Error(codes.Unavailable, "transport is closing")); ok {
		t.Fatalf("LeaderHintFromError() returned a hint for ordinary Unavailable")
	}
	if _, ok := LeaderHintFromError(status.Error(codes.FailedPrecondition, "local node is not leader; current leader=svc-u04")); ok {
		t.Fatalf("LeaderHintFromError() returned a hint for non-Unavailable status")
	}
}

func TestLeaderAwareAdminClientRetriesOnDiscoveredLeader(t *testing.T) {
	var followerCreates atomic.Int32
	var leaderCreates atomic.Int32
	var listNodes atomic.Int32

	follower := newBufconnAdminClient(t, &leaderRetryAdminServer{
		createVolume: func(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
			followerCreates.Add(1)
			return nil, status.Error(codes.Unavailable, "local node is not leader; current leader=svc-u04")
		},
		listNodes: func(context.Context, *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
			listNodes.Add(1)
			return &adminv1.ListNodesResponse{
				Nodes: []*adminv1.NodeSummary{{
					NodeId:       "svc-u04",
					GrpcEndpoint: "leader",
				}},
			}, nil
		},
	})
	leader := newBufconnAdminClient(t, &leaderRetryAdminServer{
		createVolume: func(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
			leaderCreates.Add(1)
			return &adminv1.CreateVolumeResponse{}, nil
		},
	})
	clients := map[string]adminv1.AdminServiceClient{
		"follower": follower,
		"leader":   leader,
	}
	client, err := NewLeaderAwareAdminClient(context.Background(), LeaderAwareAdminConfig{
		PrimaryEndpoint: "follower",
		Endpoints:       ParseEndpointSpecs("follower", ""),
		Dial:            mapDialer(clients),
	})
	if err != nil {
		t.Fatalf("NewLeaderAwareAdminClient: %v", err)
	}

	var resp *adminv1.CreateVolumeResponse
	err = client.Invoke(context.Background(), &adminv1.ClusterRef{ClusterId: "namrbd-lab", SbsClusterId: "sbs-lab"}, func(admin adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = admin.CreateVolume(context.Background(), &adminv1.CreateVolumeRequest{})
		return callErr
	})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp == nil {
		t.Fatalf("CreateVolume response is nil")
	}
	if got := followerCreates.Load(); got != 1 {
		t.Fatalf("follower CreateVolume calls=%d want 1", got)
	}
	if got := listNodes.Load(); got != 1 {
		t.Fatalf("ListNodes calls=%d want 1", got)
	}
	if got := leaderCreates.Load(); got != 1 {
		t.Fatalf("leader CreateVolume calls=%d want 1", got)
	}
}

func TestLeaderAwareAdminClientPreservesOriginalErrorWhenLeaderEndpointUnknown(t *testing.T) {
	var leaderCreates atomic.Int32
	follower := newBufconnAdminClient(t, &leaderRetryAdminServer{
		createVolume: func(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
			return nil, status.Error(codes.Unavailable, "local node is not leader; current leader=svc-u99")
		},
		listNodes: func(context.Context, *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
			return &adminv1.ListNodesResponse{}, nil
		},
	})
	leader := newBufconnAdminClient(t, &leaderRetryAdminServer{
		createVolume: func(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
			leaderCreates.Add(1)
			return &adminv1.CreateVolumeResponse{}, nil
		},
	})
	clients := map[string]adminv1.AdminServiceClient{
		"follower": follower,
		"leader":   leader,
	}
	client, err := NewLeaderAwareAdminClient(context.Background(), LeaderAwareAdminConfig{
		PrimaryEndpoint: "follower",
		Endpoints:       ParseEndpointSpecs("follower", ""),
		Dial:            mapDialer(clients),
	})
	if err != nil {
		t.Fatalf("NewLeaderAwareAdminClient: %v", err)
	}

	err = client.Invoke(context.Background(), &adminv1.ClusterRef{ClusterId: "namrbd-lab", SbsClusterId: "sbs-lab"}, func(admin adminv1.AdminServiceClient) error {
		_, callErr := admin.CreateVolume(context.Background(), &adminv1.CreateVolumeRequest{})
		return callErr
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Invoke error=%v want Unavailable", err)
	}
	if got := leaderCreates.Load(); got != 0 {
		t.Fatalf("leader CreateVolume calls=%d want 0", got)
	}
}

type leaderRetryAdminServer struct {
	adminv1.UnimplementedAdminServiceServer
	createVolume func(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error)
	listNodes    func(context.Context, *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error)
}

func (s *leaderRetryAdminServer) CreateVolume(ctx context.Context, req *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
	if s.createVolume == nil {
		return nil, status.Error(codes.Unimplemented, "CreateVolume")
	}
	return s.createVolume(ctx, req)
}

func (s *leaderRetryAdminServer) ListNodes(ctx context.Context, req *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	if s.listNodes == nil {
		return nil, status.Error(codes.Unimplemented, "ListNodes")
	}
	return s.listNodes(ctx, req)
}

func newBufconnAdminClient(t *testing.T, server adminv1.AdminServiceServer) adminv1.AdminServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return adminv1.NewAdminServiceClient(conn)
}

func mapDialer(clients map[string]adminv1.AdminServiceClient) AdminDialer {
	return func(_ context.Context, endpoint string) (adminv1.AdminServiceClient, func() error, error) {
		client, ok := clients[endpoint]
		if !ok {
			return nil, nil, fmt.Errorf("unexpected endpoint %q", endpoint)
		}
		return client, nil, nil
	}
}
