package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type clusterStatusAggregateOnlyServer struct {
	adminv1.UnimplementedAdminServiceServer
	listNodesCalls      int
	listVolumesCalls    int
	listVolumePageCalls int
	volumePageResponse  *adminv1.ListVolumesPageResponse
	lastVolumePageReq   *adminv1.ListVolumesPageRequest
}

func (s *clusterStatusAggregateOnlyServer) GetClusterStatus(_ context.Context, req *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error) {
	return &adminv1.GetClusterStatusResponse{
		Cluster:       req.GetCluster(),
		LeaderNodeId:  "node1",
		QuorumHealth:  adminv1.QuorumHealth_QUORUM_HEALTH_HEALTHY,
		KnownNodes:    160,
		ActiveNodes:   157,
		DrainingNodes: 2,
		RemovedNodes:  1,
		HealthyNodes:  157,
		SuspectNodes:  2,
		DownNodes:     1,
	}, nil
}

func (s *clusterStatusAggregateOnlyServer) ListNodes(context.Context, *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	s.listNodesCalls++
	return &adminv1.ListNodesResponse{}, nil
}

func (s *clusterStatusAggregateOnlyServer) ListVolumes(_ context.Context, req *adminv1.ListVolumesRequest) (*adminv1.ListVolumesResponse, error) {
	s.listVolumesCalls++
	return &adminv1.ListVolumesResponse{Cluster: req.GetCluster()}, nil
}

func (s *clusterStatusAggregateOnlyServer) ListVolumesPage(_ context.Context, req *adminv1.ListVolumesPageRequest) (*adminv1.ListVolumesPageResponse, error) {
	s.listVolumePageCalls++
	s.lastVolumePageReq = req
	if s.volumePageResponse != nil {
		return s.volumePageResponse, nil
	}
	return &adminv1.ListVolumesPageResponse{Cluster: req.GetCluster()}, nil
}

func TestClusterStatusDefaultUsesAggregateOnly(t *testing.T) {
	server := &clusterStatusAggregateOnlyServer{}
	installBufconnSBSCTLAdminDialer(t, server)

	output := captureSBSCTLStdout(t, func() {
		runClusterStatus([]string{
			"--sbs-service-endpoint", "bufnet",
			"--cluster-id", "cluster-a",
			"--sbs-cluster-id", "sbs-a",
			"--output", "json",
		})
	})
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode cluster status JSON: %v\n%s", err, output)
	}
	if server.listNodesCalls != 0 {
		t.Fatalf("cluster status completed node pages: ListNodes calls=%d", server.listNodesCalls)
	}
	if result["known_nodes"] != float64(160) || result["node_detail_included"] != false {
		t.Fatalf("cluster aggregate output=%+v", result)
	}
	if _, ok := result["nodes"]; ok {
		t.Fatalf("cluster status included node details: %+v", result)
	}
}

func TestVolumeListDefaultUsesOnePagedRPC(t *testing.T) {
	server := &clusterStatusAggregateOnlyServer{volumePageResponse: &adminv1.ListVolumesPageResponse{
		CatalogRevision: 17, ProjectionHealth: "healthy", NextPageToken: "next-volume-page", ScannedRecords: 128,
		Volumes: []*adminv1.VolumeSummary{{VolumeId: "00a1b2c3", SizeBytes: 1 << 20, Health: adminv1.VolumeHealth_VOLUME_HEALTH_HEALTHY}},
	}}
	installBufconnSBSCTLAdminDialer(t, server)
	output := captureSBSCTLStdout(t, func() {
		runVolumeList([]string{
			"--sbs-service-endpoint", "bufnet", "--cluster-id", "cluster-a", "--sbs-cluster-id", "sbs-a", "--output", "json",
		})
	})
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode volume list JSON: %v\n%s", err, output)
	}
	if server.listVolumesCalls != 0 || server.listVolumePageCalls != 1 {
		t.Fatalf("legacy/page calls=%d/%d", server.listVolumesCalls, server.listVolumePageCalls)
	}
	if server.lastVolumePageReq.GetPageSize() != volumeListDefaultPageSize || server.lastVolumePageReq.GetPageToken() != "" {
		t.Fatalf("default page request=%+v", server.lastVolumePageReq)
	}
	if result["pages_read"] != float64(1) || result["automatic_page_completion"] != false || result["next_page_token"] != "next-volume-page" {
		t.Fatalf("paged volume output=%+v", result)
	}
}

func installBufconnSBSCTLAdminDialer(t *testing.T, server adminv1.AdminServiceServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	var conn *grpc.ClientConn
	oldDial := dialAdminClient
	dialAdminClient = func(ctx context.Context, _ string) (*adminclient.Client, error) {
		var err error
		conn, err = grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		return &adminclient.Client{Admin: adminv1.NewAdminServiceClient(conn)}, nil
	}
	t.Cleanup(func() {
		dialAdminClient = oldDial
		if conn != nil {
			_ = conn.Close()
		}
	})

}
