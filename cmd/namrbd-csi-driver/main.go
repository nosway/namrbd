package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"

	"github.com/nosway/namrbd/internal/adminclient"
	csidriver "github.com/nosway/namrbd/internal/csi/driver"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "namrbd-csi-driver: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("namrbd-csi-driver", flag.ExitOnError)
	endpoint := fs.String("endpoint", "unix:///tmp/namrbd-csi.sock", "CSI listening endpoint, unix://path or tcp://host:port")
	adminEndpoint := fs.String("admin-endpoint", getenv("NAMRBD_ADMIN_ENDPOINT", "127.0.0.1:9897"), "NAMRBD sbs-service admin gRPC endpoint")
	adminEndpoints := fs.String("admin-endpoints", getenv("NAMRBD_ADMIN_ENDPOINTS", ""), "optional comma/space-separated SBS admin gRPC endpoints; entries may be node_id=endpoint")
	clusterID := fs.String("cluster-id", getenv("NAMRBD_CLUSTER_ID", "namrbd-lab"), "NAMRBD cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", getenv("SBS_CLUSTER_ID", getenv("NAMRBD_SBS_CLUSTER_ID", "sbs-lab")), "SBS cluster id")
	driverName := fs.String("driver-name", csidriver.DefaultDriverName, "CSI driver name")
	vendorVersion := fs.String("vendor-version", csidriver.DefaultVendorVersion, "CSI vendor version")
	nodeID := fs.String("node-id", getenv("NAMRBD_CSI_NODE_ID", getenv("HOSTNAME", "")), "CSI node id")
	gatewayURL := fs.String("gateway-url", getenv("NAMRBD_GATEWAY_URL", ""), "NAMRBD gateway URL used by CSI Node attach")
	namrbdctlPath := fs.String("namrbdctl", getenv("NAMRBDCTL", "namrbdctl"), "namrbdctl path used by CSI Node helper")
	if err := fs.Parse(args); err != nil {
		return err
	}

	client, err := adminclient.NewLeaderAwareAdminClient(context.Background(), adminclient.LeaderAwareAdminConfig{
		PrimaryEndpoint: *adminEndpoint,
		Endpoints:       adminclient.ParseEndpointSpecs(*adminEndpoint, *adminEndpoints),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	server, err := csidriver.New(csidriver.Config{
		DriverName:    *driverName,
		VendorVersion: *vendorVersion,
		ClusterID:     *clusterID,
		SBSClusterID:  *sbsClusterID,
		Backend:       adminBackend{client: client},
		NodeID:        *nodeID,
		GatewayURL:    *gatewayURL,
		NamrbdctlPath: *namrbdctlPath,
	})
	if err != nil {
		return err
	}
	listener, err := listenCSIEndpoint(*endpoint)
	if err != nil {
		return err
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	csipb.RegisterIdentityServer(grpcServer, server)
	csipb.RegisterControllerServer(grpcServer, server)
	csipb.RegisterNodeServer(grpcServer, server)
	return grpcServer.Serve(listener)
}

func listenCSIEndpoint(endpoint string) (net.Listener, error) {
	if strings.HasPrefix(endpoint, "unix://") {
		path := strings.TrimPrefix(endpoint, "unix://")
		if path == "" {
			return nil, fmt.Errorf("unix endpoint path is required")
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		_ = os.Remove(path)
		return net.Listen("unix", path)
	}
	if strings.HasPrefix(endpoint, "tcp://") {
		address := strings.TrimPrefix(endpoint, "tcp://")
		if address == "" {
			return nil, fmt.Errorf("tcp endpoint address is required")
		}
		return net.Listen("tcp", address)
	}
	return nil, fmt.Errorf("unsupported endpoint %q", endpoint)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type adminBackend struct {
	client *adminclient.LeaderAwareAdminClient
}

func (b adminBackend) CreateVolume(ctx context.Context, req *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
	var resp *adminv1.CreateVolumeResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.CreateVolume(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) CreateVolumeFromSnapshot(ctx context.Context, req *adminv1.CreateVolumeFromSnapshotRequest) (*adminv1.CreateVolumeFromSnapshotResponse, error) {
	var resp *adminv1.CreateVolumeFromSnapshotResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.CreateVolumeFromSnapshot(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) DeleteVolume(ctx context.Context, req *adminv1.DeleteVolumeRequest) (*adminv1.DeleteVolumeResponse, error) {
	var resp *adminv1.DeleteVolumeResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.DeleteVolume(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) GetVolume(ctx context.Context, req *adminv1.GetVolumeRequest) (*adminv1.GetVolumeResponse, error) {
	var resp *adminv1.GetVolumeResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.GetVolume(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) CreateSnapshot(ctx context.Context, req *adminv1.CreateSnapshotRequest) (*adminv1.CreateSnapshotResponse, error) {
	var resp *adminv1.CreateSnapshotResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.CreateSnapshot(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) GetSnapshot(ctx context.Context, req *adminv1.GetSnapshotRequest) (*adminv1.GetSnapshotResponse, error) {
	var resp *adminv1.GetSnapshotResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.GetSnapshot(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) ListSnapshots(ctx context.Context, req *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error) {
	var resp *adminv1.ListSnapshotsResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.ListSnapshots(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) DeleteSnapshot(ctx context.Context, req *adminv1.DeleteSnapshotRequest) (*adminv1.DeleteSnapshotResponse, error) {
	var resp *adminv1.DeleteSnapshotResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.DeleteSnapshot(ctx, req)
		return callErr
	})
	return resp, err
}

func (b adminBackend) ExpandVolume(ctx context.Context, req *adminv1.ExpandVolumeRequest) (*adminv1.ExpandVolumeResponse, error) {
	var resp *adminv1.ExpandVolumeResponse
	err := b.client.Invoke(ctx, req.GetCluster(), func(client adminv1.AdminServiceClient) error {
		var callErr error
		resp, callErr = client.ExpandVolume(ctx, req)
		return callErr
	})
	return resp, err
}
