package control

import (
	"fmt"
	"strings"

	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewAdminEndpointPlacementApplyAdapter(endpoint string) (PlacementApplyAdapter, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("placement apply adapter requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial placement apply endpoint %q: %w", endpoint, err)
	}
	return NewGRPCPlacementApplyAdapter(internalv1.NewPlacementApplyServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func NewAdminEndpointWriteSessionAdapter(endpoint string) (*GRPCWriteSessionAdapter, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("write session committer requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial write session endpoint %q: %w", endpoint, err)
	}
	return NewGRPCWriteSessionAdapter(internalv1.NewWriteSessionServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func NewAdminEndpointChunkIDAllocator(endpoint string) (ChunkIDAllocatorAdapter, func(), error) {
	return NewAdminEndpointPhysicalChunkIDAllocator(endpoint)
}

func NewAdminEndpointPhysicalChunkIDAllocator(endpoint string) (PhysicalChunkIDAllocatorAdapter, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("chunk id allocator requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial chunk id allocator endpoint %q: %w", endpoint, err)
	}
	return NewGRPCChunkIDAllocatorAdapter(internalv1.NewChunkIDAllocatorServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func NewAdminEndpointPlacementResolver(endpoint string) (PlacementResolverAdapter, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("placement resolver requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial placement resolver endpoint %q: %w", endpoint, err)
	}
	return NewGRPCPlacementResolverAdapter(internalv1.NewPlacementResolverServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func NewAdminEndpointECMetadataAdapter(endpoint string) (ECMetadataAdapter, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("ec metadata adapter requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial ec metadata endpoint %q: %w", endpoint, err)
	}
	return NewGRPCECMetadataAdapter(internalv1.NewECMetadataServiceClient(conn)), func() { _ = conn.Close() }, nil
}
