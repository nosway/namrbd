package cluster

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type NodeMembershipResolver interface {
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
}

type PublishedNodeMembershipOptions struct {
	Endpoint         string
	ClusterID        string
	SBSClusterID     string
	Fallback         NodeMembershipResolver
	AllowRawFallback bool
}

type publishedNodeMembershipResolver struct {
	adminEndpoint string
	clusterRef    *adminv1.ClusterRef
	fallback      NodeMembershipResolver
	allowFallback bool
}

func NewPublishedNodeMembershipResolver(opts PublishedNodeMembershipOptions) NodeMembershipResolver {
	adminEndpoint := strings.TrimSpace(opts.Endpoint)
	if adminEndpoint == "" && opts.Fallback != nil && opts.AllowRawFallback {
		return opts.Fallback
	}
	return &publishedNodeMembershipResolver{
		adminEndpoint: adminEndpoint,
		clusterRef: &adminv1.ClusterRef{
			ClusterId:    strings.TrimSpace(opts.ClusterID),
			SbsClusterId: strings.TrimSpace(opts.SBSClusterID),
		},
		fallback:      opts.Fallback,
		allowFallback: opts.AllowRawFallback,
	}
}

func (r *publishedNodeMembershipResolver) ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	if r == nil {
		return nil, fmt.Errorf("node membership resolver is not configured")
	}
	if strings.TrimSpace(r.adminEndpoint) != "" {
		nodes, err := r.listNodeMembershipsFromAdmin(ctx)
		if err == nil {
			return nodes, nil
		}
		if r.allowFallback && r.fallback != nil {
			log.Printf("gateway runtime falling back to legacy raw node membership metadata: %v", err)
			return r.fallback.ListNodeMemberships(ctx)
		}
		return nil, err
	}
	if r.allowFallback && r.fallback != nil {
		return r.fallback.ListNodeMemberships(ctx)
	}
	return nil, fmt.Errorf("node membership resolver requires reachable --sbs-admin-endpoint")
}

func (r *publishedNodeMembershipResolver) listNodeMembershipsFromAdmin(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(dialCtx, r.adminEndpoint)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	resp, err := adminclient.ListAllNodes(ctx, client.Admin, r.clusterRef, false)
	if err != nil {
		return nil, err
	}
	nodes := make([]metadata.NodeMembershipRecord, 0, len(resp.GetNodes()))
	for _, node := range resp.GetNodes() {
		if node == nil {
			continue
		}
		nodes = append(nodes, NodeMembershipRecordFromAdmin(node))
	}
	return nodes, nil
}

func NodeMembershipRecordFromAdmin(node *adminv1.NodeSummary) metadata.NodeMembershipRecord {
	rec := metadata.NodeMembershipRecord{
		ClusterID:          strings.TrimSpace(node.GetClusterId()),
		SBSClusterID:       strings.TrimSpace(node.GetSbsClusterId()),
		NodeID:             strings.TrimSpace(node.GetNodeId()),
		ReplicaID:          strings.TrimSpace(node.GetReplicaId()),
		StoreIDs:           append([]string(nil), node.GetStoreIds()...),
		Roles:              append([]string(nil), node.GetRoles()...),
		LifecycleState:     nodeLifecycleStateFromAdmin(node.GetLifecycle()),
		HealthState:        nodeHealthStateFromAdmin(node.GetHealth()),
		DesiredState:       strings.TrimSpace(node.GetDesiredState()),
		ObservedState:      strings.TrimSpace(node.GetObservedState()),
		Zone:               strings.TrimSpace(node.GetZone()),
		AdminHTTPEndpoint:  strings.TrimSpace(node.GetAdminHttpEndpoint()),
		LastHeartbeatUnix:  node.GetLastHeartbeatTime().GetSeconds(),
		Generation:         node.GetGeneration(),
		MembershipRevision: node.GetMembershipRevision(),
		Tombstone:          node.GetTombstone(),
		CreatedAtUnix:      node.GetCreatedTime().GetSeconds(),
		UpdatedAtUnix:      node.GetUpdatedTime().GetSeconds(),
		UpdatedBy:          strings.TrimSpace(node.GetUpdatedBy()),
		UpdateReason:       strings.TrimSpace(node.GetUpdateReason()),
	}
	if rec.ReplicaID == "" {
		rec.ReplicaID = rec.NodeID
	}
	if endpoint := sbsEndpointFromAdminAddress(node.GetGrpcEndpoint()); endpoint != nil {
		rec.SBSEndpoints = []metadata.SBSEndpoint{*endpoint}
		rec.Host = endpoint.Address
	}
	return rec
}

func nodeLifecycleStateFromAdmin(state adminv1.NodeLifecycle) metadata.NodeLifecycleState {
	switch state {
	case adminv1.NodeLifecycle_NODE_LIFECYCLE_JOINING:
		return metadata.NodeLifecycleJoining
	case adminv1.NodeLifecycle_NODE_LIFECYCLE_DRAINING:
		return metadata.NodeLifecycleDraining
	case adminv1.NodeLifecycle_NODE_LIFECYCLE_REMOVED:
		return metadata.NodeLifecycleRemoved
	case adminv1.NodeLifecycle_NODE_LIFECYCLE_ACTIVE:
		fallthrough
	default:
		return metadata.NodeLifecycleActive
	}
}

func nodeHealthStateFromAdmin(state adminv1.NodeHealth) metadata.NodeHealthState {
	switch state {
	case adminv1.NodeHealth_NODE_HEALTH_SUSPECT:
		return metadata.NodeHealthSuspect
	case adminv1.NodeHealth_NODE_HEALTH_DOWN:
		return metadata.NodeHealthDown
	case adminv1.NodeHealth_NODE_HEALTH_HEALTHY:
		fallthrough
	default:
		return metadata.NodeHealthHealthy
	}
}

func sbsEndpointFromAdminAddress(endpoint string) *metadata.SBSEndpoint {
	host, port, err := net.SplitHostPort(strings.TrimSpace(endpoint))
	if err != nil || host == "" {
		return nil
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil
	}
	return &metadata.SBSEndpoint{
		Address: host,
		Port:    uint16(parsedPort),
	}
}
