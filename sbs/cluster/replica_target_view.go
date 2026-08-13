package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"google.golang.org/protobuf/proto"
)

// ReplicaTargetViewStore provides the metadata needed to publish replica target
// availability for admin/control readers.
type ReplicaTargetViewStore interface {
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
	GetNodeHealthDetail(ctx context.Context, nodeID string) (metadata.NodeHealthDetailRecord, error)
}

// ReplicaTargetAdminEndpointFunc maps a node membership record to its node-local
// admin HTTP endpoint. The caller keeps endpoint policy at the process boundary.
type ReplicaTargetAdminEndpointFunc func(metadata.NodeMembershipRecord) string

// BuildReplicaTargetViews builds the published replica target view from node
// membership and node health metadata.
func BuildReplicaTargetViews(ctx context.Context, store ReplicaTargetViewStore, now time.Time, adminEndpointForNode ReplicaTargetAdminEndpointFunc) ([]*adminv1.ReplicaTargetView, error) {
	if store == nil {
		return nil, fmt.Errorf("replica target view store is not configured")
	}
	nodes, err := store.ListNodeMemberships(ctx)
	if err != nil {
		return nil, err
	}
	nowUnix := now.Unix()
	targets := make([]*adminv1.ReplicaTargetView, 0, len(nodes)*2)
	for _, node := range nodes {
		targets = append(targets, ReplicaTargetViewsForNode(ctx, store, node, nowUnix, adminEndpointForNode)...)
	}
	sort.SliceStable(targets, func(i, j int) bool {
		if targets[i].GetPriority() != targets[j].GetPriority() {
			return targets[i].GetPriority() > targets[j].GetPriority()
		}
		return targets[i].GetTargetId() < targets[j].GetTargetId()
	})
	return targets, nil
}

// ReplicaTargetViewsForNode builds the primary target view and optional node-ID
// alias for a single SBS data node.
func ReplicaTargetViewsForNode(ctx context.Context, store ReplicaTargetViewStore, node metadata.NodeMembershipRecord, nowUnix int64, adminEndpointForNode ReplicaTargetAdminEndpointFunc) []*adminv1.ReplicaTargetView {
	var detail metadata.NodeHealthDetailRecord
	if store != nil {
		detail, _ = store.GetNodeHealthDetail(ctx, node.NodeID)
	}
	base := BuildReplicaTargetView(node, detail, nowUnix, adminEndpointForNode)
	if base == nil {
		return nil
	}
	out := []*adminv1.ReplicaTargetView{base}
	replicaID := strings.TrimSpace(node.ReplicaID)
	nodeID := strings.TrimSpace(node.NodeID)
	if replicaID != "" && nodeID != "" && replicaID != nodeID {
		alias := proto.Clone(base).(*adminv1.ReplicaTargetView)
		alias.TargetId = nodeID
		out = append(out, alias)
	}
	return out
}

// BuildReplicaTargetView converts one node membership record into a published
// replica target view.
func BuildReplicaTargetView(node metadata.NodeMembershipRecord, detail metadata.NodeHealthDetailRecord, nowUnix int64, adminEndpointForNode ReplicaTargetAdminEndpointFunc) *adminv1.ReplicaTargetView {
	targetID := strings.TrimSpace(node.ReplicaID)
	if targetID == "" {
		targetID = strings.TrimSpace(node.NodeID)
	}
	if targetID == "" {
		return nil
	}
	reason := adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_READY
	usable := true
	priority := uint32(100)
	switch node.LifecycleState {
	case "", metadata.NodeLifecycleActive:
	case metadata.NodeLifecycleDraining:
		usable = false
		priority = 0
		reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_DRAINING
	default:
		usable = false
		priority = 0
		reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_DOWN
	}
	if usable {
		switch node.HealthState {
		case "", metadata.NodeHealthHealthy:
		case metadata.NodeHealthSuspect:
			priority = 50
			reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_SUSPECT
		default:
			usable = false
			priority = 0
			reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_DOWN
		}
	}
	if usable && detail.RecoveryEligibleAtUnix > nowUnix {
		usable = false
		priority = 0
		reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_RECOVERY_COOLDOWN
	}
	if usable && !detail.StorePlacementEligible() {
		usable = false
		priority = 0
		reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_BACKEND_UNAVAILABLE
	}
	var endpoint *adminv1.ReplicaTargetEndpoint
	if len(node.SBSEndpoints) > 0 && strings.TrimSpace(node.SBSEndpoints[0].Address) != "" && node.SBSEndpoints[0].Port != 0 {
		endpoint = &adminv1.ReplicaTargetEndpoint{
			Address:    node.SBSEndpoints[0].Address,
			Port:       uint32(node.SBSEndpoints[0].Port),
			UseTls:     node.SBSEndpoints[0].UseTLS,
			ServerName: node.SBSEndpoints[0].ServerName,
		}
	} else {
		usable = false
		priority = 0
		reason = adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_ENDPOINT_MISSING
	}
	adminEndpoint := ""
	if adminEndpointForNode != nil {
		adminEndpoint = adminEndpointForNode(node)
	}
	return &adminv1.ReplicaTargetView{
		TargetId:          targetID,
		Endpoint:          endpoint,
		AdminHttpEndpoint: adminEndpoint,
		Usable:            usable,
		Priority:          priority,
		ReasonCode:        reason,
	}
}
