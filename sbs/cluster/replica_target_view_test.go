package cluster

import (
	"context"
	"reflect"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakeReplicaTargetViewStore struct {
	nodes   []metadata.NodeMembershipRecord
	details map[string]metadata.NodeHealthDetailRecord
	err     error
}

func (s fakeReplicaTargetViewStore) ListNodeMemberships(context.Context) ([]metadata.NodeMembershipRecord, error) {
	return append([]metadata.NodeMembershipRecord(nil), s.nodes...), s.err
}

func (s fakeReplicaTargetViewStore) GetNodeHealthDetail(_ context.Context, nodeID string) (metadata.NodeHealthDetailRecord, error) {
	return s.details[nodeID], nil
}

func TestBuildReplicaTargetViewsSortsAndIncludesNodeAlias(t *testing.T) {
	store := fakeReplicaTargetViewStore{
		nodes: []metadata.NodeMembershipRecord{
			{
				NodeID:         "node-b",
				ReplicaID:      "rep-b",
				LifecycleState: metadata.NodeLifecycleActive,
				HealthState:    metadata.NodeHealthSuspect,
				SBSEndpoints:   []metadata.SBSEndpoint{{Address: "10.0.0.2", Port: 9443}},
			},
			{
				NodeID:         "node-a",
				ReplicaID:      "rep-a",
				LifecycleState: metadata.NodeLifecycleActive,
				HealthState:    metadata.NodeHealthHealthy,
				SBSEndpoints:   []metadata.SBSEndpoint{{Address: "10.0.0.1", Port: 9443}},
			},
		},
		details: map[string]metadata.NodeHealthDetailRecord{},
	}
	targets, err := BuildReplicaTargetViews(context.Background(), store, time.Unix(100, 0), func(node metadata.NodeMembershipRecord) string {
		return "http://" + node.NodeID + ":9082"
	})
	if err != nil {
		t.Fatalf("BuildReplicaTargetViews: %v", err)
	}
	gotIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		gotIDs = append(gotIDs, target.GetTargetId())
	}
	wantIDs := []string{"node-a", "rep-a", "node-b", "rep-b"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("target ids=%v want %v", gotIDs, wantIDs)
	}
	if targets[0].GetPriority() != 100 || !targets[0].GetUsable() {
		t.Fatalf("first target priority=%d usable=%v", targets[0].GetPriority(), targets[0].GetUsable())
	}
	if targets[2].GetPriority() != 50 || targets[2].GetReasonCode() != adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_SUSPECT {
		t.Fatalf("suspect target priority=%d reason=%v", targets[2].GetPriority(), targets[2].GetReasonCode())
	}
}

func TestBuildReplicaTargetViewMarksUnavailableReasons(t *testing.T) {
	nowUnix := int64(100)
	base := metadata.NodeMembershipRecord{
		NodeID:         "node-a",
		ReplicaID:      "rep-a",
		LifecycleState: metadata.NodeLifecycleActive,
		HealthState:    metadata.NodeHealthHealthy,
		SBSEndpoints:   []metadata.SBSEndpoint{{Address: "10.0.0.1", Port: 9443}},
	}
	cases := []struct {
		name   string
		node   metadata.NodeMembershipRecord
		detail metadata.NodeHealthDetailRecord
		reason adminv1.ReplicaTargetReasonCode
	}{
		{
			name:   "draining",
			node:   withNodeLifecycle(base, metadata.NodeLifecycleDraining),
			reason: adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_NODE_DRAINING,
		},
		{
			name:   "recovery-cooldown",
			node:   base,
			detail: metadata.NodeHealthDetailRecord{RecoveryEligibleAtUnix: nowUnix + 1},
			reason: adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_RECOVERY_COOLDOWN,
		},
		{
			name:   "backend-unavailable",
			node:   base,
			detail: metadata.NodeHealthDetailRecord{StoreCount: 1, WritableStoreCount: 0},
			reason: adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_BACKEND_UNAVAILABLE,
		},
		{
			name:   "endpoint-missing",
			node:   withNodeEndpoints(base, nil),
			reason: adminv1.ReplicaTargetReasonCode_REPLICA_TARGET_REASON_CODE_ENDPOINT_MISSING,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := BuildReplicaTargetView(tc.node, tc.detail, nowUnix, nil)
			if target == nil {
				t.Fatal("target is nil")
			}
			if target.GetUsable() {
				t.Fatalf("usable=true want false")
			}
			if target.GetPriority() != 0 {
				t.Fatalf("priority=%d want 0", target.GetPriority())
			}
			if target.GetReasonCode() != tc.reason {
				t.Fatalf("reason=%v want %v", target.GetReasonCode(), tc.reason)
			}
		})
	}
}

func withNodeLifecycle(node metadata.NodeMembershipRecord, lifecycle metadata.NodeLifecycleState) metadata.NodeMembershipRecord {
	node.LifecycleState = lifecycle
	return node
}

func withNodeEndpoints(node metadata.NodeMembershipRecord, endpoints []metadata.SBSEndpoint) metadata.NodeMembershipRecord {
	node.SBSEndpoints = endpoints
	return node
}
