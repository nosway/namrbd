package placement

import "testing"

func TestRFSpreadPolicyPlanInitialLayout(t *testing.T) {
	policy := NewRFSpreadPolicy()
	layout, err := policy.PlanInitialLayout(InitialLayoutRequest{
		VolumeID:          "00a1b2c3",
		SizeBytes:         (64 << 20) + (32 << 20),
		ExtentSizeBytes:   64 << 20,
		ReplicationFactor: 3,
		Candidates: []CandidateNode{
			{NodeID: "node-a", Zone: "zone-a"},
			{NodeID: "node-b", Zone: "zone-b"},
			{NodeID: "node-c", Zone: "zone-c"},
			{NodeID: "node-d", Zone: "zone-a"},
		},
	})
	if err != nil {
		t.Fatalf("PlanInitialLayout: %v", err)
	}
	if len(layout.Extents) != 2 {
		t.Fatalf("extent count=%d want=2", len(layout.Extents))
	}
	if layout.Extents[0].LengthBytes != 64<<20 {
		t.Fatalf("extent[0] length=%d", layout.Extents[0].LengthBytes)
	}
	if layout.Extents[1].LengthBytes != 32<<20 {
		t.Fatalf("extent[1] length=%d", layout.Extents[1].LengthBytes)
	}
	if layout.Extents[0].ReplicaSet.PlacementRef == layout.Extents[1].ReplicaSet.PlacementRef {
		t.Fatal("placement refs must differ per extent")
	}
	if len(layout.Extents[0].ReplicaSet.Replicas) != 3 {
		t.Fatalf("replica count=%d want=3", len(layout.Extents[0].ReplicaSet.Replicas))
	}
}

func TestRFSpreadPolicyStrictRequiresDistinctZones(t *testing.T) {
	policy := NewRFSpreadPolicy()
	_, err := policy.PlanInitialLayout(InitialLayoutRequest{
		VolumeID:          "00a1b2c3",
		SizeBytes:         64 << 20,
		ExtentSizeBytes:   64 << 20,
		ReplicationFactor: 3,
		TopologyMode:      TopologyModeStrict,
		Candidates: []CandidateNode{
			{NodeID: "node-a", Zone: "zone-a"},
			{NodeID: "node-b", Zone: "zone-a"},
			{NodeID: "node-c", Zone: "zone-b"},
		},
	})
	if err == nil {
		t.Fatal("PlanInitialLayout succeeded; want strict topology error")
	}
}

func TestRFSpreadPolicyLegacyAllowsDuplicateZones(t *testing.T) {
	policy := NewRFSpreadPolicy()
	layout, err := policy.PlanInitialLayout(InitialLayoutRequest{
		VolumeID:          "00a1b2c3",
		SizeBytes:         64 << 20,
		ExtentSizeBytes:   64 << 20,
		ReplicationFactor: 3,
		TopologyMode:      TopologyModeLegacy,
		Candidates: []CandidateNode{
			{NodeID: "node-a", Zone: "zone-a"},
			{NodeID: "node-b", Zone: "zone-a"},
			{NodeID: "node-c", Zone: "zone-b"},
		},
	})
	if err != nil {
		t.Fatalf("PlanInitialLayout: %v", err)
	}
	if len(layout.Extents[0].ReplicaSet.Replicas) != 3 {
		t.Fatalf("replica count=%d want=3", len(layout.Extents[0].ReplicaSet.Replicas))
	}
}
