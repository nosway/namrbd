package placement

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPlanECStripeLimitedZoneDistributions(t *testing.T) {
	tests := []struct {
		name       string
		data       uint32
		coding     uint32
		zones      int
		perZone    int
		wantCounts []uint32
	}{
		{name: "6+3", data: 6, coding: 3, zones: 3, perZone: 3, wantCounts: []uint32{3, 3, 3}},
		{name: "8+4", data: 8, coding: 4, zones: 3, perZone: 4, wantCounts: []uint32{4, 4, 4}},
		{name: "10+5", data: 10, coding: 5, zones: 3, perZone: 5, wantCounts: []uint32{5, 5, 5}},
		{name: "12+4", data: 12, coding: 4, zones: 4, perZone: 4, wantCounts: []uint32{4, 4, 4, 4}},
		{name: "16+4", data: 16, coding: 4, zones: 5, perZone: 4, wantCounts: []uint32{4, 4, 4, 4, 4}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := PlanECStripe(ECStripePlacementRequest{
				VolumeID:         "00a1b2c3",
				ProfileID:        "ec-" + tc.name,
				StripeID:         7,
				DataShards:       tc.data,
				CodingShards:     tc.coding,
				TopologyRevision: 11,
				Candidates:       ecTestCandidates(tc.zones, tc.perZone),
			})
			if err != nil {
				t.Fatalf("PlanECStripe: %v", err)
			}
			if len(plan.ShardTargets) != int(tc.data+tc.coding) {
				t.Fatalf("shard targets=%d want=%d", len(plan.ShardTargets), tc.data+tc.coding)
			}
			got := sortedECCountValues(plan.TopologyReport.ZoneShardCounts)
			if !reflect.DeepEqual(got, tc.wantCounts) {
				t.Fatalf("zone counts=%v want=%v report=%+v", got, tc.wantCounts, plan.TopologyReport)
			}
			if !plan.TopologyReport.ZoneToleranceOK || !plan.TopologyReport.NodeSpreadOK || !plan.TopologyReport.StoreSpreadOK {
				t.Fatalf("strict spread flags not all true: %+v", plan.TopologyReport)
			}
			if !plan.TopologyReport.DataRoleBalanceOK || !plan.TopologyReport.CodingRoleBalanceOK {
				t.Fatalf("role balance flags not all true: %+v", plan.TopologyReport)
			}
		})
	}
}

func TestPlanECStripeNineZoneRotationAndDeterminism(t *testing.T) {
	req := ECStripePlacementRequest{
		VolumeID:         "00a1b2c3",
		ProfileID:        "ec-6-3",
		StripeID:         1,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		Candidates:       ecTestCandidates(9, 1),
	}
	planA, err := PlanECStripe(req)
	if err != nil {
		t.Fatalf("PlanECStripe A: %v", err)
	}
	planB, err := PlanECStripe(req)
	if err != nil {
		t.Fatalf("PlanECStripe B: %v", err)
	}
	if !reflect.DeepEqual(planA, planB) {
		t.Fatalf("same input produced different plans:\nA=%+v\nB=%+v", planA, planB)
	}
	req.StripeID = 2
	planC, err := PlanECStripe(req)
	if err != nil {
		t.Fatalf("PlanECStripe C: %v", err)
	}
	if planA.ShardTargets[0].Zone == planC.ShardTargets[0].Zone {
		t.Fatalf("first data shard did not rotate: stripe1=%s stripe2=%s", planA.ShardTargets[0].Zone, planC.ShardTargets[0].Zone)
	}
	for zone, count := range planA.TopologyReport.ZoneShardCounts {
		if count != 1 {
			t.Fatalf("zone %s count=%d want=1", zone, count)
		}
	}
	if planA.TopologyReport.PlacementSkew != 0 {
		t.Fatalf("placement_skew=%d want=0", planA.TopologyReport.PlacementSkew)
	}
}

func TestPlanECStripeRoleBalanceAcrossConsecutiveStripes(t *testing.T) {
	for stripeID := uint64(1); stripeID <= 3; stripeID++ {
		plan, err := PlanECStripe(ECStripePlacementRequest{
			VolumeID:         "00a1b2c3",
			ProfileID:        "ec-8-4",
			StripeID:         stripeID,
			DataShards:       8,
			CodingShards:     4,
			TopologyRevision: 11,
			Candidates:       ecTestCandidates(3, 4),
		})
		if err != nil {
			t.Fatalf("PlanECStripe stripe %d: %v", stripeID, err)
		}
		if !plan.TopologyReport.DataRoleBalanceOK || !plan.TopologyReport.CodingRoleBalanceOK {
			t.Fatalf("stripe %d role balance false: %+v", stripeID, plan.TopologyReport)
		}
		if skew := ecCountSkew(plan.TopologyReport.DataZoneCounts, sortedECZonesFromReport(plan.TopologyReport.ZoneShardCounts)); skew > 1 {
			t.Fatalf("stripe %d data skew=%d counts=%v", stripeID, skew, plan.TopologyReport.DataZoneCounts)
		}
		if skew := ecCountSkew(plan.TopologyReport.CodingZoneCounts, sortedECZonesFromReport(plan.TopologyReport.ZoneShardCounts)); skew > 1 {
			t.Fatalf("stripe %d coding skew=%d counts=%v", stripeID, skew, plan.TopologyReport.CodingZoneCounts)
		}
	}
}

func TestPlanECStripeStrictRejectsInsufficientNodesInZone(t *testing.T) {
	candidates := ecTestCandidates(3, 3)
	candidates = removeECCandidate(candidates, "zone-a", "node-zone-a-03")
	plan, err := PlanECStripe(ECStripePlacementRequest{
		VolumeID:         "00a1b2c3",
		ProfileID:        "ec-6-3",
		StripeID:         1,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		Candidates:       candidates,
	})
	if err == nil {
		t.Fatalf("PlanECStripe succeeded, plan=%+v", plan)
	}
	if plan.BlockedReason != ECBlockedInsufficientNodesInZone {
		t.Fatalf("blocked_reason=%q want %q err=%v", plan.BlockedReason, ECBlockedInsufficientNodesInZone, err)
	}
}

func TestPlanECStripeStrictRejectsDuplicateStoreInZone(t *testing.T) {
	candidates := ecTestCandidates(3, 3)
	for i := range candidates {
		if candidates[i].Zone == "zone-a" {
			candidates[i].StoreID = "store-zone-a-shared"
		}
	}
	plan, err := PlanECStripe(ECStripePlacementRequest{
		VolumeID:         "00a1b2c3",
		ProfileID:        "ec-6-3",
		StripeID:         1,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		Candidates:       candidates,
	})
	if err == nil {
		t.Fatalf("PlanECStripe succeeded, plan=%+v", plan)
	}
	if plan.BlockedReason != ECBlockedStoreSpread {
		t.Fatalf("blocked_reason=%q want %q err=%v", plan.BlockedReason, ECBlockedStoreSpread, err)
	}
}

func TestPlanECStripeWeakOverrideAuditsNodeAndStoreReuse(t *testing.T) {
	candidates := ecTestCandidates(3, 3)
	candidates = removeECCandidate(candidates, "zone-a", "node-zone-a-02")
	candidates = removeECCandidate(candidates, "zone-a", "node-zone-a-03")
	plan, err := PlanECStripe(ECStripePlacementRequest{
		VolumeID:             "00a1b2c3",
		ProfileID:            "ec-6-3",
		StripeID:             1,
		DataShards:           6,
		CodingShards:         3,
		TopologyMode:         ECTopologyModeWeak,
		WeakPlacementAllowed: true,
		TopologyRevision:     11,
		Candidates:           candidates,
	})
	if err != nil {
		t.Fatalf("PlanECStripe weak: %v", err)
	}
	if !plan.TopologyReport.WeakPlacement {
		t.Fatalf("weak_placement=false report=%+v", plan.TopologyReport)
	}
	if plan.TopologyReport.BrokenStrictRule != ECBlockedInsufficientNodesInZone {
		t.Fatalf("broken_strict_rule=%q want %q", plan.TopologyReport.BrokenStrictRule, ECBlockedInsufficientNodesInZone)
	}
	if plan.TopologyReport.NodeSpreadOK || plan.TopologyReport.StoreSpreadOK {
		t.Fatalf("weak plan should expose node/store spread failure: %+v", plan.TopologyReport)
	}
	if !plan.TopologyReport.ZoneToleranceOK {
		t.Fatalf("weak node/store override must preserve zone tolerance: %+v", plan.TopologyReport)
	}
}

func TestPlanECStripeFiltersZeroWeightCandidates(t *testing.T) {
	candidates := ecTestCandidates(3, 4)
	candidates[0].StoreWeight = 0
	plan, err := PlanECStripe(ECStripePlacementRequest{
		VolumeID:         "00a1b2c3",
		ProfileID:        "ec-6-3",
		StripeID:         1,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		Candidates:       candidates,
	})
	if err != nil {
		t.Fatalf("PlanECStripe: %v", err)
	}
	for _, target := range plan.ShardTargets {
		if target.NodeID == candidates[0].NodeID && target.StoreID == candidates[0].StoreID {
			t.Fatalf("zero-weight candidate was selected: %+v", target)
		}
	}
}

func TestPlanECStripeReportJSONContainsSmokeFields(t *testing.T) {
	plan, err := PlanECStripe(ECStripePlacementRequest{
		VolumeID:         "00a1b2c3",
		ProfileID:        "ec-6-3",
		StripeID:         1,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 11,
		Candidates:       ecTestCandidates(3, 3),
	})
	if err != nil {
		t.Fatalf("PlanECStripe: %v", err)
	}
	body, err := json.Marshal(plan.TopologyReport)
	if err != nil {
		t.Fatalf("Marshal report: %v", err)
	}
	jsonText := string(body)
	for _, want := range []string{
		"zone_shard_counts",
		"data_zone_counts",
		"coding_zone_counts",
		"node_shard_counts",
		"data_node_counts",
		"coding_node_counts",
		"store_shard_counts",
		"zone_tolerance_ok",
		"node_spread_ok",
		"store_spread_ok",
		"weak_placement",
		"blocked_reason",
	} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("report JSON missing %q: %s", want, jsonText)
		}
	}
}

func ecTestCandidates(zones, perZone int) []ECPlacementCandidate {
	out := make([]ECPlacementCandidate, 0, zones*perZone)
	for z := 0; z < zones; z++ {
		zoneID := string(rune('a' + z))
		for n := 1; n <= perZone; n++ {
			out = append(out, ECPlacementCandidate{
				Zone:          "zone-" + zoneID,
				NodeID:        "node-zone-" + zoneID + "-" + twoDigit(n),
				StoreID:       "store-zone-" + zoneID + "-" + twoDigit(n),
				NodeHealth:    ECNodeHealthHealthy,
				NodeLifecycle: ECNodeLifecycleActive,
				StoreState:    ECStoreStateWritable,
				StoreWeight:   100,
			})
		}
	}
	return out
}

func removeECCandidate(in []ECPlacementCandidate, zone, nodeID string) []ECPlacementCandidate {
	out := make([]ECPlacementCandidate, 0, len(in))
	for _, candidate := range in {
		if candidate.Zone == zone && candidate.NodeID == nodeID {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func sortedECCountValues(counts map[string]uint32) []uint32 {
	values := make([]uint32, 0, len(counts))
	for _, value := range counts {
		values = append(values, value)
	}
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
	return values
}

func sortedECZonesFromReport(counts map[string]uint32) []string {
	zones := make([]string, 0, len(counts))
	for zone := range counts {
		zones = append(zones, zone)
	}
	for i := 0; i < len(zones); i++ {
		for j := i + 1; j < len(zones); j++ {
			if zones[j] < zones[i] {
				zones[i], zones[j] = zones[j], zones[i]
			}
		}
	}
	return zones
}

func twoDigit(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return string(rune('0'+(n/10))) + string(rune('0'+(n%10)))
}
