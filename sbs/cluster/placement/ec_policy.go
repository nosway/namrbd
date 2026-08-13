package placement

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

const (
	ECTopologyModeWeak = "weak"

	ECShardRoleData   ECShardRole = "data"
	ECShardRoleCoding ECShardRole = "coding"

	ECNodeHealthHealthy   = "healthy"
	ECNodeLifecycleActive = "active"
	ECStoreStateWritable  = "writable"
)

const (
	ECBlockedInvalidRequest            = "invalid_request"
	ECBlockedInsufficientCandidates    = "insufficient_candidates"
	ECBlockedInsufficientZones         = "insufficient_zones"
	ECBlockedZoneCapExceeded           = "zone_cap_exceeded"
	ECBlockedInsufficientNodesInZone   = "insufficient_nodes_in_zone"
	ECBlockedStoreSpread               = "store_spread_blocked"
	ECBlockedNoEligibleCandidateInZone = "no_eligible_candidate_in_zone"
)

type ECShardRole string

type ECPlacementCandidate struct {
	Zone           string `json:"zone"`
	NodeID         string `json:"node_id"`
	StoreID        string `json:"store_id"`
	NodeHealth     string `json:"node_health,omitempty"`
	NodeLifecycle  string `json:"node_lifecycle,omitempty"`
	StoreState     string `json:"store_state,omitempty"`
	StoreWeight    int    `json:"store_weight,omitempty"`
	AvailableBytes uint64 `json:"available_bytes,omitempty"`
}

type ECStripePlacementRequest struct {
	VolumeID             string                 `json:"volume_id"`
	ProfileID            string                 `json:"profile_id"`
	StripeID             uint64                 `json:"stripe_id"`
	StripeGeneration     uint64                 `json:"stripe_generation"`
	DataShards           uint32                 `json:"data_shards"`
	CodingShards         uint32                 `json:"coding_shards"`
	TopologyMode         string                 `json:"topology_mode"`
	WeakPlacementAllowed bool                   `json:"weak_placement_allowed,omitempty"`
	TopologyRevision     uint64                 `json:"topology_revision"`
	Candidates           []ECPlacementCandidate `json:"candidates,omitempty"`
}

type ECShardTarget struct {
	ShardID   uint32      `json:"shard_id"`
	Role      ECShardRole `json:"role"`
	RoleIndex uint32      `json:"role_index"`
	Zone      string      `json:"zone"`
	NodeID    string      `json:"node_id"`
	StoreID   string      `json:"store_id"`
}

type ECTopologyReport struct {
	ZoneShardCounts        map[string]uint32 `json:"zone_shard_counts,omitempty"`
	DataZoneCounts         map[string]uint32 `json:"data_zone_counts,omitempty"`
	CodingZoneCounts       map[string]uint32 `json:"coding_zone_counts,omitempty"`
	MaxShardsPerZone       uint32            `json:"max_shards_per_zone,omitempty"`
	ZoneToleranceOK        bool              `json:"zone_tolerance_ok"`
	NodeShardCounts        map[string]uint32 `json:"node_shard_counts,omitempty"`
	DataNodeCounts         map[string]uint32 `json:"data_node_counts,omitempty"`
	CodingNodeCounts       map[string]uint32 `json:"coding_node_counts,omitempty"`
	MaxShardsPerNode       uint32            `json:"max_shards_per_node,omitempty"`
	StoreShardCounts       map[string]uint32 `json:"store_shard_counts,omitempty"`
	MaxShardsPerStore      uint32            `json:"max_shards_per_store,omitempty"`
	NodeSpreadOK           bool              `json:"node_spread_ok"`
	StoreSpreadOK          bool              `json:"store_spread_ok"`
	DataRoleBalanceOK      bool              `json:"data_role_balance_ok"`
	CodingRoleBalanceOK    bool              `json:"coding_role_balance_ok"`
	PlacementSkew          uint32            `json:"placement_skew,omitempty"`
	WeakPlacement          bool              `json:"weak_placement"`
	BrokenStrictRule       string            `json:"broken_strict_rule,omitempty"`
	BlockedReason          string            `json:"blocked_reason"`
	EligibleZoneCount      uint32            `json:"eligible_zone_count,omitempty"`
	EligibleCandidateCount uint32            `json:"eligible_candidate_count,omitempty"`
}

type ECStripePlacementPlan struct {
	VolumeID         string           `json:"volume_id"`
	ProfileID        string           `json:"profile_id"`
	StripeID         uint64           `json:"stripe_id"`
	StripeGeneration uint64           `json:"stripe_generation"`
	TopologyRevision uint64           `json:"topology_revision"`
	ShardTargets     []ECShardTarget  `json:"shard_targets,omitempty"`
	TopologyReport   ECTopologyReport `json:"topology_report"`
	BlockedReason    string           `json:"blocked_reason,omitempty"`
}

type ecPlacementBlockedError struct {
	reason  string
	message string
}

func (e *ecPlacementBlockedError) Error() string {
	if e.message == "" {
		return e.reason
	}
	return fmt.Sprintf("%s: %s", e.reason, e.message)
}

func PlanECStripe(req ECStripePlacementRequest) (ECStripePlacementPlan, error) {
	req = normalizeECStripePlacementRequest(req)
	if err := validateECStripePlacementRequest(req); err != nil {
		return blockedECStripePlacementPlan(req, ECTopologyReport{}, ecBlockedReason(err)), err
	}
	candidates := eligibleECCandidates(req.Candidates)
	report := ECTopologyReport{EligibleCandidateCount: uint32(len(candidates))}
	if len(candidates) == 0 {
		err := newECPlacementBlockedError(ECBlockedInsufficientCandidates, "no eligible placement candidates")
		return blockedECStripePlacementPlan(req, report, ecBlockedReason(err)), err
	}
	zoneOrder, zoneTargets, err := computeECZoneTargets(req, candidates)
	if err != nil {
		report.EligibleZoneCount = uint32(len(sortedECZones(candidates)))
		return blockedECStripePlacementPlan(req, report, ecBlockedReason(err)), err
	}
	report.EligibleZoneCount = uint32(len(zoneOrder))
	dataCounts, codingCounts := computeECRoleCounts(req, zoneOrder, zoneTargets)
	plan, err := buildECStripePlacementPlan(req, candidates, zoneOrder, zoneTargets, dataCounts, codingCounts, false, "")
	if err == nil {
		return plan, nil
	}
	reason := ecBlockedReason(err)
	if normalizeECTopologyMode(req.TopologyMode) == ECTopologyModeWeak && req.WeakPlacementAllowed && ecWeakPlacementCanOverride(reason) {
		weakPlan, weakErr := buildECStripePlacementPlan(req, candidates, zoneOrder, zoneTargets, dataCounts, codingCounts, true, reason)
		if weakErr == nil {
			return weakPlan, nil
		}
	}
	return blockedECStripePlacementPlan(req, report, reason), err
}

func normalizeECStripePlacementRequest(req ECStripePlacementRequest) ECStripePlacementRequest {
	req.VolumeID = strings.TrimSpace(req.VolumeID)
	req.ProfileID = strings.TrimSpace(req.ProfileID)
	req.TopologyMode = normalizeECTopologyMode(req.TopologyMode)
	return req
}

func validateECStripePlacementRequest(req ECStripePlacementRequest) error {
	if req.VolumeID == "" {
		return newECPlacementBlockedError(ECBlockedInvalidRequest, "volume_id is required")
	}
	if req.ProfileID == "" {
		return newECPlacementBlockedError(ECBlockedInvalidRequest, "profile_id is required")
	}
	if req.DataShards == 0 {
		return newECPlacementBlockedError(ECBlockedInvalidRequest, "data_shards is required")
	}
	if req.CodingShards == 0 {
		return newECPlacementBlockedError(ECBlockedInvalidRequest, "coding_shards is required")
	}
	if len(req.Candidates) == 0 {
		return newECPlacementBlockedError(ECBlockedInsufficientCandidates, "candidates are required")
	}
	return nil
}

func normalizeECTopologyMode(raw string) string {
	switch raw {
	case ECTopologyModeWeak:
		return ECTopologyModeWeak
	default:
		return TopologyModeStrict
	}
}

func eligibleECCandidates(in []ECPlacementCandidate) []ECPlacementCandidate {
	seen := make(map[string]struct{}, len(in))
	out := make([]ECPlacementCandidate, 0, len(in))
	for _, candidate := range in {
		candidate = normalizeECCandidate(candidate)
		if candidate.Zone == "" || candidate.NodeID == "" || candidate.StoreID == "" {
			continue
		}
		if candidate.NodeHealth != "" && candidate.NodeHealth != ECNodeHealthHealthy {
			continue
		}
		if candidate.NodeLifecycle != "" && candidate.NodeLifecycle != ECNodeLifecycleActive {
			continue
		}
		if candidate.StoreState != "" && candidate.StoreState != ECStoreStateWritable {
			continue
		}
		if candidate.StoreWeight <= 0 {
			continue
		}
		key := ecCandidateKey(candidate)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, candidate)
	}
	sortECCandidates(out)
	return out
}

func normalizeECCandidate(candidate ECPlacementCandidate) ECPlacementCandidate {
	candidate.Zone = strings.TrimSpace(candidate.Zone)
	candidate.NodeID = strings.TrimSpace(candidate.NodeID)
	candidate.StoreID = strings.TrimSpace(candidate.StoreID)
	candidate.NodeHealth = strings.TrimSpace(candidate.NodeHealth)
	candidate.NodeLifecycle = strings.TrimSpace(candidate.NodeLifecycle)
	candidate.StoreState = strings.TrimSpace(candidate.StoreState)
	return candidate
}

func computeECZoneTargets(req ECStripePlacementRequest, candidates []ECPlacementCandidate) ([]string, map[string]int, error) {
	zones := sortedECZones(candidates)
	if len(zones) == 0 {
		return nil, nil, newECPlacementBlockedError(ECBlockedInsufficientZones, "no eligible zones")
	}
	totalShards := int(req.DataShards + req.CodingShards)
	zoneCap := int(req.CodingShards)
	if ceilDiv(totalShards, len(zones)) > zoneCap {
		return nil, nil, newECPlacementBlockedError(
			ECBlockedZoneCapExceeded,
			fmt.Sprintf("ceil(%d/%d) exceeds zone cap %d", totalShards, len(zones), zoneCap),
		)
	}
	usedZoneCount := totalShards
	if len(zones) < usedZoneCount {
		usedZoneCount = len(zones)
	}
	rotated := rotateStrings(zones, int(ecPlacementSeed(req)%uint64(len(zones))))
	zoneOrder := append([]string(nil), rotated[:usedZoneCount]...)
	base := totalShards / usedZoneCount
	remainder := totalShards % usedZoneCount
	targets := make(map[string]int, usedZoneCount)
	for i, zone := range zoneOrder {
		count := base
		if i < remainder {
			count++
		}
		if count > zoneCap {
			return nil, nil, newECPlacementBlockedError(
				ECBlockedZoneCapExceeded,
				fmt.Sprintf("zone %q target count %d exceeds cap %d", zone, count, zoneCap),
			)
		}
		targets[zone] = count
	}
	return zoneOrder, targets, nil
}

func computeECRoleCounts(req ECStripePlacementRequest, zoneOrder []string, zoneTargets map[string]int) (map[string]int, map[string]int) {
	dataCounts := zeroECCounts(zoneOrder)
	codingCounts := zeroECCounts(zoneOrder)
	if req.CodingShards <= req.DataShards {
		codingCounts = balancedECRoleCounts(int(req.CodingShards), zoneOrder, zoneTargets)
		for _, zone := range zoneOrder {
			dataCounts[zone] = zoneTargets[zone] - codingCounts[zone]
		}
		return dataCounts, codingCounts
	}
	dataCounts = balancedECRoleCounts(int(req.DataShards), zoneOrder, zoneTargets)
	for _, zone := range zoneOrder {
		codingCounts[zone] = zoneTargets[zone] - dataCounts[zone]
	}
	return dataCounts, codingCounts
}

func balancedECRoleCounts(total int, zoneOrder []string, zoneTargets map[string]int) map[string]int {
	counts := zeroECCounts(zoneOrder)
	for i := 0; i < total; i++ {
		selected := ""
		selectedCount := 0
		for _, zone := range zoneOrder {
			if counts[zone] >= zoneTargets[zone] {
				continue
			}
			if selected == "" || counts[zone] < selectedCount {
				selected = zone
				selectedCount = counts[zone]
			}
		}
		if selected == "" {
			return counts
		}
		counts[selected]++
	}
	return counts
}

func buildECStripePlacementPlan(req ECStripePlacementRequest, candidates []ECPlacementCandidate, zoneOrder []string, zoneTargets, dataCounts, codingCounts map[string]int, allowReuse bool, brokenStrictRule string) (ECStripePlacementPlan, error) {
	assignments := make(map[string][]ECPlacementCandidate, len(zoneOrder))
	for _, zone := range zoneOrder {
		selected, err := selectECCandidatesInZone(candidatesForECZone(candidates, zone), zoneTargets[zone], ecPlacementSeed(req)+uint64(len(zone)), allowReuse)
		if err != nil {
			return ECStripePlacementPlan{}, err
		}
		assignments[zone] = selected
	}

	zoneIndexes := make(map[string]int, len(zoneOrder))
	targets := make([]ECShardTarget, 0, int(req.DataShards+req.CodingShards))
	for roleIndex, zone := range expandECZoneSequence(zoneOrder, dataCounts) {
		candidate := assignments[zone][zoneIndexes[zone]]
		zoneIndexes[zone]++
		targets = append(targets, ECShardTarget{
			ShardID:   uint32(roleIndex),
			Role:      ECShardRoleData,
			RoleIndex: uint32(roleIndex),
			Zone:      candidate.Zone,
			NodeID:    candidate.NodeID,
			StoreID:   candidate.StoreID,
		})
	}
	for roleIndex, zone := range expandECZoneSequence(zoneOrder, codingCounts) {
		candidate := assignments[zone][zoneIndexes[zone]]
		zoneIndexes[zone]++
		targets = append(targets, ECShardTarget{
			ShardID:   req.DataShards + uint32(roleIndex),
			Role:      ECShardRoleCoding,
			RoleIndex: uint32(roleIndex),
			Zone:      candidate.Zone,
			NodeID:    candidate.NodeID,
			StoreID:   candidate.StoreID,
		})
	}

	plan := ECStripePlacementPlan{
		VolumeID:         req.VolumeID,
		ProfileID:        req.ProfileID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		TopologyRevision: req.TopologyRevision,
		ShardTargets:     targets,
	}
	plan.TopologyReport = buildECTopologyReport(req, zoneOrder, len(candidates), targets, allowReuse, brokenStrictRule)
	return plan, nil
}

func selectECCandidatesInZone(candidates []ECPlacementCandidate, want int, seed uint64, allowReuse bool) ([]ECPlacementCandidate, error) {
	if want == 0 {
		return nil, nil
	}
	if len(candidates) == 0 {
		return nil, newECPlacementBlockedError(ECBlockedNoEligibleCandidateInZone, "zone has no eligible candidates")
	}
	rotated := rotateECCandidates(candidates, seed)
	if allowReuse {
		out := make([]ECPlacementCandidate, 0, want)
		for i := 0; i < want; i++ {
			out = append(out, rotated[i%len(rotated)])
		}
		return out, nil
	}
	if uniqueECNodeCount(candidates) < want {
		return nil, newECPlacementBlockedError(ECBlockedInsufficientNodesInZone, fmt.Sprintf("unique nodes=%d want=%d", uniqueECNodeCount(candidates), want))
	}
	if uniqueECStoreCount(candidates) < want {
		return nil, newECPlacementBlockedError(ECBlockedStoreSpread, fmt.Sprintf("unique stores=%d want=%d", uniqueECStoreCount(candidates), want))
	}
	selected, ok := selectStrictECCandidates(rotated, want)
	if !ok {
		return nil, newECPlacementBlockedError(ECBlockedStoreSpread, "could not satisfy one-shard-per-node and one-shard-per-store")
	}
	return selected, nil
}

func selectStrictECCandidates(candidates []ECPlacementCandidate, want int) ([]ECPlacementCandidate, bool) {
	selected := make([]ECPlacementCandidate, 0, want)
	usedNodes := make(map[string]struct{}, want)
	usedStores := make(map[string]struct{}, want)
	var search func(start int) bool
	search = func(start int) bool {
		if len(selected) == want {
			return true
		}
		remainingSlots := want - len(selected)
		if len(candidates)-start < remainingSlots {
			return false
		}
		for i := start; i < len(candidates); i++ {
			candidate := candidates[i]
			if _, ok := usedNodes[candidate.NodeID]; ok {
				continue
			}
			if _, ok := usedStores[candidate.StoreID]; ok {
				continue
			}
			usedNodes[candidate.NodeID] = struct{}{}
			usedStores[candidate.StoreID] = struct{}{}
			selected = append(selected, candidate)
			if search(i + 1) {
				return true
			}
			selected = selected[:len(selected)-1]
			delete(usedStores, candidate.StoreID)
			delete(usedNodes, candidate.NodeID)
		}
		return false
	}
	if !search(0) {
		return nil, false
	}
	return selected, true
}

func buildECTopologyReport(req ECStripePlacementRequest, zoneOrder []string, eligibleCandidateCount int, targets []ECShardTarget, weak bool, brokenStrictRule string) ECTopologyReport {
	report := ECTopologyReport{
		ZoneShardCounts:        make(map[string]uint32),
		DataZoneCounts:         make(map[string]uint32),
		CodingZoneCounts:       make(map[string]uint32),
		NodeShardCounts:        make(map[string]uint32),
		DataNodeCounts:         make(map[string]uint32),
		CodingNodeCounts:       make(map[string]uint32),
		StoreShardCounts:       make(map[string]uint32),
		WeakPlacement:          weak,
		BrokenStrictRule:       brokenStrictRule,
		EligibleZoneCount:      uint32(len(zoneOrder)),
		EligibleCandidateCount: uint32(eligibleCandidateCount),
	}
	for _, target := range targets {
		report.ZoneShardCounts[target.Zone]++
		report.NodeShardCounts[target.NodeID]++
		report.StoreShardCounts[target.StoreID]++
		switch target.Role {
		case ECShardRoleData:
			report.DataZoneCounts[target.Zone]++
			report.DataNodeCounts[target.NodeID]++
		case ECShardRoleCoding:
			report.CodingZoneCounts[target.Zone]++
			report.CodingNodeCounts[target.NodeID]++
		}
	}
	report.MaxShardsPerZone = maxECCount(report.ZoneShardCounts)
	report.MaxShardsPerNode = maxECCount(report.NodeShardCounts)
	report.MaxShardsPerStore = maxECCount(report.StoreShardCounts)
	report.ZoneToleranceOK = report.MaxShardsPerZone <= req.CodingShards
	report.NodeSpreadOK = report.MaxShardsPerNode <= 1
	report.StoreSpreadOK = report.MaxShardsPerStore <= 1
	report.PlacementSkew = ecCountSkew(report.ZoneShardCounts, zoneOrder)
	report.DataRoleBalanceOK = ecCountSkew(report.DataZoneCounts, zoneOrder) <= 1
	report.CodingRoleBalanceOK = ecCountSkew(report.CodingZoneCounts, zoneOrder) <= 1
	return report
}

func blockedECStripePlacementPlan(req ECStripePlacementRequest, report ECTopologyReport, reason string) ECStripePlacementPlan {
	report.BlockedReason = reason
	return ECStripePlacementPlan{
		VolumeID:         req.VolumeID,
		ProfileID:        req.ProfileID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		TopologyRevision: req.TopologyRevision,
		TopologyReport:   report,
		BlockedReason:    reason,
	}
}

func ecWeakPlacementCanOverride(reason string) bool {
	switch reason {
	case ECBlockedInsufficientNodesInZone, ECBlockedStoreSpread:
		return true
	default:
		return false
	}
}

func newECPlacementBlockedError(reason, message string) error {
	return &ecPlacementBlockedError{reason: reason, message: message}
}

func ecBlockedReason(err error) string {
	var blocked *ecPlacementBlockedError
	if errors.As(err, &blocked) {
		return blocked.reason
	}
	if err == nil {
		return ""
	}
	return ECBlockedInvalidRequest
}

func sortedECZones(candidates []ECPlacementCandidate) []string {
	seen := make(map[string]struct{})
	for _, candidate := range candidates {
		if candidate.Zone == "" {
			continue
		}
		seen[candidate.Zone] = struct{}{}
	}
	zones := make([]string, 0, len(seen))
	for zone := range seen {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	return zones
}

func candidatesForECZone(candidates []ECPlacementCandidate, zone string) []ECPlacementCandidate {
	out := make([]ECPlacementCandidate, 0)
	for _, candidate := range candidates {
		if candidate.Zone == zone {
			out = append(out, candidate)
		}
	}
	return out
}

func sortECCandidates(candidates []ECPlacementCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Zone != candidates[j].Zone {
			return candidates[i].Zone < candidates[j].Zone
		}
		if candidates[i].NodeID != candidates[j].NodeID {
			return candidates[i].NodeID < candidates[j].NodeID
		}
		return candidates[i].StoreID < candidates[j].StoreID
	})
}

func rotateECCandidates(candidates []ECPlacementCandidate, seed uint64) []ECPlacementCandidate {
	if len(candidates) == 0 {
		return nil
	}
	rotation := int(seed % uint64(len(candidates)))
	out := make([]ECPlacementCandidate, 0, len(candidates))
	for i := 0; i < len(candidates); i++ {
		out = append(out, candidates[(rotation+i)%len(candidates)])
	}
	return out
}

func rotateStrings(in []string, rotation int) []string {
	if len(in) == 0 {
		return nil
	}
	rotation %= len(in)
	out := make([]string, 0, len(in))
	for i := 0; i < len(in); i++ {
		out = append(out, in[(rotation+i)%len(in)])
	}
	return out
}

func zeroECCounts(zones []string) map[string]int {
	out := make(map[string]int, len(zones))
	for _, zone := range zones {
		out[zone] = 0
	}
	return out
}

func expandECZoneSequence(zoneOrder []string, counts map[string]int) []string {
	maxCount := 0
	total := 0
	for _, zone := range zoneOrder {
		if counts[zone] > maxCount {
			maxCount = counts[zone]
		}
		total += counts[zone]
	}
	out := make([]string, 0, total)
	for round := 0; round < maxCount; round++ {
		for _, zone := range zoneOrder {
			if counts[zone] > round {
				out = append(out, zone)
			}
		}
	}
	return out
}

func uniqueECNodeCount(candidates []ECPlacementCandidate) int {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		seen[candidate.NodeID] = struct{}{}
	}
	return len(seen)
}

func uniqueECStoreCount(candidates []ECPlacementCandidate) int {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		seen[candidate.StoreID] = struct{}{}
	}
	return len(seen)
}

func maxECCount(counts map[string]uint32) uint32 {
	var max uint32
	for _, count := range counts {
		if count > max {
			max = count
		}
	}
	return max
}

func ecCountSkew(counts map[string]uint32, zones []string) uint32 {
	if len(zones) == 0 {
		return 0
	}
	var min uint32
	var max uint32
	for i, zone := range zones {
		count := counts[zone]
		if i == 0 || count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}
	return max - min
}

func ecCandidateKey(candidate ECPlacementCandidate) string {
	return candidate.Zone + "\x00" + candidate.NodeID + "\x00" + candidate.StoreID
}

func ecPlacementSeed(req ECStripePlacementRequest) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(req.VolumeID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(req.ProfileID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprintf("%d", req.StripeID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprintf("%d", req.TopologyRevision)))
	return h.Sum64()
}

func ceilDiv(n, d int) int {
	if d == 0 {
		return 0
	}
	return (n + d - 1) / d
}
