package service

import (
	"reflect"
	"sort"
	"time"
)

const (
	runtimePathPlanReapplyEscalationDelay    = 15 * time.Second
	runtimePathExpansionStabilizationDelay   = 10 * time.Second
	handoffGenerationRotationEscalationDelay = 15 * time.Second
	runtimePathReductionHoldThreshold        = 2
)

func nextRuntimeExpansionDelay(status VolumeStatusRecord) int64 {
	level := status.RuntimePathExpansionBackoffLevel
	if level > 3 {
		level = 3
	}
	return int64(runtimePathExpansionStabilizationDelay/time.Second) * int64(1<<level)
}

func GatewayObservedForPathPlan(state GatewayConnectionState) bool {
	switch state {
	case GatewayStateUp, GatewayStateDegraded, GatewayStateUnknown:
		return true
	default:
		return false
	}
}

func GatewayDesiredForPathPlan(state GatewayConnectionState) bool {
	return state == GatewayStateUp
}

func ComputeObservedActiveGatewaySet(gateways []GatewayRecord) []string {
	out := make([]string, 0, len(gateways))
	for _, gateway := range gateways {
		if gateway.GatewayID == "" || !GatewayObservedForPathPlan(gateway.ConnectionState) {
			continue
		}
		out = append(out, gateway.GatewayID)
	}
	sort.Strings(out)
	return out
}

func ComputeDesiredActiveGatewaySet(status VolumeStatusRecord, gateways []GatewayRecord, observed []string) []string {
	out := make([]string, 0, len(gateways))
	for _, gateway := range gateways {
		if gateway.GatewayID == "" || !GatewayDesiredForPathPlan(gateway.ConnectionState) {
			continue
		}
		out = append(out, gateway.GatewayID)
	}
	sort.Strings(out)
	if len(out) == 0 {
		fallback := append([]string(nil), observed...)
		if shouldPreferFewerDesiredGateways(status, gateways) && len(fallback) > 1 {
			return reduceDesiredGatewaySet(status, gateways, fallback)
		}
		return fallback
	}
	if shouldPreferFewerDesiredGateways(status, gateways) && len(out) > 1 {
		return reduceDesiredGatewaySet(status, gateways, out)
	}
	return out
}

func reduceDesiredGatewaySet(status VolumeStatusRecord, gateways []GatewayRecord, candidates []string) []string {
	if len(candidates) <= 1 {
		return candidates
	}
	if status.InUse &&
		status.CurrentGatewayID != "" &&
		containsGatewayID(candidates, status.CurrentGatewayID) &&
		gatewayDesiredByID(gateways, status.CurrentGatewayID) &&
		!shouldForceCurrentGatewayHandoff(status) {
		return []string{status.CurrentGatewayID}
	}
	if status.CurrentGatewayID != "" && containsGatewayID(candidates, status.CurrentGatewayID) && shouldForceCurrentGatewayHandoff(status) {
		return []string{selectForcedHandoffGateway(status, candidates)}
	}
	return candidates[:1]
}

func selectForcedHandoffGateway(status VolumeStatusRecord, candidates []string) string {
	alternatives := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate != status.CurrentGatewayID {
			alternatives = append(alternatives, candidate)
		}
	}
	if len(alternatives) == 0 {
		return candidates[0]
	}
	if shouldEscalatePendingGenerationRotation(status) &&
		len(status.DesiredActiveGatewaySet) == 1 &&
		status.DesiredActiveGatewaySet[0] != status.CurrentGatewayID &&
		containsGatewayID(alternatives, status.DesiredActiveGatewaySet[0]) {
		if !shouldAdvancePendingGenerationRotationEscalation(status) {
			return status.DesiredActiveGatewaySet[0]
		}
		if len(alternatives) == 1 {
			return alternatives[0]
		}
		for idx, candidate := range alternatives {
			if candidate == status.DesiredActiveGatewaySet[0] {
				return alternatives[(idx+1)%len(alternatives)]
			}
		}
	}
	return alternatives[0]
}

func gatewayDesiredByID(gateways []GatewayRecord, gatewayID string) bool {
	for _, gateway := range gateways {
		if gateway.GatewayID != gatewayID {
			continue
		}
		return GatewayDesiredForPathPlan(gateway.ConnectionState)
	}
	return false
}

func runtimePathPlanConverged(status VolumeStatusRecord) bool {
	return status.PathPlanRevision > 0 && status.RuntimeAppliedPathPlanRevision >= status.PathPlanRevision
}

func runtimePathExpansionEligible(status VolumeStatusRecord) bool {
	return status.RuntimePathExpansionEligibleAtUnix > 0 && status.RuntimePathExpansionEligibleAtUnix <= time.Now().Unix()
}

func ControllerPathPlanRuntimeExpansionEligible(status VolumeStatusRecord) bool {
	return runtimePathExpansionEligible(status)
}

func desiredGatewayCount(gateways []GatewayRecord) int {
	count := 0
	for _, gateway := range gateways {
		if GatewayDesiredForPathPlan(gateway.ConnectionState) {
			count++
		}
	}
	return count
}

func shouldDelayRuntimeDesiredReExpansion(status VolumeStatusRecord, gateways []GatewayRecord) bool {
	if status.RuntimePathNeedsAttention || !runtimePathPlanConverged(status) {
		return false
	}
	if len(status.DesiredActiveGatewaySet) != 1 || desiredGatewayCount(gateways) <= 1 {
		return false
	}
	return status.RuntimePathFeedbackCount > 0 || status.RuntimePathReductionHoldUntilUnix > 0
}

func shouldPreferFewerDesiredGateways(status VolumeStatusRecord, gateways []GatewayRecord) bool {
	if shouldEscalateRuntimePathPlanReapply(status) {
		return true
	}
	if containsGatewayID(status.RuntimePathRecommendedActions, "prefer_fewer_active_paths") {
		return true
	}
	if status.RuntimePathExpansionEligibleAtUnix > time.Now().Unix() {
		return true
	}
	if status.RuntimePathReductionHoldUntilUnix > time.Now().Unix() {
		return true
	}
	for _, gateway := range gateways {
		switch gateway.ConnectionState {
		case GatewayStateDegraded, GatewayStateUnknown:
			return true
		}
	}
	return false
}

func shouldPreferAggressiveRuntimeHandoff(status VolumeStatusRecord) bool {
	if status.RuntimePathReductionHoldUntilUnix <= time.Now().Unix() {
		return false
	}
	return containsGatewayID(status.RuntimePathAttentionReasons, "lane_unavailable") ||
		containsGatewayID(status.RuntimePathAttentionReasons, "lane_down_preferred")
}

func shouldForceCurrentGatewayHandoff(status VolumeStatusRecord) bool {
	return shouldPreferAggressiveRuntimeHandoff(status) ||
		shouldEscalateRuntimePathPlanReapply(status) ||
		shouldEscalatePendingGenerationRotation(status)
}

func handoffReasonForStatus(status VolumeStatusRecord) string {
	if shouldEscalatePendingGenerationRotation(status) {
		return "handoff_generation_rotation_stalled_current_gateway"
	}
	if shouldEscalateRuntimePathPlanReapply(status) {
		return "runtime_path_plan_reapply_stalled_current_gateway"
	}
	if shouldPreferAggressiveRuntimeHandoff(status) {
		return "runtime_hold_borderline_current_gateway"
	}
	return "current_gateway_not_desired"
}

func pathPlanReapplyReason(status VolumeStatusRecord) string {
	for _, reason := range []string{
		"lane_unavailable",
		"lane_down_preferred",
		"lane_degraded_without_up_fallback",
	} {
		if containsGatewayID(status.RuntimePathAttentionReasons, reason) {
			return "runtime_" + reason
		}
	}
	if containsGatewayID(status.RuntimePathRecommendedActions, "reapply_latest_path_plan") {
		return "runtime_reapply_latest_path_plan"
	}
	if containsGatewayID(status.RuntimePathRecommendedActions, "refresh_gateway_path_plan") {
		return "runtime_refresh_gateway_path_plan"
	}
	return ""
}

func shouldRequestPathPlanReapply(status VolumeStatusRecord) (bool, string) {
	if runtimePathPlanConverged(status) {
		return false, ""
	}
	if status.RuntimePathNeedsAttention &&
		(containsGatewayID(status.RuntimePathRecommendedActions, "refresh_gateway_path_plan") ||
			containsGatewayID(status.RuntimePathRecommendedActions, "reapply_latest_path_plan")) {
		return true, pathPlanReapplyReason(status)
	}
	if status.PathPlanReapplyRequested && status.RuntimePathReductionHoldUntilUnix > time.Now().Unix() {
		reason := status.PathPlanReapplyReason
		if reason == "" {
			reason = pathPlanReapplyReason(status)
		}
		return true, reason
	}
	return false, ""
}

func shouldEscalateRuntimePathPlanReapply(status VolumeStatusRecord) bool {
	if !status.InUse || !status.PathPlanReapplyRequested || !status.RuntimePathNeedsAttention {
		return false
	}
	if runtimePathPlanConverged(status) || status.PathPlanReapplyRequestedAtUnix == 0 {
		return false
	}
	if time.Now().Unix()-status.PathPlanReapplyRequestedAtUnix < int64(runtimePathPlanReapplyEscalationDelay/time.Second) {
		return false
	}
	return containsGatewayID(status.RuntimePathAttentionReasons, "lane_unavailable") ||
		containsGatewayID(status.RuntimePathAttentionReasons, "lane_down_preferred")
}

func shouldEscalatePendingGenerationRotation(status VolumeStatusRecord) bool {
	if !status.InUse || !status.HandoffRequired {
		return false
	}
	if handoffStage(status) != "pending_generation_rotation" || status.HandoffRequestedAtUnix == 0 {
		return false
	}
	return time.Now().Unix()-status.HandoffRequestedAtUnix >= int64(handoffGenerationRotationEscalationDelay/time.Second)
}

func shouldAdvancePendingGenerationRotationEscalation(status VolumeStatusRecord) bool {
	if !shouldEscalatePendingGenerationRotation(status) {
		return false
	}
	return status.HandoffNextEscalationAtUnix == 0 || time.Now().Unix() >= status.HandoffNextEscalationAtUnix
}

func handoffBackoffState(status VolumeStatusRecord) string {
	if status.HandoffReason != "handoff_generation_rotation_stalled_current_gateway" {
		return ""
	}
	if status.HandoffNextEscalationAtUnix == 0 || time.Now().Unix() >= status.HandoffNextEscalationAtUnix {
		return "eligible"
	}
	return "waiting"
}

func nextHandoffEscalationDelay(count uint64) int64 {
	if count > 3 {
		count = 3
	}
	return int64(handoffGenerationRotationEscalationDelay/time.Second) * int64(1<<count)
}

func EqualGatewayIDSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsGatewayID(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func pathPlanAttention(gateways []GatewayRecord, observed, desired []string) (bool, []string, []string) {
	reasons := make([]string, 0, 3)
	actions := make([]string, 0, 4)
	if len(observed) == 0 {
		reasons = append(reasons, "no_observed_gateway")
		actions = append(actions, "refresh_gateway_path_plan", "reopen_or_reapply_path_plan")
	}
	if len(desired) == 0 && len(observed) > 0 {
		reasons = append(reasons, "no_desired_gateway")
		actions = append(actions, "refresh_gateway_path_plan", "prefer_fewer_active_paths")
	}
	for _, gateway := range gateways {
		if gateway.GatewayID == "" {
			continue
		}
		switch gateway.ConnectionState {
		case GatewayStateDegraded:
			reasons = append(reasons, "observed_degraded_gateway")
			actions = append(actions, "refresh_gateway_path_plan", "prefer_fewer_active_paths")
		case GatewayStateUnknown:
			reasons = append(reasons, "observed_unknown_gateway")
			actions = append(actions, "refresh_gateway_path_plan")
		}
	}
	reasons = dedupeStrings(reasons)
	actions = dedupeStrings(actions)
	return len(reasons) > 0, reasons, actions
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func ControllerPathPlanNeedsAttention(status VolumeStatusRecord) bool {
	return status.PathPlanNeedsAttention || status.RuntimePathNeedsAttention
}

func ControllerPathPlanAttentionReasons(status VolumeStatusRecord) []string {
	return dedupeStrings(append(append([]string(nil), status.PathPlanAttentionReasons...), status.RuntimePathAttentionReasons...))
}

func handoffRecommendedActions(status VolumeStatusRecord) []string {
	if !status.HandoffRequired {
		return nil
	}
	actions := []string{"complete_gateway_handoff"}
	switch status.HandoffReason {
	case "runtime_hold_borderline_current_gateway",
		"runtime_path_plan_reapply_stalled_current_gateway":
		actions = append(actions, "complete_gateway_handoff_aggressively", "prefer_fewer_active_paths")
	case "handoff_generation_rotation_stalled_current_gateway":
		actions = append(actions, "prefer_fewer_active_paths")
		if handoffBackoffState(status) == "eligible" {
			actions = append(actions, "complete_gateway_handoff_aggressively")
		}
	}
	return actions
}

func ControllerPathPlanRecommendedActions(status VolumeStatusRecord) []string {
	actions := append(append([]string(nil), status.PathPlanRecommendedActions...), status.RuntimePathRecommendedActions...)
	actions = append(actions, handoffRecommendedActions(status)...)
	actions = append(actions, runtimeNoPathRecommendedActions(status)...)
	if runtimePathExpansionEligible(status) {
		actions = append(actions, "refresh_gateway_path_plan")
	}
	return dedupeStrings(actions)
}

func OperatorPathPlanRecommendedActions(status VolumeStatusRecord) []string {
	return ControllerPathPlanRecommendedActions(status)
}

func runtimeNoPathRecommendedActions(status VolumeStatusRecord) []string {
	switch status.RuntimeNoPathState {
	case "queueing":
		actions := []string{"restore_gateway_path", "start_replacement_gateway", "check_gateway_registry"}
		if status.RuntimeNoPathRetryMode == "queue" || status.RuntimeNoPathRetryMode == "timed" {
			actions = append(actions, "disable_no_path_queueing_if_unwanted")
		}
		return actions
	case "failing":
		return []string{"restore_gateway_path", "check_gateway_registry"}
	default:
		return nil
	}
}

func RuntimeNoPathSummary(status VolumeStatusRecord) map[string]any {
	return map[string]any{
		"state":              status.RuntimeNoPathState,
		"retry_mode":         status.RuntimeNoPathRetryMode,
		"retry_seconds":      status.RuntimeNoPathRetrySeconds,
		"queued_reqs":        status.RuntimeNoPathQueuedReqs,
		"requeued_reqs":      status.RuntimeNoPathRequeuedReqs,
		"failed_reqs":        status.RuntimeNoPathFailedReqs,
		"recovered_reqs":     status.RuntimeNoPathRecoveredReqs,
		"enter_count":        status.RuntimeNoPathEnterCount,
		"last_reason":        status.RuntimeNoPathLastReason,
		"last_feedback_unix": status.RuntimeNoPathLastFeedbackUnix,
	}
}

func ClusterPriorityRecommendedActions(currentClass, topClass string) []string {
	if topClass == "" || currentClass == topClass {
		return nil
	}
	switch topClass {
	case "aggressive_handoff":
		return []string{"complete_gateway_handoff_aggressively"}
	case "handoff":
		return []string{"complete_gateway_handoff"}
	case "expansion_ready", "refresh", "attention":
		return []string{"refresh_gateway_path_plan"}
	default:
		return nil
	}
}

func OperatorPathPlanRecommendedActionsWithCluster(status VolumeStatusRecord, topClass string) []string {
	actions := OperatorPathPlanRecommendedActions(status)
	return dedupeStrings(append(actions, ClusterPriorityRecommendedActions(OperatorPathPlanPriorityClass(status), topClass)...))
}

func operatorPathPlanPriorityClass(status VolumeStatusRecord, actions []string) string {
	switch {
	case containsGatewayID(actions, "complete_gateway_handoff_aggressively"):
		return "aggressive_handoff"
	case containsGatewayID(actions, "complete_gateway_handoff"):
		return "handoff"
	case ControllerPathPlanRuntimeExpansionEligible(status):
		return "expansion_ready"
	case containsGatewayID(actions, "refresh_gateway_path_plan") || containsGatewayID(actions, "reapply_latest_path_plan"):
		return "refresh"
	case ControllerPathPlanNeedsAttention(status):
		return "attention"
	default:
		return "normal"
	}
}

func OperatorPathPlanPriorityClass(status VolumeStatusRecord) string {
	return operatorPathPlanPriorityClass(status, OperatorPathPlanRecommendedActions(status))
}

func OperatorPathPlanPriorityClassWithCluster(status VolumeStatusRecord, topClass string) string {
	return operatorPathPlanPriorityClass(status, OperatorPathPlanRecommendedActionsWithCluster(status, topClass))
}

func reconcileRequestForStatus(status VolumeStatusRecord) (int64, string) {
	nowUnix := time.Now().Unix()
	switch {
	case runtimePathExpansionEligible(status):
		return nowUnix, "runtime_expansion_eligible"
	case shouldEscalatePendingGenerationRotation(status) && handoffBackoffState(status) == "eligible":
		return nowUnix, "handoff_generation_rotation_backoff_eligible"
	case shouldEscalateRuntimePathPlanReapply(status):
		return nowUnix, "runtime_path_plan_reapply_stalled"
	default:
		return 0, ""
	}
}

func reconcileScheduleForStatus(status VolumeStatusRecord) (int64, string) {
	nowUnix := time.Now().Unix()
	if status.HandoffRequired &&
		handoffStage(status) == "ready_to_complete" &&
		status.HandoffCompletionEligibleAtUnix > nowUnix {
		return status.HandoffCompletionEligibleAtUnix, "handoff_completion_ready"
	}
	if status.RuntimePathExpansionEligibleAtUnix > nowUnix && !status.RuntimePathNeedsAttention && runtimePathPlanConverged(status) {
		return status.RuntimePathExpansionEligibleAtUnix, "runtime_expansion_eligible"
	}
	if status.HandoffReason == "handoff_generation_rotation_stalled_current_gateway" &&
		status.HandoffNextEscalationAtUnix > nowUnix &&
		handoffBackoffState(status) == "waiting" {
		return status.HandoffNextEscalationAtUnix, "handoff_generation_rotation_backoff_eligible"
	}
	if status.PathPlanReapplyRequested &&
		status.PathPlanReapplyRequestedAtUnix > 0 &&
		status.RuntimePathNeedsAttention &&
		!runtimePathPlanConverged(status) {
		nextAt := status.PathPlanReapplyRequestedAtUnix + int64(runtimePathPlanReapplyEscalationDelay/time.Second)
		if nextAt > nowUnix {
			return nextAt, "runtime_path_plan_reapply_stalled"
		}
	}
	return 0, ""
}

func ReconcileVolumePathPlanStatus(status VolumeStatusRecord, gateways []GatewayRecord) (VolumeStatusRecord, bool, bool) {
	original := status
	nowUnix := time.Now().Unix()
	if shouldDelayRuntimeDesiredReExpansion(status, gateways) {
		if status.RuntimePathExpansionEligibleAtUnix == 0 {
			status.RuntimePathExpansionEligibleAtUnix = nowUnix + nextRuntimeExpansionDelay(status)
		}
	} else if status.RuntimePathNeedsAttention || !runtimePathPlanConverged(status) {
		status.RuntimePathExpansionEligibleAtUnix = 0
	}
	if runtimePathPlanConverged(status) && !status.RuntimePathNeedsAttention && status.RuntimePathExpansionEligibleAtUnix > 0 && status.RuntimePathExpansionEligibleAtUnix <= nowUnix {
		status.RuntimePathReductionHoldUntilUnix = 0
		status.RuntimePathFeedbackCount = 0
		status.RuntimePathExpansionEligibleAtUnix = 0
		if status.RuntimePathExpansionBackoffLevel > 0 {
			status.RuntimePathExpansionBackoffLevel--
		}
	}
	prevReapplyRequested := status.PathPlanReapplyRequested
	prevReapplyReason := status.PathPlanReapplyReason
	observed := ComputeObservedActiveGatewaySet(gateways)
	desired := ComputeDesiredActiveGatewaySet(status, gateways, observed)
	needsAttention, attentionReasons, recommendedActions := pathPlanAttention(gateways, observed, desired)
	reapplyRequested, reapplyReason := shouldRequestPathPlanReapply(status)
	if reapplyRequested {
		needsAttention = true
		attentionReasons = append(attentionReasons, "runtime_path_plan_reapply_requested")
		recommendedActions = append(recommendedActions, "reapply_latest_path_plan")
	}
	handoffRequired := status.InUse && status.CurrentGatewayID != "" && len(desired) > 0 && !containsGatewayID(desired, status.CurrentGatewayID)
	handoffReason := ""
	handoffTargets := []string(nil)
	pendingGenerationRotationEscalated := shouldEscalatePendingGenerationRotation(status)
	pendingGenerationRotationAdvance := false
	if status.HandoffRequired && !handoffCompletionSatisfied(status) && handoffStage(status) != "" {
		handoffRequired = true
	}
	if handoffCompletionSatisfied(status) {
		handoffRequired = false
	}
	if handoffRequired {
		handoffReason = handoffReasonForStatus(status)
		pendingGenerationRotationAdvance = handoffReason == "handoff_generation_rotation_stalled_current_gateway" && shouldAdvancePendingGenerationRotationEscalation(status)
		handoffTargets = append([]string(nil), desired...)
		stageStatus := status
		stageStatus.HandoffRequired = true
		stageStatus.HandoffTargetGatewaySet = handoffTargets
		stageStatus.HandoffReason = handoffReason
		needsAttention = true
		attentionReasons = append(attentionReasons, "handoff_required")
		recommendedActions = append(recommendedActions, "complete_gateway_handoff", "prefer_fewer_active_paths")
		if handoffReason == "runtime_hold_borderline_current_gateway" ||
			handoffReason == "runtime_path_plan_reapply_stalled_current_gateway" ||
			(handoffReason == "handoff_generation_rotation_stalled_current_gateway" && pendingGenerationRotationAdvance) {
			recommendedActions = append(recommendedActions, "complete_gateway_handoff_aggressively")
		}
	}
	if pendingGenerationRotationEscalated {
		needsAttention = true
		attentionReasons = append(attentionReasons, "handoff_generation_rotation_stalled")
		recommendedActions = append(recommendedActions, "complete_gateway_handoff", "prefer_fewer_active_paths")
		if pendingGenerationRotationAdvance {
			recommendedActions = append(recommendedActions, "complete_gateway_handoff_aggressively")
		}
	}
	if shouldEscalateRuntimePathPlanReapply(status) {
		needsAttention = true
		attentionReasons = append(attentionReasons, "runtime_path_plan_reapply_stalled")
		recommendedActions = append(recommendedActions, "complete_gateway_handoff", "complete_gateway_handoff_aggressively", "prefer_fewer_active_paths")
	}
	attentionReasons = dedupeStrings(attentionReasons)
	recommendedActions = dedupeStrings(recommendedActions)
	observedChanged := !EqualGatewayIDSet(status.ObservedActiveGatewaySet, observed)
	desiredChanged := !EqualGatewayIDSet(status.DesiredActiveGatewaySet, desired)
	prevHandoffRequired := status.HandoffRequired
	prevHandoffReason := status.HandoffReason
	prevHandoffRequestedAtUnix := status.HandoffRequestedAtUnix
	prevHandoffEscalationCount := status.HandoffEscalationCount
	prevHandoffNextEscalationAtUnix := status.HandoffNextEscalationAtUnix
	advanceGenerationRotationEscalation := pendingGenerationRotationAdvance
	status.ObservedActiveGatewaySet = observed
	status.DesiredActiveGatewaySet = desired
	status.PathPlanNeedsAttention = needsAttention
	status.PathPlanAttentionReasons = attentionReasons
	status.PathPlanRecommendedActions = recommendedActions
	if reapplyRequested {
		if !prevReapplyRequested || prevReapplyReason != reapplyReason {
			status.PathPlanReapplyRequestedAtUnix = time.Now().Unix()
			if !desiredChanged {
				status.PathPlanRevision++
				if status.PathPlanRevision == 0 {
					status.PathPlanRevision = 1
				}
			}
		}
	} else {
		status.PathPlanReapplyRequestedAtUnix = 0
	}
	status.PathPlanReapplyRequested = reapplyRequested
	status.PathPlanReapplyReason = reapplyReason
	if handoffRequired {
		if !prevHandoffRequired || !EqualGatewayIDSet(status.HandoffTargetGatewaySet, handoffTargets) {
			status.HandoffRequestedAtUnix = nowUnix
			status.HandoffEscalationCount = 0
			status.HandoffNextEscalationAtUnix = 0
		} else if prevHandoffRequestedAtUnix != 0 {
			status.HandoffRequestedAtUnix = prevHandoffRequestedAtUnix
			status.HandoffEscalationCount = prevHandoffEscalationCount
			status.HandoffNextEscalationAtUnix = prevHandoffNextEscalationAtUnix
		}
	} else {
		status.HandoffRequestedAtUnix = 0
		status.HandoffAckedAtUnix = 0
		status.HandoffAckedGeneration = 0
		status.HandoffCompletionEligibleAtUnix = 0
		status.HandoffEscalationCount = 0
		status.HandoffNextEscalationAtUnix = 0
	}
	if advanceGenerationRotationEscalation {
		status.HandoffEscalationCount = prevHandoffEscalationCount + 1
		status.HandoffNextEscalationAtUnix = nowUnix + nextHandoffEscalationDelay(status.HandoffEscalationCount)
	}
	if handoffRequired && (!prevHandoffRequired || !EqualGatewayIDSet(status.HandoffTargetGatewaySet, handoffTargets) || prevHandoffReason != handoffReason || advanceGenerationRotationEscalation) {
		status.WriterFencingEpoch++
		if status.WriterFencingEpoch == 0 {
			status.WriterFencingEpoch = 1
		}
	}
	status.HandoffRequired = handoffRequired
	status.HandoffReason = handoffReason
	status.HandoffTargetGatewaySet = handoffTargets
	if desiredChanged {
		status.PathPlanRevision++
		if status.PathPlanRevision == 0 {
			status.PathPlanRevision = 1
		}
	}
	if status.RuntimePathNeedsAttention && containsGatewayID(status.RuntimePathRecommendedActions, "prefer_fewer_active_paths") {
		level := uint32(status.RuntimePathFeedbackCount / uint64(runtimePathReductionHoldThreshold))
		if level > 3 {
			level = 3
		}
		if level > status.RuntimePathExpansionBackoffLevel {
			status.RuntimePathExpansionBackoffLevel = level
		}
	}
	if handoffRequired {
		status.HandoffStage = handoffStage(status)
	} else {
		status.HandoffStage = ""
	}
	updateHandoffCompletionEligibility(&status, nowUnix)
	status.ControllerReconcileRequestedAtUnix, status.ControllerReconcileReason = reconcileRequestForStatus(status)
	status.ControllerReconcileScheduledAtUnix, status.ControllerReconcileScheduledReason = reconcileScheduleForStatus(status)
	if reflect.DeepEqual(status, original) {
		return status, false, false
	}
	return status, observedChanged, desiredChanged
}
