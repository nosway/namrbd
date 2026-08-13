package service

import (
	"testing"
	"time"
)

func TestReconcileVolumePathPlanStatusPrefersUpForDesiredSet(t *testing.T) {
	status, observedChanged, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
		{GatewayID: "gw-c", ConnectionState: GatewayStateDown},
	})
	if !observedChanged || !desiredChanged {
		t.Fatalf("expected changes: observed=%t desired=%t", observedChanged, desiredChanged)
	}
	if status.PathPlanRevision != 1 {
		t.Fatalf("unexpected revision: %+v", status)
	}
	if len(status.ObservedActiveGatewaySet) != 2 || status.ObservedActiveGatewaySet[0] != "gw-a" || status.ObservedActiveGatewaySet[1] != "gw-b" {
		t.Fatalf("unexpected observed set: %+v", status.ObservedActiveGatewaySet)
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("unexpected desired set: %+v", status.DesiredActiveGatewaySet)
	}
	if !status.PathPlanNeedsAttention {
		t.Fatalf("expected attention for degraded observed gateway: %+v", status)
	}
	if len(status.PathPlanRecommendedActions) == 0 || status.PathPlanRecommendedActions[0] != "refresh_gateway_path_plan" {
		t.Fatalf("unexpected recommended actions: %+v", status.PathPlanRecommendedActions)
	}
}

func TestReconcileVolumePathPlanStatusFallsBackToObservedWhenNoUpGateway(t *testing.T) {
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{}, []GatewayRecord{
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
		{GatewayID: "gw-c", ConnectionState: GatewayStateUnknown},
	})
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("unexpected desired fallback set: %+v", status.DesiredActiveGatewaySet)
	}
}

func TestReconcileVolumePathPlanStatusDoesNotBumpRevisionWhenOnlyObservedChanges(t *testing.T) {
	initial := VolumeStatusRecord{
		DesiredActiveGatewaySet:  []string{"gw-a"},
		ObservedActiveGatewaySet: []string{"gw-a"},
		PathPlanRevision:         7,
	}
	status, observedChanged, desiredChanged := ReconcileVolumePathPlanStatus(initial, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
	})
	if !observedChanged || desiredChanged {
		t.Fatalf("unexpected change flags: observed=%t desired=%t", observedChanged, desiredChanged)
	}
	if status.PathPlanRevision != 7 {
		t.Fatalf("revision should stay stable when desired set is unchanged: %+v", status)
	}
	if len(status.ObservedActiveGatewaySet) != 2 {
		t.Fatalf("unexpected observed set: %+v", status.ObservedActiveGatewaySet)
	}
}

func TestReconcileVolumePathPlanStatusBumpsRevisionForRuntimeReapplyWhenDesiredStable(t *testing.T) {
	initial := VolumeStatusRecord{
		DesiredActiveGatewaySet:       []string{"gw-a", "gw-b"},
		ObservedActiveGatewaySet:      []string{"gw-a", "gw-b"},
		PathPlanRevision:              7,
		RuntimePathNeedsAttention:     true,
		RuntimePathAttentionReasons:   []string{"lane_unavailable"},
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "reapply_latest_path_plan"},
	}
	status, observedChanged, desiredChanged := ReconcileVolumePathPlanStatus(initial, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if observedChanged || desiredChanged {
		t.Fatalf("unexpected change flags: observed=%t desired=%t", observedChanged, desiredChanged)
	}
	if status.PathPlanRevision != 8 {
		t.Fatalf("expected runtime reapply revision bump: %+v", status)
	}
	if !status.PathPlanReapplyRequested || status.PathPlanReapplyReason != "runtime_lane_unavailable" || status.PathPlanReapplyRequestedAtUnix == 0 {
		t.Fatalf("expected runtime reapply request metadata: %+v", status)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "reapply_latest_path_plan") {
		t.Fatalf("expected reapply action in path-plan recommendations: %+v", status.PathPlanRecommendedActions)
	}
}

func TestReconcileVolumePathPlanStatusDoesNotRepeatedlyBumpRuntimeReapplyRevision(t *testing.T) {
	initial := VolumeStatusRecord{
		DesiredActiveGatewaySet:        []string{"gw-a", "gw-b"},
		ObservedActiveGatewaySet:       []string{"gw-a", "gw-b"},
		PathPlanRevision:               8,
		PathPlanReapplyRequested:       true,
		PathPlanReapplyReason:          "runtime_lane_unavailable",
		PathPlanReapplyRequestedAtUnix: time.Now().Add(-10 * time.Second).Unix(),
		RuntimePathNeedsAttention:      true,
		RuntimePathAttentionReasons:    []string{"lane_unavailable"},
		RuntimePathRecommendedActions:  []string{"refresh_gateway_path_plan", "reapply_latest_path_plan"},
	}
	status, observedChanged, desiredChanged := ReconcileVolumePathPlanStatus(initial, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if observedChanged || desiredChanged {
		t.Fatalf("unexpected change flags: observed=%t desired=%t", observedChanged, desiredChanged)
	}
	if status.PathPlanRevision != 8 {
		t.Fatalf("expected stable revision for repeated runtime reapply pressure: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusClearsRuntimeReapplyRequestAfterConvergence(t *testing.T) {
	initial := VolumeStatusRecord{
		DesiredActiveGatewaySet:          []string{"gw-a", "gw-b"},
		ObservedActiveGatewaySet:         []string{"gw-a", "gw-b"},
		PathPlanRevision:                 8,
		PathPlanReapplyRequested:         true,
		PathPlanReapplyReason:            "runtime_lane_unavailable",
		PathPlanReapplyRequestedAtUnix:   time.Now().Add(-10 * time.Second).Unix(),
		RuntimePathNeedsAttention:        true,
		RuntimePathAttentionReasons:      []string{"lane_unavailable"},
		RuntimePathRecommendedActions:    []string{"refresh_gateway_path_plan", "reapply_latest_path_plan"},
		RuntimeAppliedPathPlanRevision:   8,
		RuntimeAppliedPathReportedAtUnix: time.Now().Unix(),
	}
	status, observedChanged, desiredChanged := ReconcileVolumePathPlanStatus(initial, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if observedChanged || desiredChanged {
		t.Fatalf("unexpected change flags: observed=%t desired=%t", observedChanged, desiredChanged)
	}
	if status.PathPlanReapplyRequested || status.PathPlanReapplyReason != "" || status.PathPlanReapplyRequestedAtUnix != 0 {
		t.Fatalf("expected runtime reapply request to clear after convergence: %+v", status)
	}
	if containsGatewayID(status.PathPlanRecommendedActions, "reapply_latest_path_plan") {
		t.Fatalf("did not expect reapply action after convergence: %+v", status.PathPlanRecommendedActions)
	}
}

func TestReconcileVolumePathPlanStatusClearsRuntimeHoldAfterConvergedApply(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		DesiredActiveGatewaySet:           []string{"gw-a"},
		ObservedActiveGatewaySet:          []string{"gw-a", "gw-b"},
		PathPlanRevision:                  8,
		RuntimePathNeedsAttention:         false,
		RuntimePathFeedbackCount:          2,
		RuntimePathExpansionBackoffLevel:  1,
		RuntimePathReductionHoldUntilUnix: time.Now().Add(30 * time.Second).Unix(),
		RuntimeAppliedPathPlanRevision:    8,
		RuntimeAppliedPathReportedAtUnix:  time.Now().Unix(),
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if desiredChanged {
		t.Fatalf("did not expect immediate desired re-expansion during stabilization window")
	}
	if status.RuntimePathReductionHoldUntilUnix == 0 || status.RuntimePathFeedbackCount == 0 {
		t.Fatalf("expected runtime hold bookkeeping to remain until expansion eligibility: %+v", status)
	}
	if status.RuntimePathExpansionEligibleAtUnix <= time.Now().Unix() {
		t.Fatalf("expected future expansion eligibility after convergence: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("expected desired set to remain shrunk during stabilization window: %+v", status.DesiredActiveGatewaySet)
	}
	if status.ControllerReconcileRequestedAtUnix != 0 || status.ControllerReconcileReason != "" {
		t.Fatalf("did not expect reconcile request before expansion eligibility: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix <= time.Now().Unix() || status.ControllerReconcileScheduledReason != "runtime_expansion_eligible" {
		t.Fatalf("expected future reconcile schedule for runtime expansion eligibility: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusExpandsDesiredSetAfterRuntimeExpansionEligibility(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		DesiredActiveGatewaySet:            []string{"gw-a"},
		ObservedActiveGatewaySet:           []string{"gw-a", "gw-b"},
		PathPlanRevision:                   8,
		RuntimePathNeedsAttention:          false,
		RuntimePathFeedbackCount:           2,
		RuntimePathReductionHoldUntilUnix:  time.Now().Add(30 * time.Second).Unix(),
		RuntimePathExpansionEligibleAtUnix: time.Now().Add(-1 * time.Second).Unix(),
		RuntimeAppliedPathPlanRevision:     8,
		RuntimeAppliedPathReportedAtUnix:   time.Now().Unix(),
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired re-expansion after expansion eligibility")
	}
	if status.RuntimePathReductionHoldUntilUnix != 0 || status.RuntimePathFeedbackCount != 0 || status.RuntimePathExpansionEligibleAtUnix != 0 {
		t.Fatalf("expected runtime hold bookkeeping to clear after expansion eligibility: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 2 {
		t.Fatalf("expected desired re-expansion after stabilization window: %+v", status.DesiredActiveGatewaySet)
	}
	if status.ControllerReconcileRequestedAtUnix != 0 || status.ControllerReconcileReason != "" {
		t.Fatalf("did not expect reconcile request after expansion was already applied: %+v", status)
	}
	if status.RuntimePathExpansionBackoffLevel != 0 {
		t.Fatalf("expected expansion backoff level to decay after successful re-expansion: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusUsesLongerExpansionDelayForRepeatedRuntimeFeedback(t *testing.T) {
	nowUnix := time.Now().Unix()
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		DesiredActiveGatewaySet:           []string{"gw-a"},
		ObservedActiveGatewaySet:          []string{"gw-a", "gw-b"},
		PathPlanRevision:                  8,
		RuntimePathNeedsAttention:         false,
		RuntimePathFeedbackCount:          4,
		RuntimePathExpansionBackoffLevel:  2,
		RuntimePathReductionHoldUntilUnix: time.Now().Add(30 * time.Second).Unix(),
		RuntimeAppliedPathPlanRevision:    8,
		RuntimeAppliedPathReportedAtUnix:  nowUnix,
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	minExpected := nowUnix + int64(runtimePathExpansionStabilizationDelay/time.Second)*4
	if status.RuntimePathExpansionEligibleAtUnix < minExpected {
		t.Fatalf("expected longer expansion delay from backoff level: got=%d want_at_least=%d status=%+v", status.RuntimePathExpansionEligibleAtUnix, minExpected, status)
	}
	if status.ControllerReconcileScheduledAtUnix != status.RuntimePathExpansionEligibleAtUnix || status.ControllerReconcileScheduledReason != "runtime_expansion_eligible" {
		t.Fatalf("expected reconcile schedule to follow expanded runtime delay: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusEscalatesStalledRuntimeReapplyToAggressiveHandoff(t *testing.T) {
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                          true,
		CurrentGatewayID:               "gw-a",
		DesiredActiveGatewaySet:        []string{"gw-a"},
		ObservedActiveGatewaySet:       []string{"gw-a", "gw-b"},
		PathPlanRevision:               9,
		PathPlanReapplyRequested:       true,
		PathPlanReapplyReason:          "runtime_lane_unavailable",
		PathPlanReapplyRequestedAtUnix: time.Now().Add(-20 * time.Second).Unix(),
		RuntimePathNeedsAttention:      true,
		RuntimePathAttentionReasons:    []string{"lane_unavailable"},
		RuntimePathRecommendedActions:  []string{"refresh_gateway_path_plan", "reapply_latest_path_plan"},
		RuntimeAppliedPathPlanRevision: 7,
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !containsGatewayID(status.PathPlanAttentionReasons, "runtime_path_plan_reapply_stalled") {
		t.Fatalf("expected stalled reapply attention reason: %+v", status.PathPlanAttentionReasons)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("expected aggressive handoff escalation: %+v", status.PathPlanRecommendedActions)
	}
	if status.ControllerReconcileRequestedAtUnix == 0 || status.ControllerReconcileReason != "runtime_path_plan_reapply_stalled" {
		t.Fatalf("expected reconcile request for stalled runtime reapply: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix != 0 || status.ControllerReconcileScheduledReason != "" {
		t.Fatalf("did not expect future reconcile schedule once stalled runtime reapply is already eligible: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusMarksNoObservedGatewayAttention(t *testing.T) {
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateDown},
		{GatewayID: "gw-b", ConnectionState: GatewayStateDetached},
	})
	if !status.PathPlanNeedsAttention {
		t.Fatalf("expected attention: %+v", status)
	}
	if len(status.PathPlanAttentionReasons) != 1 || status.PathPlanAttentionReasons[0] != "no_observed_gateway" {
		t.Fatalf("unexpected attention reasons: %+v", status.PathPlanAttentionReasons)
	}
	if len(status.PathPlanRecommendedActions) < 2 {
		t.Fatalf("unexpected recommended actions: %+v", status.PathPlanRecommendedActions)
	}
}

func TestReconcileVolumePathPlanStatusExpandsDesiredSetAfterRecovery(t *testing.T) {
	initial, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{}, []GatewayRecord{
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
		{GatewayID: "gw-c", ConnectionState: GatewayStateUnknown},
	})
	if len(initial.DesiredActiveGatewaySet) != 1 || initial.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("unexpected initial desired fallback set: %+v", initial.DesiredActiveGatewaySet)
	}
	recovered, _, desiredChanged := ReconcileVolumePathPlanStatus(initial, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired set expansion after recovery")
	}
	if len(recovered.DesiredActiveGatewaySet) != 2 || recovered.DesiredActiveGatewaySet[0] != "gw-a" || recovered.DesiredActiveGatewaySet[1] != "gw-b" {
		t.Fatalf("unexpected recovered desired set: %+v", recovered.DesiredActiveGatewaySet)
	}
	if recovered.PathPlanNeedsAttention {
		t.Fatalf("recovered desired set should clear attention: %+v", recovered)
	}
}

func TestReconcileVolumePathPlanStatusShrinksDesiredSetForRuntimePreferFewerActivePaths(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		RuntimePathNeedsAttention:     true,
		RuntimePathFeedbackCount:      4,
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("expected runtime-driven desired shrink: %+v", status.DesiredActiveGatewaySet)
	}
	if status.RuntimePathExpansionBackoffLevel < 2 {
		t.Fatalf("expected repeated runtime feedback to raise expansion backoff level: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusKeepsCurrentHealthyGatewayWhenShrinkingForRuntimeFeedback(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                         true,
		CurrentGatewayID:              "gw-b",
		RuntimePathNeedsAttention:     true,
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("expected current healthy gateway to be preserved: %+v", status.DesiredActiveGatewaySet)
	}
	if status.HandoffRequired {
		t.Fatalf("did not expect handoff when current healthy gateway is preserved: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusPrefersAggressiveHandoffForBorderlineRuntimeHold(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                             true,
		CurrentGatewayID:                  "gw-b",
		WriterFencingEpoch:                9,
		RuntimePathNeedsAttention:         true,
		RuntimePathAttentionReasons:       []string{"lane_unavailable"},
		RuntimePathRecommendedActions:     []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
		RuntimePathReductionHoldUntilUnix: time.Now().Add(30 * time.Second).Unix(),
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("expected aggressive handoff target to move off current gateway: %+v", status.DesiredActiveGatewaySet)
	}
	if !status.HandoffRequired || status.HandoffReason != "runtime_hold_borderline_current_gateway" || len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-a" {
		t.Fatalf("expected handoff toward gw-a: %+v", status)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("expected aggressive handoff action: %+v", status.PathPlanRecommendedActions)
	}
	if status.WriterFencingEpoch != 10 {
		t.Fatalf("expected fencing epoch bump: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusTriggersHandoffWhenCurrentGatewayNotHealthyUnderRuntimeShrink(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                         true,
		CurrentGatewayID:              "gw-b",
		WriterFencingEpoch:            4,
		RuntimePathNeedsAttention:     true,
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("expected desired target gw-a: %+v", status.DesiredActiveGatewaySet)
	}
	if !status.HandoffRequired || len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-a" {
		t.Fatalf("expected handoff to gw-a: %+v", status)
	}
	if status.WriterFencingEpoch != 5 {
		t.Fatalf("expected fencing epoch bump: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusClearsCompletedHandoffAfterConvergence(t *testing.T) {
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                           true,
		CurrentGatewayID:                "gw-b",
		DesiredActiveGatewaySet:         []string{"gw-b"},
		ObservedActiveGatewaySet:        []string{"gw-b"},
		AttachmentGeneration:            3,
		PathPlanRevision:                9,
		RuntimeAppliedPathPlanRevision:  9,
		HandoffRequired:                 true,
		HandoffReason:                   "current_gateway_not_desired",
		HandoffTargetGatewaySet:         []string{"gw-b"},
		HandoffAckedAtUnix:              time.Now().Unix(),
		HandoffAckedGeneration:          3,
		HandoffCompletionEligibleAtUnix: time.Now().Add(-1 * time.Second).Unix(),
	}, []GatewayRecord{
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if status.HandoffRequired || status.HandoffStage != "" || status.HandoffReason != "" || len(status.HandoffTargetGatewaySet) != 0 {
		t.Fatalf("expected converged handoff to clear: %+v", status)
	}
	if status.HandoffAckedAtUnix != 0 || status.HandoffAckedGeneration != 0 {
		t.Fatalf("expected handoff ack bookkeeping to clear: %+v", status)
	}
	if status.HandoffCompletionEligibleAtUnix != 0 {
		t.Fatalf("expected handoff completion eligibility to clear: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusSchedulesReadyToCompleteHandoff(t *testing.T) {
	nowUnix := time.Now().Unix()
	status, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                           true,
		CurrentGatewayID:                "gw-b",
		DesiredActiveGatewaySet:         []string{"gw-b"},
		ObservedActiveGatewaySet:        []string{"gw-b"},
		AttachmentGeneration:            3,
		PathPlanRevision:                9,
		RuntimeAppliedPathPlanRevision:  9,
		HandoffRequired:                 true,
		HandoffReason:                   "current_gateway_not_desired",
		HandoffTargetGatewaySet:         []string{"gw-b"},
		HandoffAckedAtUnix:              nowUnix,
		HandoffAckedGeneration:          3,
		HandoffCompletionEligibleAtUnix: nowUnix + 5,
	}, []GatewayRecord{
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !status.HandoffRequired || status.HandoffStage != "ready_to_complete" {
		t.Fatalf("expected ready_to_complete handoff to remain pending during hold: %+v", status)
	}
	if status.ControllerReconcileRequestedAtUnix != 0 || status.ControllerReconcileReason != "" {
		t.Fatalf("did not expect immediate reconcile request during completion hold: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix != nowUnix+5 || status.ControllerReconcileScheduledReason != "handoff_completion_ready" {
		t.Fatalf("expected scheduled reconcile for handoff completion hold: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusEscalatesStalledReapplyToCurrentGatewayHandoff(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                          true,
		CurrentGatewayID:               "gw-a",
		WriterFencingEpoch:             12,
		PathPlanRevision:               9,
		PathPlanReapplyRequested:       true,
		PathPlanReapplyReason:          "runtime_lane_unavailable",
		PathPlanReapplyRequestedAtUnix: time.Now().Add(-20 * time.Second).Unix(),
		RuntimePathNeedsAttention:      true,
		RuntimePathAttentionReasons:    []string{"lane_unavailable"},
		RuntimePathRecommendedActions:  []string{"refresh_gateway_path_plan", "reapply_latest_path_plan"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("expected stalled reapply to move desired gateway away from current: %+v", status.DesiredActiveGatewaySet)
	}
	if !status.HandoffRequired || status.HandoffReason != "runtime_path_plan_reapply_stalled_current_gateway" || len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-b" {
		t.Fatalf("expected stalled reapply handoff toward gw-b: %+v", status)
	}
	if status.HandoffStage != "pending_generation_rotation" {
		t.Fatalf("expected stalled reapply handoff stage to await generation rotation: %+v", status)
	}
	if !containsGatewayID(status.PathPlanAttentionReasons, "runtime_path_plan_reapply_stalled") {
		t.Fatalf("expected stalled reapply attention reason: %+v", status.PathPlanAttentionReasons)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("expected aggressive handoff action during stalled reapply: %+v", status.PathPlanRecommendedActions)
	}
	if status.WriterFencingEpoch != 13 {
		t.Fatalf("expected fencing epoch bump: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusEscalatesStalledGenerationRotationHandoff(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                         true,
		CurrentGatewayID:              "gw-a",
		WriterFencingEpoch:            20,
		HandoffRequired:               true,
		HandoffRequestedAtUnix:        time.Now().Add(-20 * time.Second).Unix(),
		HandoffStage:                  "pending_generation_rotation",
		HandoffReason:                 "current_gateway_not_desired",
		HandoffTargetGatewaySet:       []string{"gw-b"},
		RuntimePathRecommendedActions: []string{"prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("expected desired handoff target to remain gw-b: %+v", status.DesiredActiveGatewaySet)
	}
	if !status.HandoffRequired || status.HandoffReason != "handoff_generation_rotation_stalled_current_gateway" {
		t.Fatalf("expected stalled generation rotation handoff escalation: %+v", status)
	}
	if status.HandoffStage != "pending_generation_rotation" {
		t.Fatalf("expected handoff stage to remain pending_generation_rotation: %+v", status)
	}
	if !containsGatewayID(status.PathPlanAttentionReasons, "handoff_generation_rotation_stalled") {
		t.Fatalf("expected generation rotation stalled attention reason: %+v", status.PathPlanAttentionReasons)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("expected aggressive handoff action during stalled generation rotation: %+v", status.PathPlanRecommendedActions)
	}
	if status.WriterFencingEpoch != 21 {
		t.Fatalf("expected fencing epoch bump on generation rotation escalation: %+v", status)
	}
	if status.HandoffEscalationCount != 1 || status.HandoffNextEscalationAtUnix == 0 {
		t.Fatalf("expected handoff escalation bookkeeping: %+v", status)
	}
	if status.ControllerReconcileRequestedAtUnix != 0 || status.ControllerReconcileReason != "" {
		t.Fatalf("did not expect reconcile request after generation-rotation escalation was already consumed: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix <= time.Now().Unix() || status.ControllerReconcileScheduledReason != "handoff_generation_rotation_backoff_eligible" {
		t.Fatalf("expected future reconcile schedule for next generation-rotation backoff window: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusKeepsTargetAndDoesNotRebumpBeforeNextGenerationRotationBackoff(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                         true,
		CurrentGatewayID:              "gw-a",
		DesiredActiveGatewaySet:       []string{"gw-b"},
		WriterFencingEpoch:            30,
		HandoffRequired:               true,
		HandoffRequestedAtUnix:        time.Now().Add(-20 * time.Second).Unix(),
		HandoffEscalationCount:        1,
		HandoffNextEscalationAtUnix:   time.Now().Add(30 * time.Second).Unix(),
		HandoffStage:                  "pending_generation_rotation",
		HandoffReason:                 "handoff_generation_rotation_stalled_current_gateway",
		HandoffTargetGatewaySet:       []string{"gw-b"},
		RuntimePathRecommendedActions: []string{"prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-c", ConnectionState: GatewayStateUp},
	})
	if desiredChanged {
		t.Fatalf("expected no desired rotation before backoff window: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-b" {
		t.Fatalf("expected desired target to stay on gw-b before backoff window: %+v", status.DesiredActiveGatewaySet)
	}
	if status.WriterFencingEpoch != 30 || status.HandoffEscalationCount != 1 {
		t.Fatalf("expected no rebump before next backoff window: %+v", status)
	}
	if status.ControllerReconcileScheduledAtUnix != status.HandoffNextEscalationAtUnix || status.ControllerReconcileScheduledReason != "handoff_generation_rotation_backoff_eligible" {
		t.Fatalf("expected waiting handoff backoff to remain scheduled: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusRotatesTargetAndRebumpsAfterNextGenerationRotationBackoff(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:                         true,
		CurrentGatewayID:              "gw-a",
		DesiredActiveGatewaySet:       []string{"gw-b"},
		WriterFencingEpoch:            30,
		HandoffRequired:               true,
		HandoffRequestedAtUnix:        time.Now().Add(-40 * time.Second).Unix(),
		HandoffEscalationCount:        1,
		HandoffNextEscalationAtUnix:   time.Now().Add(-1 * time.Second).Unix(),
		HandoffStage:                  "pending_generation_rotation",
		HandoffReason:                 "handoff_generation_rotation_stalled_current_gateway",
		HandoffTargetGatewaySet:       []string{"gw-b"},
		RuntimePathRecommendedActions: []string{"prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-c", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired target rotation after backoff window: %+v", status)
	}
	if len(status.DesiredActiveGatewaySet) != 1 || status.DesiredActiveGatewaySet[0] != "gw-c" {
		t.Fatalf("expected desired target to rotate to gw-c after backoff window: %+v", status.DesiredActiveGatewaySet)
	}
	if status.WriterFencingEpoch != 31 || status.HandoffEscalationCount != 2 || status.HandoffNextEscalationAtUnix == 0 {
		t.Fatalf("expected rebump and escalation count advance after backoff window: %+v", status)
	}
}

func TestReconcileVolumePathPlanStatusReExpandsDesiredSetAfterRuntimeFeedbackClears(t *testing.T) {
	initial, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		RuntimePathNeedsAttention:     true,
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if len(initial.DesiredActiveGatewaySet) != 1 || initial.DesiredActiveGatewaySet[0] != "gw-a" {
		t.Fatalf("unexpected initial desired shrink: %+v", initial.DesiredActiveGatewaySet)
	}
	recovered, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		RuntimePathNeedsAttention:     false,
		RuntimePathRecommendedActions: nil,
		DesiredActiveGatewaySet:       initial.DesiredActiveGatewaySet,
		ObservedActiveGatewaySet:      initial.ObservedActiveGatewaySet,
		PathPlanRevision:              initial.PathPlanRevision,
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if !desiredChanged {
		t.Fatalf("expected desired re-expansion")
	}
	if len(recovered.DesiredActiveGatewaySet) != 2 || recovered.DesiredActiveGatewaySet[0] != "gw-a" || recovered.DesiredActiveGatewaySet[1] != "gw-b" {
		t.Fatalf("unexpected re-expanded desired set: %+v", recovered.DesiredActiveGatewaySet)
	}
}

func TestReconcileVolumePathPlanStatusKeepsDesiredSetShrunkWhileRuntimeHoldIsActive(t *testing.T) {
	initial, _, _ := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		RuntimePathNeedsAttention:         true,
		RuntimePathRecommendedActions:     []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
		RuntimePathReductionHoldUntilUnix: time.Now().Add(30 * time.Second).Unix(),
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if len(initial.DesiredActiveGatewaySet) != 1 {
		t.Fatalf("expected initial shrink: %+v", initial.DesiredActiveGatewaySet)
	}
	held, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		RuntimePathNeedsAttention:         false,
		RuntimePathRecommendedActions:     nil,
		RuntimePathReductionHoldUntilUnix: time.Now().Add(30 * time.Second).Unix(),
		DesiredActiveGatewaySet:           initial.DesiredActiveGatewaySet,
		ObservedActiveGatewaySet:          initial.ObservedActiveGatewaySet,
		PathPlanRevision:                  initial.PathPlanRevision,
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateUp},
	})
	if desiredChanged {
		t.Fatalf("did not expect desired re-expansion while hold is active")
	}
	if len(held.DesiredActiveGatewaySet) != 1 {
		t.Fatalf("expected desired shrink to persist: %+v", held.DesiredActiveGatewaySet)
	}
}

func TestReconcileVolumePathPlanStatusMarksHandoffAndBumpsFencingEpoch(t *testing.T) {
	status, _, desiredChanged := ReconcileVolumePathPlanStatus(VolumeStatusRecord{
		InUse:              true,
		CurrentGatewayID:   "gw-b",
		WriterFencingEpoch: 7,
	}, []GatewayRecord{
		{GatewayID: "gw-a", ConnectionState: GatewayStateUp},
		{GatewayID: "gw-b", ConnectionState: GatewayStateDegraded},
	})
	if !desiredChanged {
		t.Fatalf("expected desired change")
	}
	if !status.HandoffRequired || status.HandoffReason != "current_gateway_not_desired" {
		t.Fatalf("expected handoff requirement: %+v", status)
	}
	if !status.PathPlanNeedsAttention {
		t.Fatalf("expected handoff attention: %+v", status)
	}
	if !containsGatewayID(status.PathPlanRecommendedActions, "complete_gateway_handoff") || !containsGatewayID(status.PathPlanRecommendedActions, "prefer_fewer_active_paths") {
		t.Fatalf("expected handoff recommended actions: %+v", status.PathPlanRecommendedActions)
	}
	if len(status.HandoffTargetGatewaySet) != 1 || status.HandoffTargetGatewaySet[0] != "gw-a" {
		t.Fatalf("unexpected handoff target set: %+v", status.HandoffTargetGatewaySet)
	}
	if status.WriterFencingEpoch != 8 {
		t.Fatalf("expected fencing epoch bump: %+v", status)
	}
}

func TestControllerPathPlanFeedbackMergesRuntimeSignals(t *testing.T) {
	status := VolumeStatusRecord{
		PathPlanNeedsAttention:        true,
		PathPlanAttentionReasons:      []string{"handoff_required"},
		PathPlanRecommendedActions:    []string{"complete_gateway_handoff"},
		RuntimePathNeedsAttention:     true,
		RuntimePathAttentionReasons:   []string{"lane_unavailable"},
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
	}
	if !ControllerPathPlanNeedsAttention(status) {
		t.Fatalf("expected merged controller attention")
	}
	reasons := ControllerPathPlanAttentionReasons(status)
	if len(reasons) != 2 || !containsGatewayID(reasons, "handoff_required") || !containsGatewayID(reasons, "lane_unavailable") {
		t.Fatalf("unexpected merged reasons: %+v", reasons)
	}
	actions := ControllerPathPlanRecommendedActions(status)
	if len(actions) != 3 ||
		!containsGatewayID(actions, "complete_gateway_handoff") ||
		!containsGatewayID(actions, "refresh_gateway_path_plan") ||
		!containsGatewayID(actions, "prefer_fewer_active_paths") {
		t.Fatalf("unexpected merged actions: %+v", actions)
	}
}

func TestControllerPathPlanRecommendedActionsIncludeRefreshForRuntimeExpansionEligibility(t *testing.T) {
	status := VolumeStatusRecord{
		PathPlanRevision:                   7,
		RuntimeAppliedPathPlanRevision:     7,
		RuntimePathExpansionEligibleAtUnix: time.Now().Add(-1 * time.Second).Unix(),
	}
	actions := ControllerPathPlanRecommendedActions(status)
	if len(actions) != 1 || !containsGatewayID(actions, "refresh_gateway_path_plan") {
		t.Fatalf("expected refresh action for runtime expansion eligibility: %+v", actions)
	}
}

func TestControllerPathPlanRecommendedActionsEscalateStalledGenerationRotationHandoff(t *testing.T) {
	status := VolumeStatusRecord{
		HandoffRequired:             true,
		HandoffReason:               "handoff_generation_rotation_stalled_current_gateway",
		HandoffNextEscalationAtUnix: time.Now().Add(-1 * time.Second).Unix(),
	}
	actions := ControllerPathPlanRecommendedActions(status)
	if !containsGatewayID(actions, "complete_gateway_handoff") ||
		!containsGatewayID(actions, "complete_gateway_handoff_aggressively") ||
		!containsGatewayID(actions, "prefer_fewer_active_paths") {
		t.Fatalf("expected stalled generation rotation handoff actions: %+v", actions)
	}
	if got := OperatorPathPlanPriorityClass(status); got != "aggressive_handoff" {
		t.Fatalf("expected aggressive handoff class for stalled generation rotation, got %s", got)
	}
}

func TestControllerPathPlanRecommendedActionsWaitDuringGenerationRotationBackoff(t *testing.T) {
	status := VolumeStatusRecord{
		HandoffRequired:             true,
		HandoffReason:               "handoff_generation_rotation_stalled_current_gateway",
		HandoffNextEscalationAtUnix: time.Now().Add(30 * time.Second).Unix(),
	}
	actions := ControllerPathPlanRecommendedActions(status)
	if !containsGatewayID(actions, "complete_gateway_handoff") || !containsGatewayID(actions, "prefer_fewer_active_paths") {
		t.Fatalf("expected base handoff actions during backoff wait: %+v", actions)
	}
	if containsGatewayID(actions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("did not expect aggressive action during backoff wait: %+v", actions)
	}
	if got := OperatorPathPlanPriorityClass(status); got != "handoff" {
		t.Fatalf("expected handoff class during backoff wait, got %s", got)
	}
}

func TestOperatorPathPlanPriorityClass(t *testing.T) {
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{
		PathPlanRecommendedActions: []string{"complete_gateway_handoff_aggressively"},
	}); got != "aggressive_handoff" {
		t.Fatalf("unexpected aggressive handoff class: %s", got)
	}
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{
		PathPlanRecommendedActions: []string{"complete_gateway_handoff"},
	}); got != "handoff" {
		t.Fatalf("unexpected handoff class: %s", got)
	}
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{
		PathPlanRevision:                   7,
		RuntimeAppliedPathPlanRevision:     7,
		RuntimePathExpansionEligibleAtUnix: time.Now().Add(-1 * time.Second).Unix(),
	}); got != "expansion_ready" {
		t.Fatalf("unexpected expansion ready class: %s", got)
	}
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan"},
	}); got != "refresh" {
		t.Fatalf("unexpected refresh class: %s", got)
	}
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{
		PathPlanNeedsAttention: true,
	}); got != "attention" {
		t.Fatalf("unexpected attention class: %s", got)
	}
	if got := OperatorPathPlanPriorityClass(VolumeStatusRecord{}); got != "normal" {
		t.Fatalf("unexpected normal class: %s", got)
	}
}

func TestOperatorPathPlanRecommendedActionsWithCluster(t *testing.T) {
	status := VolumeStatusRecord{
		RuntimePathRecommendedActions: []string{"refresh_gateway_path_plan"},
	}
	actions := OperatorPathPlanRecommendedActionsWithCluster(status, "aggressive_handoff")
	if !containsGatewayID(actions, "refresh_gateway_path_plan") {
		t.Fatalf("expected base refresh action to stay present: %+v", actions)
	}
	if !containsGatewayID(actions, "complete_gateway_handoff_aggressively") {
		t.Fatalf("expected cluster-top aggressive escalation: %+v", actions)
	}
	if got := OperatorPathPlanPriorityClassWithCluster(status, "aggressive_handoff"); got != "aggressive_handoff" {
		t.Fatalf("unexpected cluster-aware priority class: %s", got)
	}
}
