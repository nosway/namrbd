package service

import "time"

const handoffCompletionStabilizationDelay = 5 * time.Second

func gatewayAllowedDuringHandoff(status VolumeStatusRecord, gatewayID string) bool {
	if !status.HandoffRequired {
		return true
	}
	switch handoffStage(status) {
	case "target_attached_pending_ack", "acknowledged_pending_convergence", "acknowledged_pending_runtime_attention", "ready_to_complete":
		return status.CurrentGatewayID != "" && status.CurrentGatewayID == gatewayID
	}
	return containsGatewayID(status.HandoffTargetGatewaySet, gatewayID)
}

func handoffCompletionSatisfied(status VolumeStatusRecord) bool {
	if !status.HandoffRequired {
		return false
	}
	return handoffStage(status) == "ready_to_complete" &&
		status.HandoffCompletionEligibleAtUnix > 0 &&
		time.Now().Unix() >= status.HandoffCompletionEligibleAtUnix
}

func handoffAcknowledged(status VolumeStatusRecord) bool {
	if !status.HandoffRequired {
		return false
	}
	if status.CurrentGatewayID == "" || !containsGatewayID(status.HandoffTargetGatewaySet, status.CurrentGatewayID) {
		return false
	}
	if status.AttachmentGeneration == 0 {
		return false
	}
	return status.HandoffAckedAtUnix > 0 && status.HandoffAckedGeneration == status.AttachmentGeneration
}

func handoffStage(status VolumeStatusRecord) string {
	if !status.HandoffRequired {
		return ""
	}
	if status.CurrentGatewayID == "" || !containsGatewayID(status.HandoffTargetGatewaySet, status.CurrentGatewayID) || status.AttachmentGeneration == 0 {
		return "pending_generation_rotation"
	}
	if !handoffAcknowledged(status) {
		return "target_attached_pending_ack"
	}
	if status.PathPlanRevision == 0 || !runtimePathPlanConverged(status) {
		return "acknowledged_pending_convergence"
	}
	if status.RuntimePathNeedsAttention {
		return "acknowledged_pending_runtime_attention"
	}
	return "ready_to_complete"
}

func updateHandoffCompletionEligibility(status *VolumeStatusRecord, nowUnix int64) {
	if !status.HandoffRequired {
		status.HandoffCompletionEligibleAtUnix = 0
		return
	}
	if handoffStage(*status) == "ready_to_complete" {
		if status.HandoffCompletionEligibleAtUnix == 0 {
			status.HandoffCompletionEligibleAtUnix = nowUnix + int64(handoffCompletionStabilizationDelay/time.Second)
		}
		return
	}
	status.HandoffCompletionEligibleAtUnix = 0
}
