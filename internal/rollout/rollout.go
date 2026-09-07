// Package rollout implements the pure, restart-safe Phase AD canary/wave
// state machine. It emits idempotent transport instructions and records their
// externally supplied results; it does not contact TiKV, hosts, daemons, or
// storage devices.
package rollout

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
)

const (
	APIVersion    = "namrbd.io/v1alpha1"
	OperationKind = "SBSClusterRolloutOperation"

	StateReady     = "ready"
	StateRunning   = "running"
	StatePaused    = "paused"
	StateCompleted = "completed"

	WavePending   = "pending"
	WaveRunning   = "running"
	WaveFailed    = "failed"
	WaveSucceeded = "succeeded"

	WaveKindCanary = "canary"
	WaveKindBatch  = "wave"

	ActionPending    = "pending"
	ActionInProgress = "in_progress"
	ActionFailed     = "failed"
	ActionSucceeded  = "succeeded"

	OutcomeSuccess = "success"
	OutcomeFailure = "failure"

	PauseCauseFailure  = "failure"
	PauseCauseOperator = "operator"

	TransitionFailurePause   = "failure_pause"
	TransitionOperatorPause  = "operator_pause"
	TransitionOperatorResume = "operator_resume"
	TransitionRetry          = "retry"
)

type StartRequest struct {
	Manifest  *clustermanifest.Manifest
	Policy    clustermanifest.ValidationPolicy
	JoinPlan  *installpreflight.JoinPlan
	StartedAt time.Time
}

type Operation struct {
	APIVersion               string             `json:"api_version"`
	Kind                     string             `json:"kind"`
	OperationID              string             `json:"operation_id"`
	OperationDigest          string             `json:"operation_digest"`
	Revision                 uint64             `json:"revision"`
	PlanID                   string             `json:"plan_id"`
	JoinPlanID               string             `json:"join_plan_id"`
	JoinPlanDigest           string             `json:"join_plan_digest"`
	ManifestDigest           string             `json:"manifest_digest"`
	State                    string             `json:"state"`
	PauseCause               string             `json:"pause_cause,omitempty"`
	PauseReason              string             `json:"pause_reason,omitempty"`
	CurrentWaveIndex         int                `json:"current_wave_index"`
	MaxParallelPerZone       int                `json:"max_parallel_per_zone"`
	StartedAt                time.Time          `json:"started_at"`
	UpdatedAt                time.Time          `json:"updated_at"`
	Waves                    []Wave             `json:"waves"`
	TransitionHistory        []TransitionRecord `json:"transition_history"`
	RollbackPlan             RollbackPlan       `json:"rollback_plan"`
	ProjectedNodeActionCount int                `json:"projected_node_action_count"`
	SucceededNodeCount       int                `json:"succeeded_node_count"`
	FailedNodeCount          int                `json:"failed_node_count"`
	InProgressNodeCount      int                `json:"in_progress_node_count"`
	PendingNodeCount         int                `json:"pending_node_count"`
	FirstError               string             `json:"first_error"`
	LastError                string             `json:"last_error"`
	ExecutedTiKVMutations    int                `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                `json:"executed_storage_mutations"`
	NoMutationStateMachine   bool               `json:"no_mutation_state_machine"`
	Physical160Claimed       bool               `json:"physical_160_claimed"`
}

type Wave struct {
	WaveID  string       `json:"wave_id"`
	Zone    string       `json:"zone"`
	Kind    string       `json:"kind"`
	Ordinal int          `json:"ordinal"`
	State   string       `json:"state"`
	Actions []NodeAction `json:"actions"`
}

type NodeAction struct {
	NodeID         string    `json:"node_id"`
	Hostname       string    `json:"hostname"`
	BundleDigest   string    `json:"bundle_digest"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	LastOutcomeAt  time.Time `json:"last_outcome_at,omitempty"`
	FirstError     string    `json:"first_error,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

type Instruction struct {
	OperationID    string `json:"operation_id"`
	PlanID         string `json:"plan_id"`
	JoinPlanID     string `json:"join_plan_id"`
	ManifestDigest string `json:"manifest_digest"`
	WaveID         string `json:"wave_id"`
	Zone           string `json:"zone"`
	NodeID         string `json:"node_id"`
	Hostname       string `json:"hostname"`
	BundleDigest   string `json:"bundle_digest"`
	IdempotencyKey string `json:"idempotency_key"`
	Attempt        int    `json:"attempt"`
	Replayed       bool   `json:"replayed"`
}

type TransitionRecord struct {
	Type     string    `json:"type"`
	Reason   string    `json:"reason"`
	WaveID   string    `json:"wave_id"`
	Revision uint64    `json:"revision"`
	At       time.Time `json:"at"`
}

type RollbackPlan struct {
	Required           bool     `json:"required"`
	AutomaticExecution bool     `json:"automatic_execution"`
	Reason             string   `json:"reason,omitempty"`
	CandidateNodeIDs   []string `json:"candidate_node_ids"`
	ResolvedBy         string   `json:"resolved_by,omitempty"`
}

func Start(request StartRequest) (*Operation, error) {
	if request.Manifest == nil {
		return nil, fmt.Errorf("manifest is required")
	}
	if request.StartedAt.IsZero() {
		return nil, fmt.Errorf("rollout start time is required")
	}
	if err := installpreflight.ValidateJoinPlan(request.JoinPlan); err != nil {
		return nil, err
	}
	canonical, err := clustermanifest.Canonicalize(request.Manifest)
	if err != nil {
		return nil, err
	}
	rendered, err := clustermanifest.Render(canonical, request.Policy)
	if err != nil {
		return nil, err
	}
	manifestPlan, err := clustermanifest.BuildPlan(canonical, nil, request.Policy)
	if err != nil {
		return nil, err
	}
	if request.JoinPlan.PlanID != manifestPlan.PlanID || request.JoinPlan.ManifestDigest != rendered.ManifestDigest {
		return nil, fmt.Errorf("join plan does not match the deterministic manifest plan and digest")
	}
	if err := validateJoinPlanAgainstManifest(request.JoinPlan, canonical, rendered); err != nil {
		return nil, err
	}
	operationID := "ad-rollout-" + strings.TrimPrefix(request.JoinPlan.JoinPlanDigest, "sha256:")[:16]
	operation := &Operation{
		APIVersion: APIVersion, Kind: OperationKind, OperationID: operationID,
		Revision: 1, PlanID: request.JoinPlan.PlanID, JoinPlanID: request.JoinPlan.JoinPlanID,
		JoinPlanDigest: request.JoinPlan.JoinPlanDigest, ManifestDigest: request.JoinPlan.ManifestDigest,
		State: StateReady, CurrentWaveIndex: 0,
		MaxParallelPerZone: canonical.Spec.Rollout.MaxParallelPerZone,
		StartedAt:          request.StartedAt.UTC(), UpdatedAt: request.StartedAt.UTC(),
		NoMutationStateMachine: true, Physical160Claimed: false,
	}
	for _, zone := range request.JoinPlan.Zones {
		operation.Waves = append(operation.Waves, newWave(operationID, zone.Zone, WaveKindCanary, 0, zone.Nodes[:1]))
		remaining := zone.Nodes[1:]
		for ordinal := 1; len(remaining) > 0; ordinal++ {
			count := operation.MaxParallelPerZone
			if count > len(remaining) {
				count = len(remaining)
			}
			operation.Waves = append(operation.Waves, newWave(operationID, zone.Zone, WaveKindBatch, ordinal, remaining[:count]))
			remaining = remaining[count:]
		}
	}
	finalizeOperation(operation)
	return operation, Validate(operation)
}

func IssueCurrentWave(operation *Operation, at time.Time) (*Operation, []Instruction, error) {
	if err := Validate(operation); err != nil {
		return nil, nil, err
	}
	if err := validateTransitionTime(operation, at); err != nil {
		return nil, nil, err
	}
	if operation.State == StatePaused {
		return nil, nil, fmt.Errorf("rollout is paused: %s", operation.PauseReason)
	}
	if operation.State == StateCompleted || operation.CurrentWaveIndex >= len(operation.Waves) {
		return nil, nil, fmt.Errorf("rollout is already completed")
	}
	next := cloneOperation(operation)
	wave := &next.Waves[next.CurrentWaveIndex]
	if wave.State == WaveFailed {
		return nil, nil, fmt.Errorf("failed wave requires explicit retry")
	}
	instructions := make([]Instruction, 0, len(wave.Actions))
	changed := false
	for i := range wave.Actions {
		action := &wave.Actions[i]
		replayed := action.Status == ActionInProgress
		if action.Status != ActionPending && !replayed {
			continue
		}
		if action.Status == ActionPending {
			action.Status = ActionInProgress
			action.Attempts++
			changed = true
		}
		instructions = append(instructions, Instruction{
			OperationID: next.OperationID, PlanID: next.PlanID, JoinPlanID: next.JoinPlanID,
			ManifestDigest: next.ManifestDigest, WaveID: wave.WaveID, Zone: wave.Zone,
			NodeID: action.NodeID, Hostname: action.Hostname, BundleDigest: action.BundleDigest,
			IdempotencyKey: action.IdempotencyKey, Attempt: action.Attempts, Replayed: replayed,
		})
	}
	if len(instructions) == 0 {
		return nil, nil, fmt.Errorf("current wave has no issuable or replayable actions")
	}
	if changed {
		wave.State = WaveRunning
		next.State = StateRunning
		next.Revision++
		next.UpdatedAt = at.UTC()
		finalizeOperation(next)
	}
	return next, instructions, Validate(next)
}

// RecordResult consumes a result for an emitted instruction. A duplicate
// success/failure report with the same idempotency key is ignored without a
// revision change. Conflicting terminal results are rejected.
func RecordResult(operation *Operation, nodeID, idempotencyKey, outcome, errorMessage string, at time.Time) (*Operation, bool, error) {
	if err := Validate(operation); err != nil {
		return nil, false, err
	}
	if err := validateTransitionTime(operation, at); err != nil {
		return nil, false, err
	}
	waveIndex, actionIndex := findAction(operation, nodeID)
	if waveIndex < 0 {
		return nil, false, fmt.Errorf("node %q is not in the rollout operation", nodeID)
	}
	action := operation.Waves[waveIndex].Actions[actionIndex]
	if action.IdempotencyKey != idempotencyKey {
		return nil, false, fmt.Errorf("node %s result idempotency key does not match", nodeID)
	}
	if outcome != OutcomeSuccess && outcome != OutcomeFailure {
		return nil, false, fmt.Errorf("outcome must be %s or %s", OutcomeSuccess, OutcomeFailure)
	}
	if outcome == OutcomeFailure && strings.TrimSpace(errorMessage) == "" {
		return nil, false, fmt.Errorf("failure outcome requires an error message")
	}
	if action.Status == ActionSucceeded {
		if outcome == OutcomeSuccess {
			return cloneOperation(operation), true, nil
		}
		return nil, false, fmt.Errorf("node %s already succeeded; conflicting failure is rejected", nodeID)
	}
	if action.Status == ActionFailed {
		if outcome == OutcomeFailure {
			return cloneOperation(operation), true, nil
		}
		return nil, false, fmt.Errorf("node %s is failed; explicit retry is required before success", nodeID)
	}
	if waveIndex != operation.CurrentWaveIndex || action.Status != ActionInProgress {
		return nil, false, fmt.Errorf("node %s does not have an in-progress action in the current wave", nodeID)
	}

	next := cloneOperation(operation)
	wave := &next.Waves[waveIndex]
	target := &wave.Actions[actionIndex]
	target.LastOutcomeAt = at.UTC()
	if outcome == OutcomeSuccess {
		target.Status = ActionSucceeded
	} else {
		target.Status = ActionFailed
		targetError := strings.TrimSpace(errorMessage)
		if target.FirstError == "" {
			target.FirstError = targetError
		}
		target.LastError = targetError
		if next.FirstError == "" {
			next.FirstError = nodeID + ": " + targetError
		}
		next.LastError = nodeID + ": " + targetError
	}
	updateWaveAndOperationState(next, waveIndex, at)
	next.Revision++
	next.UpdatedAt = at.UTC()
	finalizeOperation(next)
	return next, false, Validate(next)
}

func Pause(operation *Operation, reason string, at time.Time) (*Operation, error) {
	if err := Validate(operation); err != nil {
		return nil, err
	}
	if err := validateTransitionTime(operation, at); err != nil {
		return nil, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, fmt.Errorf("operator pause reason is required")
	}
	if operation.State == StateCompleted {
		return nil, fmt.Errorf("completed rollout cannot be paused")
	}
	if operation.State == StatePaused {
		if operation.PauseCause == PauseCauseOperator && operation.PauseReason == reason {
			return cloneOperation(operation), nil
		}
		return nil, fmt.Errorf("rollout is already paused by %s: %s", operation.PauseCause, operation.PauseReason)
	}
	next := cloneOperation(operation)
	next.State = StatePaused
	next.PauseCause = PauseCauseOperator
	next.PauseReason = reason
	next.TransitionHistory = append(next.TransitionHistory, TransitionRecord{
		Type: TransitionOperatorPause, Reason: reason, WaveID: currentWaveID(next),
		Revision: next.Revision + 1, At: at.UTC(),
	})
	next.Revision++
	next.UpdatedAt = at.UTC()
	finalizeOperation(next)
	return next, Validate(next)
}

func Resume(operation *Operation, reason string, at time.Time) (*Operation, error) {
	if err := Validate(operation); err != nil {
		return nil, err
	}
	if err := validateTransitionTime(operation, at); err != nil {
		return nil, err
	}
	if operation.State != StatePaused || operation.PauseCause != PauseCauseOperator {
		return nil, fmt.Errorf("only an operator-paused rollout can be resumed; failure pause requires retry")
	}
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("operator resume reason is required")
	}
	next := cloneOperation(operation)
	next.PauseCause, next.PauseReason = "", ""
	next.State = StateReady
	if next.CurrentWaveIndex < len(next.Waves) && next.Waves[next.CurrentWaveIndex].State == WaveRunning {
		next.State = StateRunning
	}
	next.TransitionHistory = append(next.TransitionHistory, TransitionRecord{
		Type: TransitionOperatorResume, Reason: strings.TrimSpace(reason), WaveID: currentWaveID(next),
		Revision: next.Revision + 1, At: at.UTC(),
	})
	next.Revision++
	next.UpdatedAt = at.UTC()
	finalizeOperation(next)
	return next, Validate(next)
}

func RetryFailed(operation *Operation, reason string, at time.Time) (*Operation, error) {
	if err := Validate(operation); err != nil {
		return nil, err
	}
	if err := validateTransitionTime(operation, at); err != nil {
		return nil, err
	}
	if operation.State != StatePaused || operation.PauseCause != PauseCauseFailure || operation.CurrentWaveIndex >= len(operation.Waves) {
		return nil, fmt.Errorf("rollout is not paused on a failed current wave")
	}
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("retry reason is required")
	}
	next := cloneOperation(operation)
	wave := &next.Waves[next.CurrentWaveIndex]
	failed := 0
	for i := range wave.Actions {
		switch wave.Actions[i].Status {
		case ActionInProgress:
			return nil, fmt.Errorf("cannot retry while node %s still has an in-progress action", wave.Actions[i].NodeID)
		case ActionFailed:
			wave.Actions[i].Status = ActionPending
			failed++
		}
	}
	if failed == 0 {
		return nil, fmt.Errorf("current wave has no failed actions to retry")
	}
	wave.State = WavePending
	next.State = StateReady
	next.PauseCause, next.PauseReason = "", ""
	next.RollbackPlan.Required = false
	next.RollbackPlan.ResolvedBy = "retry: " + strings.TrimSpace(reason)
	next.TransitionHistory = append(next.TransitionHistory, TransitionRecord{
		Type: TransitionRetry, Reason: strings.TrimSpace(reason), WaveID: wave.WaveID,
		Revision: next.Revision + 1, At: at.UTC(),
	})
	next.Revision++
	next.UpdatedAt = at.UTC()
	finalizeOperation(next)
	return next, Validate(next)
}

func NextInstructionCount(operation *Operation) int {
	if operation == nil || operation.State == StatePaused || operation.State == StateCompleted || operation.CurrentWaveIndex < 0 || operation.CurrentWaveIndex >= len(operation.Waves) {
		return 0
	}
	count := 0
	for _, action := range operation.Waves[operation.CurrentWaveIndex].Actions {
		if action.Status == ActionPending || action.Status == ActionInProgress {
			count++
		}
	}
	return count
}

func MarshalOperation(operation *Operation) ([]byte, error) {
	if err := Validate(operation); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(operation, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal rollout operation: %w", err)
	}
	return append(raw, '\n'), nil
}

func ParseOperation(raw []byte) (*Operation, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var operation Operation
	if err := dec.Decode(&operation); err != nil {
		return nil, fmt.Errorf("decode rollout operation: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode rollout operation: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode rollout operation trailing data: %w", err)
	}
	if err := Validate(&operation); err != nil {
		return nil, err
	}
	return &operation, nil
}

func Validate(operation *Operation) error {
	if operation == nil {
		return fmt.Errorf("rollout operation is nil")
	}
	if operation.APIVersion != APIVersion || operation.Kind != OperationKind {
		return fmt.Errorf("rollout operation identity must be api_version=%s kind=%s", APIVersion, OperationKind)
	}
	if operation.OperationID == "" || operation.PlanID == "" || operation.JoinPlanID == "" || operation.JoinPlanDigest == "" || operation.ManifestDigest == "" || operation.Revision == 0 {
		return fmt.Errorf("rollout operation identity and revision are required")
	}
	if operation.StartedAt.IsZero() || operation.UpdatedAt.IsZero() || operation.UpdatedAt.Before(operation.StartedAt) {
		return fmt.Errorf("rollout operation timestamps are invalid")
	}
	if operation.MaxParallelPerZone <= 0 || len(operation.Waves) == 0 || operation.CurrentWaveIndex < 0 || operation.CurrentWaveIndex > len(operation.Waves) {
		return fmt.Errorf("rollout operation wave bounds are invalid")
	}
	switch operation.State {
	case StateReady, StateRunning, StatePaused, StateCompleted:
	default:
		return fmt.Errorf("rollout operation has invalid state %q", operation.State)
	}
	if operation.State == StatePaused {
		if (operation.PauseCause != PauseCauseFailure && operation.PauseCause != PauseCauseOperator) || operation.PauseReason == "" {
			return fmt.Errorf("paused rollout requires a failure or operator cause and reason")
		}
	} else if operation.PauseCause != "" || operation.PauseReason != "" {
		return fmt.Errorf("non-paused rollout cannot retain an active pause cause")
	}
	if operation.State == StateCompleted && operation.CurrentWaveIndex != len(operation.Waves) {
		return fmt.Errorf("completed rollout has an incomplete current wave index")
	}
	seenWaves, seenNodes, seenKeys := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, wave := range operation.Waves {
		if wave.WaveID == "" || seenWaves[wave.WaveID] || wave.Zone == "" {
			return fmt.Errorf("rollout has an empty or duplicate wave identity %q", wave.WaveID)
		}
		seenWaves[wave.WaveID] = true
		if wave.Kind != WaveKindCanary && wave.Kind != WaveKindBatch {
			return fmt.Errorf("wave %s has invalid kind %q", wave.WaveID, wave.Kind)
		}
		if (wave.Kind == WaveKindCanary && len(wave.Actions) != 1) || (wave.Kind == WaveKindBatch && (len(wave.Actions) == 0 || len(wave.Actions) > operation.MaxParallelPerZone)) {
			return fmt.Errorf("wave %s violates canary/max-parallel bounds", wave.WaveID)
		}
		if wave.State != derivedWaveState(wave) {
			return fmt.Errorf("wave %s state %q is inconsistent with its actions", wave.WaveID, wave.State)
		}
		if i < operation.CurrentWaveIndex && wave.State != WaveSucceeded {
			return fmt.Errorf("wave %s precedes current index but is not succeeded", wave.WaveID)
		}
		if i > operation.CurrentWaveIndex && wave.State != WavePending {
			return fmt.Errorf("future wave %s is not pending", wave.WaveID)
		}
		for _, action := range wave.Actions {
			if action.NodeID == "" || action.Hostname == "" || action.BundleDigest == "" || action.IdempotencyKey == "" || seenNodes[action.NodeID] || seenKeys[action.IdempotencyKey] {
				return fmt.Errorf("wave %s has incomplete or duplicate node action %q", wave.WaveID, action.NodeID)
			}
			seenNodes[action.NodeID], seenKeys[action.IdempotencyKey] = true, true
			switch action.Status {
			case ActionPending:
				if action.Attempts < 0 {
					return fmt.Errorf("node %s has invalid attempt count", action.NodeID)
				}
			case ActionInProgress, ActionFailed, ActionSucceeded:
				if action.Attempts < 1 {
					return fmt.Errorf("node %s terminal/in-progress action has no attempt", action.NodeID)
				}
			default:
				return fmt.Errorf("node %s has invalid action status %q", action.NodeID, action.Status)
			}
		}
	}
	if len(seenNodes) != clustermanifest.ExactNodeCount || operation.ProjectedNodeActionCount != clustermanifest.ExactNodeCount {
		return fmt.Errorf("rollout operation does not contain the exact %d-node action set", clustermanifest.ExactNodeCount)
	}
	succeeded, failed, inProgress, pending := actionCounts(operation)
	if operation.SucceededNodeCount != succeeded || operation.FailedNodeCount != failed || operation.InProgressNodeCount != inProgress || operation.PendingNodeCount != pending {
		return fmt.Errorf("rollout operation action counters are inconsistent")
	}
	if !operation.NoMutationStateMachine || operation.ExecutedTiKVMutations != 0 || operation.ExecutedDaemonActions != 0 || operation.ExecutedStorageMutations != 0 || operation.Physical160Claimed {
		return fmt.Errorf("rollout operation violates the no-mutation state-machine boundary")
	}
	if operation.RollbackPlan.AutomaticExecution {
		return fmt.Errorf("rollout operation must not automatically execute rollback")
	}
	if operation.State == StateReady && operation.CurrentWaveIndex < len(operation.Waves) && operation.Waves[operation.CurrentWaveIndex].State != WavePending {
		return fmt.Errorf("ready rollout current wave is not pending")
	}
	if operation.State == StateRunning && operation.CurrentWaveIndex < len(operation.Waves) && operation.Waves[operation.CurrentWaveIndex].State != WaveRunning {
		return fmt.Errorf("running rollout current wave is not running")
	}
	if operation.State == StatePaused && operation.PauseCause == PauseCauseFailure {
		if operation.CurrentWaveIndex >= len(operation.Waves) || operation.Waves[operation.CurrentWaveIndex].State != WaveFailed || !operation.RollbackPlan.Required {
			return fmt.Errorf("failure-paused rollout lacks a failed current wave and required rollback decision")
		}
		wantCandidates := successfulOperationNodeIDs(operation)
		if !sameStrings(operation.RollbackPlan.CandidateNodeIDs, wantCandidates) {
			return fmt.Errorf("failure rollback candidates do not match successful current-wave nodes")
		}
	}
	if operation.RollbackPlan.Required && operation.RollbackPlan.Reason == "" {
		return fmt.Errorf("required rollback decision has no reason")
	}
	if err := validateTransitionHistory(operation, seenWaves); err != nil {
		return err
	}
	if operation.OperationDigest != digestOperation(operation) {
		return fmt.Errorf("rollout operation digest does not match canonical content")
	}
	return nil
}

func validateJoinPlanAgainstManifest(plan *installpreflight.JoinPlan, manifest *clustermanifest.Manifest, rendered *clustermanifest.RenderSet) error {
	bundles := map[string]clustermanifest.NodeBundle{}
	nodes := map[string]clustermanifest.Node{}
	for _, bundle := range rendered.Bundles {
		bundles[bundle.NodeID] = bundle
	}
	for _, node := range manifest.Spec.Nodes {
		nodes[node.ID] = node
	}
	if len(plan.Zones) != len(manifest.Spec.Rollout.Order) {
		return fmt.Errorf("join plan zone order does not match manifest rollout order")
	}
	for i, zone := range plan.Zones {
		if zone.Zone != manifest.Spec.Rollout.Order[i] {
			return fmt.Errorf("join plan zone %d is %q, want %q", i, zone.Zone, manifest.Spec.Rollout.Order[i])
		}
		for _, admitted := range zone.Nodes {
			node, ok := nodes[admitted.NodeID]
			if !ok || node.Location.Zone != zone.Zone || node.Hostname != admitted.Hostname || bundles[admitted.NodeID].BundleDigest != admitted.BundleDigest {
				return fmt.Errorf("join plan node %s does not match manifest zone/hostname/bundle", admitted.NodeID)
			}
		}
	}
	return nil
}

func newWave(operationID, zone, kind string, ordinal int, nodes []installpreflight.JoinNode) Wave {
	waveID := zone + "-canary"
	if kind == WaveKindBatch {
		waveID = fmt.Sprintf("%s-wave-%02d", zone, ordinal)
	}
	wave := Wave{WaveID: waveID, Zone: zone, Kind: kind, Ordinal: ordinal, State: WavePending}
	for _, node := range nodes {
		wave.Actions = append(wave.Actions, NodeAction{
			NodeID: node.NodeID, Hostname: node.Hostname, BundleDigest: node.BundleDigest,
			IdempotencyKey: operationID + "/join/" + node.NodeID, Status: ActionPending,
		})
	}
	return wave
}

func updateWaveAndOperationState(operation *Operation, waveIndex int, at time.Time) {
	wave := &operation.Waves[waveIndex]
	wave.State = derivedWaveState(*wave)
	if wave.State == WaveFailed {
		wasFailurePaused := operation.State == StatePaused && operation.PauseCause == PauseCauseFailure
		operation.State = StatePaused
		operation.PauseCause = PauseCauseFailure
		operation.PauseReason = operation.LastError
		if !wasFailurePaused {
			operation.TransitionHistory = append(operation.TransitionHistory, TransitionRecord{
				Type: TransitionFailurePause, Reason: operation.LastError, WaveID: wave.WaveID,
				Revision: operation.Revision + 1, At: at.UTC(),
			})
		}
		operation.RollbackPlan.Required = true
		operation.RollbackPlan.AutomaticExecution = false
		operation.RollbackPlan.Reason = "current wave failed; retry or a separately reviewed rollback plan is required"
		operation.RollbackPlan.ResolvedBy = ""
		operation.RollbackPlan.CandidateNodeIDs = successfulOperationNodeIDs(operation)
		return
	}
	if operation.State == StatePaused && operation.PauseCause == PauseCauseFailure {
		operation.RollbackPlan.CandidateNodeIDs = successfulOperationNodeIDs(operation)
		return
	}
	if wave.State == WaveSucceeded {
		operation.CurrentWaveIndex++
		if operation.CurrentWaveIndex == len(operation.Waves) {
			operation.State = StateCompleted
		} else {
			operation.State = StateReady
		}
		return
	}
	operation.State = StateRunning
}

func derivedWaveState(wave Wave) string {
	allSucceeded := len(wave.Actions) > 0
	hasInProgress := false
	for _, action := range wave.Actions {
		switch action.Status {
		case ActionFailed:
			return WaveFailed
		case ActionInProgress:
			hasInProgress = true
			allSucceeded = false
		case ActionPending:
			allSucceeded = false
		case ActionSucceeded:
		default:
			allSucceeded = false
		}
	}
	if allSucceeded {
		return WaveSucceeded
	}
	if hasInProgress {
		return WaveRunning
	}
	return WavePending
}

func successfulOperationNodeIDs(operation *Operation) []string {
	result := []string{}
	for _, wave := range operation.Waves {
		for _, action := range wave.Actions {
			if action.Status == ActionSucceeded {
				result = append(result, action.NodeID)
			}
		}
	}
	return result
}

func validateTransitionHistory(operation *Operation, waves map[string]bool) error {
	var previousRevision uint64
	var previousTime time.Time
	for i, record := range operation.TransitionHistory {
		switch record.Type {
		case TransitionFailurePause, TransitionOperatorPause, TransitionOperatorResume, TransitionRetry:
		default:
			return fmt.Errorf("transition history %d has invalid type %q", i, record.Type)
		}
		if record.Reason == "" || !waves[record.WaveID] || record.Revision < 2 || record.Revision > operation.Revision || record.At.IsZero() {
			return fmt.Errorf("transition history %d has incomplete operation binding", i)
		}
		if i > 0 && (record.Revision <= previousRevision || record.At.Before(previousTime)) {
			return fmt.Errorf("transition history %d is not revision/time ordered", i)
		}
		previousRevision, previousTime = record.Revision, record.At
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func currentWaveID(operation *Operation) string {
	if operation.CurrentWaveIndex >= 0 && operation.CurrentWaveIndex < len(operation.Waves) {
		return operation.Waves[operation.CurrentWaveIndex].WaveID
	}
	return ""
}

func findAction(operation *Operation, nodeID string) (int, int) {
	for i := range operation.Waves {
		for j := range operation.Waves[i].Actions {
			if operation.Waves[i].Actions[j].NodeID == nodeID {
				return i, j
			}
		}
	}
	return -1, -1
}

func validateTransitionTime(operation *Operation, at time.Time) error {
	if at.IsZero() {
		return fmt.Errorf("transition time is required")
	}
	if at.Before(operation.UpdatedAt) {
		return fmt.Errorf("transition time precedes operation revision timestamp")
	}
	return nil
}

func actionCounts(operation *Operation) (succeeded, failed, inProgress, pending int) {
	for _, wave := range operation.Waves {
		for _, action := range wave.Actions {
			switch action.Status {
			case ActionSucceeded:
				succeeded++
			case ActionFailed:
				failed++
			case ActionInProgress:
				inProgress++
			case ActionPending:
				pending++
			}
		}
	}
	return
}

func finalizeOperation(operation *Operation) {
	operation.ProjectedNodeActionCount = 0
	for _, wave := range operation.Waves {
		operation.ProjectedNodeActionCount += len(wave.Actions)
	}
	operation.SucceededNodeCount, operation.FailedNodeCount, operation.InProgressNodeCount, operation.PendingNodeCount = actionCounts(operation)
	operation.ExecutedTiKVMutations = 0
	operation.ExecutedDaemonActions = 0
	operation.ExecutedStorageMutations = 0
	operation.NoMutationStateMachine = true
	operation.Physical160Claimed = false
	operation.OperationDigest = digestOperation(operation)
}

func digestOperation(operation *Operation) string {
	copyOperation := *operation
	copyOperation.OperationDigest = ""
	raw, _ := json.Marshal(copyOperation)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func cloneOperation(operation *Operation) *Operation {
	raw, _ := json.Marshal(operation)
	var clone Operation
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
