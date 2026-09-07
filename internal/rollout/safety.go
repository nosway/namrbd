package rollout

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
)

const (
	ServiceSnapshotKind  = "SBSServiceActivationSnapshot"
	ServiceOperationKind = "SBSServiceActivationOperation"

	ServiceActivationReady     = "ready"
	ServiceActivationRunning   = "running"
	ServiceActivationPaused    = "paused"
	ServiceActivationCompleted = "completed"

	MaintenanceSnapshotKind      = "SBSHostMaintenanceSnapshot"
	MaintenancePlanKind          = "SBSHostMaintenancePlan"
	MaintenanceOperationKind     = "SBSHostMaintenanceOperation"
	MaintenanceExitPreflightKind = "SBSHostMaintenanceExitPreflight"

	MaintenanceStateEnterReady = "enter_ready"
	MaintenanceStateExitReady  = "exit_ready"

	MaintenanceCheckServiceQuorum = "AD_MAINT_SERVICE_QUORUM"
	MaintenanceCheckLeaderImpact  = "AD_MAINT_LEADER_IMPACT"
	MaintenanceCheckReplicaSafety = "AD_MAINT_REPLICA_FAILURE_DOMAIN"
	MaintenanceCheckCapacity      = "AD_MAINT_CAPACITY"
	MaintenanceCheckConflict      = "AD_MAINT_CONFLICTING_OPERATION"
	MaintenanceCheckConcurrency   = "AD_MAINT_CONCURRENCY"
)

const (
	defaultSafetySnapshotAge = 5 * time.Minute
	defaultSafetyFutureSkew  = 30 * time.Second
	maximumOverrideLifetime  = 24 * time.Hour
)

var requiredMaintenanceChecks = []string{
	MaintenanceCheckCapacity,
	MaintenanceCheckConcurrency,
	MaintenanceCheckConflict,
	MaintenanceCheckLeaderImpact,
	MaintenanceCheckReplicaSafety,
	MaintenanceCheckServiceQuorum,
}

type ServiceNodeObservation struct {
	NodeID         string `json:"node_id"`
	ManifestDigest string `json:"manifest_digest"`
	BundleDigest   string `json:"bundle_digest"`
	BinaryDigest   string `json:"binary_digest"`
	ConfigRevision int    `json:"config_revision"`
	DaemonRunning  bool   `json:"daemon_running"`
	Isolated       bool   `json:"isolated"`
	HealthReady    bool   `json:"health_ready"`
	MutationReady  bool   `json:"mutation_ready"`
	PDReachable    bool   `json:"pd_reachable"`
	TLSReady       bool   `json:"tls_ready"`
	ClockReady     bool   `json:"clock_ready"`
}

type ServiceActivationSnapshot struct {
	APIVersion                    string                   `json:"api_version"`
	Kind                          string                   `json:"kind"`
	SnapshotDigest                string                   `json:"snapshot_digest"`
	ManifestDigest                string                   `json:"manifest_digest"`
	ObservedAt                    time.Time                `json:"observed_at"`
	InFlightActivationOperationID string                   `json:"in_flight_activation_operation_id,omitempty"`
	LeaderNodeID                  string                   `json:"leader_node_id"`
	ProjectionHealthy             bool                     `json:"projection_healthy"`
	Nodes                         []ServiceNodeObservation `json:"nodes"`
	ExecutedTiKVMutations         int                      `json:"executed_tikv_mutations"`
	ExecutedDaemonActions         int                      `json:"executed_daemon_actions"`
	ExecutedStorageMutations      int                      `json:"executed_storage_mutations"`
	Physical160Claimed            bool                     `json:"physical_160_claimed"`
}

type ServiceActivationOperation struct {
	APIVersion                 string    `json:"api_version"`
	Kind                       string    `json:"kind"`
	OperationID                string    `json:"operation_id"`
	OperationDigest            string    `json:"operation_digest"`
	Revision                   uint64    `json:"revision"`
	State                      string    `json:"state"`
	ManifestDigest             string    `json:"manifest_digest"`
	JoinPlanDigest             string    `json:"join_plan_digest"`
	PreflightSnapshotDigest    string    `json:"preflight_snapshot_digest"`
	VerificationSnapshotDigest string    `json:"verification_snapshot_digest,omitempty"`
	FailedActiveNodeID         string    `json:"failed_active_node_id"`
	CandidateNodeID            string    `json:"candidate_node_id"`
	CandidateBundleDigest      string    `json:"candidate_bundle_digest"`
	ExpectedBinaryDigest       string    `json:"expected_binary_digest"`
	ExpectedConfigRevision     int       `json:"expected_config_revision"`
	Caller                     string    `json:"caller"`
	IncidentID                 string    `json:"incident_id"`
	Reason                     string    `json:"reason"`
	StartedAt                  time.Time `json:"started_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
	IdempotencyKey             string    `json:"idempotency_key"`
	Attempt                    int       `json:"attempt"`
	FirstError                 string    `json:"first_error"`
	LastError                  string    `json:"last_error"`
	RollbackRequired           bool      `json:"rollback_required"`
	AutomaticRollback          bool      `json:"automatic_rollback"`
	ProjectedDaemonActions     int       `json:"projected_daemon_actions"`
	ExecutedTiKVMutations      int       `json:"executed_tikv_mutations"`
	ExecutedDaemonActions      int       `json:"executed_daemon_actions"`
	ExecutedStorageMutations   int       `json:"executed_storage_mutations"`
	NoMutationRunbook          bool      `json:"no_mutation_runbook"`
	Physical160Claimed         bool      `json:"physical_160_claimed"`
}

type ServiceActivationInstruction struct {
	OperationID    string `json:"operation_id"`
	NodeID         string `json:"node_id"`
	Action         string `json:"action"`
	IdempotencyKey string `json:"idempotency_key"`
	Attempt        int    `json:"attempt"`
	Replayed       bool   `json:"replayed"`
}

type ServiceActivationRequest struct {
	Manifest           *clustermanifest.Manifest
	Policy             clustermanifest.ValidationPolicy
	JoinPlan           *installpreflight.JoinPlan
	Snapshot           *ServiceActivationSnapshot
	FailedActiveNodeID string
	CandidateNodeID    string
	Caller             string
	IncidentID         string
	Reason             string
	At                 time.Time
}

func BuildServiceActivation(request ServiceActivationRequest) (*ServiceActivationOperation, error) {
	if request.Manifest == nil || request.JoinPlan == nil || request.Snapshot == nil {
		return nil, fmt.Errorf("manifest, join plan, and service snapshot are required")
	}
	if err := installpreflight.ValidateJoinPlan(request.JoinPlan); err != nil {
		return nil, err
	}
	if err := ValidateServiceActivationSnapshot(request.Snapshot); err != nil {
		return nil, err
	}
	if err := validateSafetyTime(request.Snapshot.ObservedAt, request.At); err != nil {
		return nil, err
	}
	caller, incident, reason, err := validateAuditIdentity(request.Caller, request.IncidentID, request.Reason)
	if err != nil {
		return nil, err
	}
	canonical, rendered, manifestDigest, err := canonicalSafetyInputs(request.Manifest, request.Policy)
	if err != nil {
		return nil, err
	}
	if request.JoinPlan.ManifestDigest != manifestDigest || request.Snapshot.ManifestDigest != manifestDigest {
		return nil, fmt.Errorf("service activation manifest identity does not match")
	}
	if request.Snapshot.InFlightActivationOperationID != "" {
		return nil, fmt.Errorf("service activation %s is already in flight", request.Snapshot.InFlightActivationOperationID)
	}
	failed := strings.TrimSpace(request.FailedActiveNodeID)
	candidate := strings.TrimSpace(request.CandidateNodeID)
	if !containsSafetyString(canonical.Spec.ServicePlacement.ActiveHosts, failed) {
		return nil, fmt.Errorf("failed active node %q is not an active service host", failed)
	}
	if !containsSafetyString(canonical.Spec.ServicePlacement.StandbyCandidates, candidate) {
		return nil, fmt.Errorf("candidate node %q is not a standby service candidate", candidate)
	}
	observations, err := validateServiceSnapshotAgainstManifest(request.Snapshot, canonical, rendered, request.JoinPlan)
	if err != nil {
		return nil, err
	}
	failedObservation := observations[failed]
	if failedObservation.DaemonRunning || !failedObservation.Isolated {
		return nil, fmt.Errorf("failed active node %s must be stopped and isolated before standby activation", failed)
	}
	healthyActive := 0
	for _, nodeID := range canonical.Spec.ServicePlacement.ActiveHosts {
		if nodeID == failed {
			continue
		}
		observation := observations[nodeID]
		if !serviceNodeMutationReady(observation) || observation.Isolated {
			return nil, fmt.Errorf("remaining active service node %s is not healthy and mutation-ready", nodeID)
		}
		healthyActive++
	}
	if healthyActive != 2 || !request.Snapshot.ProjectionHealthy || !containsSafetyString(canonical.Spec.ServicePlacement.ActiveHosts, request.Snapshot.LeaderNodeID) || request.Snapshot.LeaderNodeID == failed {
		return nil, fmt.Errorf("remaining service leader/projection safety is not established")
	}
	for _, nodeID := range canonical.Spec.ServicePlacement.StandbyCandidates {
		observation := observations[nodeID]
		if observation.DaemonRunning {
			return nil, fmt.Errorf("standby candidate %s is already running; simultaneous activation is rejected", nodeID)
		}
	}
	candidateObservation := observations[candidate]
	if candidateObservation.Isolated || !candidateObservation.PDReachable || !candidateObservation.TLSReady || !candidateObservation.ClockReady {
		return nil, fmt.Errorf("standby candidate %s failed PD/TLS/clock activation preflight", candidate)
	}
	operationIdentityDigest := digestSafetyJSON(struct {
		SnapshotDigest  string `json:"snapshot_digest"`
		FailedNodeID    string `json:"failed_node_id"`
		CandidateNodeID string `json:"candidate_node_id"`
		IncidentID      string `json:"incident_id"`
	}{request.Snapshot.SnapshotDigest, failed, candidate, incident})
	operationID := "ad-service-activate-" + strings.TrimPrefix(operationIdentityDigest, "sha256:")[:16]
	operation := &ServiceActivationOperation{
		APIVersion: APIVersion, Kind: ServiceOperationKind, OperationID: operationID, Revision: 1,
		State: ServiceActivationReady, ManifestDigest: manifestDigest, JoinPlanDigest: request.JoinPlan.JoinPlanDigest,
		PreflightSnapshotDigest: request.Snapshot.SnapshotDigest, FailedActiveNodeID: failed, CandidateNodeID: candidate,
		CandidateBundleDigest: candidateObservation.BundleDigest, ExpectedBinaryDigest: candidateObservation.BinaryDigest,
		ExpectedConfigRevision: candidateObservation.ConfigRevision, Caller: caller, IncidentID: incident, Reason: reason,
		StartedAt: request.At.UTC(), UpdatedAt: request.At.UTC(),
		IdempotencyKey:         operationID + "/start-sbs-service/" + candidate,
		ProjectedDaemonActions: 1, NoMutationRunbook: true,
	}
	finalizeServiceActivation(operation)
	return operation, ValidateServiceActivationOperation(operation)
}

func IssueServiceActivation(operation *ServiceActivationOperation, at time.Time) (*ServiceActivationOperation, ServiceActivationInstruction, error) {
	if err := ValidateServiceActivationOperation(operation); err != nil {
		return nil, ServiceActivationInstruction{}, err
	}
	if err := validateTransitionTimeForSafety(operation.UpdatedAt, at); err != nil {
		return nil, ServiceActivationInstruction{}, err
	}
	if operation.State == ServiceActivationPaused || operation.State == ServiceActivationCompleted {
		return nil, ServiceActivationInstruction{}, fmt.Errorf("service activation is %s", operation.State)
	}
	next := cloneServiceActivation(operation)
	replayed := next.State == ServiceActivationRunning
	if !replayed {
		next.State = ServiceActivationRunning
		next.Attempt++
		next.Revision++
		next.UpdatedAt = at.UTC()
		finalizeServiceActivation(next)
	}
	instruction := ServiceActivationInstruction{
		OperationID: next.OperationID, NodeID: next.CandidateNodeID, Action: "start_sbs_service",
		IdempotencyKey: next.IdempotencyKey, Attempt: next.Attempt, Replayed: replayed,
	}
	return next, instruction, ValidateServiceActivationOperation(next)
}

func VerifyServiceActivation(operation *ServiceActivationOperation, manifest *clustermanifest.Manifest, policy clustermanifest.ValidationPolicy, snapshot *ServiceActivationSnapshot, at time.Time) (*ServiceActivationOperation, bool, error) {
	if err := ValidateServiceActivationOperation(operation); err != nil {
		return nil, false, err
	}
	if operation.State != ServiceActivationRunning {
		return nil, false, fmt.Errorf("service activation verification requires running state")
	}
	if err := ValidateServiceActivationSnapshot(snapshot); err != nil {
		return nil, false, err
	}
	if err := validateSafetyTime(snapshot.ObservedAt, at); err != nil {
		return nil, false, err
	}
	if err := validateTransitionTimeForSafety(operation.UpdatedAt, at); err != nil {
		return nil, false, err
	}
	canonical, rendered, manifestDigest, err := canonicalSafetyInputs(manifest, policy)
	if err != nil {
		return nil, false, err
	}
	if manifestDigest != operation.ManifestDigest || snapshot.ManifestDigest != manifestDigest {
		return nil, false, fmt.Errorf("service verification manifest identity does not match")
	}
	if snapshot.InFlightActivationOperationID != operation.OperationID {
		return nil, false, fmt.Errorf("service verification snapshot is not bound to operation %s", operation.OperationID)
	}
	observations, err := validateServiceSnapshotIdentity(snapshot, canonical, rendered)
	if err != nil {
		return nil, false, err
	}
	problems := []string{}
	if observation := observations[operation.FailedActiveNodeID]; observation.DaemonRunning || !observation.Isolated {
		problems = append(problems, "failed active is not stopped and isolated")
	}
	running := []string{}
	for _, nodeID := range append(append([]string{}, canonical.Spec.ServicePlacement.ActiveHosts...), canonical.Spec.ServicePlacement.StandbyCandidates...) {
		observation := observations[nodeID]
		if observation.DaemonRunning {
			running = append(running, nodeID)
			if !serviceNodeMutationReady(observation) || observation.Isolated {
				problems = append(problems, nodeID+" is running but not mutation-ready")
			}
		}
	}
	sort.Strings(running)
	wantRunning := []string{operation.CandidateNodeID}
	for _, nodeID := range canonical.Spec.ServicePlacement.ActiveHosts {
		if nodeID != operation.FailedActiveNodeID {
			wantRunning = append(wantRunning, nodeID)
		}
	}
	sort.Strings(wantRunning)
	if !sameStrings(running, wantRunning) {
		problems = append(problems, fmt.Sprintf("running service set is %v, want %v", running, wantRunning))
	}
	if !snapshot.ProjectionHealthy || !containsSafetyString(wantRunning, snapshot.LeaderNodeID) {
		problems = append(problems, "leader/projection verification failed")
	}
	next := cloneServiceActivation(operation)
	next.VerificationSnapshotDigest = snapshot.SnapshotDigest
	next.Revision++
	next.UpdatedAt = at.UTC()
	if len(problems) == 0 {
		next.State = ServiceActivationCompleted
	} else {
		next.State = ServiceActivationPaused
		next.FirstError = problems[0]
		next.LastError = problems[len(problems)-1]
		next.RollbackRequired = true
	}
	finalizeServiceActivation(next)
	return next, len(problems) == 0, ValidateServiceActivationOperation(next)
}

func ParseServiceActivationSnapshot(raw []byte) (*ServiceActivationSnapshot, error) {
	value := new(ServiceActivationSnapshot)
	if err := decodeStrict(raw, value, "service activation snapshot"); err != nil {
		return nil, err
	}
	return value, ValidateServiceActivationSnapshot(value)
}

func MarshalServiceActivationSnapshot(snapshot *ServiceActivationSnapshot) ([]byte, error) {
	if err := ValidateServiceActivationSnapshot(snapshot); err != nil {
		return nil, err
	}
	return marshalIndented(snapshot, "service activation snapshot")
}

func FinalizeServiceActivationSnapshot(snapshot *ServiceActivationSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("service activation snapshot is nil")
	}
	sort.Slice(snapshot.Nodes, func(i, j int) bool { return snapshot.Nodes[i].NodeID < snapshot.Nodes[j].NodeID })
	snapshot.SnapshotDigest = digestServiceSnapshot(snapshot)
	return ValidateServiceActivationSnapshot(snapshot)
}

func ValidateServiceActivationSnapshot(snapshot *ServiceActivationSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("service activation snapshot is nil")
	}
	if snapshot.APIVersion != APIVersion || snapshot.Kind != ServiceSnapshotKind || !canonicalSafetyDigest(snapshot.ManifestDigest) || snapshot.ObservedAt.IsZero() {
		return fmt.Errorf("service activation snapshot identity is invalid")
	}
	if len(snapshot.Nodes) != clustermanifest.ExactActiveServiceCount+clustermanifest.ExactStandbyServiceCount {
		return fmt.Errorf("service activation snapshot must contain exactly five service nodes")
	}
	seen := map[string]bool{}
	previous := ""
	for _, node := range snapshot.Nodes {
		if node.NodeID == "" || seen[node.NodeID] || (previous != "" && node.NodeID <= previous) || !canonicalSafetyDigest(node.ManifestDigest) || !canonicalSafetyDigest(node.BundleDigest) || !canonicalSafetyDigest(node.BinaryDigest) || node.ConfigRevision <= 0 {
			return fmt.Errorf("service activation snapshot has invalid, duplicate, or unsorted node %q", node.NodeID)
		}
		seen[node.NodeID], previous = true, node.NodeID
	}
	if snapshot.ExecutedTiKVMutations != 0 || snapshot.ExecutedDaemonActions != 0 || snapshot.ExecutedStorageMutations != 0 || snapshot.Physical160Claimed {
		return fmt.Errorf("service activation snapshot violates the observation-only boundary")
	}
	if snapshot.SnapshotDigest != digestServiceSnapshot(snapshot) {
		return fmt.Errorf("service activation snapshot digest does not match canonical content")
	}
	return nil
}

func ParseServiceActivationOperation(raw []byte) (*ServiceActivationOperation, error) {
	value := new(ServiceActivationOperation)
	if err := decodeStrict(raw, value, "service activation operation"); err != nil {
		return nil, err
	}
	return value, ValidateServiceActivationOperation(value)
}

func MarshalServiceActivationOperation(operation *ServiceActivationOperation) ([]byte, error) {
	if err := ValidateServiceActivationOperation(operation); err != nil {
		return nil, err
	}
	return marshalIndented(operation, "service activation operation")
}

func ValidateServiceActivationOperation(operation *ServiceActivationOperation) error {
	if operation == nil {
		return fmt.Errorf("service activation operation is nil")
	}
	if operation.APIVersion != APIVersion || operation.Kind != ServiceOperationKind || operation.OperationID == "" || operation.Revision == 0 || !canonicalSafetyDigest(operation.ManifestDigest) || !canonicalSafetyDigest(operation.JoinPlanDigest) || !canonicalSafetyDigest(operation.PreflightSnapshotDigest) {
		return fmt.Errorf("service activation operation identity is invalid")
	}
	if operation.FailedActiveNodeID == "" || operation.CandidateNodeID == "" || operation.FailedActiveNodeID == operation.CandidateNodeID || !canonicalSafetyDigest(operation.CandidateBundleDigest) || !canonicalSafetyDigest(operation.ExpectedBinaryDigest) || operation.ExpectedConfigRevision <= 0 || operation.Caller == "" || operation.IncidentID == "" || operation.Reason == "" || operation.IdempotencyKey == "" {
		return fmt.Errorf("service activation operation preflight binding is incomplete")
	}
	if operation.StartedAt.IsZero() || operation.UpdatedAt.Before(operation.StartedAt) || operation.Attempt < 0 || operation.Attempt > 1 || operation.ProjectedDaemonActions != 1 || !operation.NoMutationRunbook || operation.AutomaticRollback || operation.ExecutedTiKVMutations != 0 || operation.ExecutedDaemonActions != 0 || operation.ExecutedStorageMutations != 0 || operation.Physical160Claimed {
		return fmt.Errorf("service activation operation violates runbook invariants")
	}
	switch operation.State {
	case ServiceActivationReady:
		if operation.Attempt != 0 || operation.VerificationSnapshotDigest != "" {
			return fmt.Errorf("ready service activation has issued or verified state")
		}
	case ServiceActivationRunning:
		if operation.Attempt != 1 || operation.VerificationSnapshotDigest != "" {
			return fmt.Errorf("running service activation has invalid attempt/verification state")
		}
	case ServiceActivationPaused:
		if operation.Attempt != 1 || !canonicalSafetyDigest(operation.VerificationSnapshotDigest) || operation.FirstError == "" || operation.LastError == "" || !operation.RollbackRequired {
			return fmt.Errorf("paused service activation lacks failure/rollback evidence")
		}
	case ServiceActivationCompleted:
		if operation.Attempt != 1 || !canonicalSafetyDigest(operation.VerificationSnapshotDigest) || operation.FirstError != "" || operation.LastError != "" || operation.RollbackRequired {
			return fmt.Errorf("completed service activation has inconsistent verification evidence")
		}
	default:
		return fmt.Errorf("service activation operation has invalid state %q", operation.State)
	}
	if operation.OperationDigest != digestServiceOperation(operation) {
		return fmt.Errorf("service activation operation digest does not match canonical content")
	}
	return nil
}

type MaintenanceCheck struct {
	ID       string `json:"id"`
	Safe     bool   `json:"safe"`
	Observed string `json:"observed"`
	Message  string `json:"message,omitempty"`
}

type HostMaintenanceSnapshot struct {
	APIVersion               string             `json:"api_version"`
	Kind                     string             `json:"kind"`
	SnapshotDigest           string             `json:"snapshot_digest"`
	ManifestDigest           string             `json:"manifest_digest"`
	NodeID                   string             `json:"node_id"`
	BundleDigest             string             `json:"bundle_digest"`
	ObservedAt               time.Time          `json:"observed_at"`
	AffectedPlacementCount   int                `json:"affected_placement_count"`
	ExpectedMoveBytes        uint64             `json:"expected_move_bytes"`
	ActiveMaintenanceCount   int                `json:"active_maintenance_count"`
	MaintenanceConcurrency   int                `json:"maintenance_concurrency"`
	Checks                   []MaintenanceCheck `json:"checks"`
	ExecutedTiKVMutations    int                `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                `json:"executed_storage_mutations"`
	Physical160Claimed       bool               `json:"physical_160_claimed"`
}

type HostMaintenancePlan struct {
	APIVersion               string    `json:"api_version"`
	Kind                     string    `json:"kind"`
	PlanID                   string    `json:"plan_id"`
	PlanDigest               string    `json:"plan_digest"`
	ManifestDigest           string    `json:"manifest_digest"`
	SnapshotDigest           string    `json:"snapshot_digest"`
	NodeID                   string    `json:"node_id"`
	BundleDigest             string    `json:"bundle_digest"`
	CreatedAt                time.Time `json:"created_at"`
	AffectedPlacementCount   int       `json:"affected_placement_count"`
	ExpectedMoveBytes        uint64    `json:"expected_move_bytes"`
	UnsafeCheckIDs           []string  `json:"unsafe_check_ids"`
	ProjectedDrainActions    int       `json:"projected_drain_actions"`
	ProjectedDaemonActions   int       `json:"projected_daemon_actions"`
	ExecutedTiKVMutations    int       `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int       `json:"executed_daemon_actions"`
	ExecutedStorageMutations int       `json:"executed_storage_mutations"`
	NoMutationPlan           bool      `json:"no_mutation_plan"`
	Physical160Claimed       bool      `json:"physical_160_claimed"`
}

type MaintenanceOverrideAudit struct {
	CheckIDs   []string  `json:"check_ids"`
	Caller     string    `json:"caller"`
	IncidentID string    `json:"incident_id"`
	Reason     string    `json:"reason"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type MaintenanceInstruction struct {
	Order          int    `json:"order"`
	Action         string `json:"action"`
	NodeID         string `json:"node_id"`
	IdempotencyKey string `json:"idempotency_key"`
	SafetyGate     string `json:"safety_gate"`
}

type HostMaintenanceOperation struct {
	APIVersion               string                    `json:"api_version"`
	Kind                     string                    `json:"kind"`
	OperationID              string                    `json:"operation_id"`
	OperationDigest          string                    `json:"operation_digest"`
	Revision                 uint64                    `json:"revision"`
	State                    string                    `json:"state"`
	PlanID                   string                    `json:"plan_id"`
	PlanDigest               string                    `json:"plan_digest"`
	ManifestDigest           string                    `json:"manifest_digest"`
	NodeID                   string                    `json:"node_id"`
	BundleDigest             string                    `json:"bundle_digest"`
	Caller                   string                    `json:"caller"`
	IncidentID               string                    `json:"incident_id"`
	Reason                   string                    `json:"reason"`
	CreatedAt                time.Time                 `json:"created_at"`
	UpdatedAt                time.Time                 `json:"updated_at"`
	Override                 *MaintenanceOverrideAudit `json:"override,omitempty"`
	UnsafeCheckIDs           []string                  `json:"unsafe_check_ids"`
	AffectedPlacementCount   int                       `json:"affected_placement_count"`
	ExpectedMoveBytes        uint64                    `json:"expected_move_bytes"`
	Instructions             []MaintenanceInstruction  `json:"instructions"`
	ExitPreflightDigest      string                    `json:"exit_preflight_digest,omitempty"`
	ProjectedDrainActions    int                       `json:"projected_drain_actions"`
	ProjectedDaemonActions   int                       `json:"projected_daemon_actions"`
	ExecutedTiKVMutations    int                       `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                       `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                       `json:"executed_storage_mutations"`
	NoMutationRunbook        bool                      `json:"no_mutation_runbook"`
	Physical160Claimed       bool                      `json:"physical_160_claimed"`
}

type HostMaintenanceExitPreflight struct {
	APIVersion               string    `json:"api_version"`
	Kind                     string    `json:"kind"`
	PreflightDigest          string    `json:"preflight_digest"`
	OperationID              string    `json:"operation_id"`
	PlanID                   string    `json:"plan_id"`
	ManifestDigest           string    `json:"manifest_digest"`
	NodeID                   string    `json:"node_id"`
	BundleDigest             string    `json:"bundle_digest"`
	HostReportDigest         string    `json:"host_report_digest"`
	ObservedAt               time.Time `json:"observed_at"`
	MaintenanceActive        bool      `json:"maintenance_active"`
	DrainComplete            bool      `json:"drain_complete"`
	DaemonStopped            bool      `json:"daemon_stopped"`
	HostIdentityVerified     bool      `json:"host_identity_verified"`
	HostPreflightPassed      bool      `json:"host_preflight_passed"`
	StorageClaimsMatch       bool      `json:"storage_claims_match"`
	BinaryDigestMatch        bool      `json:"binary_digest_match"`
	ConfigRevisionMatch      bool      `json:"config_revision_match"`
	LocalStoreHealthy        bool      `json:"local_store_healthy"`
	ExecutedTiKVMutations    int       `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int       `json:"executed_daemon_actions"`
	ExecutedStorageMutations int       `json:"executed_storage_mutations"`
	Physical160Claimed       bool      `json:"physical_160_claimed"`
}

func BuildHostMaintenancePlan(manifest *clustermanifest.Manifest, policy clustermanifest.ValidationPolicy, snapshot *HostMaintenanceSnapshot, at time.Time) (*HostMaintenancePlan, error) {
	if err := ValidateHostMaintenanceSnapshot(snapshot); err != nil {
		return nil, err
	}
	if err := validateSafetyTime(snapshot.ObservedAt, at); err != nil {
		return nil, err
	}
	canonical, rendered, manifestDigest, err := canonicalSafetyInputs(manifest, policy)
	if err != nil {
		return nil, err
	}
	bundles := safetyBundleMap(rendered)
	if snapshot.ManifestDigest != manifestDigest || !safetyManifestNode(canonical, snapshot.NodeID) || bundles[snapshot.NodeID].BundleDigest != snapshot.BundleDigest {
		return nil, fmt.Errorf("maintenance snapshot does not match the manifest node/bundle identity")
	}
	unsafe := []string{}
	for _, check := range snapshot.Checks {
		if !check.Safe {
			unsafe = append(unsafe, check.ID)
		}
	}
	plan := &HostMaintenancePlan{
		APIVersion: APIVersion, Kind: MaintenancePlanKind, ManifestDigest: manifestDigest,
		SnapshotDigest: snapshot.SnapshotDigest, NodeID: snapshot.NodeID, BundleDigest: snapshot.BundleDigest,
		CreatedAt: at.UTC(), AffectedPlacementCount: snapshot.AffectedPlacementCount,
		ExpectedMoveBytes: snapshot.ExpectedMoveBytes, UnsafeCheckIDs: unsafe,
		ProjectedDrainActions: 1, ProjectedDaemonActions: 1, NoMutationPlan: true,
	}
	plan.PlanDigest = digestMaintenancePlan(plan)
	plan.PlanID = "ad-maint-plan-" + strings.TrimPrefix(plan.PlanDigest, "sha256:")[:16]
	return plan, ValidateHostMaintenancePlan(plan)
}

func EnterHostMaintenance(plan *HostMaintenancePlan, caller, incident, reason string, override *MaintenanceOverrideAudit, at time.Time) (*HostMaintenanceOperation, error) {
	if err := ValidateHostMaintenancePlan(plan); err != nil {
		return nil, err
	}
	caller, incident, reason, err := validateAuditIdentity(caller, incident, reason)
	if err != nil {
		return nil, err
	}
	if at.Before(plan.CreatedAt) {
		return nil, fmt.Errorf("maintenance enter time precedes plan creation")
	}
	if err := validateMaintenanceOverride(plan.UnsafeCheckIDs, override, caller, incident, at); err != nil {
		return nil, err
	}
	operationIdentityDigest := digestSafetyJSON(struct {
		PlanDigest string `json:"plan_digest"`
		Caller     string `json:"caller"`
		IncidentID string `json:"incident_id"`
	}{plan.PlanDigest, caller, incident})
	operationID := "ad-maint-" + strings.TrimPrefix(operationIdentityDigest, "sha256:")[:16]
	operation := &HostMaintenanceOperation{
		APIVersion: APIVersion, Kind: MaintenanceOperationKind, OperationID: operationID,
		Revision: 1, State: MaintenanceStateEnterReady, PlanID: plan.PlanID, PlanDigest: plan.PlanDigest,
		ManifestDigest: plan.ManifestDigest, NodeID: plan.NodeID, BundleDigest: plan.BundleDigest,
		Caller: caller, IncidentID: incident, Reason: reason, CreatedAt: at.UTC(), UpdatedAt: at.UTC(),
		Override: cloneMaintenanceOverride(override), UnsafeCheckIDs: append([]string(nil), plan.UnsafeCheckIDs...),
		AffectedPlacementCount: plan.AffectedPlacementCount, ExpectedMoveBytes: plan.ExpectedMoveBytes,
		ProjectedDrainActions: 1, ProjectedDaemonActions: 1, NoMutationRunbook: true,
		Instructions: []MaintenanceInstruction{
			{Order: 1, Action: "enqueue_affected_placement_drain", NodeID: plan.NodeID, IdempotencyKey: operationID + "/drain", SafetyGate: "affected-set-only"},
			{Order: 2, Action: "stop_sbs_data_after_drain", NodeID: plan.NodeID, IdempotencyKey: operationID + "/stop-sbs-data", SafetyGate: "drain-complete-and-replica-safe"},
		},
	}
	finalizeMaintenanceOperation(operation)
	return operation, ValidateHostMaintenanceOperation(operation)
}

func ExitHostMaintenance(operation *HostMaintenanceOperation, preflight *HostMaintenanceExitPreflight, at time.Time) (*HostMaintenanceOperation, error) {
	if err := ValidateHostMaintenanceOperation(operation); err != nil {
		return nil, err
	}
	if operation.State != MaintenanceStateEnterReady {
		return nil, fmt.Errorf("maintenance exit requires an enter-ready operation")
	}
	if err := ValidateHostMaintenanceExitPreflight(preflight); err != nil {
		return nil, err
	}
	if err := validateSafetyTime(preflight.ObservedAt, at); err != nil {
		return nil, err
	}
	if err := validateTransitionTimeForSafety(operation.UpdatedAt, at); err != nil {
		return nil, err
	}
	if preflight.OperationID != operation.OperationID || preflight.PlanID != operation.PlanID || preflight.ManifestDigest != operation.ManifestDigest || preflight.NodeID != operation.NodeID || preflight.BundleDigest != operation.BundleDigest {
		return nil, fmt.Errorf("maintenance exit preflight does not match operation identity")
	}
	if !preflight.MaintenanceActive || !preflight.DrainComplete || !preflight.DaemonStopped || !preflight.HostIdentityVerified || !preflight.HostPreflightPassed || !preflight.StorageClaimsMatch || !preflight.BinaryDigestMatch || !preflight.ConfigRevisionMatch || !preflight.LocalStoreHealthy {
		return nil, fmt.Errorf("maintenance exit preflight is blocked")
	}
	next := cloneMaintenanceOperation(operation)
	next.State = MaintenanceStateExitReady
	next.Revision++
	next.UpdatedAt = at.UTC()
	next.ExitPreflightDigest = preflight.PreflightDigest
	next.Instructions = []MaintenanceInstruction{
		{Order: 1, Action: "start_sbs_data", NodeID: next.NodeID, IdempotencyKey: next.OperationID + "/start-sbs-data", SafetyGate: "exit-preflight-passed"},
		{Order: 2, Action: "restore_scheduling_eligibility_after_health", NodeID: next.NodeID, IdempotencyKey: next.OperationID + "/restore-eligibility", SafetyGate: "daemon-health-and-identity-reverified"},
	}
	finalizeMaintenanceOperation(next)
	return next, ValidateHostMaintenanceOperation(next)
}

func FinalizeHostMaintenanceSnapshot(snapshot *HostMaintenanceSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("host maintenance snapshot is nil")
	}
	sort.Slice(snapshot.Checks, func(i, j int) bool { return snapshot.Checks[i].ID < snapshot.Checks[j].ID })
	snapshot.SnapshotDigest = digestMaintenanceSnapshot(snapshot)
	return ValidateHostMaintenanceSnapshot(snapshot)
}

func ValidateHostMaintenanceSnapshot(snapshot *HostMaintenanceSnapshot) error {
	if snapshot == nil || snapshot.APIVersion != APIVersion || snapshot.Kind != MaintenanceSnapshotKind || !canonicalSafetyDigest(snapshot.ManifestDigest) || !canonicalSafetyDigest(snapshot.BundleDigest) || snapshot.NodeID == "" || snapshot.ObservedAt.IsZero() {
		return fmt.Errorf("host maintenance snapshot identity is invalid")
	}
	if snapshot.AffectedPlacementCount < 0 || snapshot.ActiveMaintenanceCount < 0 || snapshot.MaintenanceConcurrency <= 0 {
		return fmt.Errorf("host maintenance snapshot counts are invalid")
	}
	ids := []string{}
	for _, check := range snapshot.Checks {
		if check.ID == "" || strings.TrimSpace(check.Observed) == "" || (!check.Safe && strings.TrimSpace(check.Message) == "") {
			return fmt.Errorf("host maintenance snapshot has incomplete check %q", check.ID)
		}
		ids = append(ids, check.ID)
	}
	if !sameStrings(ids, requiredMaintenanceChecks) {
		return fmt.Errorf("host maintenance snapshot check set is not the exact required set")
	}
	if snapshot.ExecutedTiKVMutations != 0 || snapshot.ExecutedDaemonActions != 0 || snapshot.ExecutedStorageMutations != 0 || snapshot.Physical160Claimed {
		return fmt.Errorf("host maintenance snapshot violates the observation-only boundary")
	}
	if snapshot.SnapshotDigest != digestMaintenanceSnapshot(snapshot) {
		return fmt.Errorf("host maintenance snapshot digest does not match canonical content")
	}
	return nil
}

func ParseHostMaintenanceSnapshot(raw []byte) (*HostMaintenanceSnapshot, error) {
	value := new(HostMaintenanceSnapshot)
	if err := decodeStrict(raw, value, "host maintenance snapshot"); err != nil {
		return nil, err
	}
	return value, ValidateHostMaintenanceSnapshot(value)
}

func MarshalHostMaintenanceSnapshot(snapshot *HostMaintenanceSnapshot) ([]byte, error) {
	if err := ValidateHostMaintenanceSnapshot(snapshot); err != nil {
		return nil, err
	}
	return marshalIndented(snapshot, "host maintenance snapshot")
}

func ParseHostMaintenancePlan(raw []byte) (*HostMaintenancePlan, error) {
	value := new(HostMaintenancePlan)
	if err := decodeStrict(raw, value, "host maintenance plan"); err != nil {
		return nil, err
	}
	return value, ValidateHostMaintenancePlan(value)
}

func MarshalHostMaintenancePlan(plan *HostMaintenancePlan) ([]byte, error) {
	if err := ValidateHostMaintenancePlan(plan); err != nil {
		return nil, err
	}
	return marshalIndented(plan, "host maintenance plan")
}

func ValidateHostMaintenancePlan(plan *HostMaintenancePlan) error {
	if plan == nil || plan.APIVersion != APIVersion || plan.Kind != MaintenancePlanKind || plan.PlanID == "" || !canonicalSafetyDigest(plan.ManifestDigest) || !canonicalSafetyDigest(plan.SnapshotDigest) || !canonicalSafetyDigest(plan.BundleDigest) || plan.NodeID == "" || plan.CreatedAt.IsZero() {
		return fmt.Errorf("host maintenance plan identity is invalid")
	}
	if !sort.StringsAreSorted(plan.UnsafeCheckIDs) || hasDuplicateSafetyStrings(plan.UnsafeCheckIDs) {
		return fmt.Errorf("host maintenance plan unsafe check IDs are not unique and sorted")
	}
	for _, id := range plan.UnsafeCheckIDs {
		if !containsSafetyString(requiredMaintenanceChecks, id) {
			return fmt.Errorf("host maintenance plan has unknown unsafe check %s", id)
		}
	}
	if plan.ProjectedDrainActions != 1 || plan.ProjectedDaemonActions != 1 || !plan.NoMutationPlan || plan.ExecutedTiKVMutations != 0 || plan.ExecutedDaemonActions != 0 || plan.ExecutedStorageMutations != 0 || plan.Physical160Claimed {
		return fmt.Errorf("host maintenance plan violates no-mutation invariants")
	}
	expectedDigest := digestMaintenancePlan(plan)
	if plan.PlanDigest != expectedDigest || plan.PlanID != "ad-maint-plan-"+strings.TrimPrefix(expectedDigest, "sha256:")[:16] {
		return fmt.Errorf("host maintenance plan digest/ID does not match canonical content")
	}
	return nil
}

func ParseHostMaintenanceOperation(raw []byte) (*HostMaintenanceOperation, error) {
	value := new(HostMaintenanceOperation)
	if err := decodeStrict(raw, value, "host maintenance operation"); err != nil {
		return nil, err
	}
	return value, ValidateHostMaintenanceOperation(value)
}

func MarshalHostMaintenanceOperation(operation *HostMaintenanceOperation) ([]byte, error) {
	if err := ValidateHostMaintenanceOperation(operation); err != nil {
		return nil, err
	}
	return marshalIndented(operation, "host maintenance operation")
}

func ValidateHostMaintenanceOperation(operation *HostMaintenanceOperation) error {
	if operation == nil || operation.APIVersion != APIVersion || operation.Kind != MaintenanceOperationKind || operation.OperationID == "" || operation.Revision == 0 || operation.PlanID == "" || !canonicalSafetyDigest(operation.PlanDigest) || !canonicalSafetyDigest(operation.ManifestDigest) || !canonicalSafetyDigest(operation.BundleDigest) || operation.NodeID == "" {
		return fmt.Errorf("host maintenance operation identity is invalid")
	}
	if operation.Caller == "" || operation.IncidentID == "" || operation.Reason == "" || operation.CreatedAt.IsZero() || operation.UpdatedAt.Before(operation.CreatedAt) || operation.AffectedPlacementCount < 0 || operation.ProjectedDrainActions != 1 || operation.ProjectedDaemonActions != 1 || !operation.NoMutationRunbook || operation.ExecutedTiKVMutations != 0 || operation.ExecutedDaemonActions != 0 || operation.ExecutedStorageMutations != 0 || operation.Physical160Claimed {
		return fmt.Errorf("host maintenance operation violates runbook invariants")
	}
	if !sort.StringsAreSorted(operation.UnsafeCheckIDs) || hasDuplicateSafetyStrings(operation.UnsafeCheckIDs) {
		return fmt.Errorf("host maintenance operation unsafe checks are not unique and sorted")
	}
	if len(operation.UnsafeCheckIDs) == 0 && operation.Override != nil {
		return fmt.Errorf("safe maintenance operation must not contain an override")
	}
	if len(operation.UnsafeCheckIDs) > 0 {
		if err := validateMaintenanceOverride(operation.UnsafeCheckIDs, operation.Override, operation.Caller, operation.IncidentID, operation.CreatedAt); err != nil {
			return err
		}
	}
	if len(operation.Instructions) != 2 || operation.Instructions[0].Order != 1 || operation.Instructions[1].Order != 2 {
		return fmt.Errorf("host maintenance operation must contain two ordered guarded instructions")
	}
	switch operation.State {
	case MaintenanceStateEnterReady:
		if operation.Revision != 1 || operation.ExitPreflightDigest != "" || operation.Instructions[0].Action != "enqueue_affected_placement_drain" || operation.Instructions[1].Action != "stop_sbs_data_after_drain" {
			return fmt.Errorf("enter-ready maintenance operation has invalid instructions")
		}
	case MaintenanceStateExitReady:
		if operation.Revision != 2 || !canonicalSafetyDigest(operation.ExitPreflightDigest) || operation.Instructions[0].Action != "start_sbs_data" || operation.Instructions[1].Action != "restore_scheduling_eligibility_after_health" {
			return fmt.Errorf("exit-ready maintenance operation has invalid preflight/instructions")
		}
	default:
		return fmt.Errorf("host maintenance operation has invalid state %q", operation.State)
	}
	for _, instruction := range operation.Instructions {
		if instruction.NodeID != operation.NodeID || instruction.IdempotencyKey == "" || instruction.SafetyGate == "" {
			return fmt.Errorf("host maintenance instruction is not bound to operation node/safety")
		}
	}
	if operation.OperationDigest != digestMaintenanceOperation(operation) {
		return fmt.Errorf("host maintenance operation digest does not match canonical content")
	}
	return nil
}

func FinalizeHostMaintenanceExitPreflight(preflight *HostMaintenanceExitPreflight) error {
	if preflight == nil {
		return fmt.Errorf("host maintenance exit preflight is nil")
	}
	preflight.PreflightDigest = digestMaintenanceExitPreflight(preflight)
	return ValidateHostMaintenanceExitPreflight(preflight)
}

func ParseHostMaintenanceExitPreflight(raw []byte) (*HostMaintenanceExitPreflight, error) {
	value := new(HostMaintenanceExitPreflight)
	if err := decodeStrict(raw, value, "host maintenance exit preflight"); err != nil {
		return nil, err
	}
	return value, ValidateHostMaintenanceExitPreflight(value)
}

func MarshalHostMaintenanceExitPreflight(preflight *HostMaintenanceExitPreflight) ([]byte, error) {
	if err := ValidateHostMaintenanceExitPreflight(preflight); err != nil {
		return nil, err
	}
	return marshalIndented(preflight, "host maintenance exit preflight")
}

func ValidateHostMaintenanceExitPreflight(preflight *HostMaintenanceExitPreflight) error {
	if preflight == nil || preflight.APIVersion != APIVersion || preflight.Kind != MaintenanceExitPreflightKind || preflight.OperationID == "" || preflight.PlanID == "" || !canonicalSafetyDigest(preflight.ManifestDigest) || preflight.NodeID == "" || !canonicalSafetyDigest(preflight.BundleDigest) || !canonicalSafetyDigest(preflight.HostReportDigest) || preflight.ObservedAt.IsZero() {
		return fmt.Errorf("host maintenance exit preflight identity is invalid")
	}
	if preflight.ExecutedTiKVMutations != 0 || preflight.ExecutedDaemonActions != 0 || preflight.ExecutedStorageMutations != 0 || preflight.Physical160Claimed {
		return fmt.Errorf("host maintenance exit preflight violates observation-only invariants")
	}
	if preflight.PreflightDigest != digestMaintenanceExitPreflight(preflight) {
		return fmt.Errorf("host maintenance exit preflight digest does not match canonical content")
	}
	return nil
}

func validateServiceSnapshotAgainstManifest(snapshot *ServiceActivationSnapshot, manifest *clustermanifest.Manifest, rendered *clustermanifest.RenderSet, joinPlan *installpreflight.JoinPlan) (map[string]ServiceNodeObservation, error) {
	observations, err := validateServiceSnapshotIdentity(snapshot, manifest, rendered)
	if err != nil {
		return nil, err
	}
	joinBundles := map[string]string{}
	for _, zone := range joinPlan.Zones {
		for _, node := range zone.Nodes {
			joinBundles[node.NodeID] = node.BundleDigest
		}
	}
	for nodeID, observation := range observations {
		if joinBundles[nodeID] != observation.BundleDigest {
			return nil, fmt.Errorf("service node %s bundle is not admitted by the join plan", nodeID)
		}
	}
	return observations, nil
}

func validateServiceSnapshotIdentity(snapshot *ServiceActivationSnapshot, manifest *clustermanifest.Manifest, rendered *clustermanifest.RenderSet) (map[string]ServiceNodeObservation, error) {
	bundles := safetyBundleMap(rendered)
	expected := append(append([]string{}, manifest.Spec.ServicePlacement.ActiveHosts...), manifest.Spec.ServicePlacement.StandbyCandidates...)
	sort.Strings(expected)
	observations := map[string]ServiceNodeObservation{}
	observedIDs := []string{}
	for _, observation := range snapshot.Nodes {
		observations[observation.NodeID] = observation
		observedIDs = append(observedIDs, observation.NodeID)
	}
	if !sameStrings(observedIDs, expected) {
		return nil, fmt.Errorf("service snapshot node set is %v, want %v", observedIDs, expected)
	}
	wantBinary := manifest.Spec.Artifact.BinaryDigests["sbs-service"]
	for _, nodeID := range expected {
		observation := observations[nodeID]
		if observation.ManifestDigest != snapshot.ManifestDigest || observation.BundleDigest != bundles[nodeID].BundleDigest || observation.BinaryDigest != wantBinary || observation.ConfigRevision != manifest.Spec.Bundle.ConfigRevision {
			return nil, fmt.Errorf("service node %s has manifest/bundle/binary/config drift", nodeID)
		}
	}
	return observations, nil
}

func canonicalSafetyInputs(manifest *clustermanifest.Manifest, policy clustermanifest.ValidationPolicy) (*clustermanifest.Manifest, *clustermanifest.RenderSet, string, error) {
	if manifest == nil {
		return nil, nil, "", fmt.Errorf("manifest is required")
	}
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		return nil, nil, "", err
	}
	rendered, err := clustermanifest.Render(canonical, policy)
	if err != nil {
		return nil, nil, "", err
	}
	return canonical, rendered, rendered.ManifestDigest, nil
}

func safetyBundleMap(rendered *clustermanifest.RenderSet) map[string]clustermanifest.NodeBundle {
	result := map[string]clustermanifest.NodeBundle{}
	for _, bundle := range rendered.Bundles {
		result[bundle.NodeID] = bundle
	}
	return result
}

func safetyManifestNode(manifest *clustermanifest.Manifest, nodeID string) bool {
	for _, node := range manifest.Spec.Nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

func serviceNodeMutationReady(observation ServiceNodeObservation) bool {
	return observation.DaemonRunning && observation.HealthReady && observation.MutationReady && observation.PDReachable && observation.TLSReady && observation.ClockReady
}

func validateMaintenanceOverride(unsafe []string, override *MaintenanceOverrideAudit, caller, incident string, at time.Time) error {
	if len(unsafe) == 0 {
		if override != nil {
			return fmt.Errorf("maintenance override is not allowed when every safety check passes")
		}
		return nil
	}
	if override == nil {
		return fmt.Errorf("unsafe maintenance checks require a check-specific audited override: %s", strings.Join(unsafe, ","))
	}
	if !sameStrings(override.CheckIDs, unsafe) {
		return fmt.Errorf("maintenance override check IDs must exactly match unsafe checks")
	}
	if strings.TrimSpace(override.Caller) != caller || strings.TrimSpace(override.IncidentID) != incident || strings.TrimSpace(override.Reason) == "" {
		return fmt.Errorf("maintenance override caller/incident/reason audit identity is incomplete or mismatched")
	}
	if !override.ExpiresAt.After(at) || override.ExpiresAt.Sub(at) > maximumOverrideLifetime {
		return fmt.Errorf("maintenance override expiry must be after admission and within 24 hours")
	}
	return nil
}

func validateAuditIdentity(caller, incident, reason string) (string, string, string, error) {
	caller, incident, reason = strings.TrimSpace(caller), strings.TrimSpace(incident), strings.TrimSpace(reason)
	if caller == "" || incident == "" || reason == "" {
		return "", "", "", fmt.Errorf("caller, incident ID, and reason are required")
	}
	return caller, incident, reason, nil
}

func validateSafetyTime(observed, at time.Time) error {
	if observed.IsZero() || at.IsZero() {
		return fmt.Errorf("observation and admission times are required")
	}
	if observed.After(at.Add(defaultSafetyFutureSkew)) {
		return fmt.Errorf("safety snapshot is too far in the future")
	}
	if at.Sub(observed) > defaultSafetySnapshotAge {
		return fmt.Errorf("safety snapshot is stale")
	}
	return nil
}

func validateTransitionTimeForSafety(updated, at time.Time) error {
	if at.IsZero() || at.Before(updated) {
		return fmt.Errorf("transition time is missing or precedes the current revision")
	}
	return nil
}

func finalizeServiceActivation(operation *ServiceActivationOperation) {
	operation.ExecutedTiKVMutations = 0
	operation.ExecutedDaemonActions = 0
	operation.ExecutedStorageMutations = 0
	operation.NoMutationRunbook = true
	operation.Physical160Claimed = false
	operation.AutomaticRollback = false
	operation.OperationDigest = digestServiceOperation(operation)
}

func finalizeMaintenanceOperation(operation *HostMaintenanceOperation) {
	operation.ExecutedTiKVMutations = 0
	operation.ExecutedDaemonActions = 0
	operation.ExecutedStorageMutations = 0
	operation.NoMutationRunbook = true
	operation.Physical160Claimed = false
	operation.OperationDigest = digestMaintenanceOperation(operation)
}

func digestServiceSnapshot(value *ServiceActivationSnapshot) string {
	copyValue := *value
	copyValue.SnapshotDigest = ""
	return digestSafetyJSON(copyValue)
}

func digestServiceOperation(value *ServiceActivationOperation) string {
	copyValue := *value
	copyValue.OperationDigest = ""
	return digestSafetyJSON(copyValue)
}

func digestMaintenanceSnapshot(value *HostMaintenanceSnapshot) string {
	copyValue := *value
	copyValue.SnapshotDigest = ""
	return digestSafetyJSON(copyValue)
}

func digestMaintenancePlan(value *HostMaintenancePlan) string {
	copyValue := *value
	copyValue.PlanID, copyValue.PlanDigest = "", ""
	return digestSafetyJSON(copyValue)
}

func digestMaintenanceOperation(value *HostMaintenanceOperation) string {
	copyValue := *value
	copyValue.OperationDigest = ""
	return digestSafetyJSON(copyValue)
}

func digestMaintenanceExitPreflight(value *HostMaintenanceExitPreflight) string {
	copyValue := *value
	copyValue.PreflightDigest = ""
	return digestSafetyJSON(copyValue)
}

func digestSafetyJSON(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalSafetyDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func decodeStrict(raw []byte, value any, name string) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode %s: multiple JSON documents are not allowed", name)
		}
		return fmt.Errorf("decode %s trailing data: %w", name, err)
	}
	return nil
}

func marshalIndented(value any, name string) ([]byte, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", name, err)
	}
	return append(raw, '\n'), nil
}

func cloneServiceActivation(value *ServiceActivationOperation) *ServiceActivationOperation {
	raw, _ := json.Marshal(value)
	result := new(ServiceActivationOperation)
	_ = json.Unmarshal(raw, result)
	return result
}

func cloneMaintenanceOperation(value *HostMaintenanceOperation) *HostMaintenanceOperation {
	raw, _ := json.Marshal(value)
	result := new(HostMaintenanceOperation)
	_ = json.Unmarshal(raw, result)
	return result
}

func cloneMaintenanceOverride(value *MaintenanceOverrideAudit) *MaintenanceOverrideAudit {
	if value == nil {
		return nil
	}
	result := *value
	result.CheckIDs = append([]string(nil), value.CheckIDs...)
	return &result
}

func containsSafetyString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasDuplicateSafetyStrings(values []string) bool {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return true
		}
	}
	return false
}
