package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
	"github.com/nosway/namrbd/internal/rollout"
)

type phaseADSafetySummary struct {
	Phase                    string                                `json:"phase"`
	Slice                    string                                `json:"slice"`
	Result                   string                                `json:"result"`
	Entrypoint               string                                `json:"entrypoint"`
	ValidationBoundary       string                                `json:"validation_boundary"`
	OperationID              string                                `json:"operation_id,omitempty"`
	OperationDigest          string                                `json:"operation_digest,omitempty"`
	PlanID                   string                                `json:"plan_id,omitempty"`
	PlanDigest               string                                `json:"plan_digest,omitempty"`
	ManifestDigest           string                                `json:"manifest_digest,omitempty"`
	NodeID                   string                                `json:"node_id,omitempty"`
	FailedActiveNodeID       string                                `json:"failed_active_node_id,omitempty"`
	CandidateNodeID          string                                `json:"candidate_node_id,omitempty"`
	State                    string                                `json:"state,omitempty"`
	Revision                 uint64                                `json:"revision,omitempty"`
	UnsafeCheckIDs           []string                              `json:"unsafe_check_ids,omitempty"`
	OverrideCheckIDs         []string                              `json:"override_check_ids,omitempty"`
	OverrideCaller           string                                `json:"override_caller,omitempty"`
	OverrideIncidentID       string                                `json:"override_incident_id,omitempty"`
	OverrideReason           string                                `json:"override_reason,omitempty"`
	OverrideExpiresAt        time.Time                             `json:"override_expires_at,omitempty"`
	ServiceInstruction       *rollout.ServiceActivationInstruction `json:"service_instruction,omitempty"`
	MaintenanceInstructions  []rollout.MaintenanceInstruction      `json:"maintenance_instructions,omitempty"`
	InstructionCount         int                                   `json:"instruction_count"`
	ActivationVerified       bool                                  `json:"activation_verified"`
	RollbackRequired         bool                                  `json:"rollback_required"`
	AutomaticRollback        bool                                  `json:"automatic_rollback"`
	FirstError               string                                `json:"first_error"`
	LastError                string                                `json:"last_error"`
	ArtifactWritten          bool                                  `json:"artifact_written"`
	ProjectedDrainActions    int                                   `json:"projected_drain_actions"`
	ProjectedDaemonActions   int                                   `json:"projected_daemon_actions"`
	AffectedPlacementCount   int                                   `json:"affected_placement_count"`
	ExpectedMoveBytes        uint64                                `json:"expected_move_bytes"`
	ExecutedTiKVMutations    int                                   `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                                   `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                                   `json:"executed_storage_mutations"`
	NoMutationRunbook        bool                                  `json:"no_mutation_runbook"`
	RemoteLabUsed            bool                                  `json:"remote_lab_used"`
	Physical160Claimed       bool                                  `json:"physical_160_claimed"`
	OKCount                  int                                   `json:"ok_count"`
	ErrorCount               int                                   `json:"error_count"`
}

func runClusterManifestStandby(args []string) {
	if len(args) == 0 {
		clusterManifestStandbyUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "plan":
		runClusterManifestStandbyPlan(args[1:])
	case "issue":
		runClusterManifestStandbyIssue(args[1:])
	case "verify":
		runClusterManifestStandbyVerify(args[1:])
	case "status":
		runClusterManifestStandbyStatus(args[1:])
	default:
		clusterManifestStandbyUsage()
		os.Exit(2)
	}
}

func runClusterManifestStandbyPlan(args []string) {
	fs := flag.NewFlagSet("cluster manifest standby plan", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	manifestFile := fs.String("file", "", "reviewed cluster manifest YAML path")
	joinPlanFile := fs.String("join-plan", "", "admitted SBSHostJoinPlan JSON path")
	snapshotFile := fs.String("snapshot", "", "service activation observation snapshot JSON path")
	failedNode := fs.String("failed-active-node", "", "stopped and isolated active service node")
	candidateNode := fs.String("candidate-node", "", "single standby candidate to activate")
	caller := fs.String("caller", "", "operator identity recorded in the activation audit")
	incidentID := fs.String("incident-id", "", "incident/change identity recorded in the activation audit")
	reason := fs.String("reason", "", "activation reason recorded in the audit")
	atValue := fs.String("at", "", "activation plan time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the service activation operation")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest standby plan"
	requireSafetyFields(*output, entrypoint, map[string]string{
		"--file": *manifestFile, "--join-plan": *joinPlanFile, "--snapshot": *snapshotFile,
		"--failed-active-node": *failedNode, "--candidate-node": *candidateNode, "--caller": *caller,
		"--incident-id": *incidentID, "--reason": *reason, "--at": *atValue, "--operation-output": *operationOutput,
	})
	manifest, err := clustermanifest.Load(*manifestFile)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	joinPlan := loadJoinPlanForSafety(*output, entrypoint, *joinPlanFile)
	snapshot := loadServiceSnapshotForSafety(*output, entrypoint, *snapshotFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	operation, err := rollout.BuildServiceActivation(rollout.ServiceActivationRequest{
		Manifest: manifest, Policy: clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true},
		JoinPlan: joinPlan, Snapshot: snapshot, FailedActiveNodeID: *failedNode, CandidateNodeID: *candidateNode,
		Caller: *caller, IncidentID: *incidentID, Reason: *reason, At: at,
	})
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeServiceActivationOperation(*output, entrypoint, *operationOutput, operation)
	writeServiceSafetySummary(*output, entrypoint, operation, nil, false, true)
}

func runClusterManifestStandbyIssue(args []string) {
	fs := flag.NewFlagSet("cluster manifest standby issue", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "input service activation operation JSON path")
	atValue := fs.String("at", "", "instruction time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the next operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest standby issue"
	requireSafetyFields(*output, entrypoint, map[string]string{"--operation": *operationFile, "--at": *atValue, "--operation-output": *operationOutput})
	operation := loadServiceOperationForSafety(*output, entrypoint, *operationFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	next, instruction, err := rollout.IssueServiceActivation(operation, at)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeServiceActivationOperation(*output, entrypoint, *operationOutput, next)
	writeServiceSafetySummary(*output, entrypoint, next, &instruction, false, true)
}

func runClusterManifestStandbyVerify(args []string) {
	fs := flag.NewFlagSet("cluster manifest standby verify", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	manifestFile := fs.String("file", "", "reviewed cluster manifest YAML path")
	operationFile := fs.String("operation", "", "running service activation operation JSON path")
	snapshotFile := fs.String("snapshot", "", "post-activation service observation snapshot JSON path")
	atValue := fs.String("at", "", "verification time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the verified operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest standby verify"
	requireSafetyFields(*output, entrypoint, map[string]string{"--file": *manifestFile, "--operation": *operationFile, "--snapshot": *snapshotFile, "--at": *atValue, "--operation-output": *operationOutput})
	manifest, err := clustermanifest.Load(*manifestFile)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	operation := loadServiceOperationForSafety(*output, entrypoint, *operationFile)
	snapshot := loadServiceSnapshotForSafety(*output, entrypoint, *snapshotFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	next, verified, err := rollout.VerifyServiceActivation(operation, manifest, clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}, snapshot, at)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeServiceActivationOperation(*output, entrypoint, *operationOutput, next)
	writeServiceSafetySummary(*output, entrypoint, next, nil, verified, true)
	if !verified {
		os.Exit(1)
	}
}

func runClusterManifestStandbyStatus(args []string) {
	fs := flag.NewFlagSet("cluster manifest standby status", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "service activation operation JSON path")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest standby status"
	requireSafetyFields(*output, entrypoint, map[string]string{"--operation": *operationFile})
	writeServiceSafetySummary(*output, entrypoint, loadServiceOperationForSafety(*output, entrypoint, *operationFile), nil, false, false)
}

func runHostMaintenance(args []string) {
	if len(args) == 0 {
		hostMaintenanceUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "plan":
		runHostMaintenancePlan(args[1:])
	case "enter":
		runHostMaintenanceEnter(args[1:])
	case "exit":
		runHostMaintenanceExit(args[1:])
	case "status":
		runHostMaintenanceStatus(args[1:])
	default:
		hostMaintenanceUsage()
		os.Exit(2)
	}
}

func runHostMaintenancePlan(args []string) {
	fs := flag.NewFlagSet("host maintenance plan", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	manifestFile := fs.String("manifest", "", "reviewed cluster manifest YAML path")
	snapshotFile := fs.String("snapshot", "", "host maintenance safety snapshot JSON path")
	atValue := fs.String("at", "", "plan creation time in RFC3339 format")
	planOutput := fs.String("plan-output", "", "new path for the maintenance plan")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl host maintenance plan"
	requireSafetyFields(*output, entrypoint, map[string]string{"--manifest": *manifestFile, "--snapshot": *snapshotFile, "--at": *atValue, "--plan-output": *planOutput})
	manifest, err := clustermanifest.Load(*manifestFile)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	snapshot := loadMaintenanceSnapshotForSafety(*output, entrypoint, *snapshotFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	plan, err := rollout.BuildHostMaintenancePlan(manifest, clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}, snapshot, at)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	raw, err := rollout.MarshalHostMaintenancePlan(plan)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeSafetyNewFile(*output, entrypoint, *planOutput, raw)
	writeMaintenancePlanSummary(*output, entrypoint, plan, true)
}

func runHostMaintenanceEnter(args []string) {
	fs := flag.NewFlagSet("host maintenance enter", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	planFile := fs.String("plan", "", "reviewed host maintenance plan JSON path")
	caller := fs.String("caller", "", "operator identity")
	incidentID := fs.String("incident-id", "", "incident/change identity")
	reason := fs.String("reason", "", "maintenance entry reason")
	atValue := fs.String("at", "", "entry admission time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the maintenance operation")
	overrideReason := fs.String("override-reason", "", "specific unsafe-check override reason")
	overrideExpires := fs.String("override-expires-at", "", "override expiry in RFC3339 format, maximum 24h")
	output := fs.String("output", "table", "summary output format: table|json")
	var overrideChecks repeatedStringFlag
	fs.Var(&overrideChecks, "override-check", "exact unsafe check ID to override; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl host maintenance enter"
	requireSafetyFields(*output, entrypoint, map[string]string{"--plan": *planFile, "--caller": *caller, "--incident-id": *incidentID, "--reason": *reason, "--at": *atValue, "--operation-output": *operationOutput})
	plan := loadMaintenancePlanForSafety(*output, entrypoint, *planFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	var override *rollout.MaintenanceOverrideAudit
	if len(overrideChecks) > 0 || strings.TrimSpace(*overrideReason) != "" || strings.TrimSpace(*overrideExpires) != "" {
		if len(overrideChecks) == 0 || strings.TrimSpace(*overrideReason) == "" || strings.TrimSpace(*overrideExpires) == "" {
			safetyCommandError(*output, entrypoint, fmt.Errorf("--override-check, --override-reason, and --override-expires-at must be provided together"))
		}
		sort.Strings(overrideChecks)
		expires := parseSafetyTime(*output, entrypoint, "--override-expires-at", *overrideExpires)
		override = &rollout.MaintenanceOverrideAudit{CheckIDs: []string(overrideChecks), Caller: *caller, IncidentID: *incidentID, Reason: *overrideReason, ExpiresAt: expires}
	}
	operation, err := rollout.EnterHostMaintenance(plan, *caller, *incidentID, *reason, override, at)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeMaintenanceOperation(*output, entrypoint, *operationOutput, operation)
	writeMaintenanceOperationSummary(*output, entrypoint, operation, true)
}

func runHostMaintenanceExit(args []string) {
	fs := flag.NewFlagSet("host maintenance exit", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "enter-ready maintenance operation JSON path")
	preflightFile := fs.String("preflight", "", "host maintenance exit preflight JSON path")
	atValue := fs.String("at", "", "exit admission time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the exit-ready operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl host maintenance exit"
	requireSafetyFields(*output, entrypoint, map[string]string{"--operation": *operationFile, "--preflight": *preflightFile, "--at": *atValue, "--operation-output": *operationOutput})
	operation := loadMaintenanceOperationForSafety(*output, entrypoint, *operationFile)
	preflight := loadMaintenanceExitPreflightForSafety(*output, entrypoint, *preflightFile)
	at := parseSafetyTime(*output, entrypoint, "--at", *atValue)
	next, err := rollout.ExitHostMaintenance(operation, preflight, at)
	if err != nil {
		safetyCommandError(*output, entrypoint, err)
	}
	writeMaintenanceOperation(*output, entrypoint, *operationOutput, next)
	writeMaintenanceOperationSummary(*output, entrypoint, next, true)
}

func runHostMaintenanceStatus(args []string) {
	fs := flag.NewFlagSet("host maintenance status", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "host maintenance operation JSON path")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl host maintenance status"
	requireSafetyFields(*output, entrypoint, map[string]string{"--operation": *operationFile})
	writeMaintenanceOperationSummary(*output, entrypoint, loadMaintenanceOperationForSafety(*output, entrypoint, *operationFile), false)
}

func loadJoinPlanForSafety(output, entrypoint, path string) *installpreflight.JoinPlan {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read join plan: %w", err))
	}
	value, err := installpreflight.ParseJoinPlan(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadServiceSnapshotForSafety(output, entrypoint, path string) *rollout.ServiceActivationSnapshot {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read service snapshot: %w", err))
	}
	value, err := rollout.ParseServiceActivationSnapshot(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadServiceOperationForSafety(output, entrypoint, path string) *rollout.ServiceActivationOperation {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read service activation operation: %w", err))
	}
	value, err := rollout.ParseServiceActivationOperation(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadMaintenanceSnapshotForSafety(output, entrypoint, path string) *rollout.HostMaintenanceSnapshot {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read maintenance snapshot: %w", err))
	}
	value, err := rollout.ParseHostMaintenanceSnapshot(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadMaintenancePlanForSafety(output, entrypoint, path string) *rollout.HostMaintenancePlan {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read maintenance plan: %w", err))
	}
	value, err := rollout.ParseHostMaintenancePlan(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadMaintenanceOperationForSafety(output, entrypoint, path string) *rollout.HostMaintenanceOperation {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read maintenance operation: %w", err))
	}
	value, err := rollout.ParseHostMaintenanceOperation(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func loadMaintenanceExitPreflightForSafety(output, entrypoint, path string) *rollout.HostMaintenanceExitPreflight {
	raw, err := os.ReadFile(path)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("read maintenance exit preflight: %w", err))
	}
	value, err := rollout.ParseHostMaintenanceExitPreflight(raw)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	return value
}

func writeServiceActivationOperation(output, entrypoint, path string, operation *rollout.ServiceActivationOperation) {
	raw, err := rollout.MarshalServiceActivationOperation(operation)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	writeSafetyNewFile(output, entrypoint, path, raw)
}

func writeMaintenanceOperation(output, entrypoint, path string, operation *rollout.HostMaintenanceOperation) {
	raw, err := rollout.MarshalHostMaintenanceOperation(operation)
	if err != nil {
		safetyCommandError(output, entrypoint, err)
	}
	writeSafetyNewFile(output, entrypoint, path, raw)
}

func writeSafetyNewFile(output, entrypoint, path string, raw []byte) {
	if err := clustermanifest.WriteNewFile(path, raw, 0o640); err != nil {
		safetyCommandError(output, entrypoint, err)
	}
}

func writeServiceSafetySummary(output, entrypoint string, operation *rollout.ServiceActivationOperation, instruction *rollout.ServiceActivationInstruction, verified, written bool) {
	summary := phaseADSafetySummary{
		Phase: "AD", Slice: "AD-IMPL-002D", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_service_standby_activation_safety", OperationID: operation.OperationID,
		OperationDigest: operation.OperationDigest, ManifestDigest: operation.ManifestDigest,
		FailedActiveNodeID: operation.FailedActiveNodeID, CandidateNodeID: operation.CandidateNodeID,
		State: operation.State, Revision: operation.Revision, ServiceInstruction: instruction,
		ActivationVerified: verified || operation.State == rollout.ServiceActivationCompleted,
		RollbackRequired:   operation.RollbackRequired,
		AutomaticRollback:  operation.AutomaticRollback, FirstError: operation.FirstError, LastError: operation.LastError,
		ArtifactWritten: written, ProjectedDaemonActions: operation.ProjectedDaemonActions,
		ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
		NoMutationRunbook: true, RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if operation.State == rollout.ServiceActivationPaused {
		summary.Result = "blocked"
		summary.OKCount = 0
		summary.ErrorCount = 1
	}
	if instruction != nil {
		summary.InstructionCount = 1
	}
	writeSafetySummary(output, summary)
}

func writeMaintenancePlanSummary(output, entrypoint string, plan *rollout.HostMaintenancePlan, written bool) {
	writeSafetySummary(output, phaseADSafetySummary{
		Phase: "AD", Slice: "AD-IMPL-002D", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_host_maintenance_safety", PlanID: plan.PlanID, PlanDigest: plan.PlanDigest,
		ManifestDigest: plan.ManifestDigest, NodeID: plan.NodeID, UnsafeCheckIDs: plan.UnsafeCheckIDs,
		ArtifactWritten: written, ProjectedDrainActions: plan.ProjectedDrainActions,
		ProjectedDaemonActions: plan.ProjectedDaemonActions, AffectedPlacementCount: plan.AffectedPlacementCount,
		ExpectedMoveBytes: plan.ExpectedMoveBytes, ExecutedTiKVMutations: 0,
		ExecutedDaemonActions: 0, ExecutedStorageMutations: 0, NoMutationRunbook: true,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	})
}

func writeMaintenanceOperationSummary(output, entrypoint string, operation *rollout.HostMaintenanceOperation, written bool) {
	summary := phaseADSafetySummary{
		Phase: "AD", Slice: "AD-IMPL-002D", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_host_maintenance_safety", OperationID: operation.OperationID,
		OperationDigest: operation.OperationDigest, PlanID: operation.PlanID, PlanDigest: operation.PlanDigest,
		ManifestDigest: operation.ManifestDigest, NodeID: operation.NodeID, State: operation.State,
		Revision: operation.Revision, UnsafeCheckIDs: operation.UnsafeCheckIDs,
		MaintenanceInstructions: operation.Instructions, InstructionCount: len(operation.Instructions),
		ArtifactWritten: written, ProjectedDrainActions: operation.ProjectedDrainActions,
		ProjectedDaemonActions: operation.ProjectedDaemonActions, AffectedPlacementCount: operation.AffectedPlacementCount,
		ExpectedMoveBytes: operation.ExpectedMoveBytes, ExecutedTiKVMutations: 0,
		ExecutedDaemonActions: 0, ExecutedStorageMutations: 0, NoMutationRunbook: true,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if operation.Override != nil {
		summary.OverrideCheckIDs = operation.Override.CheckIDs
		summary.OverrideCaller = operation.Override.Caller
		summary.OverrideIncidentID = operation.Override.IncidentID
		summary.OverrideReason = operation.Override.Reason
		summary.OverrideExpiresAt = operation.Override.ExpiresAt
	}
	writeSafetySummary(output, summary)
}

func writeSafetySummary(output string, summary phaseADSafetySummary) {
	if output == "json" || globalJSONOutput {
		writeJSON(summary)
		return
	}
	if summary.OperationID != "" {
		fmt.Printf("operation_id: %s\n", summary.OperationID)
	}
	if summary.PlanID != "" {
		fmt.Printf("plan_id: %s\n", summary.PlanID)
	}
	if summary.State != "" {
		fmt.Printf("state: %s\n", summary.State)
	}
	fmt.Printf("instruction_count: %d\n", summary.InstructionCount)
	fmt.Println("executed_tikv_mutations: 0")
	fmt.Println("executed_daemon_actions: 0")
}

func requireSafetyFields(output, entrypoint string, values map[string]string) {
	if output != "table" && output != "json" {
		safetyCommandError(output, entrypoint, fmt.Errorf("unsupported output format %q", output))
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.TrimSpace(values[key]) == "" {
			safetyCommandError(output, entrypoint, fmt.Errorf("%s is required", key))
		}
	}
}

func parseSafetyTime(output, entrypoint, field, value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		safetyCommandError(output, entrypoint, fmt.Errorf("parse %s: %w", field, err))
	}
	return parsed
}

func safetyCommandError(output, entrypoint string, err error) {
	if output == "json" || globalJSONOutput {
		writeJSON(phaseADSafetySummary{
			Phase: "AD", Slice: "AD-IMPL-002D", Result: "blocked", Entrypoint: entrypoint,
			ValidationBoundary: "phase_ad_service_and_host_maintenance_safety",
			FirstError:         err.Error(), LastError: err.Error(), ErrorCount: 1,
			ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
			NoMutationRunbook: true, RemoteLabUsed: false, Physical160Claimed: false,
		})
		os.Exit(1)
	}
	fatalf("Phase AD safety command failed: %v", err)
}

func clusterManifestStandbyUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl cluster manifest standby plan|issue|verify|status ...")
}

func hostMaintenanceUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl host maintenance plan|enter|exit|status ...")
}
