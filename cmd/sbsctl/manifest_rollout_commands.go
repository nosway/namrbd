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

type manifestRolloutSummary struct {
	Phase                    string                `json:"phase"`
	Slice                    string                `json:"slice"`
	Result                   string                `json:"result"`
	Entrypoint               string                `json:"entrypoint"`
	ValidationBoundary       string                `json:"validation_boundary"`
	OperationID              string                `json:"operation_id,omitempty"`
	OperationDigest          string                `json:"operation_digest,omitempty"`
	Revision                 uint64                `json:"revision,omitempty"`
	PlanID                   string                `json:"plan_id,omitempty"`
	JoinPlanID               string                `json:"join_plan_id,omitempty"`
	ManifestDigest           string                `json:"manifest_digest,omitempty"`
	State                    string                `json:"state,omitempty"`
	PauseCause               string                `json:"pause_cause,omitempty"`
	PauseReason              string                `json:"pause_reason,omitempty"`
	CurrentWaveIndex         int                   `json:"current_wave_index"`
	CurrentWaveID            string                `json:"current_wave_id,omitempty"`
	WaveCount                int                   `json:"wave_count"`
	TransitionCount          int                   `json:"transition_count"`
	NextInstructionCount     int                   `json:"next_instruction_count"`
	Instructions             []rollout.Instruction `json:"instructions,omitempty"`
	InstructionCount         int                   `json:"instruction_count"`
	DuplicateResultIgnored   bool                  `json:"duplicate_result_ignored"`
	ProjectedNodeActionCount int                   `json:"projected_node_action_count"`
	SucceededNodeCount       int                   `json:"succeeded_node_count"`
	FailedNodeCount          int                   `json:"failed_node_count"`
	InProgressNodeCount      int                   `json:"in_progress_node_count"`
	PendingNodeCount         int                   `json:"pending_node_count"`
	FirstError               string                `json:"first_error"`
	LastError                string                `json:"last_error"`
	RollbackRequired         bool                  `json:"rollback_required"`
	AutomaticRollback        bool                  `json:"automatic_rollback"`
	OperationArtifactWritten bool                  `json:"operation_artifact_written"`
	NoMutationStateMachine   bool                  `json:"no_mutation_state_machine"`
	ExecutedTiKVMutations    int                   `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                   `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                   `json:"executed_storage_mutations"`
	RemoteLabUsed            bool                  `json:"remote_lab_used"`
	Physical160Claimed       bool                  `json:"physical_160_claimed"`
	OKCount                  int                   `json:"ok_count"`
	ErrorCount               int                   `json:"error_count"`
}

func runClusterManifestRollout(args []string) {
	if len(args) == 0 {
		clusterManifestRolloutUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "start":
		runClusterManifestRolloutStart(args[1:])
	case "issue":
		runClusterManifestRolloutIssue(args[1:])
	case "record":
		runClusterManifestRolloutRecord(args[1:])
	case "pause":
		runClusterManifestRolloutPause(args[1:])
	case "resume":
		runClusterManifestRolloutResume(args[1:])
	case "retry":
		runClusterManifestRolloutRetry(args[1:])
	case "status":
		runClusterManifestRolloutStatus(args[1:])
	default:
		clusterManifestRolloutUsage()
		os.Exit(2)
	}
}

func runClusterManifestRolloutStart(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout start", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	manifestFile := fs.String("file", "", "reviewed cluster manifest YAML path")
	joinPlanFile := fs.String("join-plan", "", "admitted SBSHostJoinPlan JSON path")
	startedAtValue := fs.String("started-at", "", "operation start time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for rollout operation revision 1")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest rollout start"
	requireRolloutFields(*output, entrypoint, map[string]string{"--file": *manifestFile, "--join-plan": *joinPlanFile, "--started-at": *startedAtValue, "--operation-output": *operationOutput})
	manifest, err := clustermanifest.Load(*manifestFile)
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	joinPlanRaw, err := os.ReadFile(*joinPlanFile)
	if err != nil {
		manifestRolloutError(*output, entrypoint, fmt.Errorf("read join plan: %w", err))
	}
	joinPlan, err := installpreflight.ParseJoinPlan(joinPlanRaw)
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	startedAt := parseRolloutTime(*output, entrypoint, "--started-at", *startedAtValue)
	operation, err := rollout.Start(rollout.StartRequest{
		Manifest: manifest, Policy: clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true},
		JoinPlan: joinPlan, StartedAt: startedAt,
	})
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	writeRolloutOperation(*output, entrypoint, *operationOutput, operation)
	writeRolloutSummary(*output, entrypoint, operation, nil, false, true)
}

func runClusterManifestRolloutIssue(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout issue", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "input rollout operation JSON path")
	atValue := fs.String("at", "", "instruction issue time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the next rollout operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest rollout issue"
	requireRolloutFields(*output, entrypoint, map[string]string{"--operation": *operationFile, "--at": *atValue, "--operation-output": *operationOutput})
	operation := loadRolloutOperation(*output, entrypoint, *operationFile)
	at := parseRolloutTime(*output, entrypoint, "--at", *atValue)
	next, instructions, err := rollout.IssueCurrentWave(operation, at)
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	writeRolloutOperation(*output, entrypoint, *operationOutput, next)
	writeRolloutSummary(*output, entrypoint, next, instructions, false, true)
}

func runClusterManifestRolloutRecord(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout record", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "input rollout operation JSON path")
	nodeID := fs.String("node-id", "", "node whose issued result is being recorded")
	idempotencyKey := fs.String("idempotency-key", "", "exact key from the emitted instruction")
	outcome := fs.String("result", "", "external transport result: success|failure")
	errorMessage := fs.String("error", "", "required failure detail")
	atValue := fs.String("at", "", "result observation time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the next rollout operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest rollout record"
	requireRolloutFields(*output, entrypoint, map[string]string{
		"--operation": *operationFile, "--node-id": *nodeID, "--idempotency-key": *idempotencyKey,
		"--result": *outcome, "--at": *atValue, "--operation-output": *operationOutput,
	})
	operation := loadRolloutOperation(*output, entrypoint, *operationFile)
	at := parseRolloutTime(*output, entrypoint, "--at", *atValue)
	next, duplicate, err := rollout.RecordResult(operation, strings.TrimSpace(*nodeID), *idempotencyKey, *outcome, *errorMessage, at)
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	writeRolloutOperation(*output, entrypoint, *operationOutput, next)
	writeRolloutSummary(*output, entrypoint, next, nil, duplicate, true)
}

func runClusterManifestRolloutPause(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout pause", flag.ExitOnError)
	runClusterManifestRolloutControl("pause", fs, args, rollout.Pause)
}

func runClusterManifestRolloutResume(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout resume", flag.ExitOnError)
	runClusterManifestRolloutControl("resume", fs, args, rollout.Resume)
}

func runClusterManifestRolloutRetry(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout retry", flag.ExitOnError)
	runClusterManifestRolloutControl("retry", fs, args, rollout.RetryFailed)
}

func runClusterManifestRolloutControl(name string, fs *flag.FlagSet, args []string, transition func(*rollout.Operation, string, time.Time) (*rollout.Operation, error)) {
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "input rollout operation JSON path")
	reason := fs.String("reason", "", "operator reason recorded with the transition")
	atValue := fs.String("at", "", "transition time in RFC3339 format")
	operationOutput := fs.String("operation-output", "", "new path for the next rollout operation revision")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest rollout " + name
	requireRolloutFields(*output, entrypoint, map[string]string{"--operation": *operationFile, "--reason": *reason, "--at": *atValue, "--operation-output": *operationOutput})
	operation := loadRolloutOperation(*output, entrypoint, *operationFile)
	at := parseRolloutTime(*output, entrypoint, "--at", *atValue)
	next, err := transition(operation, *reason, at)
	if err != nil {
		manifestRolloutError(*output, entrypoint, err)
	}
	writeRolloutOperation(*output, entrypoint, *operationOutput, next)
	writeRolloutSummary(*output, entrypoint, next, nil, false, true)
}

func runClusterManifestRolloutStatus(args []string) {
	fs := flag.NewFlagSet("cluster manifest rollout status", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	operationFile := fs.String("operation", "", "rollout operation JSON path")
	output := fs.String("output", "table", "summary output format: table|json")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest rollout status"
	requireRolloutFields(*output, entrypoint, map[string]string{"--operation": *operationFile})
	operation := loadRolloutOperation(*output, entrypoint, *operationFile)
	writeRolloutSummary(*output, entrypoint, operation, nil, false, false)
}

func loadRolloutOperation(output, entrypoint, path string) *rollout.Operation {
	raw, err := os.ReadFile(path)
	if err != nil {
		manifestRolloutError(output, entrypoint, fmt.Errorf("read rollout operation: %w", err))
	}
	operation, err := rollout.ParseOperation(raw)
	if err != nil {
		manifestRolloutError(output, entrypoint, err)
	}
	return operation
}

func writeRolloutOperation(output, entrypoint, path string, operation *rollout.Operation) {
	raw, err := rollout.MarshalOperation(operation)
	if err != nil {
		manifestRolloutError(output, entrypoint, err)
	}
	if err := clustermanifest.WriteNewFile(path, raw, 0o640); err != nil {
		manifestRolloutError(output, entrypoint, err)
	}
}

func parseRolloutTime(output, entrypoint, name, value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		manifestRolloutError(output, entrypoint, fmt.Errorf("parse %s: %w", name, err))
	}
	return parsed
}

func requireRolloutFields(output, entrypoint string, values map[string]string) {
	if output != "table" && output != "json" {
		manifestRolloutError(output, entrypoint, fmt.Errorf("unsupported output format %q", output))
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := values[name]
		if strings.TrimSpace(value) == "" {
			manifestRolloutError(output, entrypoint, fmt.Errorf("%s is required", name))
		}
	}
}

func writeRolloutSummary(output, entrypoint string, operation *rollout.Operation, instructions []rollout.Instruction, duplicate, written bool) {
	summary := manifestRolloutSummary{
		Phase: "AD", Slice: "AD-IMPL-002C", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_canary_wave_no_mutation_state_machine",
		OperationID:        operation.OperationID, OperationDigest: operation.OperationDigest,
		Revision: operation.Revision, PlanID: operation.PlanID, JoinPlanID: operation.JoinPlanID,
		ManifestDigest: operation.ManifestDigest, State: operation.State,
		PauseCause: operation.PauseCause, PauseReason: operation.PauseReason,
		CurrentWaveIndex: operation.CurrentWaveIndex, WaveCount: len(operation.Waves), TransitionCount: len(operation.TransitionHistory),
		NextInstructionCount: rollout.NextInstructionCount(operation), Instructions: instructions,
		InstructionCount: len(instructions), DuplicateResultIgnored: duplicate,
		ProjectedNodeActionCount: operation.ProjectedNodeActionCount,
		SucceededNodeCount:       operation.SucceededNodeCount, FailedNodeCount: operation.FailedNodeCount,
		InProgressNodeCount: operation.InProgressNodeCount, PendingNodeCount: operation.PendingNodeCount,
		FirstError: operation.FirstError, LastError: operation.LastError,
		RollbackRequired: operation.RollbackPlan.Required, AutomaticRollback: operation.RollbackPlan.AutomaticExecution,
		OperationArtifactWritten: written, NoMutationStateMachine: true,
		ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if operation.CurrentWaveIndex >= 0 && operation.CurrentWaveIndex < len(operation.Waves) {
		summary.CurrentWaveID = operation.Waves[operation.CurrentWaveIndex].WaveID
	}
	if output == "json" || globalJSONOutput {
		writeJSON(summary)
		return
	}
	fmt.Printf("operation_id: %s\n", summary.OperationID)
	fmt.Printf("revision: %d\n", summary.Revision)
	fmt.Printf("state: %s\n", summary.State)
	fmt.Printf("current_wave: %s\n", summary.CurrentWaveID)
	fmt.Printf("next_instructions: %d\n", summary.NextInstructionCount)
	fmt.Println("executed_tikv_mutations: 0")
	fmt.Println("executed_daemon_actions: 0")
}

func manifestRolloutError(output, entrypoint string, err error) {
	if output == "json" || globalJSONOutput {
		writeJSON(manifestRolloutSummary{
			Phase: "AD", Slice: "AD-IMPL-002C", Result: "blocked", Entrypoint: entrypoint,
			ValidationBoundary: "phase_ad_canary_wave_no_mutation_state_machine",
			FirstError:         err.Error(), LastError: err.Error(), ErrorCount: 1,
			NoMutationStateMachine: true, ExecutedTiKVMutations: 0,
			ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
			RemoteLabUsed: false, Physical160Claimed: false,
		})
		os.Exit(1)
	}
	fatalf("cluster manifest rollout failed: %v", err)
}

func clusterManifestRolloutUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl cluster manifest rollout start|issue|record|pause|resume|retry|status ...")
}
