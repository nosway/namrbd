package rollout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
)

const rolloutTestArtifactDigest = "sha256:c3bd6d1fdc4b1a1239e4036e294b71fb7ea0880ba990a486d1cedaede3f4eecf"

func TestStartBuildsExactCanaryAndBoundedZoneWaves(t *testing.T) {
	request := exactStartRequest(t)
	operation, err := Start(request)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if operation.State != StateReady || operation.CurrentWaveIndex != 0 || len(operation.Waves) != 40 {
		t.Fatalf("operation state=%s current=%d waves=%d", operation.State, operation.CurrentWaveIndex, len(operation.Waves))
	}
	zoneWaveCounts := map[string]int{}
	for _, wave := range operation.Waves {
		zoneWaveCounts[wave.Zone]++
		if wave.Kind == WaveKindCanary && len(wave.Actions) != 1 {
			t.Fatalf("canary %s actions=%d", wave.WaveID, len(wave.Actions))
		}
		if wave.Kind == WaveKindBatch && len(wave.Actions) > clustermanifest.DefaultMaxParallelPerZone {
			t.Fatalf("wave %s exceeds max parallel: %d", wave.WaveID, len(wave.Actions))
		}
	}
	for zone, count := range zoneWaveCounts {
		if count != 5 {
			t.Fatalf("zone %s waves=%d want 5", zone, count)
		}
	}
	if operation.ProjectedNodeActionCount != 160 || operation.PendingNodeCount != 160 || NextInstructionCount(operation) != 1 {
		t.Fatalf("operation counters=%+v next=%d", operation, NextInstructionCount(operation))
	}
	if operation.ExecutedTiKVMutations != 0 || operation.ExecutedDaemonActions != 0 || operation.ExecutedStorageMutations != 0 || !operation.NoMutationStateMachine {
		t.Fatalf("operation reported runtime mutation: %+v", operation)
	}
	second, err := Start(request)
	if err != nil {
		t.Fatal(err)
	}
	if operation.OperationID != second.OperationID || operation.OperationDigest != second.OperationDigest {
		t.Fatalf("deterministic start mismatch: %s/%s != %s/%s", operation.OperationID, operation.OperationDigest, second.OperationID, second.OperationDigest)
	}
}

func TestFailurePausesNextWaveAndRetryIsIdempotentAcrossRestart(t *testing.T) {
	operation := startOperation(t)
	now := operation.StartedAt
	operation = succeedCurrentWave(t, operation, &now)
	if operation.CurrentWaveIndex != 1 || NextInstructionCount(operation) != 5 {
		t.Fatalf("after canary current=%d next=%d", operation.CurrentWaveIndex, NextInstructionCount(operation))
	}

	now = now.Add(time.Second)
	issued, instructions, err := IssueCurrentWave(operation, now)
	if err != nil {
		t.Fatal(err)
	}
	operation = issued
	if len(instructions) != 5 {
		t.Fatalf("batch instructions=%d", len(instructions))
	}
	failedInstruction := instructions[len(instructions)-1]
	for _, instruction := range instructions[:len(instructions)-1] {
		now = now.Add(time.Second)
		operation, _, err = RecordResult(operation, instruction.NodeID, instruction.IdempotencyKey, OutcomeSuccess, "", now)
		if err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Second)
	operation, duplicate, err := RecordResult(operation, failedInstruction.NodeID, failedInstruction.IdempotencyKey, OutcomeFailure, "fixture join failed", now)
	if err != nil || duplicate {
		t.Fatalf("RecordResult failure duplicate=%t err=%v", duplicate, err)
	}
	if operation.State != StatePaused || operation.PauseCause != PauseCauseFailure || NextInstructionCount(operation) != 0 {
		t.Fatalf("failure did not stop next wave: state=%s cause=%s next=%d", operation.State, operation.PauseCause, NextInstructionCount(operation))
	}
	if operation.CurrentWaveIndex != 1 || operation.SucceededNodeCount != 5 || operation.FailedNodeCount != 1 {
		t.Fatalf("partial failure counters=%+v", operation)
	}
	if !operation.RollbackPlan.Required || operation.RollbackPlan.AutomaticExecution || len(operation.RollbackPlan.CandidateNodeIDs) != 5 {
		t.Fatalf("rollback plan=%+v", operation.RollbackPlan)
	}
	if len(operation.TransitionHistory) != 1 || operation.TransitionHistory[0].Type != TransitionFailurePause || operation.TransitionHistory[0].Revision != operation.Revision {
		t.Fatalf("failure transition history=%+v revision=%d", operation.TransitionHistory, operation.Revision)
	}
	if operation.FirstError == "" || operation.LastError == "" || operation.Waves[1].Actions[4].FirstError == "" {
		t.Fatalf("failure evidence was not preserved: %+v", operation)
	}

	raw, err := MarshalOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := ParseOperation(raw)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	restarted, err = RetryFailed(restarted, "operator approved retry", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.TransitionHistory) != 2 || restarted.TransitionHistory[1].Type != TransitionRetry || restarted.TransitionHistory[1].Reason != "operator approved retry" {
		t.Fatalf("retry transition history=%+v", restarted.TransitionHistory)
	}
	now = now.Add(time.Second)
	restarted, retryInstructions, err := IssueCurrentWave(restarted, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(retryInstructions) != 1 {
		t.Fatalf("retry reissued %d actions, want only failed action", len(retryInstructions))
	}
	retry := retryInstructions[0]
	if retry.NodeID != failedInstruction.NodeID || retry.IdempotencyKey != failedInstruction.IdempotencyKey || retry.Attempt != 2 {
		t.Fatalf("retry instruction=%+v initial=%+v", retry, failedInstruction)
	}
	now = now.Add(time.Second)
	restarted, duplicate, err = RecordResult(restarted, retry.NodeID, retry.IdempotencyKey, OutcomeSuccess, "", now)
	if err != nil || duplicate {
		t.Fatalf("retry success duplicate=%t err=%v", duplicate, err)
	}
	if restarted.CurrentWaveIndex != 2 || restarted.State != StateReady || restarted.FailedNodeCount != 0 || restarted.SucceededNodeCount != 6 {
		t.Fatalf("retry did not complete same wave: %+v", restarted)
	}
	revision, digest := restarted.Revision, restarted.OperationDigest
	replayed, duplicate, err := RecordResult(restarted, retry.NodeID, retry.IdempotencyKey, OutcomeSuccess, "", now.Add(time.Second))
	if err != nil || !duplicate {
		t.Fatalf("duplicate success duplicate=%t err=%v", duplicate, err)
	}
	if replayed.Revision != revision || replayed.OperationDigest != digest {
		t.Fatal("duplicate result changed the operation")
	}
}

func TestFailureWaitsForInflightResultsBeforeRetry(t *testing.T) {
	operation := startOperation(t)
	now := operation.StartedAt
	operation = succeedCurrentWave(t, operation, &now)
	now = now.Add(time.Second)
	operation, instructions, err := IssueCurrentWave(operation, now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	operation, _, err = RecordResult(operation, instructions[0].NodeID, instructions[0].IdempotencyKey, OutcomeFailure, "first batch node failed", now)
	if err != nil {
		t.Fatal(err)
	}
	if NextInstructionCount(operation) != 0 {
		t.Fatal("failure pause exposed next instructions")
	}
	if _, err := RetryFailed(operation, "too early", now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "in-progress") {
		t.Fatalf("early retry error=%v", err)
	}
	for _, instruction := range instructions[1:] {
		now = now.Add(time.Second)
		operation, _, err = RecordResult(operation, instruction.NodeID, instruction.IdempotencyKey, OutcomeSuccess, "", now)
		if err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Second)
	if _, err := RetryFailed(operation, "all in-flight results recorded", now); err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
}

func TestRestartReplaysInflightInstructionWithStableIdentity(t *testing.T) {
	operation := startOperation(t)
	issuedAt := operation.StartedAt.Add(time.Second)
	issued, instructions, err := IssueCurrentWave(operation, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(instructions) != 1 || instructions[0].Replayed {
		t.Fatalf("initial instructions=%+v", instructions)
	}
	raw, err := MarshalOperation(issued)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := ParseOperation(raw)
	if err != nil {
		t.Fatal(err)
	}
	replayedState, replayed, err := IssueCurrentWave(restarted, issuedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || !replayed[0].Replayed || replayed[0].Attempt != 1 || replayed[0].IdempotencyKey != instructions[0].IdempotencyKey {
		t.Fatalf("replayed=%+v initial=%+v", replayed, instructions)
	}
	if replayedState.Revision != issued.Revision || replayedState.OperationDigest != issued.OperationDigest {
		t.Fatal("in-flight replay mutated the persisted operation")
	}
}

func TestOperatorPauseResumeAndFullRestartSafeCompletion(t *testing.T) {
	operation := startOperation(t)
	now := operation.StartedAt.Add(time.Second)
	paused, err := Pause(operation, "change window hold", now)
	if err != nil {
		t.Fatal(err)
	}
	if NextInstructionCount(paused) != 0 {
		t.Fatal("operator pause exposed instructions")
	}
	now = now.Add(time.Second)
	operation, err = Resume(paused, "change window opened", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(operation.TransitionHistory) != 2 || operation.TransitionHistory[0].Type != TransitionOperatorPause || operation.TransitionHistory[1].Type != TransitionOperatorResume || operation.TransitionHistory[1].Reason != "change window opened" {
		t.Fatalf("operator transition history=%+v", operation.TransitionHistory)
	}
	for operation.State != StateCompleted {
		operation = succeedCurrentWave(t, operation, &now)
		raw, err := MarshalOperation(operation)
		if err != nil {
			t.Fatal(err)
		}
		operation, err = ParseOperation(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	if operation.CurrentWaveIndex != len(operation.Waves) || operation.SucceededNodeCount != 160 || operation.PendingNodeCount != 0 || operation.FailedNodeCount != 0 || operation.InProgressNodeCount != 0 {
		t.Fatalf("completed operation counters=%+v", operation)
	}
	keys := map[string]bool{}
	for _, wave := range operation.Waves {
		for _, action := range wave.Actions {
			if action.Attempts != 1 || keys[action.IdempotencyKey] {
				t.Fatalf("non-idempotent action=%+v", action)
			}
			keys[action.IdempotencyKey] = true
		}
	}
	if len(keys) != 160 {
		t.Fatalf("unique idempotency keys=%d", len(keys))
	}
}

func TestParseOperationRejectsTamperAndUnknownField(t *testing.T) {
	operation := startOperation(t)
	raw, err := MarshalOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["plan_id"] = "wrong-plan"
	tampered, _ := json.Marshal(object)
	if _, err := ParseOperation(tampered); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tamper error=%v", err)
	}
	withUnknown := strings.Replace(string(raw), "{", "{\"unknown\":true,", 1)
	if _, err := ParseOperation([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error=%v", err)
	}
}

func startOperation(t *testing.T) *Operation {
	t.Helper()
	operation, err := Start(exactStartRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func succeedCurrentWave(t *testing.T, operation *Operation, now *time.Time) *Operation {
	t.Helper()
	*now = now.Add(time.Second)
	next, instructions, err := IssueCurrentWave(operation, *now)
	if err != nil {
		t.Fatal(err)
	}
	for _, instruction := range instructions {
		*now = now.Add(time.Second)
		next, _, err = RecordResult(next, instruction.NodeID, instruction.IdempotencyKey, OutcomeSuccess, "", *now)
		if err != nil {
			t.Fatal(err)
		}
	}
	return next
}

func exactStartRequest(t *testing.T) StartRequest {
	t.Helper()
	manifest, err := clustermanifest.Load(filepath.Join("..", "..", "configs", "sbs-cluster-160.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string{rolloutTestArtifactDigest}, RequireArtifactApproval: true}
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := clustermanifest.Render(canonical, policy)
	if err != nil {
		t.Fatal(err)
	}
	manifestPlan, err := clustermanifest.BuildPlan(canonical, nil, policy)
	if err != nil {
		t.Fatal(err)
	}
	bundles := map[string]clustermanifest.NodeBundle{}
	for _, bundle := range rendered.Bundles {
		bundles[bundle.NodeID] = bundle
	}
	admissionTime := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	joinPlan := &installpreflight.JoinPlan{
		APIVersion: installpreflight.APIVersion, Kind: installpreflight.JoinPlanKind,
		PlanID: manifestPlan.PlanID, ManifestDigest: rendered.ManifestDigest,
		AdmissionTime: admissionTime, AcceptedReportCount: 160, RejectedReportCount: 0,
		ProjectedNodeJoinCount: 160, NoMutationAdmission: true,
	}
	for _, zoneName := range canonical.Spec.Rollout.Order {
		zone := installpreflight.JoinZone{Zone: zoneName}
		for _, node := range canonical.Spec.Nodes {
			if node.Location.Zone != zoneName {
				continue
			}
			zone.Nodes = append(zone.Nodes, installpreflight.JoinNode{
				NodeID: node.ID, Hostname: node.Hostname, BundleDigest: bundles[node.ID].BundleDigest,
				ReportDigest:       testSHA256("report/" + node.ID),
				SignedReportDigest: testSHA256("signed/" + node.ID),
				SignerKeyID:        "host-" + node.ID, ObservedAt: admissionTime.Add(-time.Minute),
			})
		}
		joinPlan.Zones = append(joinPlan.Zones, zone)
	}
	joinPlan.JoinPlanDigest = testJoinPlanDigest(joinPlan)
	joinPlan.JoinPlanID = "ad-join-" + strings.TrimPrefix(joinPlan.JoinPlanDigest, "sha256:")[:16]
	return StartRequest{Manifest: canonical, Policy: policy, JoinPlan: joinPlan, StartedAt: admissionTime.Add(time.Minute)}
}

func testSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func testJoinPlanDigest(plan *installpreflight.JoinPlan) string {
	copyPlan := *plan
	copyPlan.JoinPlanID, copyPlan.JoinPlanDigest = "", ""
	raw, _ := json.Marshal(copyPlan)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
