package rollout

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

func TestServiceActivationRequiresOneIsolatedFailureAndOneStandby(t *testing.T) {
	request, snapshot := exactServiceActivationRequest(t)
	operation, err := BuildServiceActivation(request)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != ServiceActivationReady || operation.CandidateNodeID != "node61" || operation.ProjectedDaemonActions != 1 || operation.ExecutedDaemonActions != 0 {
		t.Fatalf("unexpected operation: %+v", operation)
	}
	alternateRequest := request
	alternateRequest.CandidateNodeID = "node81"
	alternate, err := BuildServiceActivation(alternateRequest)
	if err != nil {
		t.Fatal(err)
	}
	if alternate.OperationID == operation.OperationID || alternate.IdempotencyKey == operation.IdempotencyKey {
		t.Fatal("different standby candidates shared an operation/idempotency identity")
	}

	busy := cloneServiceSnapshot(t, snapshot)
	busy.InFlightActivationOperationID = "another-operation"
	refinalizeServiceSnapshot(t, busy)
	request.Snapshot = busy
	if _, err := BuildServiceActivation(request); err == nil || !strings.Contains(err.Error(), "already in flight") {
		t.Fatalf("simultaneous activation error=%v", err)
	}

	drift := cloneServiceSnapshot(t, snapshot)
	for index := range drift.Nodes {
		if drift.Nodes[index].NodeID == "node61" {
			drift.Nodes[index].BinaryDigest = testSHA256("wrong-binary")
		}
	}
	refinalizeServiceSnapshot(t, drift)
	request.Snapshot = drift
	if _, err := BuildServiceActivation(request); err == nil || !strings.Contains(err.Error(), "drift") {
		t.Fatalf("candidate drift error=%v", err)
	}

	unisolated := cloneServiceSnapshot(t, snapshot)
	for index := range unisolated.Nodes {
		if unisolated.Nodes[index].NodeID == "node41" {
			unisolated.Nodes[index].Isolated = false
		}
	}
	refinalizeServiceSnapshot(t, unisolated)
	request.Snapshot = unisolated
	if _, err := BuildServiceActivation(request); err == nil || !strings.Contains(err.Error(), "stopped and isolated") {
		t.Fatalf("failed active isolation error=%v", err)
	}
}

func TestServiceActivationReplayAndLeaderProjectionVerification(t *testing.T) {
	request, preflight := exactServiceActivationRequest(t)
	operation, err := BuildServiceActivation(request)
	if err != nil {
		t.Fatal(err)
	}
	issued, instruction, err := IssueServiceActivation(operation, request.At.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if instruction.NodeID != "node61" || instruction.Attempt != 1 || instruction.Replayed || instruction.IdempotencyKey == "" {
		t.Fatalf("instruction=%+v", instruction)
	}
	replayedState, replayed, err := IssueServiceActivation(issued, request.At.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.IdempotencyKey != instruction.IdempotencyKey || replayedState.Revision != issued.Revision || replayedState.OperationDigest != issued.OperationDigest {
		t.Fatalf("replayed instruction/state changed: %+v", replayed)
	}
	raw, err := MarshalServiceActivationOperation(issued)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := ParseServiceActivationOperation(raw)
	if err != nil {
		t.Fatal(err)
	}
	post := cloneServiceSnapshot(t, preflight)
	post.ObservedAt = request.At.Add(3 * time.Second)
	post.InFlightActivationOperationID = restarted.OperationID
	for index := range post.Nodes {
		if post.Nodes[index].NodeID == "node61" {
			post.Nodes[index].DaemonRunning = true
			post.Nodes[index].HealthReady = true
			post.Nodes[index].MutationReady = true
		}
	}
	refinalizeServiceSnapshot(t, post)
	completed, verified, err := VerifyServiceActivation(restarted, request.Manifest, request.Policy, post, request.At.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !verified || completed.State != ServiceActivationCompleted || completed.RollbackRequired || completed.ExecutedDaemonActions != 0 {
		t.Fatalf("completed=%+v verified=%t", completed, verified)
	}

	failedPost := cloneServiceSnapshot(t, post)
	failedPost.ProjectionHealthy = false
	refinalizeServiceSnapshot(t, failedPost)
	paused, verified, err := VerifyServiceActivation(restarted, request.Manifest, request.Policy, failedPost, request.At.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if verified || paused.State != ServiceActivationPaused || !paused.RollbackRequired || paused.FirstError == "" || paused.AutomaticRollback {
		t.Fatalf("paused verification=%+v verified=%t", paused, verified)
	}
}

func TestMaintenanceSafetyOverrideAndExitPreflight(t *testing.T) {
	request := exactStartRequest(t)
	manifest := request.Manifest
	now := request.StartedAt.Add(time.Hour)
	safeSnapshot := exactMaintenanceSnapshot(t, manifest, request.Policy, "node37", now)
	plan, err := BuildHostMaintenancePlan(manifest, request.Policy, safeSnapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.UnsafeCheckIDs) != 0 || plan.ExecutedDaemonActions != 0 || !plan.NoMutationPlan {
		t.Fatalf("safe plan=%+v", plan)
	}
	operation, err := EnterHostMaintenance(plan, "operator-a", "INC-2001", "replace disk", nil, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != MaintenanceStateEnterReady || len(operation.Instructions) != 2 || operation.Instructions[1].SafetyGate != "drain-complete-and-replica-safe" {
		t.Fatalf("enter operation=%+v", operation)
	}
	alternate, err := EnterHostMaintenance(plan, "operator-a", "INC-2001-B", "replace disk", nil, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if alternate.OperationID == operation.OperationID || alternate.Instructions[0].IdempotencyKey == operation.Instructions[0].IdempotencyKey {
		t.Fatal("different maintenance incidents shared an operation/idempotency identity")
	}
	raw, err := MarshalHostMaintenanceOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := ParseHostMaintenanceOperation(raw)
	if err != nil {
		t.Fatal(err)
	}
	exitPreflight := exactExitPreflight(restarted, now.Add(3*time.Second))
	exited, err := ExitHostMaintenance(restarted, exitPreflight, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if exited.State != MaintenanceStateExitReady || exited.Revision != 2 || exited.Instructions[0].Action != "start_sbs_data" || exited.ExecutedDaemonActions != 0 {
		t.Fatalf("exit operation=%+v", exited)
	}

	blockedExit := exactExitPreflight(restarted, now.Add(3*time.Second))
	blockedExit.LocalStoreHealthy = false
	if err := FinalizeHostMaintenanceExitPreflight(blockedExit); err != nil {
		t.Fatal(err)
	}
	if _, err := ExitHostMaintenance(restarted, blockedExit, now.Add(4*time.Second)); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("blocked exit error=%v", err)
	}
}

func TestMaintenanceUnsafeCheckNeedsExactAuditedOverride(t *testing.T) {
	request := exactStartRequest(t)
	now := request.StartedAt.Add(time.Hour)
	snapshot := exactMaintenanceSnapshot(t, request.Manifest, request.Policy, "node37", now)
	for index := range snapshot.Checks {
		if snapshot.Checks[index].ID == MaintenanceCheckCapacity {
			snapshot.Checks[index].Safe = false
			snapshot.Checks[index].Observed = "remaining=8GiB required=100GiB"
			snapshot.Checks[index].Message = "insufficient target capacity"
		}
	}
	if err := FinalizeHostMaintenanceSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildHostMaintenancePlan(request.Manifest, request.Policy, snapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !sameStrings(plan.UnsafeCheckIDs, []string{MaintenanceCheckCapacity}) {
		t.Fatalf("unsafe checks=%v", plan.UnsafeCheckIDs)
	}
	if _, err := EnterHostMaintenance(plan, "operator-a", "INC-2002", "capacity incident", nil, now.Add(2*time.Second)); err == nil || !strings.Contains(err.Error(), "check-specific") {
		t.Fatalf("missing override error=%v", err)
	}
	blanket := &MaintenanceOverrideAudit{
		CheckIDs: []string{MaintenanceCheckCapacity, MaintenanceCheckReplicaSafety}, Caller: "operator-a",
		IncidentID: "INC-2002", Reason: "blanket", ExpiresAt: now.Add(time.Hour),
	}
	if _, err := EnterHostMaintenance(plan, "operator-a", "INC-2002", "capacity incident", blanket, now.Add(2*time.Second)); err == nil || !strings.Contains(err.Error(), "exactly match") {
		t.Fatalf("blanket override error=%v", err)
	}
	override := &MaintenanceOverrideAudit{
		CheckIDs: []string{MaintenanceCheckCapacity}, Caller: "operator-a", IncidentID: "INC-2002",
		Reason: "temporary emergency capacity exception", ExpiresAt: now.Add(time.Hour),
	}
	operation, err := EnterHostMaintenance(plan, "operator-a", "INC-2002", "capacity incident", override, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if operation.Override == nil || !sameStrings(operation.Override.CheckIDs, []string{MaintenanceCheckCapacity}) || operation.Override.IncidentID != "INC-2002" {
		t.Fatalf("override audit=%+v", operation.Override)
	}
}

func TestSafetyArtifactsRejectTamperAndUnknownFields(t *testing.T) {
	request, snapshot := exactServiceActivationRequest(t)
	raw, err := MarshalServiceActivationSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := strings.Replace(string(raw), "{", "{\"unknown\":true,", 1)
	if _, err := ParseServiceActivationSnapshot([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("snapshot unknown field error=%v", err)
	}
	operation, err := BuildServiceActivation(request)
	if err != nil {
		t.Fatal(err)
	}
	opRaw, err := MarshalServiceActivationOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(opRaw, &object); err != nil {
		t.Fatal(err)
	}
	object["candidate_node_id"] = "node81"
	tampered, _ := json.Marshal(object)
	if _, err := ParseServiceActivationOperation(tampered); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("operation tamper error=%v", err)
	}
}

func exactServiceActivationRequest(t *testing.T) (ServiceActivationRequest, *ServiceActivationSnapshot) {
	t.Helper()
	request := exactStartRequest(t)
	rendered, err := clustermanifest.Render(request.Manifest, request.Policy)
	if err != nil {
		t.Fatal(err)
	}
	bundles := safetyBundleMap(rendered)
	snapshot := &ServiceActivationSnapshot{
		APIVersion: APIVersion, Kind: ServiceSnapshotKind, ManifestDigest: rendered.ManifestDigest,
		ObservedAt: request.StartedAt.Add(time.Hour), LeaderNodeID: "node1", ProjectionHealthy: true,
	}
	serviceNodes := append(append([]string{}, request.Manifest.Spec.ServicePlacement.ActiveHosts...), request.Manifest.Spec.ServicePlacement.StandbyCandidates...)
	for _, nodeID := range serviceNodes {
		observation := ServiceNodeObservation{
			NodeID: nodeID, ManifestDigest: rendered.ManifestDigest, BundleDigest: bundles[nodeID].BundleDigest,
			BinaryDigest: request.Manifest.Spec.Artifact.BinaryDigests["sbs-service"], ConfigRevision: request.Manifest.Spec.Bundle.ConfigRevision,
			PDReachable: true, TLSReady: true, ClockReady: true,
		}
		if containsSafetyString(request.Manifest.Spec.ServicePlacement.ActiveHosts, nodeID) && nodeID != "node41" {
			observation.DaemonRunning, observation.HealthReady, observation.MutationReady = true, true, true
		}
		if nodeID == "node41" {
			observation.Isolated = true
		}
		snapshot.Nodes = append(snapshot.Nodes, observation)
	}
	if err := FinalizeServiceActivationSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	return ServiceActivationRequest{
		Manifest: request.Manifest, Policy: request.Policy, JoinPlan: request.JoinPlan, Snapshot: snapshot,
		FailedActiveNodeID: "node41", CandidateNodeID: "node61", Caller: "operator-a",
		IncidentID: "INC-1001", Reason: "replace isolated active", At: snapshot.ObservedAt.Add(time.Second),
	}, snapshot
}

func exactMaintenanceSnapshot(t *testing.T, manifest *clustermanifest.Manifest, policy clustermanifest.ValidationPolicy, nodeID string, observedAt time.Time) *HostMaintenanceSnapshot {
	t.Helper()
	rendered, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &HostMaintenanceSnapshot{
		APIVersion: APIVersion, Kind: MaintenanceSnapshotKind, ManifestDigest: rendered.ManifestDigest,
		NodeID: nodeID, BundleDigest: safetyBundleMap(rendered)[nodeID].BundleDigest, ObservedAt: observedAt,
		AffectedPlacementCount: 12, ExpectedMoveBytes: 1 << 30, ActiveMaintenanceCount: 0, MaintenanceConcurrency: 1,
	}
	for _, id := range requiredMaintenanceChecks {
		snapshot.Checks = append(snapshot.Checks, MaintenanceCheck{ID: id, Safe: true, Observed: "safe"})
	}
	if err := FinalizeHostMaintenanceSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func exactExitPreflight(operation *HostMaintenanceOperation, observedAt time.Time) *HostMaintenanceExitPreflight {
	preflight := &HostMaintenanceExitPreflight{
		APIVersion: APIVersion, Kind: MaintenanceExitPreflightKind, OperationID: operation.OperationID,
		PlanID: operation.PlanID, ManifestDigest: operation.ManifestDigest, NodeID: operation.NodeID,
		BundleDigest: operation.BundleDigest, HostReportDigest: testSHA256("exit-report/" + operation.NodeID), ObservedAt: observedAt,
		MaintenanceActive: true, DrainComplete: true, DaemonStopped: true, HostIdentityVerified: true,
		HostPreflightPassed: true, StorageClaimsMatch: true, BinaryDigestMatch: true,
		ConfigRevisionMatch: true, LocalStoreHealthy: true,
	}
	_ = FinalizeHostMaintenanceExitPreflight(preflight)
	return preflight
}

func cloneServiceSnapshot(t *testing.T, value *ServiceActivationSnapshot) *ServiceActivationSnapshot {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	result := new(ServiceActivationSnapshot)
	if err := json.Unmarshal(raw, result); err != nil {
		t.Fatal(err)
	}
	return result
}

func refinalizeServiceSnapshot(t *testing.T, value *ServiceActivationSnapshot) {
	t.Helper()
	value.SnapshotDigest = ""
	if err := FinalizeServiceActivationSnapshot(value); err != nil {
		t.Fatal(err)
	}
}
