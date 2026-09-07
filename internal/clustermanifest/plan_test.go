package clustermanifest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildPlanClassifiesExactActionsAndDoesNotExecuteThem(t *testing.T) {
	manifest := exactManifestForTest()
	rendered, err := Render(manifest, testPolicy())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	current, err := ObservedStateFromRenderSet(rendered, "running")
	if err != nil {
		t.Fatalf("ObservedStateFromRenderSet: %v", err)
	}
	same, err := BuildPlan(manifest, current, testPolicy())
	if err != nil {
		t.Fatalf("BuildPlan same: %v", err)
	}
	if same.ActionCounts[ActionNoOp] != ExactNodeCount || len(same.Actions) != ExactNodeCount {
		t.Fatalf("same plan counts=%v actions=%d", same.ActionCounts, len(same.Actions))
	}
	if same.ExecutedTiKVMutations != 0 || same.ExecutedDaemonActions != 0 || same.ExecutedStorageMutations != 0 || !same.NoMutationDryRun {
		t.Fatalf("same plan executed an action: %+v", same)
	}

	mixed, err := ObservedStateFromRenderSet(rendered, "running")
	if err != nil {
		t.Fatalf("ObservedStateFromRenderSet mixed: %v", err)
	}
	mixed.Nodes = mixed.Nodes[1:]
	mixed.Nodes[0].BundleDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	mixed.Nodes[0].DaemonState = "stopped"
	mixed.Nodes[1].BundleDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mixed.Nodes[1].DaemonState = "running"
	mixed.Nodes[2].BlockedReasons = []string{"SBS_CONFIG_DRIFT"}
	plan, err := BuildPlan(manifest, mixed, testPolicy())
	if err != nil {
		t.Fatalf("BuildPlan mixed: %v", err)
	}
	want := map[string]int{ActionCreate: 1, ActionUpdate: 1, ActionRestart: 1, ActionBlocked: 1, ActionNoOp: 156}
	for action, count := range want {
		if plan.ActionCounts[action] != count {
			t.Fatalf("action %s count=%d want=%d all=%v", action, plan.ActionCounts[action], count, plan.ActionCounts)
		}
	}
	if plan.ProjectedDaemonRestarts != 1 || plan.PlanUnblocked {
		t.Fatalf("mixed plan restart/admission=%+v", plan)
	}
	if plan.ExecutedTiKVMutations != 0 || plan.ExecutedDaemonActions != 0 || plan.ExecutedStorageMutations != 0 {
		t.Fatalf("mixed plan executed an action: %+v", plan)
	}
}

func TestPlanAndCanonicalExportAreDeterministicAndNoOverwrite(t *testing.T) {
	manifest := exactManifestForTest()
	first, err := BuildPlan(manifest, nil, testPolicy())
	if err != nil {
		t.Fatalf("BuildPlan first: %v", err)
	}
	second, err := BuildPlan(manifest, nil, testPolicy())
	if err != nil {
		t.Fatalf("BuildPlan second: %v", err)
	}
	if first.PlanID != second.PlanID || first.PlanDigest != second.PlanDigest || first.ActionCounts[ActionCreate] != ExactNodeCount {
		t.Fatalf("plan is not deterministic: first=%+v second=%+v", first, second)
	}

	raw, err := CanonicalYAML(manifest)
	if err != nil {
		t.Fatalf("CanonicalYAML: %v", err)
	}
	exported, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse exported: %v", err)
	}
	before, _ := Digest(manifest)
	after, _ := Digest(exported)
	if before != after {
		t.Fatalf("export/reparse digest=%s want=%s", after, before)
	}
	path := filepath.Join(t.TempDir(), "export.yaml")
	if err := WriteNewFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteNewFile: %v", err)
	}
	if err := WriteNewFile(path, []byte("overwrite"), 0o644); err == nil {
		t.Fatalf("WriteNewFile overwrote an existing artifact")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("existing export changed after overwrite refusal")
	}
}
