package metadata

import (
	"context"
	"errors"
	"testing"
)

func TestNodeHealthAffectedProgressIsDurableAndRevisionFenced(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-health")
	if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{
		NodeID: "node-a", LifecycleState: NodeLifecycleActive, HealthState: NodeHealthDown,
	}); err != nil {
		t.Fatal(err)
	}
	node, err := repo.GetNodeMembership(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	progress, err := repo.BeginNodeHealthAffectedProgress(ctx, node)
	if err != nil || progress.Completed || progress.SourceCursor != "" {
		t.Fatalf("begin progress=%+v err=%v", progress, err)
	}
	point, err := repo.GetNodeHealthAffectedProgressPoint(ctx, node.NodeID)
	if err != nil || point.PointGetCount != 1 || point.Progress.ProgressDigest != progress.ProgressDigest || point.BackendFullScanCount != 0 || point.FullCompletionCount != 0 || point.NestedCompletionCount != 0 {
		t.Fatalf("point=%+v err=%v", point, err)
	}
	progress, err = repo.AdvanceNodeHealthAffectedProgress(ctx, progress, "cursor-1", false)
	if err != nil || progress.SourceCursor != "cursor-1" {
		t.Fatalf("advance progress=%+v err=%v", progress, err)
	}
	restarted := NewRepository(kv, "phase-ad-health")
	resumed, err := restarted.BeginNodeHealthAffectedProgress(ctx, node)
	if err != nil || resumed.ProgressDigest != progress.ProgressDigest {
		t.Fatalf("resume progress=%+v err=%v", resumed, err)
	}

	stale := node
	node.HealthState = NodeHealthSuspect
	if err := restarted.PutNodeMembership(ctx, node); err != nil {
		t.Fatal(err)
	}
	node, err = restarted.GetNodeMembership(ctx, node.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	next, err := restarted.BeginNodeHealthAffectedProgress(ctx, node)
	if err != nil || next.MembershipRevision <= stale.MembershipRevision || next.SourceCursor != "" || next.Completed {
		t.Fatalf("next progress=%+v stale=%+v err=%v", next, stale, err)
	}
	if _, err := restarted.BeginNodeHealthAffectedProgress(ctx, stale); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale begin error=%v", err)
	}
	completed, err := restarted.AdvanceNodeHealthAffectedProgress(ctx, next, "", true)
	if err != nil || !completed.Completed {
		t.Fatalf("completed progress=%+v err=%v", completed, err)
	}
}
