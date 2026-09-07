package metadata

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAuthorityMutationsUpdatePromotedSummaryInSameTransaction(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-summary-authority")
	repo.now = func() time.Time { return time.Unix(3000, 123).UTC() }

	membership := NodeMembershipRecord{NodeID: "node1", LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy}
	volume := VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}
	spec := VolumeSpecRecord{VolumeID: volume.VolumeID, SizeBytes: 1024, BlockSize: 4096, ChunkSizeBytes: 256}
	transition := PlacementTransitionRecord{VolumeID: volume.VolumeID, PlacementRef: "placement-1", Reason: "repair", State: PlacementTransitionQueued}
	if err := repo.PutNodeMembership(ctx, membership); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutVolumeSpec(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	contributions := []SummaryContribution{
		mustSummaryContribution(t, summarySubject("membership", membership.NodeID), summaryMembershipContribution(membership, true)),
		mustSummaryContribution(t, summarySubject("placement-transition", transition.VolumeID, transition.PlacementRef), summaryTransitionContribution(transition, true)),
		mustSummaryContribution(t, summarySubject("volume", volume.VolumeID), summaryVolumeContribution(volume, true, spec, true)),
	}
	source := &fixtureSummaryRebuildSource{revision: 2500, contributions: contributions}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "authority-live", len(contributions)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "authority-live", 2500); err != nil {
		t.Fatal(err)
	}

	membership, err := repo.GetNodeMembership(ctx, membership.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	membership.LifecycleState = NodeLifecycleDraining
	membership.HealthState = NodeHealthDown
	if _, _, err := repo.CompareAndSetNodeMembership(ctx, membership, membership.Generation); err != nil {
		t.Fatal(err)
	}
	volume.Status = VolumeStatusDegraded
	volume.Revision++
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	spec.SizeBytes = 2048
	if err := repo.PutVolumeSpec(ctx, spec); err != nil {
		t.Fatal(err)
	}
	transition.State = PlacementTransitionCompleted
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}

	read, err := repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	want := SummaryCounters{
		KnownNodes: 1, DrainingNodes: 1, DownNodes: 1,
		VolumeCount: 1, DegradedVolumes: 1, TotalBytes: 2048, AllocatedChunks: 8,
	}
	if read.Counters != want {
		t.Fatalf("summary counters=%+v want=%+v", read.Counters, want)
	}
	if read.MaximumSourceRevision != summaryMutationRevision(repo.now()) {
		t.Fatalf("maximum source revision=%d", read.MaximumSourceRevision)
	}

	// A revision-only foreground write advances authority without touching the
	// aggregate. A represented counter change writes exactly its one shard and
	// does not leave an applied-event row or update the global state key.
	kv.resetSetCalls()
	volume.Revision++
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	for key, count := range kv.setCalls {
		if strings.HasPrefix(key, repo.root+"/derived/ad/v1/aggregate/cluster/") && count != 0 {
			t.Fatalf("revision-only aggregate write key=%s count=%d", key, count)
		}
	}
	kv.resetSetCalls()
	volume.Status = VolumeStatusHealthy
	volume.Revision++
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	shardWrites := 0
	appliedEventWrites := 0
	for key, count := range kv.setCalls {
		if strings.HasPrefix(key, repo.root+"/derived/ad/v1/aggregate/cluster/") {
			shardWrites += count
		}
		if strings.HasPrefix(key, repo.root+"/derived/ad/v1/applied/") {
			appliedEventWrites += count
		}
	}
	if shardWrites != 1 || appliedEventWrites != 0 || kv.setCallCount(summaryAggregateStateKey(repo.root, SummaryKindCluster)) != 0 {
		t.Fatalf("counter-change writes shard=%d applied_event=%d global=%d", shardWrites, appliedEventWrites, kv.setCallCount(summaryAggregateStateKey(repo.root, SummaryKindCluster)))
	}
}

func TestPromotedSummaryRejectsUnrepresentedAuthorityMutationAtomically(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-summary-fail-closed")
	repo.now = func() time.Time { return time.Unix(4000, 0).UTC() }
	volume := VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 3000}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "empty-live", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "empty-live", 3000); err != nil {
		t.Fatal(err)
	}

	volume.Status = VolumeStatusDegraded
	volume.Revision++
	err = repo.PutVolumeState(ctx, volume)
	if !errors.Is(err, ErrSummaryCounterUnderflow) {
		t.Fatalf("error=%v want summary underflow", err)
	}
	stored, err := repo.GetVolumeState(ctx, volume.VolumeID)
	if err != nil || stored.Status != VolumeStatusHealthy || stored.Revision != 1 {
		t.Fatalf("authority escaped failed summary transaction: stored=%+v err=%v", stored, err)
	}
}

func TestVolumeAuthorityPairUpdatesPromotedSummaryAtomically(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-summary-volume-pair")
	repo.now = func() time.Time { return time.Unix(4500, 0).UTC() }
	source := &fixtureSummaryRebuildSource{revision: 4000}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "empty-volume-pair", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "empty-volume-pair", 4000); err != nil {
		t.Fatal(err)
	}
	state := VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}
	spec := VolumeSpecRecord{VolumeID: state.VolumeID, SizeBytes: 8192, BlockSize: 4096, ChunkSizeBytes: 1024}
	if err := repo.PutVolumeAuthority(ctx, state, spec); err != nil {
		t.Fatal(err)
	}
	read, err := repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if want := (SummaryCounters{VolumeCount: 1, HealthyVolumes: 1, TotalBytes: 8192, AllocatedChunks: 8}); read.Counters != want {
		t.Fatalf("created counters=%+v want=%+v", read.Counters, want)
	}
	if err := repo.DeleteVolumeAuthority(ctx, state.VolumeID); err != nil {
		t.Fatal(err)
	}
	read, err = repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.Counters != (SummaryCounters{}) {
		t.Fatalf("deleted counters=%+v", read.Counters)
	}
}

func TestFailedPlacementTransitionDoesNotClaimMutationBatchFailure(t *testing.T) {
	record := PlacementTransitionRecord{VolumeID: "00a1b2c3", PlacementRef: "failed", Reason: "repair", State: PlacementTransitionFailed}
	if got := summaryTransitionContribution(record, true); got != (SummaryCounters{}) {
		t.Fatalf("failed placement row is not mutation-batch telemetry: %+v", got)
	}
}

func mustSummaryContribution(t *testing.T, subject string, counters SummaryCounters) SummaryContribution {
	t.Helper()
	contribution, err := NewSummaryContribution(subject, counters)
	if err != nil {
		t.Fatal(err)
	}
	return contribution
}
