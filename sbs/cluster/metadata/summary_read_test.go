package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestClusterSummaryReadUsesOneBoundedSnapshotBatch(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-summary-read")
	repo.now = func() time.Time { return time.Unix(1000, 0).UTC() }
	counters := SummaryCounters{
		VolumeCount: 1, HealthyVolumes: 1,
		DegradedExtents: 4,
		RepairBacklog:   2, RepairBacklogBytes: 200, RepairBacklogChunks: 20,
		RebalanceBacklog: 3, RebalanceBacklogBytes: 300, RebalanceBacklogChunks: 30,
		DrainBacklog: 4, DrainBacklogBytes: 400, DrainBacklogChunks: 40,
	}
	contribution, err := NewSummaryContribution("volume:summary-read", counters)
	if err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 140, contributions: []SummaryContribution{contribution}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-read", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-read", 140); err != nil {
		t.Fatal(err)
	}

	read, err := repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.Counters != counters || read.State.ActiveEpoch != "epoch-read" || read.BaselineSourceRevision != 140 || read.MaximumSourceRevision != 140 {
		t.Fatalf("summary read=%+v", read)
	}
	if read.PointGetCount != 1 || read.BatchGetCount != 1 || read.BatchGetKeyCount != 64 || read.BackendFullScanCount != 0 || read.FullCompletionCount != 0 || read.NestedCompletionCount != 0 {
		t.Fatalf("request-class evidence=%+v", read)
	}

	before := counters
	after := counters
	after.HealthyVolumes = 0
	after.DegradedVolumes = 1
	delta, err := NewSummaryDelta("event-summary-read-update", "volume:summary-read", 141, before, after)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.ApplySummaryDelta(ctx, delta); err != nil || !applied {
		t.Fatalf("update=%t err=%v", applied, err)
	}
	read, err = repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.Counters != after || read.BaselineSourceRevision != 140 || read.MaximumSourceRevision != 141 {
		t.Fatalf("updated summary read=%+v", read)
	}

	pending, err := NewSummaryDelta("event-summary-read-pending", "operation:pending", 142, SummaryCounters{}, SummaryCounters{})
	if err != nil {
		t.Fatal(err)
	}
	if enqueued, err := repo.EnqueueSummaryDelta(ctx, pending); err != nil || !enqueued {
		t.Fatalf("pending=%t err=%v", enqueued, err)
	}
	if _, err := repo.GetClusterSummary(ctx, SummaryKindCluster); !errors.Is(err, ErrSummaryOutboxPending) {
		t.Fatalf("pending summary read error=%v", err)
	}
}

type checksumChurningSummaryKV struct {
	*fakeTransactionalKV
	root       string
	batchCalls int
}

func (kv *checksumChurningSummaryKV) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, found, err := kv.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			values[key] = value
		}
	}
	kv.batchCalls++
	if kv.batchCalls == 1 {
		key := summaryAggregateKey(kv.root, SummaryKindCluster, 0)
		raw, found, err := kv.Get(ctx, key)
		if err != nil || !found {
			return nil, err
		}
		var shard SummaryAggregateShard
		if err := json.Unmarshal(raw, &shard); err != nil {
			return nil, err
		}
		shard.UpdatedAtUnix++
		shard.Checksum = digestSummaryAggregateShard(shard)
		encoded, err := json.Marshal(shard)
		if err != nil {
			return nil, err
		}
		if err := kv.Set(ctx, key, encoded); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func TestClusterSummaryReadFallbackRejectsChecksumOnlyChange(t *testing.T) {
	ctx := context.Background()
	root := "phase-ad-summary-read-checksum-fence"
	base := newFakeTransactionalKV()
	repo := NewRepository(base, root)
	repo.now = func() time.Time { return time.Unix(1200, 0).UTC() }
	contribution, err := NewSummaryContribution("volume:checksum-fence", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 160, contributions: []SummaryContribution{contribution}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-checksum-fence", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-checksum-fence", 160); err != nil {
		t.Fatal(err)
	}
	churning := &checksumChurningSummaryKV{fakeTransactionalKV: base, root: root}
	_, err = NewRepository(churning, root).GetClusterSummary(ctx, SummaryKindCluster)
	if !errors.Is(err, ErrSummaryShadowAggregateChanged) {
		t.Fatalf("checksum-only change error=%v", err)
	}
}

func TestClusterSummaryReadFallbackDoubleChecksAggregate(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-summary-read-fallback")
	repo.now = func() time.Time { return time.Unix(1100, 0).UTC() }
	contribution, err := NewSummaryContribution("volume:fallback", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 150, contributions: []SummaryContribution{contribution}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-fallback", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-fallback", 150); err != nil {
		t.Fatal(err)
	}
	read, err := repo.GetClusterSummary(ctx, SummaryKindCluster)
	if err != nil {
		t.Fatal(err)
	}
	if read.PointGetCount != 2 || read.BatchGetCount != 2 || read.BatchGetKeyCount != 128 {
		t.Fatalf("fallback request-class evidence=%+v", read)
	}
}

func TestClusterSummaryAssessmentClassifiesFreshStaleExpiredAndPartial(t *testing.T) {
	ctx := context.Background()
	root := "phase-ad-summary-assessment"
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, root)
	repo.now = func() time.Time { return time.Unix(2000, 0).UTC() }
	contribution, err := NewSummaryContribution("volume:assessment", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 200, contributions: []SummaryContribution{contribution}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-assessment", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-assessment", 200); err != nil {
		t.Fatal(err)
	}
	policy := ClusterSummaryPolicy{DegradedAfter: 10 * time.Second, RebuildRequiredAfter: 20 * time.Second}

	ready, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2005, 0).UTC(), policy)
	if err != nil || ready.Health != SummaryHealthReady || ready.Reason != "none" || ready.Partial || ready.Stale || ready.RebuildRequired || ready.FreshnessAgeMillis != 5000 {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	degraded, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2015, 0).UTC(), policy)
	if err != nil || degraded.Health != SummaryHealthDegraded || degraded.Reason != "freshness_stale" || degraded.Partial || !degraded.Stale || degraded.RebuildRequired {
		t.Fatalf("degraded=%+v err=%v", degraded, err)
	}
	expired, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2025, 0).UTC(), policy)
	if err != nil || expired.Health != SummaryHealthRebuildRequired || expired.Reason != "freshness_expired" || expired.Partial || !expired.Stale || !expired.RebuildRequired {
		t.Fatalf("expired=%+v err=%v", expired, err)
	}
	future, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(1990, 0).UTC(), policy)
	if err != nil || future.Health != SummaryHealthRebuildRequired || future.Reason != "aggregate_timestamp_in_future" || !future.Partial || future.Stale || !future.RebuildRequired {
		t.Fatalf("future=%+v err=%v", future, err)
	}
	if _, err := repo.RefreshClusterSummaryFreshness(ctx, SummaryKindCluster, time.Unix(2012, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	refreshed, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2015, 0).UTC(), policy)
	if err != nil || refreshed.Health != SummaryHealthReady || refreshed.FreshnessAgeMillis != 3000 || refreshed.FreshnessUpdatedUnix != 2012 {
		t.Fatalf("refreshed=%+v err=%v", refreshed, err)
	}

	pending, err := NewSummaryDelta("event-assessment-pending", "operation:assessment", 201, SummaryCounters{}, SummaryCounters{})
	if err != nil {
		t.Fatal(err)
	}
	if enqueued, err := repo.EnqueueSummaryDelta(ctx, pending); err != nil || !enqueued {
		t.Fatalf("pending=%t err=%v", enqueued, err)
	}
	partial, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2005, 0).UTC(), policy)
	if err != nil || partial.Health != SummaryHealthDegraded || partial.Reason != "outbox_pending" || !partial.Partial || partial.Stale || partial.RebuildRequired || partial.Read.Counters != (SummaryCounters{}) {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
	if result, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", SummaryOutboxMaxPageSize); err != nil || result.ProcessedCount != 1 {
		t.Fatalf("process pending=%+v err=%v", result, err)
	}
	key := summaryAggregateKey(root, SummaryKindCluster, 0)
	raw, found, err := kv.Get(ctx, key)
	if err != nil || !found {
		t.Fatalf("get shard found=%t err=%v", found, err)
	}
	var shard SummaryAggregateShard
	if err := json.Unmarshal(raw, &shard); err != nil {
		t.Fatal(err)
	}
	shard.Checksum = "sha256:invalid"
	raw, err = json.Marshal(shard)
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, key, raw); err != nil {
		t.Fatal(err)
	}
	invalid, err := repo.AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2005, 0).UTC(), policy)
	if err != nil || invalid.Health != SummaryHealthRebuildRequired || invalid.Reason != "aggregate_invalid" || !invalid.Partial || !invalid.RebuildRequired || invalid.Read.Counters != (SummaryCounters{}) {
		t.Fatalf("invalid=%+v err=%v", invalid, err)
	}

	missing, err := NewRepository(newFakeTransactionalKV(), root+"-missing").AssessClusterSummary(ctx, SummaryKindCluster, time.Unix(2005, 0).UTC(), policy)
	if err != nil || missing.Health != SummaryHealthRebuildRequired || missing.Reason != "aggregate_missing" || !missing.Partial || !missing.RebuildRequired || missing.Read.Counters != (SummaryCounters{}) {
		t.Fatalf("missing=%+v err=%v", missing, err)
	}
}
