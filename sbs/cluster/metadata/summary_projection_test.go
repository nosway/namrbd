package metadata

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestSummaryDeltaIsShardLocalTransactionalAndIdempotent(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-summary")
	repo.now = func() time.Time { return time.Unix(100, 0).UTC() }
	healthy := volumeSummaryContribution(1024, 4, VolumeStatusHealthy)
	delta, err := NewSummaryDelta("event-create-volume-a", "volume:a", 10, SummaryCounters{}, healthy)
	if err != nil {
		t.Fatal(err)
	}
	err = RunInTransaction(ctx, kv, func(tx ReadWriter) error {
		if err := tx.Set(ctx, "phase-ad-summary/source/volume:a", []byte("revision=10")); err != nil {
			return err
		}
		applied, err := repo.ApplySummaryDeltaInTransaction(ctx, tx, delta)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("first transactional delta was idempotent")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := repo.GetSummaryAggregateShard(ctx, SummaryKindCluster, delta.VirtualShard)
	if err != nil {
		t.Fatal(err)
	}
	if shard.Counters != healthy || shard.SourceRevision != 10 {
		t.Fatalf("shard=%+v want counters=%+v revision=10", shard, healthy)
	}
	if kv.setCalls[summaryAggregateStateKey(repo.root, SummaryKindCluster)] != 0 {
		t.Fatal("per-mutation delta wrote the global aggregate-state key")
	}
	beforeSetCount := kv.setCalls[summaryAggregateKey(repo.root, SummaryKindCluster, delta.VirtualShard)]
	applied, err := repo.ApplySummaryDelta(ctx, delta)
	if err != nil {
		t.Fatal(err)
	}
	if applied || kv.setCalls[summaryAggregateKey(repo.root, SummaryKindCluster, delta.VirtualShard)] != beforeSetCount {
		t.Fatal("idempotent delta rewrote its aggregate shard")
	}

	degraded := volumeSummaryContribution(2048, 8, VolumeStatusDegraded)
	update, err := NewSummaryDelta("event-update-volume-a", "volume:a", 11, healthy, degraded)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.ApplySummaryDelta(ctx, update); err != nil || !applied {
		t.Fatalf("update applied=%t err=%v", applied, err)
	}
	shard, err = repo.GetSummaryAggregateShard(ctx, SummaryKindCluster, update.VirtualShard)
	if err != nil {
		t.Fatal(err)
	}
	if shard.Counters != degraded || shard.SourceRevision != 11 {
		t.Fatalf("updated shard=%+v", shard)
	}

	conflicting := update
	conflicting.After.TotalBytes++
	conflicting.DeltaDigest = digestSummaryDelta(conflicting)
	if _, err := repo.ApplySummaryDelta(ctx, conflicting); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("conflicting event error=%v want ErrCASConflict", err)
	}
	deleteMissing, err := NewSummaryDelta("event-delete-missing", "volume:missing", 12, healthy, SummaryCounters{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ApplySummaryDelta(ctx, deleteMissing); !errors.Is(err, ErrSummaryCounterUnderflow) {
		t.Fatalf("underflow error=%v", err)
	}

	plainRepo := NewRepository(newFakeKV(), "phase-ad-summary-plain")
	if _, err := plainRepo.ApplySummaryDelta(ctx, delta); !errors.Is(err, ErrSummaryTransactionRequired) {
		t.Fatalf("non-transactional apply error=%v", err)
	}
}

func TestSummaryOutboxPageIsBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-outbox")
	now := time.Unix(200, 0).UTC()
	repo.now = func() time.Time { return now }
	deltas := make([]SummaryDelta, 0, 4)
	for index := 0; index < 3; index++ {
		counters := volumeSummaryContribution(uint64(100+index), uint64(index+1), VolumeStatusHealthy)
		delta, err := NewSummaryDelta(fmt.Sprintf("event-%03d", index), fmt.Sprintf("volume:%03d", index), uint64(20+index), SummaryCounters{}, counters)
		if err != nil {
			t.Fatal(err)
		}
		deltas = append(deltas, delta)
		enqueued, err := repo.EnqueueSummaryDelta(ctx, delta)
		if err != nil || !enqueued {
			t.Fatalf("enqueue %d=%t err=%v", index, enqueued, err)
		}
	}
	now = now.Add(time.Second)
	enqueued, err := repo.EnqueueSummaryDelta(ctx, deltas[0])
	if err != nil || enqueued {
		t.Fatalf("duplicate enqueue=%t err=%v", enqueued, err)
	}
	if pending := sumSummaryPendingOutbox(t, repo); pending != 3 {
		t.Fatalf("pending outbox after enqueue=%d, want 3", pending)
	}
	first, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.ListedCount != 2 || first.ProcessedCount != 2 || first.NextCursor == "" || first.RangePageCount != 1 || first.FullCompletionCount != 0 {
		t.Fatalf("first outbox page=%+v", first)
	}
	if pending := sumSummaryPendingOutbox(t, repo); pending != 1 {
		t.Fatalf("pending outbox after first page=%d, want 1", pending)
	}
	second, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, first.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.ListedCount != 1 || second.ProcessedCount != 1 || second.NextCursor != "" || second.FullCompletionCount != 0 {
		t.Fatalf("second outbox page=%+v", second)
	}
	if pending := sumSummaryPendingOutbox(t, repo); pending != 0 {
		t.Fatalf("pending outbox after second page=%d, want 0", pending)
	}
	total := sumSummaryShards(t, repo)
	if total.VolumeCount != 3 || total.HealthyVolumes != 3 || total.TotalBytes != 303 || total.AllocatedChunks != 6 {
		t.Fatalf("outbox aggregate total=%+v", total)
	}

	// An already-applied event may still be observed in an outbox after a
	// producer retry. The shard marker makes consumption idempotent.
	if enqueued, err := repo.EnqueueSummaryDelta(ctx, deltas[0]); err != nil || !enqueued {
		t.Fatalf("reenqueue applied event=%t err=%v", enqueued, err)
	}
	if pending := sumSummaryPendingOutbox(t, repo); pending != 1 {
		t.Fatalf("pending outbox after applied-event retry=%d, want 1", pending)
	}
	idempotent, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if idempotent.ProcessedCount != 1 || idempotent.IdempotentCount != 1 {
		t.Fatalf("idempotent outbox page=%+v", idempotent)
	}
	if pending := sumSummaryPendingOutbox(t, repo); pending != 0 {
		t.Fatalf("pending outbox after idempotent consume=%d, want 0", pending)
	}
	if got := sumSummaryShards(t, repo); got != total {
		t.Fatalf("idempotent consume changed aggregate: %+v != %+v", got, total)
	}
	if _, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", SummaryOutboxMaxPageSize+1); err == nil {
		t.Fatal("oversized outbox page unexpectedly passed")
	}
}

func TestSummaryRebuildIsCursorBoundedRestartSafeAndRevisionFenced(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-rebuild")
	now := time.Unix(300, 0).UTC()
	repo.now = func() time.Time { return now }
	source := &fixtureSummaryRebuildSource{revision: 50}
	for index := 0; index < 5; index++ {
		contribution, err := NewSummaryContribution(
			fmt.Sprintf("volume:%03d", index),
			volumeSummaryContribution(uint64(1000+index), uint64(index+1), VolumeStatusHealthy),
		)
		if err != nil {
			t.Fatal(err)
		}
		source.contributions = append(source.contributions, contribution)
	}
	for expectedPage := 1; expectedPage <= 3; expectedPage++ {
		result, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-001", 2)
		if err != nil {
			t.Fatal(err)
		}
		if result.PageCount != uint64(expectedPage) || result.InputCount > 2 || result.BackendFullScanCount != 0 || result.FullCompletionCount != 0 {
			t.Fatalf("rebuild page %d=%+v", expectedPage, result)
		}
		// A new repository instance proves continuation comes from the durable
		// checkpoint rather than process memory.
		repo = NewRepository(kv, "phase-ad-rebuild")
		repo.now = func() time.Time { return now }
	}
	checkpoint, err := repo.GetSummaryRebuildCheckpoint(ctx, SummaryKindCluster, "epoch-001")
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Completed || checkpoint.ProcessedCount != 5 || checkpoint.PageCount != 3 || source.calls != 3 || source.maximumLimit != 2 {
		t.Fatalf("checkpoint=%+v source calls=%d max_limit=%d", checkpoint, source.calls, source.maximumLimit)
	}
	if kv.setCalls[summaryAggregateStateKey(repo.root, SummaryKindCluster)] != 0 {
		t.Fatal("rebuild pages wrote aggregate-state before promotion")
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-001", 51); !errors.Is(err, ErrSummaryRebuildSourceChanged) {
		t.Fatalf("stale promotion error=%v", err)
	}
	state, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-001", 50)
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveEpoch != "epoch-001" || state.SourceRevision != 50 || state.State != SummaryAggregateStateReady {
		t.Fatalf("aggregate state=%+v", state)
	}
	total := sumSummaryShards(t, repo)
	if total.VolumeCount != 5 || total.HealthyVolumes != 5 || total.TotalBytes != 5010 || total.AllocatedChunks != 15 {
		t.Fatalf("promoted totals=%+v", total)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-001", 50); err != nil {
		t.Fatalf("idempotent promotion: %v", err)
	}
	completedResult, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-001", 2)
	if err != nil {
		t.Fatal(err)
	}
	if completedResult.RangePageCount != 0 || source.calls != 3 || !completedResult.Promoted {
		t.Fatalf("completed rebuild performed another source page: %+v calls=%d", completedResult, source.calls)
	}
}

func TestSummaryRebuildRejectsMidRunRevisionChangeDuplicateAndIncompletePromotion(t *testing.T) {
	ctx := context.Background()
	contributionA, err := NewSummaryContribution("volume:a", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	contributionB, err := NewSummaryContribution("volume:b", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-rebuild-negative")
	repo.now = func() time.Time { return time.Unix(400, 0).UTC() }
	source := &fixtureSummaryRebuildSource{revision: 60, contributions: []SummaryContribution{contributionA, contributionB}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-changing", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-changing", 60); !errors.Is(err, ErrSummaryRebuildIncomplete) {
		t.Fatalf("incomplete promotion error=%v", err)
	}
	source.revision = 61
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-changing", 1); !errors.Is(err, ErrSummaryRebuildSourceChanged) {
		t.Fatalf("revision change error=%v", err)
	}

	duplicateKV := newFakeTransactionalKV()
	duplicateRepo := NewRepository(duplicateKV, "phase-ad-rebuild-duplicate")
	duplicateRepo.now = func() time.Time { return time.Unix(500, 0).UTC() }
	duplicateSource := &fixtureSummaryRebuildSource{revision: 70, contributions: []SummaryContribution{contributionA, contributionA}}
	if _, err := duplicateRepo.RunSummaryRebuildPage(ctx, duplicateSource, SummaryKindCluster, "epoch-duplicate", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := duplicateRepo.RunSummaryRebuildPage(ctx, duplicateSource, SummaryKindCluster, "epoch-duplicate", 1); !errors.Is(err, ErrSummaryRebuildDuplicateSubject) {
		t.Fatalf("duplicate subject error=%v", err)
	}
	if _, err := duplicateRepo.RunSummaryRebuildPage(ctx, duplicateSource, SummaryKindCluster, "epoch-too-large", SummaryRebuildMaxPageSize+1); err == nil {
		t.Fatal("oversized rebuild page unexpectedly passed")
	}
}

func TestSummaryRebuildPromotionRejectsPendingOutbox(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-rebuild-pending")
	repo.now = func() time.Time { return time.Unix(600, 0).UTC() }
	contribution, err := NewSummaryContribution("volume:a", volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	source := &fixtureSummaryRebuildSource{revision: 80, contributions: []SummaryContribution{contribution}}
	if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-pending", 1); err != nil {
		t.Fatal(err)
	}
	delta, err := NewSummaryDelta("event-pending", "volume:a", 80, SummaryCounters{}, volumeSummaryContribution(1, 1, VolumeStatusHealthy))
	if err != nil {
		t.Fatal(err)
	}
	if enqueued, err := repo.EnqueueSummaryDelta(ctx, delta); err != nil || !enqueued {
		t.Fatalf("enqueue=%t err=%v", enqueued, err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-pending", 80); !errors.Is(err, ErrSummaryOutboxPending) {
		t.Fatalf("pending outbox promotion error=%v", err)
	}
	if _, err := repo.GetSummaryAggregateState(ctx, SummaryKindCluster); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aggregate state after rejected promotion error=%v", err)
	}
	if _, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", 1); err != nil {
		t.Fatal(err)
	}
	advanced, err := NewSummaryDelta("event-advanced", "operation:advanced", 81, SummaryCounters{}, SummaryCounters{})
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.ApplySummaryDelta(ctx, advanced); err != nil || !applied {
		t.Fatalf("advanced delta=%t err=%v", applied, err)
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-pending", 80); !errors.Is(err, ErrSummaryRebuildSourceChanged) {
		t.Fatalf("advanced source promotion error=%v", err)
	}
}

type fixtureSummaryRebuildSource struct {
	contributions []SummaryContribution
	revision      uint64
	calls         int
	maximumLimit  int
}

func (source *fixtureSummaryRebuildSource) ListSummaryContributions(_ context.Context, kind, cursor string, limit int) (SummaryRebuildSourcePage, error) {
	if kind != SummaryKindCluster {
		return SummaryRebuildSourcePage{}, fmt.Errorf("unexpected kind %s", kind)
	}
	source.calls++
	if limit > source.maximumLimit {
		source.maximumLimit = limit
	}
	start := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil {
			return SummaryRebuildSourcePage{}, err
		}
		start = parsed
	}
	end := start + limit
	if end > len(source.contributions) {
		end = len(source.contributions)
	}
	next := ""
	if end < len(source.contributions) {
		next = strconv.Itoa(end)
	}
	return SummaryRebuildSourcePage{
		Contributions: append([]SummaryContribution(nil), source.contributions[start:end]...),
		NextCursor:    next, SourceRevision: source.revision,
	}, nil
}

func volumeSummaryContribution(bytes, chunks uint64, status VolumeStatus) SummaryCounters {
	counters := SummaryCounters{VolumeCount: 1, TotalBytes: bytes, AllocatedChunks: chunks}
	switch status {
	case VolumeStatusHealthy:
		counters.HealthyVolumes = 1
	case VolumeStatusDegraded:
		counters.DegradedVolumes = 1
	case VolumeStatusRepairing:
		counters.RepairingVolumes = 1
	case VolumeStatusRebalancing:
		counters.RebalancingVolumes = 1
	case VolumeStatusBlocked:
		counters.BlockedVolumes = 1
	}
	return counters
}

func sumSummaryShards(t *testing.T, repo *Repository) SummaryCounters {
	t.Helper()
	total := SummaryCounters{}
	for shardID := 0; shardID < SummaryVirtualShardCount; shardID++ {
		shard, err := repo.GetSummaryAggregateShard(context.Background(), SummaryKindCluster, shardID)
		if err != nil {
			t.Fatal(err)
		}
		total, err = addSummaryCounters(total, shard.Counters)
		if err != nil {
			t.Fatal(err)
		}
	}
	return total
}

func sumSummaryPendingOutbox(t *testing.T, repo *Repository) uint64 {
	t.Helper()
	var total uint64
	for shardID := 0; shardID < SummaryVirtualShardCount; shardID++ {
		shard, err := repo.GetSummaryAggregateShard(context.Background(), SummaryKindCluster, shardID)
		if err != nil {
			t.Fatal(err)
		}
		total += shard.PendingOutboxCount
	}
	return total
}
