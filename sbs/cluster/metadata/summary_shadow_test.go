package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSummaryShadowDiagnosticMatchesPromotedAggregate(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-shadow")
	repo.now = func() time.Time { return time.Unix(700, 0).UTC() }
	source := summaryShadowFixtureSource(t, 90)
	for page := 0; page < 3; page++ {
		if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-shadow", 2); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-shadow", 90); err != nil {
		t.Fatal(err)
	}
	source.calls = 0
	source.maximumLimit = 0
	comparison, err := repo.CompareSummaryShadowDiagnostic(ctx, source, SummaryKindCluster, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !comparison.Match || len(comparison.MismatchFields) != 0 || comparison.ComparedSubjectCount != 5 {
		t.Fatalf("comparison=%+v", comparison)
	}
	if comparison.LegacyRangePageCount != 3 || comparison.LegacyFullCompletionCount != 1 || comparison.AggregatePointReadCount != 130 {
		t.Fatalf("request-class evidence=%+v", comparison)
	}
	if comparison.BackendFullScanCount != 0 || comparison.NestedCompletionCount != 0 || comparison.ProductionPeriodicInvocationCount != 0 || source.maximumLimit != 2 {
		t.Fatalf("shadow boundary=%+v source max=%d", comparison, source.maximumLimit)
	}
	if err := ValidateSummaryShadowComparison(comparison); err != nil {
		t.Fatal(err)
	}
}

func TestSummaryShadowDiagnosticReportsMismatchAndRejectsRevisionDrift(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-shadow-negative")
	repo.now = func() time.Time { return time.Unix(800, 0).UTC() }
	source := summaryShadowFixtureSource(t, 100)
	for page := 0; page < 3; page++ {
		if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-negative", 2); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-negative", 100); err != nil {
		t.Fatal(err)
	}

	before := source.contributions[0].Counters
	after := before
	after.HealthyVolumes = 0
	after.DegradedVolumes = 1
	delta, err := NewSummaryDelta("event-shadow-mismatch", source.contributions[0].SubjectID, 100, before, after)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.ApplySummaryDelta(ctx, delta); err != nil || !applied {
		t.Fatalf("mismatch delta=%t err=%v", applied, err)
	}
	comparison, err := repo.CompareSummaryShadowDiagnostic(ctx, source, SummaryKindCluster, 2)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Match || !slicesEqual(comparison.MismatchFields, []string{"healthy_volumes", "degraded_volumes"}) {
		t.Fatalf("mismatch comparison=%+v", comparison)
	}

	source.revision = 101
	if _, err := repo.CompareSummaryShadowDiagnostic(ctx, source, SummaryKindCluster, 2); !errors.Is(err, ErrSummaryShadowSourceChanged) {
		t.Fatalf("revision drift error=%v", err)
	}
	if _, err := repo.CompareSummaryShadowDiagnostic(ctx, source, SummaryKindCluster, SummaryShadowMaxPageSize+1); !errors.Is(err, ErrSummaryShadowInvalidPage) {
		t.Fatalf("oversized page error=%v", err)
	}
}

func TestSummaryShadowDiagnosticRejectsPendingAndConcurrentAggregateMutation(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-ad-shadow-fence")
	repo.now = func() time.Time { return time.Unix(900, 0).UTC() }
	source := summaryShadowFixtureSource(t, 110)
	for page := 0; page < 3; page++ {
		if _, err := repo.RunSummaryRebuildPage(ctx, source, SummaryKindCluster, "epoch-fence", 2); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "epoch-fence", 110); err != nil {
		t.Fatal(err)
	}
	pending, err := NewSummaryDelta("event-shadow-pending", "operation:pending", 110, SummaryCounters{}, SummaryCounters{})
	if err != nil {
		t.Fatal(err)
	}
	if enqueued, err := repo.EnqueueSummaryDelta(ctx, pending); err != nil || !enqueued {
		t.Fatalf("pending enqueue=%t err=%v", enqueued, err)
	}
	if _, err := repo.CompareSummaryShadowDiagnostic(ctx, source, SummaryKindCluster, 2); !errors.Is(err, ErrSummaryOutboxPending) {
		t.Fatalf("pending comparison error=%v", err)
	}
	if _, err := repo.ProcessSummaryOutboxPage(ctx, SummaryKindCluster, "", 1); err != nil {
		t.Fatal(err)
	}

	mutatingSource := &mutatingShadowSource{fixtureSummaryRebuildSource: *source, repo: repo}
	if _, err := repo.CompareSummaryShadowDiagnostic(ctx, mutatingSource, SummaryKindCluster, 2); !errors.Is(err, ErrSummaryShadowAggregateChanged) {
		t.Fatalf("concurrent aggregate mutation error=%v", err)
	}
}

type mutatingShadowSource struct {
	fixtureSummaryRebuildSource
	repo    *Repository
	mutated bool
}

func (source *mutatingShadowSource) ListSummaryContributions(ctx context.Context, kind, cursor string, limit int) (SummaryRebuildSourcePage, error) {
	page, err := source.fixtureSummaryRebuildSource.ListSummaryContributions(ctx, kind, cursor, limit)
	if err != nil || source.mutated {
		return page, err
	}
	source.mutated = true
	before := source.contributions[0].Counters
	after := before
	after.HealthyVolumes = 0
	after.DegradedVolumes = 1
	delta, err := NewSummaryDelta("event-shadow-concurrent", source.contributions[0].SubjectID, source.revision, before, after)
	if err != nil {
		return SummaryRebuildSourcePage{}, err
	}
	_, err = source.repo.ApplySummaryDelta(ctx, delta)
	return page, err
}

func summaryShadowFixtureSource(t *testing.T, revision uint64) *fixtureSummaryRebuildSource {
	t.Helper()
	source := &fixtureSummaryRebuildSource{revision: revision}
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
	return source
}

func slicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
