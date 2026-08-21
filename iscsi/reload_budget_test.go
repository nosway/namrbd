package iscsi

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestReloadSnapshotSeparatesBoundedFetchesFromFullScans(t *testing.T) {
	state := &ReloadState{}
	state.Decide(1, 1)
	blob, err := json.Marshal(state.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]int64
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	if got["registry_fetch_count"] != 1 || got["tikv_registry_reload_scan_count"] != 0 {
		t.Fatalf("reload pressure JSON = %s", blob)
	}
}

// A quiet cluster must stay cheap. The registry response carries every export,
// so re-parsing and re-applying it on every poll is the cost this avoids.
func TestUnchangedRegistryIsSkipped(t *testing.T) {
	var s ReloadState
	if d := s.Decide(10, 3); d.Outcome != ReloadApply {
		t.Fatalf("the first fetch was %s, want apply", d.Outcome)
	}
	s.Applied(10, 3)

	for i := 0; i < 5; i++ {
		d := s.Decide(10, 3)
		if d.Outcome != ReloadSkip {
			t.Fatalf("poll %d was %s, want skip", i, d.Outcome)
		}
	}
	snap := s.Snapshot()
	if snap.RegistrySkippedCount != 5 {
		t.Errorf("skipped count = %d, want 5", snap.RegistrySkippedCount)
	}
	if snap.RegistryReloadCount != 1 {
		t.Errorf("reload count = %d; only the first fetch should have applied", snap.RegistryReloadCount)
	}
	if snap.RegistryFetchCount != 6 {
		t.Errorf("fetch count = %d, want 6", snap.RegistryFetchCount)
	}
}

// A response older than what is serving must be rejected. A gateway reading
// from a lagging replica would otherwise move its mappings backwards, which
// looks like a spontaneous failover to an initiator.
func TestOlderRevisionIsRejectedNotApplied(t *testing.T) {
	var s ReloadState
	s.Applied(20, 5)

	d := s.Decide(19, 5)
	if d.Outcome != ReloadRejectStale {
		t.Fatalf("an older revision was %s, want reject_stale", d.Outcome)
	}
	if !strings.Contains(d.Reason, "lagging replica") {
		t.Errorf("the reason does not say where the stale response came from: %q", d.Reason)
	}
	if s.Snapshot().RegistryReloadRevision != 20 {
		t.Error("a stale response moved the serving revision backwards")
	}
	if s.Snapshot().RegistryStaleRejectCount != 1 {
		t.Error("the stale rejection was not counted")
	}
}

// A config generation can move without the registry revision, so a lower
// generation at the same revision is stale too.
func TestOlderGenerationAtSameRevisionIsRejected(t *testing.T) {
	var s ReloadState
	s.Applied(20, 5)

	if d := s.Decide(20, 4); d.Outcome != ReloadRejectStale {
		t.Fatalf("an older generation was %s, want reject_stale", d.Outcome)
	}
	if d := s.Decide(20, 6); d.Outcome != ReloadApply {
		t.Fatalf("a newer generation at the same revision was %s, want apply", d.Outcome)
	}
}

// Recording the revision must be separate from deciding to apply it. A gateway
// that advanced its revision and then failed to apply would skip every later
// poll and serve the old mappings while reporting itself current.
func TestFailedApplyDoesNotAdvanceTheServingRevision(t *testing.T) {
	var s ReloadState
	s.Applied(10, 1)

	d := s.Decide(11, 2)
	if d.Outcome != ReloadApply {
		t.Fatalf("outcome = %s", d.Outcome)
	}
	// The caller decides not to record it, standing in for a failed apply.
	if got := s.Snapshot().RegistryReloadRevision; got != 10 {
		t.Errorf("serving revision advanced to %d without an apply", got)
	}
	// The next poll must still want to apply, not skip.
	if d := s.Decide(11, 2); d.Outcome != ReloadApply {
		t.Errorf("after a failed apply the next poll was %s, want apply", d.Outcome)
	}
}

// The decision reports where it came from and where it is going, which is what
// an operator watching a rollout needs.
func TestDecisionBracketsTheChange(t *testing.T) {
	var s ReloadState
	s.Applied(7, 1)
	d := s.Decide(9, 2)
	if d.FromRevision != 7 || d.ToRevision != 9 {
		t.Errorf("decision brackets %d..%d, want 7..9", d.FromRevision, d.ToRevision)
	}
	if !strings.Contains(d.Reason, "7") || !strings.Contains(d.Reason, "9") {
		t.Errorf("the reason does not name both revisions: %q", d.Reason)
	}
}

// A nil state must not panic; a gateway built by an older path should report
// nothing rather than crash.
func TestNilReloadStateSnapshot(t *testing.T) {
	var s *ReloadState
	if got := s.Snapshot(); got != (ReloadSnapshot{}) {
		t.Errorf("a nil state reported %+v", got)
	}
}

// Polls run from a timer while an operator may apply concurrently.
func TestReloadStateIsRaceFree(t *testing.T) {
	var s ReloadState
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Decide(uint64(n), uint64(j))
				s.Snapshot()
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Applied(uint64(n), uint64(j))
			}
		}(i)
	}
	wg.Wait()
	if s.Snapshot().RegistryFetchCount != 800 {
		t.Errorf("fetch count = %d, want 800", s.Snapshot().RegistryFetchCount)
	}
}
