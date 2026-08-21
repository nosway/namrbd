package depavail

import (
	"testing"
	"time"
)

// The requirement AA-IMPL-004 states is that no combination produces an
// undeclared behavior. This is that requirement, checked over the whole state
// space rather than the seven rows the document happens to enumerate.
func TestEveryStateResolvesToAFullyDecidedBehavior(t *testing.T) {
	states := AllStates()
	if len(states) != 27 {
		t.Fatalf("state space is %d, expected 3 availabilities x 3 x 3 freshness = 27", len(states))
	}
	for _, s := range states {
		b := Resolve(s)
		for name, value := range map[string]string{
			"data_path":         string(b.DataPath),
			"control_path":      string(b.ControlPath),
			"views":             string(b.Views),
			"membership_change": string(b.MembershipChange),
			"export_failover":   string(b.ExportFailover),
			"export_admission":  string(b.ExportAdmission),
			"readiness":         string(b.Readiness),
		} {
			if value == "" {
				t.Errorf("state %+v leaves %s undecided", s, name)
			}
		}
	}
}

// Fail-open for serving: half of the governing rule.
func TestAlreadyAdmittedIOContinuesInEveryState(t *testing.T) {
	for _, s := range AllStates() {
		b := Resolve(s)
		if b.DataPath != DataPathContinues && b.DataPath != DataPathContinuesDegraded {
			t.Errorf("state %+v stops already-admitted I/O (%s); dependency loss must be fail-open for serving", s, b.DataPath)
		}
	}
}

// Fail-closed for authority: the other half, and the one that prevents
// split-brain. A promotion while the fleet view is untrustworthy is the single
// outcome this slice exists to make impossible.
func TestNoPromotionOrMembershipChangeWhileTheViewIsUntrustworthy(t *testing.T) {
	for _, s := range AllStates() {
		trustworthy := s.Etcd == Available && s.TiKV == Available && s.Projection == ProjectionFresh
		if trustworthy {
			continue
		}
		b := Resolve(s)
		if b.ExportFailover != DecisionSuppressed {
			t.Errorf("state %+v permits failover (%s) with an untrustworthy view", s, b.ExportFailover)
		}
		if b.MembershipChange != DecisionRejected {
			t.Errorf("state %+v permits a membership change (%s) with an untrustworthy view", s, b.MembershipChange)
		}
	}
}

// A merely degraded projection is still stale enough to refuse a promotion,
// even though every dependency is reachable and writable. This is the case
// most likely to be relaxed by accident, because nothing is down.
func TestADegradedProjectionAloneSuppressesFailover(t *testing.T) {
	b := Resolve(State{Etcd: Available, TiKV: Available, Projection: ProjectionDegraded})
	if b.ControlPath != ControlReadWrite {
		t.Errorf("control path %s: a stale projection does not make the stores unwritable", b.ControlPath)
	}
	if b.ExportFailover != DecisionSuppressed {
		t.Errorf("failover %s: a stale projection is not a basis on which to promote a writer", b.ExportFailover)
	}
	if b.DataPath != DataPathContinues {
		t.Errorf("data path %s: a degraded projection does not degrade serving", b.DataPath)
	}
}

func TestExactlyOneStateAllowsEverything(t *testing.T) {
	var permissive []State
	for _, s := range AllStates() {
		b := Resolve(s)
		if b.MembershipChange == DecisionAllowed && b.ExportFailover == DecisionAllowed &&
			b.ExportAdmission == DecisionAllowed && b.Readiness == ReadinessHealthy {
			permissive = append(permissive, s)
		}
	}
	if len(permissive) != 1 {
		t.Fatalf("%d states allow everything, expected only the fully healthy one: %+v", len(permissive), permissive)
	}
	want := State{Etcd: Available, TiKV: Available, Projection: ProjectionFresh}
	if permissive[0] != want {
		t.Errorf("the permissive state is %+v, expected %+v", permissive[0], want)
	}
}

// Beyond-grace dependency loss reports degraded, not blocked. An operator who
// drains on a blocked signal would turn a metadata outage into a data-path one.
func TestBeyondGraceDependencyLossIsDegradedNotBlocked(t *testing.T) {
	for _, s := range []State{
		{Etcd: UnavailableBeyondGrace, TiKV: Available, Projection: ProjectionFresh},
		{Etcd: Available, TiKV: UnavailableBeyondGrace, Projection: ProjectionFresh},
		{Etcd: UnavailableBeyondGrace, TiKV: UnavailableBeyondGrace, Projection: ProjectionFresh},
	} {
		b := Resolve(s)
		if b.Readiness != ReadinessDegradedServingOnCache {
			t.Errorf("state %+v readiness %s, expected degraded_serving_on_cache", s, b.Readiness)
		}
		if b.DataPath != DataPathContinuesDegraded {
			t.Errorf("state %+v data path %s, expected continues_degraded", s, b.DataPath)
		}
	}
}

// Only a projection past the blocked threshold makes a view refuse to publish
// as healthy, and that is the only thing that reports readiness blocked.
func TestOnlyABlockedProjectionReportsBlocked(t *testing.T) {
	for _, s := range AllStates() {
		b := Resolve(s)
		blocked := b.Readiness == ReadinessBlocked
		if blocked != (s.Projection == ProjectionBlocked) {
			t.Errorf("state %+v readiness %s does not track projection %s", s, b.Readiness, s.Projection)
		}
	}
}

// Readiness must not collapse into a boolean: all three values must be
// reachable, or the surface is a boolean wearing three names.
func TestAllThreeReadinessValuesAreReachable(t *testing.T) {
	seen := map[Readiness]bool{}
	for _, s := range AllStates() {
		seen[Resolve(s).Readiness] = true
	}
	for _, want := range []Readiness{ReadinessHealthy, ReadinessDegradedServingOnCache, ReadinessBlocked} {
		if !seen[want] {
			t.Errorf("readiness %s is unreachable", want)
		}
	}
}

func TestEveryUnhealthyStateExplainsItself(t *testing.T) {
	for _, s := range AllStates() {
		b := Resolve(s)
		if b.Degraded() && len(b.Reasons) == 0 {
			t.Errorf("state %+v is %s with no reason attached", s, b.Readiness)
		}
		if !b.Degraded() && len(b.Reasons) != 0 {
			t.Errorf("healthy state %+v carries reasons %v", s, b.Reasons)
		}
	}
}

func TestLowerGraceAppliesOnlyWhenBothAreUnreachable(t *testing.T) {
	th := Thresholds{
		EtcdUnavailableGraceSeconds: 300,
		TiKVUnavailableGraceSeconds: 60,
		ProjectionStaleDegradedMS:   5000,
		ProjectionStaleBlockedMS:    15000,
	}

	// etcd alone down for 120s: inside its own 300s grace.
	etcd, tikv := th.EffectiveGraces(Observation{EtcdReachable: false, TiKVReachable: true})
	if etcd != 300*time.Second || tikv != 60*time.Second {
		t.Errorf("one dependency down: graces %v/%v, expected each dependency's own", etcd, tikv)
	}
	s := th.Classify(Observation{EtcdReachable: false, EtcdUnreachableFor: 120 * time.Second, TiKVReachable: true})
	if s.Etcd != UnavailableWithinGrace {
		t.Errorf("etcd down 120s of a 300s grace classified %s", s.Etcd)
	}

	// Both down: the 60s grace applies to etcd too, so the same 120s is now
	// beyond grace.
	etcd, tikv = th.EffectiveGraces(Observation{EtcdReachable: false, TiKVReachable: false})
	if etcd != 60*time.Second || tikv != 60*time.Second {
		t.Errorf("both down: graces %v/%v, expected the lower of the two for both", etcd, tikv)
	}
	s = th.Classify(Observation{
		EtcdReachable: false, EtcdUnreachableFor: 120 * time.Second,
		TiKVReachable: false, TiKVUnreachableFor: 120 * time.Second,
	})
	if s.Etcd != UnavailableBeyondGrace {
		t.Errorf("with both down the lower grace must apply to etcd; got %s", s.Etcd)
	}
}

func TestProjectionThresholdsAreInclusiveOfTheHealthierSide(t *testing.T) {
	th := DefaultThresholds()
	for _, c := range []struct {
		lag  time.Duration
		want Freshness
	}{
		{0, ProjectionFresh},
		{4999 * time.Millisecond, ProjectionFresh},
		{5000 * time.Millisecond, ProjectionFresh},
		{5001 * time.Millisecond, ProjectionDegraded},
		{14999 * time.Millisecond, ProjectionDegraded},
		{15000 * time.Millisecond, ProjectionDegraded},
		{15001 * time.Millisecond, ProjectionBlocked},
	} {
		if got := th.ClassifyProjection(c.lag); got != c.want {
			t.Errorf("lag %v classified %s, expected %s", c.lag, got, c.want)
		}
	}
}

func TestThresholdValidationRejectsUnusableSets(t *testing.T) {
	if err := DefaultThresholds().Validate(); err != nil {
		t.Fatalf("shipped defaults do not validate: %v", err)
	}
	for name, th := range map[string]Thresholds{
		"zero etcd grace":        {0, 300, 5000, 15000},
		"zero tikv grace":        {300, 0, 5000, 15000},
		"negative degraded":      {300, 300, -1, 15000},
		"blocked below degrade":  {300, 300, 15000, 5000},
		"blocked equals degrade": {300, 300, 5000, 5000},
	} {
		if err := th.Validate(); err == nil {
			t.Errorf("%s validated, but it cannot express the matrix", name)
		}
	}
}

func TestTrackerCountsStalenessAndKeepsFirstAndLastError(t *testing.T) {
	tr := NewTracker(DefaultThresholds())
	if got := tr.Status().Readiness; got != ReadinessHealthy {
		t.Fatalf("a new tracker reports %s", got)
	}

	tr.Observe(Observation{EtcdReachable: true, TiKVReachable: true, ProjectionLag: 9 * time.Second})
	tr.Observe(Observation{EtcdReachable: true, TiKVReachable: true, ProjectionLag: 20 * time.Second})
	tr.RecordError("etcd: context deadline exceeded")
	tr.RecordError("etcd: no leader")

	st := tr.Status()
	if st.StaleProjectionCount != 2 {
		t.Errorf("stale_projection_count %d, expected 2", st.StaleProjectionCount)
	}
	if st.ProjectionLagMS != 20000 {
		t.Errorf("projection_lag_ms %d, expected 20000", st.ProjectionLagMS)
	}
	if st.Readiness != ReadinessBlocked {
		t.Errorf("readiness %s, expected blocked at a 20s lag", st.Readiness)
	}
	if st.FirstError != "etcd: context deadline exceeded" || st.LastError != "etcd: no leader" {
		t.Errorf("first/last error %q / %q", st.FirstError, st.LastError)
	}
	if !st.ServingContinuesOnDependencyLoss {
		t.Error("serving_continues_on_dependency_loss is false while the data path continues")
	}
}

// The boolean entry plan Section 4 names is a claim about etcd specifically, so
// it must be true in exactly the states where etcd is down.
func TestFailoverSuppressedOnEtcdLossTracksEtcd(t *testing.T) {
	for _, s := range AllStates() {
		tr := NewTracker(DefaultThresholds())
		tr.state.Store(s)
		got := tr.Status().FailoverSuppressedOnEtcdLoss
		want := s.Etcd != Available
		if got != want {
			t.Errorf("state %+v: failover_suppressed_on_etcd_loss %v, expected %v", s, got, want)
		}
	}
}

func TestRefusalCountersAreIncrementedByTheEnforcementPoint(t *testing.T) {
	tr := NewTracker(DefaultThresholds())
	tr.CountMembershipRejected()
	tr.CountFailoverSuppressed()
	tr.CountFailoverSuppressed()
	tr.CountAdmissionBlocked()
	tr.CountISCSIGatewayStale(3)
	tr.CountISCSIGatewayStale(0)

	st := tr.Status()
	if st.MembershipRejectCount != 1 || st.FailoverSuppressCount != 2 ||
		st.AdmissionBlockedCount != 1 || st.ISCSIGatewayStaleCount != 3 {
		t.Errorf("counters %+v", st)
	}
}

// The config sections carrying these thresholds are classified ReloadLive. That
// classification is only true if the tracker re-reads them, so it is checked
// here rather than assumed.
func TestReloadedThresholdsApplyWithoutRestart(t *testing.T) {
	tr := NewTracker(DefaultThresholds())
	obs := Observation{EtcdReachable: true, TiKVReachable: true, ProjectionLag: 8 * time.Second}

	if got := tr.Observe(obs).Readiness; got != ReadinessDegradedServingOnCache {
		t.Fatalf("an 8s lag against the 5s degraded threshold reports %s", got)
	}

	widened := DefaultThresholds()
	widened.ProjectionStaleDegradedMS = 10000
	if err := tr.SetThresholds(widened); err != nil {
		t.Fatalf("SetThresholds: %v", err)
	}
	if got := tr.Observe(obs).Readiness; got != ReadinessHealthy {
		t.Errorf("after widening the degraded threshold to 10s an 8s lag still reports %s", got)
	}
	if got := tr.Status().ProjectionStaleDegradedMS; got != 10000 {
		t.Errorf("status still republishes the old threshold %d", got)
	}
}

// A reload that would leave a process with no grace window at all is refused,
// and the previous set stays in force.
func TestAnInvalidReloadIsRefusedAndTheOldThresholdsRemain(t *testing.T) {
	tr := NewTracker(DefaultThresholds())
	if err := tr.SetThresholds(Thresholds{}); err == nil {
		t.Fatal("a zero threshold set was accepted")
	}
	if got := tr.Thresholds(); got != DefaultThresholds() {
		t.Errorf("thresholds after a refused reload: %+v", got)
	}
}

// The governing rule names three fail-closed operations. They are separate
// fields with separate enforcement points, so this pins that they are refused
// under the same condition; a rule change that moves one without the others
// would leave an operation permitted on a view the other two do not trust.
func TestThreeFailClosedOperationsAgreeInEveryState(t *testing.T) {
	for _, s := range AllStates() {
		b := Resolve(s)
		refused := map[string]bool{
			"membership_change": b.MembershipChange != DecisionAllowed,
			"export_failover":   b.ExportFailover != DecisionAllowed,
			"export_admission":  b.ExportAdmission != DecisionAllowed,
		}
		first := refused["membership_change"]
		for name, r := range refused {
			if r != first {
				t.Errorf("state %+v: %s is %v while membership_change is %v; the fail-closed set has split",
					s, name, r, first)
			}
		}
	}
}
