package depavail

import (
	"errors"
	"testing"
	"time"
)

// fakeClock advances only when a test says so.
type fakeClock struct{ t time.Time }

func newClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)}
}
func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestAProcessThatHasTouchedNothingIsHealthy(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	if got := tr.Refresh().Readiness; got != ReadinessHealthy {
		t.Errorf("readiness %s before any dependency call; silence is not evidence of an outage", got)
	}
}

// The grace clock starts at the first consecutive failure and is not restarted
// by later ones. Without this a steadily failing dependency would never reach
// its grace window and the process would call itself healthy through an outage
// of any length.
func TestSteadyFailuresDoNotRestartTheGraceClock(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	boom := errors.New("context deadline exceeded")

	tr.Report(DependencyEtcd, boom)
	for i := 0; i < 10; i++ {
		c.advance(40 * time.Second)
		tr.Report(DependencyEtcd, boom)
	}
	// 400s of continuous failure against a 300s grace.
	b := tr.Refresh()
	if tr.State().Etcd != UnavailableBeyondGrace {
		t.Errorf("etcd classified %s after 400s of steady failure", tr.State().Etcd)
	}
	if b.DataPath != DataPathContinuesDegraded {
		t.Errorf("data path %s, expected continues_degraded past the grace window", b.DataPath)
	}
	if b.ExportFailover != DecisionSuppressed {
		t.Errorf("failover %s during an etcd outage", b.ExportFailover)
	}
}

func TestGraceWindowIsCrossedOnlyAfterItElapses(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.Report(DependencyTiKV, errors.New("pd unreachable"))

	c.advance(299 * time.Second)
	tr.Refresh()
	if got := tr.State().TiKV; got != UnavailableWithinGrace {
		t.Errorf("at 299s of a 300s grace TiKV is %s", got)
	}
	if got := tr.Status().Readiness; got != ReadinessDegradedServingOnCache {
		t.Errorf("readiness %s inside the grace window", got)
	}

	c.advance(2 * time.Second)
	tr.Refresh()
	if got := tr.State().TiKV; got != UnavailableBeyondGrace {
		t.Errorf("at 301s of a 300s grace TiKV is %s", got)
	}
}

// One success ends the outage. A dependency that answers is available, and
// holding it degraded for a cool-down would keep promotion suppressed after the
// reason for suppressing it went away.
func TestASingleSuccessClearsTheOutage(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.Report(DependencyEtcd, errors.New("no leader"))
	c.advance(10 * time.Minute)
	tr.Refresh()
	if tr.State().Etcd != UnavailableBeyondGrace {
		t.Fatalf("setup: etcd is %s", tr.State().Etcd)
	}

	tr.Report(DependencyEtcd, nil)
	b := tr.Refresh()
	if tr.State().Etcd != Available {
		t.Errorf("etcd still %s after a successful call", tr.State().Etcd)
	}
	if b.Readiness != ReadinessHealthy {
		t.Errorf("readiness %s after recovery", b.Readiness)
	}
	if b.ExportFailover != DecisionAllowed {
		t.Errorf("failover still %s after recovery", b.ExportFailover)
	}

	// The errors stay. They are the record of what happened, and clearing them
	// on recovery would erase the incident an operator is about to investigate.
	if st := tr.Status(); st.FirstError == "" || st.LastError == "" {
		t.Errorf("recovery erased the error record: %q / %q", st.FirstError, st.LastError)
	}
}

// The two dependencies are tracked independently: an etcd outage must not make
// TiKV look unreachable, or the lower-of-two-graces rule would fire on one
// failure.
func TestDependenciesAreTrackedIndependently(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.Report(DependencyEtcd, errors.New("down"))
	tr.Report(DependencyTiKV, nil)
	c.advance(30 * time.Second)
	tr.Refresh()

	s := tr.State()
	if s.Etcd != UnavailableWithinGrace {
		t.Errorf("etcd %s", s.Etcd)
	}
	if s.TiKV != Available {
		t.Errorf("tikv %s; an etcd failure must not implicate TiKV", s.TiKV)
	}
}

func TestReportingAnUnknownDependencyIsIgnored(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.Report(Dependency("postgres"), errors.New("down"))
	if got := tr.Refresh().Readiness; got != ReadinessHealthy {
		t.Errorf("an unrecognized dependency changed readiness to %s", got)
	}
}

func TestProjectionLagFeedsClassification(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.SetProjectionLag(20 * time.Second)
	if got := tr.Refresh().Readiness; got != ReadinessBlocked {
		t.Errorf("readiness %s at a 20s projection lag", got)
	}
	tr.SetProjectionLag(-5 * time.Second)
	if got := tr.Refresh().Readiness; got != ReadinessHealthy {
		t.Errorf("a negative lag classified %s rather than fresh", got)
	}
}

// Status must not move a counter. An operator scraping an endpoint should not
// be able to change what the endpoint reports.
func TestStatusIsAPureRead(t *testing.T) {
	c := newClock()
	tr := NewTrackerWithClock(DefaultThresholds(), c.now)
	tr.SetProjectionLag(9 * time.Second)
	tr.Refresh()

	first := tr.Status().StaleProjectionCount
	for i := 0; i < 20; i++ {
		tr.Status()
	}
	if got := tr.Status().StaleProjectionCount; got != first {
		t.Errorf("stale_projection_count moved from %d to %d across reads alone", first, got)
	}
}
