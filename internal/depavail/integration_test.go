package depavail

import (
	"errors"
	"testing"
	"time"
)

// TestDependencyLossIntegrationFixture is the deterministic core of
// AA-IMPL-004C. Process packages exercise their own readiness and enforcement
// points; this test drives the outage/grace/recovery sequence they all consume.
func TestDependencyLossIntegrationFixture(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	thresholds := DefaultThresholds()
	thresholds.EtcdUnavailableGraceSeconds = 300
	thresholds.TiKVUnavailableGraceSeconds = 120
	tr := NewTrackerWithClock(thresholds, func() time.Time { return now })

	tr.Report(DependencyEtcd, errors.New("connection refused"))
	tr.Report(DependencyTiKV, errors.New("pd unavailable"))
	tr.SetProjectionLag(6 * time.Second)
	b := tr.Refresh()
	if b.DataPath != DataPathContinues || b.ControlPath != ControlReadOnly {
		t.Fatalf("within grace behavior=%+v, want cached serving and read-only control", b)
	}
	if b.MembershipChange != DecisionRejected || b.ExportAdmission != DecisionBlocked || b.ExportFailover != DecisionSuppressed {
		t.Fatalf("within grace fail-closed decisions=%+v", b)
	}
	if b.Readiness != ReadinessDegradedServingOnCache {
		t.Fatalf("within grace readiness=%s", b.Readiness)
	}

	tr.CountMembershipRejected()
	tr.CountAdmissionBlocked()
	tr.CountFailoverSuppressed()

	now = now.Add(121 * time.Second)
	tr.SetProjectionLag(16 * time.Second)
	b = tr.Refresh()
	state := tr.State()
	if state.Etcd != UnavailableBeyondGrace || state.TiKV != UnavailableBeyondGrace || state.Projection != ProjectionBlocked {
		t.Fatalf("lower grace state=%+v", state)
	}
	if b.Readiness != ReadinessBlocked || b.DataPath != DataPathContinuesDegraded {
		t.Fatalf("past lower grace behavior=%+v", b)
	}
	st := tr.Status()
	if !st.ServingContinuesOnDependencyLoss || !st.FailoverSuppressedOnEtcdLoss {
		t.Fatalf("outage status=%+v", st)
	}
	if st.MembershipRejectCount != 1 || st.AdmissionBlockedCount != 1 || st.FailoverSuppressCount != 1 {
		t.Fatalf("refusal counters=%+v", st)
	}
	if st.FirstError == "" || st.LastError == "" {
		t.Fatalf("outage errors were not retained: %+v", st)
	}

	tr.Report(DependencyEtcd, nil)
	tr.Report(DependencyTiKV, nil)
	tr.SetProjectionLag(0)
	if got := tr.Refresh().Readiness; got != ReadinessHealthy {
		t.Fatalf("recovery readiness=%s", got)
	}
}
