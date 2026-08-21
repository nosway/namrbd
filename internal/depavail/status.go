package depavail

import (
	"sync/atomic"
	"time"
)

// Status is what a process publishes about its dependency situation.
//
// Every field named in the Observable column of entry plan Section 4 appears
// here, and the doc-agreement test checks both directions: a name in the
// document that no process emits sends an operator looking for a number that
// does not exist, and a number with no documentation is one they will find and
// have to guess at.
type Status struct {
	Readiness Readiness `json:"dependency_readiness"`

	EtcdAvailability Availability `json:"etcd_availability"`
	TiKVAvailability Availability `json:"tikv_availability"`
	ProjectionState  Freshness    `json:"projection_freshness"`

	// The configured thresholds, republished so an operator reading a degraded
	// process does not have to go find its config to know what it was measured
	// against.
	EtcdUnavailableGraceSeconds int `json:"etcd_unavailable_grace_seconds"`
	TiKVUnavailableGraceSeconds int `json:"tikv_unavailable_grace_seconds"`
	ProjectionStaleDegradedMS   int `json:"projection_stale_degraded_ms"`
	ProjectionStaleBlockedMS    int `json:"projection_stale_blocked_ms"`

	ProjectionLagMS int64 `json:"projection_lag_ms"`

	// Counters. These are what an alert is written against.
	StaleProjectionCount   int64 `json:"stale_projection_count"`
	ISCSIGatewayStaleCount int64 `json:"iscsi_gateway_stale_count"`
	MembershipRejectCount  int64 `json:"membership_change_rejected_count"`
	FailoverSuppressCount  int64 `json:"failover_suppressed_count"`
	AdmissionBlockedCount  int64 `json:"export_admission_blocked_count"`

	// The two booleans entry plan Section 4 names directly. They are the
	// summary an operator checks first: is the cluster still serving, and did
	// anything get promoted while the view was untrustworthy.
	ServingContinuesOnDependencyLoss bool `json:"serving_continues_on_dependency_loss"`
	FailoverSuppressedOnEtcdLoss     bool `json:"failover_suppressed_on_etcd_loss"`

	FirstError string `json:"first_error"`
	LastError  string `json:"last_error"`
}

// Tracker accumulates the counters across state transitions and renders Status.
//
// It holds no clock and no dependency handle. A process observes its own
// dependencies and calls Observe; that keeps the matrix testable at every state
// without standing up etcd, and keeps the decision to degrade in one place
// instead of at each call site that noticed a timeout.
type Tracker struct {
	// thresholds is held atomically because the config sections that carry it
	// are classified ReloadLive: an operator who widens a grace window during
	// an incident must have it apply without a restart, and a restart during
	// an incident is exactly what widening the window was meant to avoid.
	thresholds atomic.Value // Thresholds

	state atomic.Value // State

	// Dependency outcomes, fed by the call sites that already talk to each
	// store. See reporter.go.
	etcd     dependencyProbe
	tikv     dependencyProbe
	lagNanos atomic.Int64

	clock Clock

	staleProjection  atomic.Int64
	iscsiStale       atomic.Int64
	membershipReject atomic.Int64
	failoverSuppress atomic.Int64
	admissionBlocked atomic.Int64
	projectionLagMS  atomic.Int64

	firstError atomic.Value // string
	lastError  atomic.Value // string
}

// NewTracker returns a tracker starting from a fully healthy state.
func NewTracker(t Thresholds) *Tracker { return NewTrackerWithClock(t, time.Now) }

// NewTrackerWithClock is NewTracker with an injectable clock, so grace-window
// transitions can be tested without sleeping through them.
func NewTrackerWithClock(t Thresholds, clock Clock) *Tracker {
	if clock == nil {
		clock = time.Now
	}
	tr := &Tracker{clock: clock}
	tr.thresholds.Store(t)
	tr.state.Store(State{Etcd: Available, TiKV: Available, Projection: ProjectionFresh})
	tr.firstError.Store("")
	tr.lastError.Store("")
	return tr
}

func (t *Tracker) now() time.Time {
	if t.clock == nil {
		return time.Now()
	}
	return t.clock()
}

// Thresholds returns the thresholds currently in force.
func (t *Tracker) Thresholds() Thresholds { return t.thresholds.Load().(Thresholds) }

// SetThresholds applies a reloaded threshold set. An invalid set is refused and
// the previous one stays in force, so a bad reload cannot leave a process with
// no grace window at all.
func (t *Tracker) SetThresholds(th Thresholds) error {
	if err := th.Validate(); err != nil {
		return err
	}
	t.thresholds.Store(th)
	return nil
}

// State returns the last classified state.
func (t *Tracker) State() State { return t.state.Load().(State) }

// Behavior returns the declared behavior for the last classified state.
func (t *Tracker) Behavior() Behavior { return Resolve(t.State()) }

// Observe classifies an observation, records it, and returns the behavior the
// caller must now follow.
func (t *Tracker) Observe(o Observation) Behavior {
	s := t.Thresholds().Classify(o)
	t.state.Store(s)
	t.projectionLagMS.Store(o.ProjectionLag.Milliseconds())
	if s.Projection != ProjectionFresh {
		t.staleProjection.Add(1)
	}
	return Resolve(s)
}

// RecordError records a dependency error message, keeping the first and the
// most recent. The first is what an incident timeline is built from; the last
// is what is still happening.
func (t *Tracker) RecordError(msg string) {
	if msg == "" {
		return
	}
	if t.firstError.Load().(string) == "" {
		t.firstError.Store(msg)
	}
	t.lastError.Store(msg)
}

// The three refusal counters. Each is incremented by the enforcement point that
// actually refused, not by the matrix, so a counter that stays at zero during
// an outage means the enforcement is missing rather than that the matrix was
// permissive.
func (t *Tracker) CountMembershipRejected() { t.membershipReject.Add(1) }
func (t *Tracker) CountFailoverSuppressed() { t.failoverSuppress.Add(1) }
func (t *Tracker) CountAdmissionBlocked()   { t.admissionBlocked.Add(1) }

// CountISCSIGatewayStale records an iSCSI gateway entry serving from a fleet
// listing that could not be refreshed.
func (t *Tracker) CountISCSIGatewayStale(n int64) {
	if n > 0 {
		t.iscsiStale.Add(n)
	}
}

// Status renders the current picture.
func (t *Tracker) Status() Status {
	s := t.State()
	b := Resolve(s)
	th := t.Thresholds()
	return Status{
		Readiness:                        b.Readiness,
		EtcdAvailability:                 s.Etcd,
		TiKVAvailability:                 s.TiKV,
		ProjectionState:                  s.Projection,
		EtcdUnavailableGraceSeconds:      th.EtcdUnavailableGraceSeconds,
		TiKVUnavailableGraceSeconds:      th.TiKVUnavailableGraceSeconds,
		ProjectionStaleDegradedMS:        th.ProjectionStaleDegradedMS,
		ProjectionStaleBlockedMS:         th.ProjectionStaleBlockedMS,
		ProjectionLagMS:                  t.projectionLagMS.Load(),
		StaleProjectionCount:             t.staleProjection.Load(),
		ISCSIGatewayStaleCount:           t.iscsiStale.Load(),
		MembershipRejectCount:            t.membershipReject.Load(),
		FailoverSuppressCount:            t.failoverSuppress.Load(),
		AdmissionBlockedCount:            t.admissionBlocked.Load(),
		ServingContinuesOnDependencyLoss: b.DataPath == DataPathContinues || b.DataPath == DataPathContinuesDegraded,
		FailoverSuppressedOnEtcdLoss:     s.Etcd.unavailable() && b.ExportFailover == DecisionSuppressed,
		FirstError:                       t.firstError.Load().(string),
		LastError:                        t.lastError.Load().(string),
	}
}

// NewValidatedTracker builds a tracker from a threshold set, refusing an
// unusable one.
//
// Startup is the right place to refuse: a process that came up with a zero
// grace window would report a fleet-wide outage at the first reconnect, and it
// would do so long after the operator who wrote the file stopped watching.
func NewValidatedTracker(t Thresholds) (*Tracker, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return NewTracker(t), nil
}
