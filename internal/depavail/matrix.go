// Package depavail is the dependency availability matrix as an executable
// specification.
//
// AA-IMPL-004A owns it. docs/phase-aa-entry-plan.md Section 4 declares seven
// dependency-loss rows and requires that no combination produce an undeclared
// behavior. Seven rows cannot meet that requirement on their own: etcd, TiKV,
// and the projection each move independently, so the state space an operator
// can actually land in is the cross product, not the list. A lookup table over
// the seven declared rows would answer "undeclared" for the other twenty, and
// "undeclared" at three in the morning means whatever the code happens to do.
//
// So Resolve is total over the whole state space and derived from four rules,
// and the declared rows are used as the test oracle rather than as the
// implementation. A rule change that contradicts a declared row fails the gate;
// a state the document never anticipated still gets an answer.
package depavail

// Availability is how a dependency stands relative to its grace window.
//
// The grace window is what separates "a restart is in progress" from "this is
// an outage". Both keep serving; only the second says so out loud.
type Availability string

const (
	Available              Availability = "available"
	UnavailableWithinGrace Availability = "unavailable_within_grace"
	UnavailableBeyondGrace Availability = "unavailable_beyond_grace"
)

// Freshness classifies how far the local projection has fallen behind the
// membership revision it projects.
type Freshness string

const (
	ProjectionFresh    Freshness = "fresh"
	ProjectionDegraded Freshness = "degraded"
	ProjectionBlocked  Freshness = "blocked"
)

// State is the full dependency situation a process can be in. Every field is
// observed locally; none of it requires the dependency that may be down.
type State struct {
	Etcd       Availability
	TiKV       Availability
	Projection Freshness
}

// DataPath is what happens to I/O that was already admitted.
type DataPath string

const (
	// DataPathContinues serves normally.
	DataPathContinues DataPath = "continues"
	// DataPathContinuesDegraded serves, and says it is doing so on cache.
	DataPathContinuesDegraded DataPath = "continues_degraded"
)

// ControlPath is whether the process will accept state-changing requests.
type ControlPath string

const (
	ControlReadWrite ControlPath = "read_write"
	ControlReadOnly  ControlPath = "read_only"
)

// ViewHealth is what a fleet or membership listing claims about itself.
//
// This is separate from ControlPath because a stale projection does not stop
// writes from reaching their store; it stops a reader from believing the answer.
type ViewHealth string

const (
	ViewHealthy ViewHealth = "healthy"
	// ViewDegraded still answers, marked stale.
	ViewDegraded ViewHealth = "degraded"
	// ViewUnhealthy refuses to publish itself as healthy.
	ViewUnhealthy ViewHealth = "unhealthy"
)

// Decision is the allow/deny axis shared by membership change, failover, and
// export admission.
type Decision string

const (
	DecisionAllowed    Decision = "allowed"
	DecisionRejected   Decision = "rejected"
	DecisionSuppressed Decision = "suppressed"
	DecisionBlocked    Decision = "blocked"
)

// Readiness is the three-way surface AA-IMPL-004 requires in place of a
// boolean. Collapsing these loses the only distinction that matters during an
// outage: whether the process is still doing its job.
type Readiness string

const (
	// ReadinessHealthy: dependencies reachable, projection fresh.
	ReadinessHealthy Readiness = "healthy"
	// ReadinessDegradedServingOnCache: still serving, no longer authoritative.
	ReadinessDegradedServingOnCache Readiness = "degraded_serving_on_cache"
	// ReadinessBlocked: cannot be trusted to publish itself as healthy.
	ReadinessBlocked Readiness = "blocked"
)

// Behavior is the declared response to a State. Every field is decided, never
// defaulted, so a new field forces a decision in every state rather than
// inheriting a zero value.
type Behavior struct {
	DataPath         DataPath    `json:"data_path"`
	ControlPath      ControlPath `json:"control_path"`
	Views            ViewHealth  `json:"views"`
	MembershipChange Decision    `json:"membership_change"`
	ExportFailover   Decision    `json:"export_failover"`
	ExportAdmission  Decision    `json:"export_admission"`
	Readiness        Readiness   `json:"readiness"`
	// Reasons explains the behavior in the terms the operator sees, ordered
	// most significant first. An empty Reasons means nothing is wrong.
	Reasons []string `json:"reasons"`
}

// Degraded reports whether this behavior is anything other than fully healthy.
func (b Behavior) Degraded() bool { return b.Readiness != ReadinessHealthy }

func (a Availability) unavailable() bool { return a != Available }

func (a Availability) beyondGrace() bool { return a == UnavailableBeyondGrace }

// Resolve returns the declared behavior for any dependency state.
//
// Four rules produce it, and they are stated here rather than spread across
// call sites because the governing rule of Section 4 is a property of the whole
// matrix, not of any one process:
//
//	dependency loss is fail-open for already-admitted serving, and
//	fail-closed for every promotion, admission, or membership change.
//
// Rule 1 (serving). Already-admitted I/O always continues. It is marked
// degraded once a dependency is past its grace window or the projection is
// blocked, because past that point "still serving" and "still correct about the
// fleet" have come apart and the operator needs to know which one they have.
//
// Rule 2 (authority). Any unreachable dependency makes the control path
// read-only. A write we cannot durably record is a write that disappears on
// restart, which is worse than a rejected one.
//
// Rule 3 (trust). Promotion, admission, and membership change are refused
// whenever the fleet view is untrustworthy: a dependency is unreachable, or the
// projection is stale at all. Note this is stricter than the read-only rule.
// A fresh-looking projection that is five seconds behind is still reachable and
// still writable, and still not a basis on which to promote a writer.
//
// Rule 4 (visibility). Readiness reports blocked only when a view refuses to
// publish itself as healthy, and degraded whenever serving continues without
// authority. Beyond-grace dependency loss is degraded, not blocked, because the
// process is still doing useful work and an operator draining it on a blocked
// signal would turn a metadata outage into a data-path outage.
func Resolve(s State) Behavior {
	b := Behavior{
		DataPath:         DataPathContinues,
		ControlPath:      ControlReadWrite,
		Views:            ViewHealthy,
		MembershipChange: DecisionAllowed,
		ExportFailover:   DecisionAllowed,
		ExportAdmission:  DecisionAllowed,
		Readiness:        ReadinessHealthy,
	}

	untrusted := s.Etcd.unavailable() || s.TiKV.unavailable() || s.Projection != ProjectionFresh

	// Rule 1.
	if s.Etcd.beyondGrace() || s.TiKV.beyondGrace() || s.Projection == ProjectionBlocked {
		b.DataPath = DataPathContinuesDegraded
	}

	// Rule 2.
	if s.Etcd.unavailable() || s.TiKV.unavailable() {
		b.ControlPath = ControlReadOnly
	}

	// Rule 3. Suppression of failover is the deliberate half of the trade-off
	// recorded in entry plan Section 4: the existing active writer stays
	// authoritative, split-brain becomes impossible, and a real gateway failure
	// during the outage is not recovered from until the view is trustworthy.
	if untrusted {
		b.MembershipChange = DecisionRejected
		b.ExportFailover = DecisionSuppressed
	}

	// Admission is on the fail-closed side of the governing rule alongside
	// promotion and membership change, and for the same reason: admitting an
	// export means choosing which gateway serves it, which is a claim about the
	// fleet. A projection five seconds stale is enough to hand a new export to a
	// gateway that has already left.
	//
	// This makes admission refuse in exactly the states membership change does.
	// They are kept as separate fields rather than collapsed into one because
	// they are refused at different enforcement points and counted separately,
	// and because a later rule change to one must not silently move the other.
	if untrusted {
		b.ExportAdmission = DecisionBlocked
	}

	// Rule 4.
	switch {
	case s.Projection == ProjectionBlocked:
		b.Views = ViewUnhealthy
	case untrusted:
		b.Views = ViewDegraded
	}
	switch {
	case b.Views == ViewUnhealthy:
		b.Readiness = ReadinessBlocked
	case untrusted:
		b.Readiness = ReadinessDegradedServingOnCache
	}

	b.Reasons = reasonsFor(s)
	return b
}

func reasonsFor(s State) []string {
	var out []string
	switch s.Projection {
	case ProjectionBlocked:
		out = append(out, "projection stale past the blocked threshold; views refuse to publish as healthy")
	case ProjectionDegraded:
		out = append(out, "projection stale past the degraded threshold; views are marked degraded")
	}
	if s.Etcd.unavailable() && s.TiKV.unavailable() {
		out = append(out, "etcd and tikv are both unreachable; the lower of the two grace windows applies")
	}
	for _, d := range []struct {
		name  string
		state Availability
	}{{"etcd", s.Etcd}, {"tikv", s.TiKV}} {
		switch d.state {
		case UnavailableBeyondGrace:
			out = append(out, d.name+" has been unreachable past its grace window; serving continues on cache, marked degraded")
		case UnavailableWithinGrace:
			out = append(out, d.name+" is unreachable inside its grace window; serving continues")
		}
	}
	return out
}

// AllStates enumerates the entire state space, so a test can assert totality
// rather than sampling it.
func AllStates() []State {
	avail := []Availability{Available, UnavailableWithinGrace, UnavailableBeyondGrace}
	fresh := []Freshness{ProjectionFresh, ProjectionDegraded, ProjectionBlocked}
	out := make([]State, 0, len(avail)*len(avail)*len(fresh))
	for _, e := range avail {
		for _, t := range avail {
			for _, p := range fresh {
				out = append(out, State{Etcd: e, TiKV: t, Projection: p})
			}
		}
	}
	return out
}
