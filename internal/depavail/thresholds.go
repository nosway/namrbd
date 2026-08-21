package depavail

import (
	"fmt"
	"time"
)

// Defaults from docs/phase-aa-entry-plan.md Section 4. The doc-agreement test
// reads them back out of the table, so changing one in only one place fails.
const (
	DefaultEtcdUnavailableGraceSeconds = 300
	DefaultTiKVUnavailableGraceSeconds = 300
	DefaultProjectionStaleDegradedMS   = 5000
	DefaultProjectionStaleBlockedMS    = 15000
)

// Thresholds are the four configurable boundaries of the matrix.
type Thresholds struct {
	EtcdUnavailableGraceSeconds int `json:"etcd_unavailable_grace_seconds" yaml:"etcd_unavailable_grace_seconds"`
	TiKVUnavailableGraceSeconds int `json:"tikv_unavailable_grace_seconds" yaml:"tikv_unavailable_grace_seconds"`
	ProjectionStaleDegradedMS   int `json:"projection_stale_degraded_ms" yaml:"projection_stale_degraded_ms"`
	ProjectionStaleBlockedMS    int `json:"projection_stale_blocked_ms" yaml:"projection_stale_blocked_ms"`
}

// DefaultThresholds returns the shipped values.
func DefaultThresholds() Thresholds {
	return Thresholds{
		EtcdUnavailableGraceSeconds: DefaultEtcdUnavailableGraceSeconds,
		TiKVUnavailableGraceSeconds: DefaultTiKVUnavailableGraceSeconds,
		ProjectionStaleDegradedMS:   DefaultProjectionStaleDegradedMS,
		ProjectionStaleBlockedMS:    DefaultProjectionStaleBlockedMS,
	}
}

// Validate rejects threshold sets that cannot express the matrix.
//
// A zero grace is rejected rather than treated as "no grace": a config that
// omits the field would otherwise declare every momentary reconnect an outage,
// and the failure would show up as fleet-wide degraded flapping rather than as
// a config error.
func (t Thresholds) Validate() error {
	for _, c := range []struct {
		name  string
		value int
	}{
		{"etcd_unavailable_grace_seconds", t.EtcdUnavailableGraceSeconds},
		{"tikv_unavailable_grace_seconds", t.TiKVUnavailableGraceSeconds},
		{"projection_stale_degraded_ms", t.ProjectionStaleDegradedMS},
		{"projection_stale_blocked_ms", t.ProjectionStaleBlockedMS},
	} {
		if c.value <= 0 {
			return fmt.Errorf("%s must be positive, got %d", c.name, c.value)
		}
	}
	if t.ProjectionStaleBlockedMS <= t.ProjectionStaleDegradedMS {
		return fmt.Errorf(
			"projection_stale_blocked_ms (%d) must exceed projection_stale_degraded_ms (%d); otherwise no projection is ever merely degraded and views jump straight to unhealthy",
			t.ProjectionStaleBlockedMS, t.ProjectionStaleDegradedMS)
	}
	return nil
}

// Observation is what a process can measure locally about its dependencies.
//
// The durations are how long each dependency has been continuously unreachable,
// zero when it is reachable. Keeping this separate from State is what makes the
// grace arithmetic testable without a clock.
type Observation struct {
	EtcdUnreachableFor time.Duration
	TiKVUnreachableFor time.Duration
	EtcdReachable      bool
	TiKVReachable      bool
	ProjectionLag      time.Duration
}

// EffectiveGraces returns the grace window that actually applies to each
// dependency.
//
// When both are unreachable the lower of the two applies to both, per the
// "Both etcd and TiKV unavailable" row. The reason is that the two graces are
// not independent budgets: with both down there is no authority left anywhere,
// so honoring the longer one would keep the cluster claiming a trustworthy
// fleet view for as much as the difference between them with nothing backing
// the claim.
func (t Thresholds) EffectiveGraces(o Observation) (etcd, tikv time.Duration) {
	etcd = time.Duration(t.EtcdUnavailableGraceSeconds) * time.Second
	tikv = time.Duration(t.TiKVUnavailableGraceSeconds) * time.Second
	if !o.EtcdReachable && !o.TiKVReachable {
		lower := etcd
		if tikv < lower {
			lower = tikv
		}
		return lower, lower
	}
	return etcd, tikv
}

// Classify turns a local observation into the state the matrix is indexed by.
func (t Thresholds) Classify(o Observation) State {
	etcdGrace, tikvGrace := t.EffectiveGraces(o)
	return State{
		Etcd:       classifyDependency(o.EtcdReachable, o.EtcdUnreachableFor, etcdGrace),
		TiKV:       classifyDependency(o.TiKVReachable, o.TiKVUnreachableFor, tikvGrace),
		Projection: t.ClassifyProjection(o.ProjectionLag),
	}
}

func classifyDependency(reachable bool, unreachableFor, grace time.Duration) Availability {
	if reachable {
		return Available
	}
	if unreachableFor > grace {
		return UnavailableBeyondGrace
	}
	return UnavailableWithinGrace
}

// ClassifyProjection maps a projection lag onto its freshness band.
//
// The comparison is strictly greater than, so a lag sitting exactly on a
// threshold is still on the healthier side of it. An operator who sets the
// blocked threshold to 15s is saying fifteen seconds is tolerable, not that it
// is the first intolerable value.
func (t Thresholds) ClassifyProjection(lag time.Duration) Freshness {
	ms := lag.Milliseconds()
	switch {
	case ms > int64(t.ProjectionStaleBlockedMS):
		return ProjectionBlocked
	case ms > int64(t.ProjectionStaleDegradedMS):
		return ProjectionDegraded
	default:
		return ProjectionFresh
	}
}
