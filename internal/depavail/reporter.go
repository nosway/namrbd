package depavail

import (
	"sync"
	"time"
)

// Clock returns the current time. Injectable so grace-window arithmetic is
// testable without sleeping.
type Clock func() time.Time

// dependencyProbe tracks whether one dependency is answering, and since when.
//
// It is fed by the outcomes of calls the process already makes rather than by a
// probe of its own. That is deliberate: AA-IMPL-003 spent three slices removing
// standing dependency load, and adding a health probe per process per
// dependency would put a fraction of it back to learn something the existing
// traffic already knows. A dependency that nothing is calling is also a
// dependency whose reachability nobody is waiting on.
type dependencyProbe struct {
	mu sync.Mutex
	// failingSince is zero while the dependency is answering.
	failingSince time.Time
	lastErr      string
	// everReported is false until the first call outcome arrives. A process
	// that has not yet touched a dependency is not evidence that it is down.
	everReported bool
}

func (p *dependencyProbe) success() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.everReported = true
	p.failingSince = time.Time{}
}

func (p *dependencyProbe) failure(now time.Time, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.everReported = true
	p.lastErr = msg
	if p.failingSince.IsZero() {
		// Only the first consecutive failure starts the clock. A later failure
		// must not restart it, or a dependency failing steadily would never
		// reach its grace window and the process would call itself healthy
		// through an outage of any length.
		p.failingSince = now
	}
}

// state reports reachability and how long it has been unreachable.
func (p *dependencyProbe) state(now time.Time) (reachable bool, since time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failingSince.IsZero() {
		return true, 0
	}
	d := now.Sub(p.failingSince)
	if d < 0 {
		d = 0
	}
	return false, d
}

// Dependency names which store a reported outcome came from.
//
// Deliberately not depbudget.Store: that type classifies access paths for
// sizing, and reusing it here would tie a runtime availability decision to a
// static inventory vocabulary that has no reason to stay in step.
type Dependency string

const (
	DependencyEtcd Dependency = "etcd"
	DependencyTiKV Dependency = "tikv"
)

// Report records the outcome of a call this process already had to make.
//
// A nil error means the dependency answered. Passing the error rather than a
// boolean is what lets first_error and last_error carry something an operator
// can act on instead of "unreachable".
func (t *Tracker) Report(store Dependency, err error) {
	p := t.probe(store)
	if p == nil {
		return
	}
	if err == nil {
		p.success()
		return
	}
	msg := string(store) + ": " + err.Error()
	p.failure(t.now(), msg)
	t.RecordError(msg)
}

func (t *Tracker) probe(store Dependency) *dependencyProbe {
	switch store {
	case DependencyEtcd:
		return &t.etcd
	case DependencyTiKV:
		return &t.tikv
	}
	return nil
}

// SetProjectionLag records how far the local projection is behind. Processes
// that hold no projection never call this and stay at zero, which classifies
// fresh.
func (t *Tracker) SetProjectionLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	t.lagNanos.Store(int64(d))
}

// Refresh derives an observation from the reported outcomes and classifies it.
//
// A process calls this from a loop it already runs. It performs no I/O, so its
// cost is a few atomic loads and it can be called as often as convenient. It is
// separate from Status because Status must stay a pure read: an operator
// scraping a status endpoint should not be able to move a counter.
func (t *Tracker) Refresh() Behavior {
	now := t.now()
	etcdReachable, etcdFor := t.etcd.state(now)
	tikvReachable, tikvFor := t.tikv.state(now)
	return t.Observe(Observation{
		EtcdReachable:      etcdReachable,
		EtcdUnreachableFor: etcdFor,
		TiKVReachable:      tikvReachable,
		TiKVUnreachableFor: tikvFor,
		ProjectionLag:      time.Duration(t.lagNanos.Load()),
	})
}
