package service

import "sync/atomic"

// ReconcileGate decides whether a reconcile tick needs to read anything.
//
// The path-plan loop reads the whole gateway prefix and the whole volume prefix
// on every tick regardless of whether anything changed. At t2_large that is
// roughly 384,000 volume records and 12,000 gateway records per minute
// cluster-wide, because each gateway runs its own loop. Almost all of it
// discovers that nothing moved.
//
// The gate flips to dirty when a watch reports a change, and a tick that finds
// it clean does no reads at all. It fails safe in every direction: a gate with
// no watch attached stays permanently dirty, so a backend without change
// notification behaves exactly as before rather than quietly going idle.
type ReconcileGate struct {
	dirty    atomic.Bool
	watching atomic.Bool

	// maxSkips bounds how many consecutive ticks may be skipped.
	//
	// Change-driven wakeups alone are not enough: path-plan reconciliation has
	// transitions that fire on the passage of time rather than on a change, so
	// a cluster where nothing moves would stall them indefinitely. The floor
	// keeps those running at a far lower rate than every tick.
	maxSkips        int
	consecutiveSkip atomic.Int64

	scans        atomic.Int64
	skips        atomic.Int64
	resyncs      atomic.Int64
	changes      atomic.Int64
	floorWakeups atomic.Int64
}

// NewReconcileGate returns a gate that starts dirty.
//
// The first tick after startup must scan: nothing is known yet, and a gate that
// started clean would leave a gateway serving stale path plans until the first
// unrelated change happened to arrive.
// DefaultMaxConsecutiveSkips is how many quiet ticks pass before the gate scans
// anyway. At the shipped 5-second interval that is one scan a minute, which is
// slow enough to remove the standing cost and frequent enough that a
// time-based path-plan transition is not delayed by more than a minute.
const DefaultMaxConsecutiveSkips = 12

func NewReconcileGate() *ReconcileGate {
	return NewReconcileGateWithFloor(DefaultMaxConsecutiveSkips)
}

// NewReconcileGateWithFloor lets a caller or a test choose the floor.
func NewReconcileGateWithFloor(maxSkips int) *ReconcileGate {
	g := &ReconcileGate{maxSkips: maxSkips}
	g.dirty.Store(true)
	return g
}

// AttachWatch records that a change feed is live. Until this is called the gate
// stays dirty on every tick, which is the pre-existing behavior.
func (g *ReconcileGate) AttachWatch() {
	if g == nil {
		return
	}
	g.watching.Store(true)
}

// DetachWatch records that the change feed stopped, and marks the gate dirty.
//
// A watch that dies without this would leave the gate clean forever and the
// gateway would stop reconciling entirely, which is a far worse failure than
// the scan this change exists to avoid.
func (g *ReconcileGate) DetachWatch() {
	if g == nil {
		return
	}
	g.watching.Store(false)
	g.dirty.Store(true)
	g.resyncs.Add(1)
}

// MarkChanged is called by the watch when something under the watched prefixes
// moved.
func (g *ReconcileGate) MarkChanged() {
	if g == nil {
		return
	}
	g.changes.Add(1)
	g.dirty.Store(true)
}

// ShouldScan reports whether this tick must read, and clears the flag.
//
// Clearing before the scan rather than after is deliberate: a change arriving
// while a scan is in flight must leave the gate dirty so the next tick picks it
// up. Clearing afterwards would swallow it.
func (g *ReconcileGate) ShouldScan() bool {
	if g == nil {
		return true
	}
	if !g.watching.Load() {
		g.consecutiveSkip.Store(0)
		g.scans.Add(1)
		return true
	}
	if g.dirty.Swap(false) {
		g.consecutiveSkip.Store(0)
		g.scans.Add(1)
		return true
	}
	if g.maxSkips > 0 && g.consecutiveSkip.Add(1) >= int64(g.maxSkips) {
		g.consecutiveSkip.Store(0)
		g.floorWakeups.Add(1)
		g.scans.Add(1)
		return true
	}
	g.skips.Add(1)
	return false
}

// ReconcileGateSnapshot is the observable form.
type ReconcileGateSnapshot struct {
	ScanCount    int64 `json:"path_plan_scan_count"`
	SkipCount    int64 `json:"path_plan_skipped_tick_count"`
	ResyncCount  int64 `json:"path_plan_resync_count"`
	ChangeCount  int64 `json:"path_plan_change_event_count"`
	FloorWakeups int64 `json:"path_plan_floor_wakeup_count"`
	Watching     bool  `json:"path_plan_watch_attached"`
}

// Snapshot returns the current counts.
func (g *ReconcileGate) Snapshot() ReconcileGateSnapshot {
	if g == nil {
		return ReconcileGateSnapshot{}
	}
	return ReconcileGateSnapshot{
		ScanCount:    g.scans.Load(),
		SkipCount:    g.skips.Load(),
		ResyncCount:  g.resyncs.Load(),
		ChangeCount:  g.changes.Load(),
		FloorWakeups: g.floorWakeups.Load(),
		Watching:     g.watching.Load(),
	}
}
