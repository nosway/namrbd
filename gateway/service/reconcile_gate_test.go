package service

import (
	"sync"
	"testing"
)

// The first tick must scan: nothing is known at startup, and a gate that
// started clean would leave a gateway on stale path plans until an unrelated
// change happened to arrive.
func TestGateStartsDirty(t *testing.T) {
	g := NewReconcileGate()
	g.AttachWatch()
	if !g.ShouldScan() {
		t.Fatal("the first tick did not scan")
	}
	if g.ShouldScan() {
		t.Fatal("the second tick scanned with no change")
	}
}

// A gate with no watch attached behaves exactly as before. A backend without
// change notification must not quietly stop reconciling.
func TestGateWithoutWatchAlwaysScans(t *testing.T) {
	g := NewReconcileGate()
	for i := 0; i < 5; i++ {
		if !g.ShouldScan() {
			t.Fatalf("tick %d skipped with no watch attached", i)
		}
	}
	if got := g.Snapshot(); got.SkipCount != 0 || got.ScanCount != 5 {
		t.Errorf("snapshot = %+v", got)
	}
}

// A nil gate scans, so a caller that never built one keeps the old behavior.
func TestNilGateScans(t *testing.T) {
	var g *ReconcileGate
	if !g.ShouldScan() {
		t.Fatal("a nil gate skipped a tick")
	}
	if got := g.Snapshot(); got != (ReconcileGateSnapshot{}) {
		t.Errorf("a nil gate reported %+v", got)
	}
}

// This is the saving: a quiet cluster reads at the floor rate rather than on
// every tick.
//
// It does not read nothing. Path-plan reconciliation has transitions that fire
// on the passage of time, so a purely change-driven gate would stall them on a
// cluster where nothing moves.
func TestQuietClusterScansOnlyAtTheFloorRate(t *testing.T) {
	const floor = 12
	g := NewReconcileGateWithFloor(floor)
	g.AttachWatch()
	g.ShouldScan() // startup pass

	const ticks = 120
	scans := 0
	for i := 0; i < ticks; i++ {
		if g.ShouldScan() {
			scans++
		}
	}
	if want := ticks / floor; scans != want {
		t.Errorf("a quiet cluster scanned %d times over %d ticks, want %d at a floor of %d",
			scans, ticks, want, floor)
	}
	snap := g.Snapshot()
	if snap.FloorWakeups != int64(ticks/floor) {
		t.Errorf("floor wakeups = %d, want %d", snap.FloorWakeups, ticks/floor)
	}
	// The point of the floor is that it is far below the tick rate.
	if snap.SkipCount <= snap.ScanCount {
		t.Errorf("skips %d did not dominate scans %d; the standing cost is not removed",
			snap.SkipCount, snap.ScanCount)
	}
}

// A gate with no floor skips indefinitely. Only a caller that has no
// time-based work should ask for that.
func TestZeroFloorSkipsIndefinitely(t *testing.T) {
	g := NewReconcileGateWithFloor(0)
	g.AttachWatch()
	g.ShouldScan()
	for i := 0; i < 100; i++ {
		if g.ShouldScan() {
			t.Fatalf("tick %d scanned with no change and no floor", i)
		}
	}
}

// A change resets the floor countdown, so a busy cluster does not accumulate
// floor wakeups on top of its change-driven scans.
func TestChangeResetsTheFloorCountdown(t *testing.T) {
	g := NewReconcileGateWithFloor(5)
	g.AttachWatch()
	g.ShouldScan()

	for i := 0; i < 20; i++ {
		g.MarkChanged()
		if !g.ShouldScan() {
			t.Fatalf("tick %d did not scan after a change", i)
		}
	}
	if got := g.Snapshot().FloorWakeups; got != 0 {
		t.Errorf("floor wakeups = %d on a cluster that changed every tick", got)
	}
}

// The shipped floor has to be slow enough to remove the standing cost and
// frequent enough that a time-based transition is not delayed unreasonably.
func TestDefaultFloorIsOnceAMinuteAtTheShippedInterval(t *testing.T) {
	const shippedTickSeconds = 5
	if got := DefaultMaxConsecutiveSkips * shippedTickSeconds; got != 60 {
		t.Errorf("the default floor is one scan every %ds at the shipped %ds tick; expected 60s",
			got, shippedTickSeconds)
	}
}

func TestChangeTriggersExactlyOneScan(t *testing.T) {
	g := NewReconcileGate()
	g.AttachWatch()
	g.ShouldScan()

	g.MarkChanged()
	if !g.ShouldScan() {
		t.Fatal("a change did not trigger a scan")
	}
	if g.ShouldScan() {
		t.Fatal("the change triggered a second scan")
	}
	// Several changes between ticks still cost one scan.
	for i := 0; i < 10; i++ {
		g.MarkChanged()
	}
	if !g.ShouldScan() {
		t.Fatal("batched changes did not trigger a scan")
	}
	if g.ShouldScan() {
		t.Fatal("batched changes triggered more than one scan")
	}
	if got := g.Snapshot().ChangeCount; got != 11 {
		t.Errorf("change count = %d, want 11", got)
	}
}

// A change arriving while a scan is in flight must not be swallowed. Clearing
// the flag after the scan instead of before is the bug this pins.
func TestChangeDuringScanIsNotLost(t *testing.T) {
	g := NewReconcileGate()
	g.AttachWatch()
	g.ShouldScan()

	g.MarkChanged()
	if !g.ShouldScan() {
		t.Fatal("the change did not trigger a scan")
	}
	// The watch fires while the caller is still scanning.
	g.MarkChanged()
	if !g.ShouldScan() {
		t.Fatal("a change arriving during a scan was swallowed")
	}
}

// A watch that dies must not leave the gate clean forever; a gateway that stops
// reconciling entirely is worse than the scan this avoids.
func TestWatchLossForcesResync(t *testing.T) {
	g := NewReconcileGate()
	g.AttachWatch()
	g.ShouldScan()
	if g.ShouldScan() {
		t.Fatal("scanned with no change")
	}

	g.DetachWatch()
	for i := 0; i < 3; i++ {
		if !g.ShouldScan() {
			t.Fatalf("tick %d skipped after the watch was lost", i)
		}
	}
	snap := g.Snapshot()
	if snap.ResyncCount != 1 {
		t.Errorf("resync count = %d, want 1", snap.ResyncCount)
	}
	if snap.Watching {
		t.Error("the gate still reports a live watch")
	}

	// Reattaching must scan once before it may skip again. Changes that
	// happened while the feed was down were never reported, so a gate that
	// resumed skipping immediately would serve stale path plans indefinitely.
	g.AttachWatch()
	if !g.ShouldScan() {
		t.Error("the first tick after the watch was reattached skipped; changes missed while it was down would never be picked up")
	}
	if g.ShouldScan() {
		t.Error("kept scanning after the reattach resync with nothing changed")
	}
}

// The watch goroutine and the reconcile tick run concurrently.
func TestGateIsRaceFree(t *testing.T) {
	g := NewReconcileGate()
	g.AttachWatch()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				g.MarkChanged()
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				g.ShouldScan()
				g.Snapshot()
			}
		}()
	}
	wg.Wait()
	if got := g.Snapshot().ChangeCount; got != 2000 {
		t.Errorf("change count = %d, want 2000", got)
	}
}
