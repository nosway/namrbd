package service

import (
	"context"
	"errors"
	"testing"
)

// fakeLister stands in for the metadata repository's volume listing so the
// rotation can be driven without a backend.
type fakeLister struct {
	volumes []VolumeSpec
	calls   int
	err     error
}

func (f *fakeLister) ListVolumes(context.Context) ([]VolumeSpec, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.volumes, nil
}

func listerWith(n int) *fakeLister {
	f := &fakeLister{}
	for i := 1; i <= n; i++ {
		f.volumes = append(f.volumes, VolumeSpec{ID: HexVolumeID(i)})
	}
	return f
}

// sweeperOver builds a sweeper whose per-volume work is recorded rather than
// performed, so the rotation is what is under test.
func sweeperOver(lister *fakeLister, perPass int, swept *[]uint64, failOn map[uint64]bool) *BoundedChunkSweeper {
	s := NewBoundedChunkSweeper(nil, perPass, 8)
	s.lister = lister
	s.sweepVolume = func(_ context.Context, volumeID uint64, _ int) (ChunkGarbageSweepResult, error) {
		if failOn[volumeID] {
			return ChunkGarbageSweepResult{}, errors.New("volume is in a bad state")
		}
		*swept = append(*swept, volumeID)
		return ChunkGarbageSweepResult{VolumeID: HexVolumeID(volumeID)}, nil
	}
	return s
}

// The point of the redesign: one pass touches a bounded number of volumes
// rather than every volume in the cluster.
func TestPassTouchesABoundedNumberOfVolumes(t *testing.T) {
	var swept []uint64
	s := sweeperOver(listerWith(1000), 64, &swept, nil)

	if _, err := s.SweepPass(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if len(swept) != 64 {
		t.Errorf("one pass swept %d volumes, want 64", len(swept))
	}
}

// The volume list is what made this a standing prefix scan. It must be read
// once per rotation, not once per pass.
func TestVolumeListIsReadOncePerRotation(t *testing.T) {
	lister := listerWith(1000)
	var swept []uint64
	s := sweeperOver(lister, 64, &swept, nil)

	// 1000 volumes at 64 per pass takes 16 passes to cover once.
	const passes = 16
	for i := 0; i < passes; i++ {
		if _, err := s.SweepPass(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if lister.calls != 1 {
		t.Errorf("the volume list was read %d times over %d passes, want 1", lister.calls, passes)
	}
	if len(swept) != 1000 {
		t.Errorf("a full rotation swept %d volumes, want 1000", len(swept))
	}
	// The next pass wraps and refreshes.
	if _, err := s.SweepPass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lister.calls != 2 {
		t.Errorf("the rotation did not refresh the list on wrap: %d calls", lister.calls)
	}
	if got := s.Snapshot().SweepRotationCount; got != 1 {
		t.Errorf("rotation count = %d, want 1", got)
	}
}

// Every volume must be reached. A rotation that skipped some would leave
// garbage uncollected on them indefinitely, which is worse than a slow sweep.
func TestRotationReachesEveryVolumeExactlyOnce(t *testing.T) {
	var swept []uint64
	s := sweeperOver(listerWith(100), 7, &swept, nil) // deliberately uneven

	for i := 0; i < 15; i++ { // ceil(100/7) = 15
		if _, err := s.SweepPass(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	seen := map[uint64]int{}
	for _, id := range swept {
		seen[id]++
	}
	if len(seen) != 100 {
		t.Errorf("%d distinct volumes swept, want 100", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("volume %d swept %d times in one rotation", id, n)
		}
	}
}

// The old sweep returned on the first error, so one volume in a bad state
// stopped collection for every volume behind it, and those were never reached.
func TestOneBadVolumeDoesNotStopThePass(t *testing.T) {
	var swept []uint64
	s := sweeperOver(listerWith(10), 10, &swept, map[uint64]bool{3: true, 7: true})

	results, err := s.SweepPass(context.Background())
	if err != nil {
		t.Fatalf("a failing volume aborted the pass: %v", err)
	}
	if len(swept) != 8 {
		t.Errorf("%d volumes swept, want the 8 that were healthy", len(swept))
	}
	if len(results) != 8 {
		t.Errorf("%d results, want 8", len(results))
	}
	for _, id := range swept {
		if id == 3 || id == 7 {
			t.Errorf("a failing volume was reported as swept: %d", id)
		}
	}
	if got := s.Snapshot().SweepVolumeErrorCount; got != 2 {
		t.Errorf("volume error count = %d, want 2", got)
	}
}

// A failed list must surface, since the sweeper has nothing to work from.
func TestListFailureSurfaces(t *testing.T) {
	lister := listerWith(10)
	lister.err = errors.New("etcd is unreachable")
	var swept []uint64
	s := sweeperOver(lister, 4, &swept, nil)

	if _, err := s.SweepPass(context.Background()); err == nil {
		t.Fatal("a failed volume list did not surface")
	}
	if len(swept) != 0 {
		t.Errorf("%d volumes were swept despite the list failing", len(swept))
	}
}

// An empty cluster must not spin: a pass with no volumes does nothing and does
// not wedge the cursor.
func TestEmptyClusterSweepsNothing(t *testing.T) {
	lister := listerWith(0)
	var swept []uint64
	s := sweeperOver(lister, 4, &swept, nil)

	for i := 0; i < 3; i++ {
		results, err := s.SweepPass(context.Background())
		if err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		if len(results) != 0 {
			t.Errorf("pass %d produced results on an empty cluster", i)
		}
	}
	if len(swept) != 0 {
		t.Errorf("%d volumes swept on an empty cluster", len(swept))
	}
}

// A cancelled context must stop the pass rather than sweeping the rest.
func TestCancelledContextStopsThePass(t *testing.T) {
	var swept []uint64
	s := sweeperOver(listerWith(100), 64, &swept, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.SweepPass(ctx); err == nil {
		t.Fatal("a cancelled pass returned no error")
	}
	if len(swept) != 0 {
		t.Errorf("%d volumes were swept after cancellation", len(swept))
	}
}

// A non-positive batch falls back to the default rather than sweeping nothing
// forever or everything at once.
func TestNonPositiveBatchFallsBackToTheDefault(t *testing.T) {
	for _, n := range []int{0, -1} {
		s := NewBoundedChunkSweeper(nil, n, 8)
		if s.volumesPerPass != DefaultSweepVolumesPerPass {
			t.Errorf("batch %d produced volumesPerPass %d", n, s.volumesPerPass)
		}
	}
}

// The default batch has to be a real reduction at the tier this is sized for.
func TestDefaultBatchIsAMeaningfulReductionAtTierScale(t *testing.T) {
	const t2LargeVolumes = 1000
	passesPerRotation := (t2LargeVolumes + DefaultSweepVolumesPerPass - 1) / DefaultSweepVolumesPerPass
	if passesPerRotation < 8 {
		t.Errorf("a rotation takes %d passes at %d volumes; the list scan is barely reduced",
			passesPerRotation, t2LargeVolumes)
	}
}
