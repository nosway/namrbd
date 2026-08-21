package service

import (
	"context"
	"sync"
	"sync/atomic"
)

// DefaultSweepVolumesPerPass bounds how many volumes one periodic sweep pass
// touches.
//
// The unbounded sweep listed every volume and swept every one of them on every
// tick. At t2_large that is a 1000-record prefix scan plus 1000 per-volume
// sweeps every 30 seconds, per gateway. Garbage collection is background work
// with no deadline, so spreading it across passes costs nothing an operator can
// observe and removes a standing cost that grows with volume count.
const DefaultSweepVolumesPerPass = 64

// BoundedChunkSweeper rotates through volumes across passes instead of sweeping
// all of them on every one.
//
// The volume list is refreshed only when the rotation wraps, which is what
// takes the prefix scan off the tick. At t2_large with the default batch that
// is one list every sixteen passes rather than one per pass.
// volumeLister is the only thing the rotation needs from the repository.
// Narrowing it keeps the sweeper testable without standing up the whole
// metadata interface, and makes the dependency obvious.
type volumeLister interface {
	ListVolumes(ctx context.Context) ([]VolumeSpec, error)
}

type BoundedChunkSweeper struct {
	collector *ChunkGarbageCollector
	lister    volumeLister
	// sweepVolume is the per-volume work, injectable so the rotation can be
	// tested without a backend.
	sweepVolume    func(ctx context.Context, volumeID uint64, limit int) (ChunkGarbageSweepResult, error)
	volumesPerPass int
	perVolumeLimit int

	mu      sync.Mutex
	volumes []HexVolumeID
	cursor  int

	passes      atomic.Int64
	listRefresh atomic.Int64
	swept       atomic.Int64
	volumeErrs  atomic.Int64
	rotations   atomic.Int64
}

// NewBoundedChunkSweeper returns a sweeper bounded to volumesPerPass volumes per
// pass, each swept with perVolumeLimit candidates.
func NewBoundedChunkSweeper(collector *ChunkGarbageCollector, volumesPerPass, perVolumeLimit int) *BoundedChunkSweeper {
	if volumesPerPass <= 0 {
		volumesPerPass = DefaultSweepVolumesPerPass
	}
	var lister volumeLister
	if collector != nil {
		lister = collector.meta
	}
	sweeper := &BoundedChunkSweeper{
		collector:      collector,
		lister:         lister,
		volumesPerPass: volumesPerPass,
		perVolumeLimit: perVolumeLimit,
	}
	if collector != nil {
		sweeper.sweepVolume = collector.SweepVolume
	}
	return sweeper
}

// SweepPass sweeps the next batch of volumes.
//
// A volume that fails is counted and skipped rather than aborting the pass. The
// unbounded sweep returned on the first error, so one volume in a bad state
// stopped garbage collection for every other volume behind it, and the
// remaining volumes were never reached at all.
func (s *BoundedChunkSweeper) SweepPass(ctx context.Context) ([]ChunkGarbageSweepResult, error) {
	if s == nil || s.sweepVolume == nil {
		return nil, nil
	}
	s.passes.Add(1)

	batch, err := s.nextBatch(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]ChunkGarbageSweepResult, 0, len(batch))
	for _, volumeID := range batch {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}
		result, err := s.sweepVolume(ctx, uint64(volumeID), s.perVolumeLimit)
		if err != nil {
			s.volumeErrs.Add(1)
			continue
		}
		s.swept.Add(1)
		results = append(results, result)
	}
	return results, nil
}

// nextBatch returns the next slice of volumes, refreshing the list when the
// rotation wraps.
func (s *BoundedChunkSweeper) nextBatch(ctx context.Context) ([]HexVolumeID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cursor >= len(s.volumes) {
		volumes, err := s.lister.ListVolumes(ctx)
		if err != nil {
			return nil, err
		}
		s.listRefresh.Add(1)
		if len(s.volumes) > 0 {
			s.rotations.Add(1)
		}
		s.volumes = make([]HexVolumeID, 0, len(volumes))
		for _, v := range volumes {
			s.volumes = append(s.volumes, v.ID)
		}
		s.cursor = 0
		if len(s.volumes) == 0 {
			return nil, nil
		}
	}

	end := s.cursor + s.volumesPerPass
	if end > len(s.volumes) {
		end = len(s.volumes)
	}
	batch := append([]HexVolumeID(nil), s.volumes[s.cursor:end]...)
	s.cursor = end
	return batch, nil
}

// BoundedSweepSnapshot is the observable form.
type BoundedSweepSnapshot struct {
	SweepPassCount        int64 `json:"chunk_gc_pass_count"`
	SweepVolumeListCount  int64 `json:"chunk_gc_volume_list_count"`
	SweptVolumeCount      int64 `json:"chunk_gc_swept_volume_count"`
	SweepVolumeErrorCount int64 `json:"chunk_gc_volume_error_count"`
	SweepRotationCount    int64 `json:"chunk_gc_rotation_count"`
	VolumesPerPass        int   `json:"chunk_gc_volumes_per_pass"`
}

// Snapshot returns the current counts.
func (s *BoundedChunkSweeper) Snapshot() BoundedSweepSnapshot {
	if s == nil {
		return BoundedSweepSnapshot{}
	}
	return BoundedSweepSnapshot{
		SweepPassCount:        s.passes.Load(),
		SweepVolumeListCount:  s.listRefresh.Load(),
		SweptVolumeCount:      s.swept.Load(),
		SweepVolumeErrorCount: s.volumeErrs.Load(),
		SweepRotationCount:    s.rotations.Load(),
		VolumesPerPass:        s.volumesPerPass,
	}
}
