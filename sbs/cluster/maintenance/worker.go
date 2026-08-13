package maintenance

import (
	"context"
	"sort"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type WorkerConfig struct {
	VolumeID       string
	ReplicaClients map[string]service.SBSClient
	GatewayID      string
	HostID         string
	RetryBackoff   time.Duration
	PollInterval   time.Duration
}

type Worker struct {
	svc   *Service
	cfg   WorkerConfig
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

func NewWorker(svc *Service, cfg WorkerConfig) *Worker {
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 2 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	return &Worker{
		svc: svc,
		cfg: cfg,
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	failovers, err := w.svc.ScanAndFailoverPrimaries(ctx, w.cfg.VolumeID)
	if err != nil {
		return false, err
	}
	enqueued, err := w.svc.ScanAndEnqueueRepairs(ctx, w.cfg.VolumeID)
	if err != nil {
		return false, err
	}
	transitions, err := w.svc.store.ListPlacementTransitions(ctx, w.cfg.VolumeID)
	if err != nil {
		return false, err
	}
	candidates := make([]metadata.PlacementTransitionRecord, 0, len(transitions))
	nowUnix := w.now().Unix()
	for _, transition := range transitions {
		switch transition.State {
		case metadata.PlacementTransitionQueued, metadata.PlacementTransitionRunning:
			candidates = append(candidates, transition)
		}
	}
	if len(candidates) == 0 {
		return failovers > 0 || enqueued > 0, nil
	}
	mutationOps, err := w.svc.store.ListMutationOperations(ctx, w.cfg.VolumeID)
	if err != nil {
		return false, err
	}
	batchPriority := buildTransitionBatchPriority(candidates, mutationOps)
	sort.Slice(candidates, func(i, j int) bool {
		left := batchPriority[candidates[i].PlacementRef]
		right := batchPriority[candidates[j].PlacementRef]
		if left.failedBatches != right.failedBatches {
			return left.failedBatches > right.failedBatches
		}
		if (left.retryWindows > 0) != (right.retryWindows > 0) {
			return left.retryWindows > 0
		}
		if left.retryWindowBytes != right.retryWindowBytes {
			return left.retryWindowBytes < right.retryWindowBytes
		}
		if left.retryWindowChunks != right.retryWindowChunks {
			return left.retryWindowChunks < right.retryWindowChunks
		}
		if left.retryWindows != right.retryWindows {
			return left.retryWindows < right.retryWindows
		}
		if left.recentBatches != right.recentBatches {
			return left.recentBatches > right.recentBatches
		}
		if left.smallBatches != right.smallBatches {
			return left.smallBatches > right.smallBatches
		}
		if left.oldestFailedAtUnix != right.oldestFailedAtUnix {
			return left.oldestFailedAtUnix < right.oldestFailedAtUnix
		}
		if candidates[i].LastProgressAtUnix == candidates[j].LastProgressAtUnix {
			return candidates[i].PlacementRef < candidates[j].PlacementRef
		}
		return candidates[i].LastProgressAtUnix < candidates[j].LastProgressAtUnix
	})

	transition := candidates[0]
	if _, err := w.svc.ApplyTransition(ctx, w.cfg.VolumeID, transition.PlacementRef, w.cfg.ReplicaClients, w.cfg.GatewayID, w.cfg.HostID); err != nil {
		latest, latestErr := w.svc.store.GetPlacementTransition(ctx, w.cfg.VolumeID, transition.PlacementRef)
		if latestErr == nil {
			switch latest.State {
			case metadata.PlacementTransitionCompleted:
				structuredlog.Info("sbs.maintenance", "transition_worker_observed_completed_after_error",
					structuredlog.F("volume_id", w.cfg.VolumeID),
					structuredlog.F("placement_ref", transition.PlacementRef),
				)
				return true, nil
			case metadata.PlacementTransitionFailed:
				if !isTransitionObsoleteError(err) {
					return true, err
				}
			}
			transition = latest
		}
		if isTransitionObsoleteError(err) {
			transition.State = metadata.PlacementTransitionCompleted
			transition.LastProgressAtUnix = nowUnix
			transition.Attempt++
			_ = w.svc.store.PutPlacementTransition(ctx, transition)
			structuredlog.Info("sbs.maintenance", "transition_obsolete",
				structuredlog.F("volume_id", w.cfg.VolumeID),
				structuredlog.F("placement_ref", transition.PlacementRef),
				structuredlog.F("attempt", transition.Attempt),
				structuredlog.F("error", err.Error()),
			)
			return true, nil
		}
		if isTransitionPreconditionError(err) {
			if transitionPreconditionScope(err) == "target" {
				replanned, replanErr := w.svc.ReplanTransitionTarget(ctx, w.cfg.VolumeID, transition)
				if replanErr != nil {
					return true, replanErr
				}
				if replanned {
					return true, nil
				}
			}
			transition.State = metadata.PlacementTransitionQueued
			transition.LastProgressAtUnix = nowUnix
			transition.Attempt++
			_ = w.svc.store.PutPlacementTransition(ctx, transition)
			structuredlog.Info("sbs.maintenance", "transition_deferred",
				structuredlog.F("volume_id", w.cfg.VolumeID),
				structuredlog.F("placement_ref", transition.PlacementRef),
				structuredlog.F("attempt", transition.Attempt),
				structuredlog.F("error", err.Error()),
			)
			return true, nil
		}
		transition.State = metadata.PlacementTransitionFailed
		transition.LastProgressAtUnix = nowUnix
		transition.Attempt++
		_ = w.svc.store.PutPlacementTransition(ctx, transition)
		structuredlog.Error("sbs.maintenance", "transition_failed", err,
			structuredlog.F("volume_id", w.cfg.VolumeID),
			structuredlog.F("placement_ref", transition.PlacementRef),
			structuredlog.F("attempt", transition.Attempt),
		)
		return true, err
	}
	structuredlog.Info("sbs.maintenance", "transition_worker_applied",
		structuredlog.F("volume_id", w.cfg.VolumeID),
		structuredlog.F("placement_ref", transition.PlacementRef),
	)
	return true, nil
}

type transitionBatchPriority struct {
	failedBatches      int
	retryWindows       int
	retryWindowBytes   uint64
	retryWindowChunks  uint64
	recentBatches      int
	smallBatches       int
	oldestFailedAtUnix int64
}

func buildTransitionBatchPriority(transitions []metadata.PlacementTransitionRecord, operations []metadata.MutationOperationRecord) map[string]transitionBatchPriority {
	priorityByPlacement := make(map[string]transitionBatchPriority, len(transitions))
	parentByOperationID := make(map[string]string, len(transitions))
	for _, transition := range transitions {
		parentByOperationID[transitionMutationOperationID(transition)] = transition.PlacementRef
	}
	recentPagesByExtent := make(map[uint64]map[uint64]struct{})
	for _, operation := range operations {
		if operation.Kind == "transition" && operation.State == metadata.MutationOperationPending {
			placementRef, ok := parentByOperationID[operation.OperationID]
			if ok && len(operation.RetryPageWindows) > 0 {
				priority := priorityByPlacement[placementRef]
				priority.retryWindows = len(operation.RetryPageWindows)
				priority.retryWindowBytes = 0
				priority.retryWindowChunks = 0
				for _, window := range operation.RetryPageWindows {
					priority.retryWindowBytes += window.DataBytes
					priority.retryWindowChunks += window.DataChunks
				}
				priorityByPlacement[placementRef] = priority
			}
		}
		if operation.Kind != "write" || operation.State == metadata.MutationOperationRolledBack {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			pageSet := recentPagesByExtent[extentID]
			if pageSet == nil {
				pageSet = make(map[uint64]struct{})
				recentPagesByExtent[extentID] = pageSet
			}
			for _, pageNo := range operation.AffectedPageNos {
				pageSet[pageNo] = struct{}{}
			}
		}
	}
	for _, operation := range operations {
		if operation.Kind != "transition_batch" {
			continue
		}
		placementRef, ok := parentByOperationID[operation.IdempotencyKey]
		if !ok {
			continue
		}
		remainingPages := subtractCompletedMutationPages(operation.AffectedPageNos, operation.CompletedPageNos)
		priority := priorityByPlacement[placementRef]
		if operation.State == metadata.MutationOperationRunning || operation.State == metadata.MutationOperationPending || operation.State == metadata.MutationOperationFailed {
			if len(remainingPages) <= 1 {
				priority.smallBatches++
			}
			if mutationPagesTouchRecentMutationSet(operation.AffectedExtentIDs, remainingPages, recentPagesByExtent) {
				priority.recentBatches++
			}
		}
		if operation.State == metadata.MutationOperationFailed {
			priority.failedBatches++
			failedAtUnix := operation.LastUpdatedAtUnix
			if failedAtUnix == 0 {
				failedAtUnix = operation.StartedAtUnix
			}
			if priority.oldestFailedAtUnix == 0 || (failedAtUnix > 0 && failedAtUnix < priority.oldestFailedAtUnix) {
				priority.oldestFailedAtUnix = failedAtUnix
			}
		}
		priorityByPlacement[placementRef] = priority
	}
	return priorityByPlacement
}

func subtractCompletedMutationPages(affected, completed []uint64) []uint64 {
	if len(affected) == 0 || len(completed) == 0 {
		return append([]uint64(nil), affected...)
	}
	completedSet := make(map[uint64]struct{}, len(completed))
	for _, pageNo := range completed {
		completedSet[pageNo] = struct{}{}
	}
	remaining := make([]uint64, 0, len(affected))
	for _, pageNo := range affected {
		if _, ok := completedSet[pageNo]; ok {
			continue
		}
		remaining = append(remaining, pageNo)
	}
	return remaining
}

func mutationPagesTouchRecentMutationSet(extentIDs, pageNos []uint64, recentPagesByExtent map[uint64]map[uint64]struct{}) bool {
	if len(extentIDs) == 0 || len(pageNos) == 0 || len(recentPagesByExtent) == 0 {
		return false
	}
	for _, extentID := range extentIDs {
		pageSet := recentPagesByExtent[extentID]
		if len(pageSet) == 0 {
			continue
		}
		for _, pageNo := range pageNos {
			if _, ok := pageSet[pageNo]; ok {
				return true
			}
		}
	}
	return false
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		worked, err := w.RunOnce(ctx)
		if err != nil {
			if sleepErr := w.sleep(ctx, w.cfg.RetryBackoff); sleepErr != nil {
				return sleepErr
			}
			continue
		}
		if worked {
			continue
		}
		if err := w.sleep(ctx, w.cfg.PollInterval); err != nil {
			return err
		}
	}
}
