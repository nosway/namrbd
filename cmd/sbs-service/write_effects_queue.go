package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const serviceWriteEffectsQueueDepth = 4096
const serviceWriteEffectsMaxAttempts = 5
const defaultServiceWriteEffectsBatchMax = 16
const defaultServiceWriteEffectsBatchCoalesceWait = 0

type serviceWriteEffectsApplier interface {
	ApplyCommittedWriteEffects(ctx context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) error
}

type serviceWriteEffectsBatchApplier interface {
	ApplyCommittedWriteEffectsBatch(ctx context.Context, reqs []clustermeta.ApplyCommittedWriteEffectsRequest) error
}

type serviceAppendOnlyWriteMetadataBatchCommitter interface {
	CommitAppendOnlyWriteMetadataBatch(ctx context.Context, reqs []clustermeta.CommitWriteMetadataRequest) ([]clustermeta.VolumeState, []clustermeta.IdempotencyRecord, error)
}

type serviceAppendOnlyWriteMetadataCommitter interface {
	CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type serviceWriteEffectsRequest struct {
	ctx        context.Context
	req        clustermeta.ApplyCommittedWriteEffectsRequest
	commitReq  clustermeta.CommitWriteMetadataRequest
	commit     bool
	laneKey    string
	enqueuedAt time.Time
	done       chan serviceWriteEffectsResult
}

type serviceWriteEffectsResult struct {
	stats  clustercontrol.WriteSessionEffectsQueueStats
	state  clustermeta.VolumeState
	record clustermeta.IdempotencyRecord
	err    error
}

type serviceWriteEffectsQueue struct {
	applier                  serviceWriteEffectsApplier
	nativeAllocationFastPath bool
	batchCoalesceWait        time.Duration
	batchMax                 int
	laneBucketCount          int
	mu                       sync.Mutex
	workers                  map[string]*serviceWriteEffectsWorker
}

type serviceWriteEffectsWorker struct {
	requests chan serviceWriteEffectsRequest
}

func newServiceWriteEffectsQueue(applier serviceWriteEffectsApplier, opts ...serviceWriteEffectsQueueOption) *serviceWriteEffectsQueue {
	q := &serviceWriteEffectsQueue{
		applier:           applier,
		batchMax:          defaultServiceWriteEffectsBatchMax,
		batchCoalesceWait: defaultServiceWriteEffectsBatchCoalesceWait,
		workers:           make(map[string]*serviceWriteEffectsWorker),
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

type serviceWriteEffectsQueueOption func(*serviceWriteEffectsQueue)

func serviceWriteEffectsQueueNativeAllocationFastPath(enabled bool) serviceWriteEffectsQueueOption {
	return func(q *serviceWriteEffectsQueue) {
		q.nativeAllocationFastPath = enabled
	}
}

func serviceWriteEffectsQueueBatchCoalesceWait(wait time.Duration) serviceWriteEffectsQueueOption {
	return func(q *serviceWriteEffectsQueue) {
		if wait < 0 {
			wait = 0
		}
		q.batchCoalesceWait = wait
	}
}

func serviceWriteEffectsQueueBatchMax(maxItems int) serviceWriteEffectsQueueOption {
	return func(q *serviceWriteEffectsQueue) {
		if maxItems <= 0 {
			maxItems = defaultServiceWriteEffectsBatchMax
		}
		q.batchMax = maxItems
	}
}

func serviceWriteEffectsQueueLaneBucketCount(count int) serviceWriteEffectsQueueOption {
	return func(q *serviceWriteEffectsQueue) {
		if count < 0 {
			count = 0
		}
		q.laneBucketCount = count
	}
}

func (q *serviceWriteEffectsQueue) Enqueue(ctx context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) int {
	depth, _ := q.enqueue(ctx, req, nil)
	return depth
}

func (q *serviceWriteEffectsQueue) EnqueueAndWait(ctx context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) (clustercontrol.WriteSessionEffectsQueueStats, error) {
	if q == nil || q.applier == nil {
		return clustercontrol.WriteSessionEffectsQueueStats{}, nil
	}
	done := make(chan serviceWriteEffectsResult, 1)
	depth, queued := q.enqueue(ctx, req, done)
	if !queued {
		return clustercontrol.WriteSessionEffectsQueueStats{Depth: depth}, nil
	}
	select {
	case result := <-done:
		result.stats.Depth = depth
		return result.stats, result.err
	case <-ctx.Done():
		return clustercontrol.WriteSessionEffectsQueueStats{
			Depth:   depth,
			LaneKey: serviceWriteEffectsLaneKey(req, q.nativeAllocationFastPath, q.laneBucketCount),
		}, ctx.Err()
	}
}

func (q *serviceWriteEffectsQueue) EnqueueAppendOnlyCommitAndWait(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, clustercontrol.WriteSessionEffectsQueueStats, error) {
	if q == nil || q.applier == nil {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, clustercontrol.WriteSessionEffectsQueueStats{}, nil
	}
	done := make(chan serviceWriteEffectsResult, 1)
	depth, queued := q.enqueueCommit(ctx, req, done)
	if !queued {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, clustercontrol.WriteSessionEffectsQueueStats{Depth: depth}, nil
	}
	select {
	case result := <-done:
		result.stats.Depth = depth
		return result.state, result.record, result.stats, result.err
	case <-ctx.Done():
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, clustercontrol.WriteSessionEffectsQueueStats{
			Depth:   depth,
			LaneKey: serviceWriteEffectsLaneKey(req.EffectsApplyRequest(), q.nativeAllocationFastPath, q.laneBucketCount),
		}, ctx.Err()
	}
}

func (q *serviceWriteEffectsQueue) enqueue(ctx context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest, done chan serviceWriteEffectsResult) (int, bool) {
	if q == nil || q.applier == nil {
		return 0, false
	}
	laneKey := serviceWriteEffectsLaneKey(req, q.nativeAllocationFastPath, q.laneBucketCount)
	worker := q.workerForLane(laneKey)
	item := serviceWriteEffectsRequest{ctx: ctx, req: cloneServiceWriteEffectsRequest(req), laneKey: laneKey, enqueuedAt: time.Now(), done: done}
	select {
	case worker.requests <- item:
	default:
		go func() {
			worker.requests <- item
		}()
	}
	return len(worker.requests), true
}

func (q *serviceWriteEffectsQueue) enqueueCommit(ctx context.Context, req clustermeta.CommitWriteMetadataRequest, done chan serviceWriteEffectsResult) (int, bool) {
	if q == nil || q.applier == nil {
		return 0, false
	}
	effectsReq := req.EffectsApplyRequest()
	laneKey := serviceWriteEffectsLaneKey(effectsReq, q.nativeAllocationFastPath, q.laneBucketCount)
	worker := q.workerForLane(laneKey)
	item := serviceWriteEffectsRequest{ctx: ctx, commitReq: cloneServiceWriteMetadataRequest(req), commit: true, laneKey: laneKey, enqueuedAt: time.Now(), done: done}
	select {
	case worker.requests <- item:
	default:
		go func() {
			worker.requests <- item
		}()
	}
	return len(worker.requests), true
}

func (q *serviceWriteEffectsQueue) workerForLane(laneKey string) *serviceWriteEffectsWorker {
	q.mu.Lock()
	defer q.mu.Unlock()
	if worker := q.workers[laneKey]; worker != nil {
		return worker
	}
	worker := &serviceWriteEffectsWorker{
		requests: make(chan serviceWriteEffectsRequest, serviceWriteEffectsQueueDepth),
	}
	q.workers[laneKey] = worker
	go q.runWorker(worker)
	return worker
}

func (q *serviceWriteEffectsQueue) runWorker(worker *serviceWriteEffectsWorker) {
	for item := range worker.requests {
		items := q.drainBatch(worker, item)
		applyStart := time.Now()
		effectsCtx, cancel := detachedServiceWriteEffectsContext(item.ctx)
		states, records, errs := q.applyItems(effectsCtx, items)
		applyDuration := time.Since(applyStart)
		cancel()
		for i, item := range items {
			if item.done != nil {
				item.done <- serviceWriteEffectsResult{
					stats: clustercontrol.WriteSessionEffectsQueueStats{
						LaneKey:           item.laneKey,
						QueueWaitDuration: applyStart.Sub(item.enqueuedAt),
						ApplyDuration:     applyDuration,
						BatchSize:         len(items),
					},
					state:  states[i],
					record: records[i],
					err:    errs[i],
				}
			}
		}
	}
}

func (q *serviceWriteEffectsQueue) drainBatch(worker *serviceWriteEffectsWorker, first serviceWriteEffectsRequest) []serviceWriteEffectsRequest {
	batchMax := q.batchMax
	if batchMax <= 0 {
		batchMax = defaultServiceWriteEffectsBatchMax
	}
	items := []serviceWriteEffectsRequest{first}
	items = drainReadyServiceWriteEffects(worker, items, batchMax)
	if _, ok := q.applier.(serviceWriteEffectsBatchApplier); !ok {
		return items
	}
	if len(items) >= batchMax || q.batchCoalesceWait <= 0 {
		return items
	}

	timer := time.NewTimer(q.batchCoalesceWait)
	defer timer.Stop()
	for len(items) < batchMax {
		select {
		case next, ok := <-worker.requests:
			if !ok {
				return items
			}
			items = append(items, next)
			items = drainReadyServiceWriteEffects(worker, items, batchMax)
		case <-timer.C:
			return items
		}
	}
	return items
}

func drainReadyServiceWriteEffects(worker *serviceWriteEffectsWorker, items []serviceWriteEffectsRequest, maxItems int) []serviceWriteEffectsRequest {
	for len(items) < maxItems {
		select {
		case next, ok := <-worker.requests:
			if !ok {
				return items
			}
			items = append(items, next)
		default:
			return items
		}
	}
	return items
}

func (q *serviceWriteEffectsQueue) applyItems(ctx context.Context, items []serviceWriteEffectsRequest) ([]clustermeta.VolumeState, []clustermeta.IdempotencyRecord, []error) {
	states := make([]clustermeta.VolumeState, len(items))
	records := make([]clustermeta.IdempotencyRecord, len(items))
	errs := make([]error, len(items))
	if len(items) == 0 {
		return states, records, errs
	}
	if items[0].commit {
		return q.applyCommitItems(ctx, items)
	}
	reqs := make([]clustermeta.ApplyCommittedWriteEffectsRequest, 0, len(items))
	for _, item := range items {
		if item.commit {
			for i := range errs {
				errs[i] = fmt.Errorf("mixed append-only commit and effects-only items in one queue batch")
			}
			return states, records, errs
		}
		reqs = append(reqs, cloneServiceWriteEffectsRequest(item.req))
	}
	if batchApplier, ok := q.applier.(serviceWriteEffectsBatchApplier); ok {
		if err := q.applyBatch(ctx, batchApplier, reqs, items[0].laneKey); err == nil {
			return states, records, errs
		}
	}
	if len(items) == 1 {
		errs[0] = q.apply(ctx, items[0].req)
		return states, records, errs
	}
	for i, item := range items {
		errs[i] = q.apply(ctx, item.req)
	}
	return states, records, errs
}

func (q *serviceWriteEffectsQueue) applyCommitItems(ctx context.Context, items []serviceWriteEffectsRequest) ([]clustermeta.VolumeState, []clustermeta.IdempotencyRecord, []error) {
	states := make([]clustermeta.VolumeState, len(items))
	records := make([]clustermeta.IdempotencyRecord, len(items))
	errs := make([]error, len(items))
	reqs := make([]clustermeta.CommitWriteMetadataRequest, 0, len(items))
	for _, item := range items {
		if !item.commit {
			for i := range errs {
				errs[i] = fmt.Errorf("mixed append-only commit and effects-only items in one queue batch")
			}
			return states, records, errs
		}
		reqs = append(reqs, cloneServiceWriteMetadataRequest(item.commitReq))
	}
	if batchCommitter, ok := q.applier.(serviceAppendOnlyWriteMetadataBatchCommitter); ok {
		batchStates, batchRecords, err := q.applyAppendOnlyCommitBatch(ctx, batchCommitter, reqs, items[0].laneKey)
		if err == nil {
			copy(states, batchStates)
			copy(records, batchRecords)
			return states, records, errs
		}
	}
	if len(items) == 1 {
		states[0], records[0], errs[0] = q.applyAppendOnlyCommit(ctx, items[0].commitReq)
		return states, records, errs
	}
	for i, item := range items {
		states[i], records[i], errs[i] = q.applyAppendOnlyCommit(ctx, item.commitReq)
	}
	return states, records, errs
}

func (q *serviceWriteEffectsQueue) apply(ctx context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) error {
	start := time.Now()
	var err error
	attempt := 1
	for ; ; attempt++ {
		err = q.applier.ApplyCommittedWriteEffects(ctx, req)
		if err == nil {
			duration := time.Since(start)
			structuredlog.Info("sbs.service", "write_session_effects_deferred_applied",
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("committed_revision", req.CommittedRevision),
				structuredlog.F("mutation_operation_id", req.MutationOperationID),
				structuredlog.F("allocation_page_count", len(req.AllocationPages)),
				structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
				structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
				structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return nil
		}
		if !errors.Is(err, clustermeta.ErrCASConflict) || attempt >= serviceWriteEffectsMaxAttempts {
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_effects_deferred_apply_failed", err,
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("committed_revision", req.CommittedRevision),
				structuredlog.F("mutation_operation_id", req.MutationOperationID),
				structuredlog.F("allocation_page_count", len(req.AllocationPages)),
				structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
				structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
				structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return err
		}
		backoff := time.Duration(attempt*5) * time.Millisecond
		structuredlog.Info("sbs.service", "write_session_effects_deferred_apply_retry",
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("mutation_operation_id", req.MutationOperationID),
			structuredlog.F("error", err.Error()),
			structuredlog.F("attempt", attempt),
			structuredlog.F("backoff_ms", backoff.Milliseconds()),
		)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_effects_deferred_apply_failed", ctx.Err(),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("committed_revision", req.CommittedRevision),
				structuredlog.F("mutation_operation_id", req.MutationOperationID),
				structuredlog.F("allocation_page_count", len(req.AllocationPages)),
				structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
				structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
				structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (q *serviceWriteEffectsQueue) applyBatch(ctx context.Context, applier serviceWriteEffectsBatchApplier, reqs []clustermeta.ApplyCommittedWriteEffectsRequest, laneKey string) error {
	start := time.Now()
	var err error
	attempt := 1
	for ; ; attempt++ {
		err = applier.ApplyCommittedWriteEffectsBatch(ctx, reqs)
		if err == nil {
			duration := time.Since(start)
			structuredlog.Info("sbs.service", "write_session_effects_deferred_batch_applied",
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return nil
		}
		if !errors.Is(err, clustermeta.ErrCASConflict) || attempt >= serviceWriteEffectsMaxAttempts {
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_effects_deferred_batch_apply_failed", err,
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return err
		}
		backoff := time.Duration(attempt*5) * time.Millisecond
		structuredlog.Info("sbs.service", "write_session_effects_deferred_batch_apply_retry",
			structuredlog.F("volume_id", reqs[0].VolumeID),
			structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
			structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
			structuredlog.F("effects_queue_lane", laneKey),
			structuredlog.F("request_count", len(reqs)),
			structuredlog.F("error", err.Error()),
			structuredlog.F("attempt", attempt),
			structuredlog.F("backoff_ms", backoff.Milliseconds()),
		)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_effects_deferred_batch_apply_failed", ctx.Err(),
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (q *serviceWriteEffectsQueue) applyAppendOnlyCommit(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	committer, ok := q.applier.(serviceAppendOnlyWriteMetadataCommitter)
	if !ok {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, fmt.Errorf("append-only write metadata committer is required")
	}
	start := time.Now()
	state, record, err := committer.CommitAppendOnlyWriteStateAndQueueEffects(ctx, req)
	duration := time.Since(start)
	if err != nil {
		structuredlog.Error("sbs.service", "write_session_append_only_metadata_deferred_apply_failed", err,
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("mutation_operation_id", req.MutationOperationID),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
			structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
			structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
		)
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, err
	}
	structuredlog.Info("sbs.service", "write_session_append_only_metadata_deferred_applied",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("committed_revision", record.Revision),
		structuredlog.F("mutation_operation_id", req.MutationOperationID),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
		structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
		structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
	)
	return state, record, nil
}

func (q *serviceWriteEffectsQueue) applyAppendOnlyCommitBatch(ctx context.Context, applier serviceAppendOnlyWriteMetadataBatchCommitter, reqs []clustermeta.CommitWriteMetadataRequest, laneKey string) ([]clustermeta.VolumeState, []clustermeta.IdempotencyRecord, error) {
	start := time.Now()
	attempt := 1
	for ; ; attempt++ {
		states, records, err := applier.CommitAppendOnlyWriteMetadataBatch(ctx, reqs)
		if err == nil {
			duration := time.Since(start)
			committedRevision := uint64(0)
			if len(records) > 0 {
				committedRevision = records[len(records)-1].Revision
			}
			structuredlog.Info("sbs.service", "write_session_append_only_metadata_deferred_batch_applied",
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", committedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return states, records, nil
		}
		if !errors.Is(err, clustermeta.ErrCASConflict) || attempt >= serviceWriteEffectsMaxAttempts {
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_append_only_metadata_deferred_batch_apply_failed", err,
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return nil, nil, err
		}
		backoff := time.Duration(attempt*5) * time.Millisecond
		structuredlog.Info("sbs.service", "write_session_append_only_metadata_deferred_batch_apply_retry",
			structuredlog.F("volume_id", reqs[0].VolumeID),
			structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
			structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
			structuredlog.F("effects_queue_lane", laneKey),
			structuredlog.F("request_count", len(reqs)),
			structuredlog.F("error", err.Error()),
			structuredlog.F("attempt", attempt),
			structuredlog.F("backoff_ms", backoff.Milliseconds()),
		)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			duration := time.Since(start)
			structuredlog.Error("sbs.service", "write_session_append_only_metadata_deferred_batch_apply_failed", ctx.Err(),
				structuredlog.F("volume_id", reqs[0].VolumeID),
				structuredlog.F("first_committed_revision", reqs[0].CommittedRevision),
				structuredlog.F("last_committed_revision", reqs[len(reqs)-1].CommittedRevision),
				structuredlog.F("effects_queue_lane", laneKey),
				structuredlog.F("request_count", len(reqs)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", duration.Milliseconds()),
			)
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func detachedServiceWriteEffectsContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
}

func serviceWriteEffectsLaneKey(req clustermeta.ApplyCommittedWriteEffectsRequest, nativeAllocationFastPath bool, laneBucketCount int) string {
	volumeID := req.VolumeID
	extentIDs := uniqueSortedUint64s(append([]uint64(nil), req.NormalizeExtentMappings...))
	if !nativeAllocationFastPath || !hasSinglePageScopedAllocationEffects(req) {
		if len(extentIDs) == 1 {
			return fmt.Sprintf("volume:%s:extent:%d", volumeID, extentIDs[0])
		}
		if len(extentIDs) > 1 {
			return fmt.Sprintf("volume:%s:all", volumeID)
		}
	}

	pageNos := append([]uint64(nil), req.AffectedPageNos...)
	if len(pageNos) == 0 {
		for _, page := range req.AllocationPages {
			if page.VolumeID != "" {
				volumeID = page.VolumeID
			}
			pageNos = append(pageNos, page.PageNo)
		}
	}
	pageNos = uniqueSortedUint64s(pageNos)
	if len(pageNos) == 1 {
		if nativeAllocationFastPath && laneBucketCount > 0 {
			bucket := pageNos[0] % uint64(laneBucketCount)
			return fmt.Sprintf("volume:%s:page-bucket:%d/%d", volumeID, bucket, laneBucketCount)
		}
		return fmt.Sprintf("volume:%s:page:%d", volumeID, pageNos[0])
	}
	return fmt.Sprintf("volume:%s:all", volumeID)
}

func hasSinglePageScopedAllocationEffects(req clustermeta.ApplyCommittedWriteEffectsRequest) bool {
	if len(req.AllocationPages) == 0 {
		return false
	}
	pageNos := make([]uint64, 0, len(req.AffectedPageNos)+len(req.AllocationPages))
	pageNos = append(pageNos, req.AffectedPageNos...)
	for _, page := range req.AllocationPages {
		pageNos = append(pageNos, page.PageNo)
	}
	return len(uniqueSortedUint64s(pageNos)) == 1
}

func cloneServiceWriteEffectsRequest(req clustermeta.ApplyCommittedWriteEffectsRequest) clustermeta.ApplyCommittedWriteEffectsRequest {
	req.AllocationPages = append([]clustermeta.AllocationPageRecord(nil), req.AllocationPages...)
	req.NormalizeExtentMappings = append([]uint64(nil), req.NormalizeExtentMappings...)
	req.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	req.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	req.AffectedPageChunkRanges = append([]clustermeta.AllocationPageChunkRangeRecord(nil), req.AffectedPageChunkRanges...)
	req.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	return req
}

func cloneServiceWriteMetadataRequest(req clustermeta.CommitWriteMetadataRequest) clustermeta.CommitWriteMetadataRequest {
	req.AllocationPages = append([]clustermeta.AllocationPageRecord(nil), req.AllocationPages...)
	req.NormalizeExtentMappings = append([]uint64(nil), req.NormalizeExtentMappings...)
	req.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
	req.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
	req.AffectedPageChunkRanges = append([]clustermeta.AllocationPageChunkRangeRecord(nil), req.AffectedPageChunkRanges...)
	req.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
	req.MutationOperation.AffectedExtentIDs = append([]uint64(nil), req.MutationOperation.AffectedExtentIDs...)
	req.MutationOperation.AffectedPageNos = append([]uint64(nil), req.MutationOperation.AffectedPageNos...)
	req.MutationOperation.CompletedPageNos = append([]uint64(nil), req.MutationOperation.CompletedPageNos...)
	req.MutationOperation.RetryPageWindows = append([]clustermeta.MutationPageWindowRecord(nil), req.MutationOperation.RetryPageWindows...)
	req.MutationOperation.RetiredPhysicalChunkIDs = append([]uint64(nil), req.MutationOperation.RetiredPhysicalChunkIDs...)
	return req
}

func uniqueSortedUint64s(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	out := values[:0]
	var last uint64
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}
