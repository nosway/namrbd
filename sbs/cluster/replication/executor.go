package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

var (
	ErrIntentPending       = errors.New("idempotency intent is already pending")
	ErrIdempotencyConflict = errors.New("idempotency record conflicts with current writer context")
)

type intentStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
}

type writeIntentStore interface {
	PutWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error
}

type chunkIDAllocator interface {
	AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error)
}

type writeMetadataCommitter interface {
	CommitWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type pageScopedWriteMetadataCommitter interface {
	CommitPageScopedWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type rangeLocalWriteStateCommitter interface {
	CommitRangeLocalWriteState(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type appendOnlyWriteEffectsCommitter interface {
	CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type writeStateCommitter interface {
	CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type committedWriteEffectsApplier interface {
	ApplyCommittedWriteEffects(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) error
}

type cloneDeltaMetadataCommitter interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error
}

type writeMetadataStore interface {
	chunkIDAllocator
	writeMetadataCommitter
}

type writePlanner interface {
	PlanWrite(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, error)
}

type writePlannerWithStats interface {
	PlanWriteWithStats(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, PlanWriteStats, error)
}

type cloneWritePlanner interface {
	PlanCloneWrite(ctx context.Context, cloneID, sourceVolumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, error)
}

type Executor struct {
	intents                 intentStore
	allocator               chunkIDAllocator
	committer               writeMetadataCommitter
	pageScopedCommitter     pageScopedWriteMetadataCommitter
	rangeLocalCommitter     rangeLocalWriteStateCommitter
	appendOnlyCommitter     appendOnlyWriteEffectsCommitter
	stateCommitter          writeStateCommitter
	effectsApplier          committedWriteEffectsApplier
	cloneDeltaCommitter     cloneDeltaMetadataCommitter
	planner                 writePlanner
	preferPageScoped        bool
	preferRangeLocal        bool
	preferAppendOnly        bool
	asyncWriteEffects       bool
	parallelBeginPlan       bool
	allowMissingWriteIntent bool
	asyncEffectsQueue       *deferredWriteEffectsQueue
}

func NewExecutor(intents intentStore, planner writePlanner, writes ...writeMetadataStore) *Executor {
	var allocator chunkIDAllocator
	var committer writeMetadataCommitter
	var pageScopedCommitter pageScopedWriteMetadataCommitter
	var rangeLocalCommitter rangeLocalWriteStateCommitter
	var appendOnlyCommitter appendOnlyWriteEffectsCommitter
	var effectsApplier committedWriteEffectsApplier
	var cloneDeltaCommitter cloneDeltaMetadataCommitter
	if len(writes) > 0 {
		allocator = writes[0]
		committer = writes[0]
		cloneDeltaCommitter = inferCloneDeltaMetadataCommitter(writes[0])
		if inferred, ok := any(writes[0]).(pageScopedWriteMetadataCommitter); ok {
			pageScopedCommitter = inferred
		}
		if inferred, ok := any(writes[0]).(rangeLocalWriteStateCommitter); ok {
			rangeLocalCommitter = inferred
		}
		if inferred, ok := any(writes[0]).(appendOnlyWriteEffectsCommitter); ok {
			appendOnlyCommitter = inferred
		}
		if inferred, ok := any(writes[0]).(committedWriteEffectsApplier); ok {
			effectsApplier = inferred
		}
	} else if inferred, ok := intents.(writeMetadataStore); ok {
		allocator = inferred
		committer = inferred
		cloneDeltaCommitter = inferCloneDeltaMetadataCommitter(inferred)
		if pageScoped, ok := intents.(pageScopedWriteMetadataCommitter); ok {
			pageScopedCommitter = pageScoped
		}
		if rangeLocal, ok := intents.(rangeLocalWriteStateCommitter); ok {
			rangeLocalCommitter = rangeLocal
		}
		if appendOnly, ok := intents.(appendOnlyWriteEffectsCommitter); ok {
			appendOnlyCommitter = appendOnly
		}
		if applier, ok := intents.(committedWriteEffectsApplier); ok {
			effectsApplier = applier
		}
	}
	exec := newExecutorWithStores(intents, planner, allocator, nil, effectsApplier, committer, pageScopedCommitter, rangeLocalCommitter, appendOnlyCommitter)
	exec.cloneDeltaCommitter = cloneDeltaCommitter
	return exec
}

func NewExecutorWithStores(intents intentStore, planner writePlanner, allocator chunkIDAllocator, stateCommitter writeStateCommitter, effectsApplier committedWriteEffectsApplier, committer writeMetadataCommitter) *Executor {
	var pageScopedCommitter pageScopedWriteMetadataCommitter
	if stateCommitter == nil && effectsApplier == nil {
		if inferred, ok := committer.(pageScopedWriteMetadataCommitter); ok {
			pageScopedCommitter = inferred
		}
	}
	if pageScopedCommitter == nil {
		if inferred, ok := stateCommitter.(pageScopedWriteMetadataCommitter); ok {
			pageScopedCommitter = inferred
		}
	}
	var rangeLocalCommitter rangeLocalWriteStateCommitter
	if inferred, ok := stateCommitter.(rangeLocalWriteStateCommitter); ok {
		rangeLocalCommitter = inferred
	}
	if rangeLocalCommitter == nil {
		if inferred, ok := committer.(rangeLocalWriteStateCommitter); ok {
			rangeLocalCommitter = inferred
		}
	}
	var appendOnlyCommitter appendOnlyWriteEffectsCommitter
	if inferred, ok := stateCommitter.(appendOnlyWriteEffectsCommitter); ok {
		appendOnlyCommitter = inferred
	}
	if appendOnlyCommitter == nil {
		if inferred, ok := committer.(appendOnlyWriteEffectsCommitter); ok {
			appendOnlyCommitter = inferred
		}
	}
	exec := newExecutorWithStores(intents, planner, allocator, stateCommitter, effectsApplier, committer, pageScopedCommitter, rangeLocalCommitter, appendOnlyCommitter)
	exec.cloneDeltaCommitter = inferCloneDeltaMetadataCommitter(committer, stateCommitter, effectsApplier, allocator, intents)
	return exec
}

func inferCloneDeltaMetadataCommitter(candidates ...any) cloneDeltaMetadataCommitter {
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if inferred, ok := candidate.(cloneDeltaMetadataCommitter); ok {
			return inferred
		}
	}
	return nil
}

func newExecutorWithStores(intents intentStore, planner writePlanner, allocator chunkIDAllocator, stateCommitter writeStateCommitter, effectsApplier committedWriteEffectsApplier, committer writeMetadataCommitter, pageScopedCommitter pageScopedWriteMetadataCommitter, rangeLocalCommitter rangeLocalWriteStateCommitter, appendOnlyCommitter appendOnlyWriteEffectsCommitter) *Executor {
	if stateCommitter != nil && effectsApplier != nil {
		committer = splitWriteMetadataCommitter{stateCommitter: stateCommitter, effectsApplier: effectsApplier}
	}
	return &Executor{
		intents:             intents,
		allocator:           allocator,
		committer:           committer,
		pageScopedCommitter: pageScopedCommitter,
		rangeLocalCommitter: rangeLocalCommitter,
		appendOnlyCommitter: appendOnlyCommitter,
		stateCommitter:      stateCommitter,
		effectsApplier:      effectsApplier,
		planner:             planner,
	}
}

func (e *Executor) WithPageScopedWriteMetadata(prefer bool) *Executor {
	if e == nil {
		return e
	}
	e.preferPageScoped = prefer
	return e
}

func (e *Executor) WithRangeLocalWriteState(prefer bool) *Executor {
	if e == nil {
		return e
	}
	e.preferRangeLocal = prefer
	return e
}

func (e *Executor) WithAppendOnlyServiceWriteEffects(prefer bool) *Executor {
	if e == nil {
		return e
	}
	e.preferAppendOnly = prefer
	return e
}

func (e *Executor) WithAsyncWriteEffects(enabled bool) *Executor {
	if e == nil {
		return e
	}
	e.asyncWriteEffects = enabled
	if enabled && e.asyncEffectsQueue == nil && e.effectsApplier != nil {
		e.asyncEffectsQueue = newDeferredWriteEffectsQueue(e.effectsApplier)
	}
	if e.stateCommitter != nil && e.effectsApplier != nil {
		e.committer = splitWriteMetadataCommitter{
			stateCommitter:    e.stateCommitter,
			effectsApplier:    e.effectsApplier,
			asyncWriteEffects: enabled,
			asyncEffectsQueue: e.asyncEffectsQueue,
		}
	}
	return e
}

func (e *Executor) WithParallelBeginPlan(enabled bool) *Executor {
	if e == nil {
		return e
	}
	e.parallelBeginPlan = enabled
	return e
}

func (e *Executor) WithAppendOnlyMissingWriteIntent(enabled bool) *Executor {
	if e == nil {
		return e
	}
	e.allowMissingWriteIntent = enabled
	return e
}

func (e *Executor) WithCloneDeltaMetadataCommitter(committer cloneDeltaMetadataCommitter) *Executor {
	if e == nil {
		return e
	}
	e.cloneDeltaCommitter = committer
	return e
}

type splitWriteMetadataCommitter struct {
	stateCommitter    writeStateCommitter
	effectsApplier    committedWriteEffectsApplier
	asyncWriteEffects bool
	asyncEffectsQueue *deferredWriteEffectsQueue
}

func (c splitWriteMetadataCommitter) CommitWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	stateStart := time.Now()
	state, record, err := c.stateCommitter.CommitWriteState(ctx, req.StateCommitRequest())
	stateDuration := time.Since(stateStart)
	if err != nil {
		structuredlog.Error("sbs.replication", "write_metadata_state_commit_failed", err,
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("expected_revision", req.ExpectedRevision),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("duration_ms", stateDuration.Milliseconds()),
		)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	committedRevision := req.CommittedRevision
	if record.Revision != 0 {
		committedRevision = record.Revision
	}
	structuredlog.Info("sbs.replication", "write_metadata_state_committed",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", committedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("duration_ms", stateDuration.Milliseconds()),
	)
	effectsReq := req.EffectsApplyRequest()
	effectsReq.CommittedRevision = committedRevision
	if c.asyncWriteEffects {
		structuredlog.Info("sbs.replication", "write_metadata_effects_deferred",
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", committedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		)
		effectsReq = cloneApplyCommittedWriteEffectsRequest(effectsReq)
		if c.asyncEffectsQueue != nil {
			c.asyncEffectsQueue.Enqueue(ctx, effectsReq)
		} else {
			go func() {
				effectsCtx, cancel := detachedWriteContext(ctx)
				defer cancel()
				_ = applyCommittedWriteEffects(effectsCtx, c.effectsApplier, effectsReq, true)
			}()
		}
		return state, record, nil
	}

	if err := applyCommittedWriteEffects(ctx, c.effectsApplier, effectsReq, false); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

const deferredWriteEffectsQueueDepth = 4096
const deferredWriteEffectsMaxAttempts = 5

type deferredWriteEffectsRequest struct {
	ctx context.Context
	req metadata.ApplyCommittedWriteEffectsRequest
}

type deferredWriteEffectsQueue struct {
	applier committedWriteEffectsApplier
	mu      sync.Mutex
	workers map[string]*deferredWriteEffectsWorker
}

type deferredWriteEffectsWorker struct {
	requests chan deferredWriteEffectsRequest
}

func newDeferredWriteEffectsQueue(applier committedWriteEffectsApplier) *deferredWriteEffectsQueue {
	return &deferredWriteEffectsQueue{
		applier: applier,
		workers: make(map[string]*deferredWriteEffectsWorker),
	}
}

func (q *deferredWriteEffectsQueue) Enqueue(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) {
	if q == nil || q.applier == nil {
		return
	}
	worker := q.workerForVolume(req.VolumeID)
	item := deferredWriteEffectsRequest{ctx: ctx, req: req}
	select {
	case worker.requests <- item:
	default:
		go func() {
			worker.requests <- item
		}()
	}
}

func (q *deferredWriteEffectsQueue) workerForVolume(volumeID string) *deferredWriteEffectsWorker {
	q.mu.Lock()
	defer q.mu.Unlock()
	if worker := q.workers[volumeID]; worker != nil {
		return worker
	}
	worker := &deferredWriteEffectsWorker{
		requests: make(chan deferredWriteEffectsRequest, deferredWriteEffectsQueueDepth),
	}
	q.workers[volumeID] = worker
	go q.runWorker(worker)
	return worker
}

func (q *deferredWriteEffectsQueue) runWorker(worker *deferredWriteEffectsWorker) {
	for item := range worker.requests {
		effectsCtx, cancel := detachedWriteContext(item.ctx)
		_ = applyCommittedWriteEffects(effectsCtx, q.applier, item.req, true)
		cancel()
	}
}

func applyCommittedWriteEffects(ctx context.Context, applier committedWriteEffectsApplier, req metadata.ApplyCommittedWriteEffectsRequest, deferred bool) error {
	effectsStart := time.Now()
	var err error
	attempt := 1
	for ; ; attempt++ {
		err = applier.ApplyCommittedWriteEffects(ctx, req)
		if err == nil {
			break
		}
		if !deferred || !errors.Is(err, metadata.ErrCASConflict) || attempt >= deferredWriteEffectsMaxAttempts {
			effectsDuration := time.Since(effectsStart)
			event := "write_metadata_effects_apply_failed"
			if deferred {
				event = "write_metadata_effects_deferred_apply_failed"
			}
			structuredlog.Error("sbs.replication", event, err,
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("committed_revision", req.CommittedRevision),
				structuredlog.F("mutation_operation_id", req.MutationOperationID),
				structuredlog.F("allocation_page_count", len(req.AllocationPages)),
				structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", effectsDuration.Milliseconds()),
			)
			return err
		}
		backoff := time.Duration(attempt*5) * time.Millisecond
		structuredlog.Info("sbs.replication", "write_metadata_effects_deferred_apply_retry",
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("mutation_operation_id", req.MutationOperationID),
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
			effectsDuration := time.Since(effectsStart)
			structuredlog.Error("sbs.replication", "write_metadata_effects_deferred_apply_failed", ctx.Err(),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("committed_revision", req.CommittedRevision),
				structuredlog.F("mutation_operation_id", req.MutationOperationID),
				structuredlog.F("allocation_page_count", len(req.AllocationPages)),
				structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
				structuredlog.F("attempt_count", attempt),
				structuredlog.F("duration_ms", effectsDuration.Milliseconds()),
			)
			return ctx.Err()
		case <-timer.C:
		}
	}
	effectsDuration := time.Since(effectsStart)
	event := "write_metadata_effects_applied"
	if deferred {
		event = "write_metadata_effects_deferred_applied"
	}
	structuredlog.Info("sbs.replication", event,
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("mutation_operation_id", req.MutationOperationID),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		structuredlog.F("attempt_count", attempt),
		structuredlog.F("duration_ms", effectsDuration.Milliseconds()),
	)
	return nil
}

func cloneApplyCommittedWriteEffectsRequest(req metadata.ApplyCommittedWriteEffectsRequest) metadata.ApplyCommittedWriteEffectsRequest {
	out := req
	out.AllocationPages = cloneAllocationPages(req.AllocationPages)
	out.NormalizeExtentMappings = slices.Clone(req.NormalizeExtentMappings)
	out.AffectedExtentIDs = slices.Clone(req.AffectedExtentIDs)
	out.AffectedPageNos = slices.Clone(req.AffectedPageNos)
	out.RetiredPhysicalChunkIDs = slices.Clone(req.RetiredPhysicalChunkIDs)
	return out
}

func cloneAllocationPages(pages []metadata.AllocationPageRecord) []metadata.AllocationPageRecord {
	if len(pages) == 0 {
		return nil
	}
	out := make([]metadata.AllocationPageRecord, len(pages))
	for i, page := range pages {
		page.Extents = slices.Clone(page.Extents)
		out[i] = page
	}
	return out
}

type BeginWriteRequest struct {
	VolumeID                      string
	CloneID                       string
	RequestID                     string
	AttachmentID                  string
	Generation                    uint64
	IdempotencyKey                string
	OffsetBytes                   uint64
	LengthBytes                   uint64
	PageBytes                     uint32
	ChunkSizeBytes                uint32
	ZeroSemantic                  bool
	AllowZeroNoop                 bool
	UnsafeZeroNoopSkipIdempotency bool
}

type BeginWriteResult struct {
	Execution                        *WriteExecution
	Replay                           *metadata.IdempotencyRecord
	Noop                             *BeginWriteNoopResult
	UnsafeZeroNoopIdempotencySkipped bool
	Stats                            BeginWriteStats
}

type BeginWriteNoopResult struct {
	VolumeID       string
	IdempotencyKey string
	Revision       uint64
	ExtentCount    int
}

type BeginWriteStats struct {
	GetVolumeStateDuration          time.Duration
	GetIdempotencyDuration          time.Duration
	GetMutationDuration             time.Duration
	PlanDuration                    time.Duration
	PlanResolvePlacementsDuration   time.Duration
	PlanResolveAllocationsDuration  time.Duration
	PlanSourceCOWDuration           time.Duration
	PlanBuildTargetsDuration        time.Duration
	PlanResolvedPlacementCount      int
	PlanResolvedAllocationPageCount int
	PlanCopyOnWrite                 bool
	AllocationPrepareDuration       time.Duration
	PutIntentDuration               time.Duration
	PutIdempotencyDuration          time.Duration
	PutMutationDuration             time.Duration
}

type beginWriteStateResult struct {
	state    metadata.VolumeState
	duration time.Duration
	err      error
}

type beginWriteIdempotencyResult struct {
	record   metadata.IdempotencyRecord
	duration time.Duration
	err      error
}

type beginWritePlanResult struct {
	plan  *WritePlan
	stats BeginWriteStats
	err   error
}

func (e *Executor) BeginWrite(ctx context.Context, req BeginWriteRequest) (*BeginWriteResult, error) {
	if e != nil && e.parallelBeginPlan {
		return e.beginWriteWithParallelPlan(ctx, req)
	}
	var stats BeginWriteStats
	stepStart := time.Now()
	state, err := e.intents.GetVolumeState(ctx, req.VolumeID)
	stats.GetVolumeStateDuration = time.Since(stepStart)
	if err != nil {
		return nil, err
	}

	var planned *WritePlan
	if e.allowMissingWriteIntent {
		var planErr error
		planned, planErr = e.planWrite(ctx, req, &stats)
		if planErr != nil {
			return nil, planErr
		}
		if !writePlanIsZeroNoop(planned, req) && e.canUseMissingWriteIntent(planned) {
			return e.beginNewWriteWithPlan(ctx, state, req, planned, stats)
		}
	}

	stepStart = time.Now()
	existing, err := e.intents.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
	stats.GetIdempotencyDuration = time.Since(stepStart)
	switch {
	case err == nil:
		if existing.AttachmentID != req.AttachmentID || existing.Generation != req.Generation || existing.Operation != "write" {
			return nil, ErrIdempotencyConflict
		}
		switch existing.ResultState {
		case metadata.IdempotencyCommitted:
			stepStart = time.Now()
			operation, opErr := e.intents.GetMutationOperation(ctx, req.VolumeID, writeMutationOperationID(req.IdempotencyKey))
			stats.GetMutationDuration = time.Since(stepStart)
			if opErr == nil && operation.State != metadata.MutationOperationCommitted {
				return nil, ErrIntentPending
			}
			if opErr != nil && !errors.Is(opErr, metadata.ErrNotFound) {
				return nil, opErr
			}
			return &BeginWriteResult{Replay: &existing, Stats: stats}, nil
		case metadata.IdempotencyPending:
			if planned != nil {
				return e.beginRecoverablePendingWriteWithPlan(ctx, state, req, planned, stats)
			}
			return e.beginRecoverablePendingWrite(ctx, state, req, stats)
		case metadata.IdempotencyFailed:
			// Retry is allowed by replacing the failed record with a new pending intent below.
		default:
			return nil, fmt.Errorf("unknown idempotency result state %q", existing.ResultState)
		}
	case errors.Is(err, metadata.ErrNotFound):
	default:
		return nil, err
	}

	plan := planned
	if plan == nil {
		plan, err = e.planWrite(ctx, req, &stats)
		if err != nil {
			return nil, err
		}
	}
	if writePlanIsZeroNoop(plan, req) {
		return e.beginWriteZeroNoop(ctx, state, req, plan, stats)
	}
	stepStart = time.Now()
	if err := e.prepareWritePlanAllocations(ctx, plan, req); err != nil {
		return nil, err
	}
	stats.AllocationPrepareDuration = time.Since(stepStart)
	exec := NewWriteExecution(plan, req.RequestID, req.AttachmentID, req.Generation, req.IdempotencyKey, state.Epoch, state.Revision)
	exec.MarkValidated()
	exec.MarkIntentPending()

	record := metadata.IdempotencyRecord{
		IdempotencyKey: req.IdempotencyKey,
		VolumeID:       req.VolumeID,
		AttachmentID:   req.AttachmentID,
		Generation:     req.Generation,
		Epoch:          state.Epoch,
		Revision:       state.Revision,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	operation := newWriteMutationOperationRecord(state, req, time.Now().Unix())
	exec.MutationOperation = operation
	if err := e.putWriteIntent(ctx, record, operation, &stats); err != nil {
		return nil, err
	}
	return &BeginWriteResult{Execution: exec, Stats: stats}, nil
}

func (e *Executor) beginWriteWithParallelPlan(ctx context.Context, req BeginWriteRequest) (*BeginWriteResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stateCh := make(chan beginWriteStateResult, 1)
	planCh := make(chan beginWritePlanResult, 1)
	var idemCh chan beginWriteIdempotencyResult
	skipPreIntentLookup := e.allowMissingWriteIntent

	go func() {
		stepStart := time.Now()
		state, err := e.intents.GetVolumeState(ctx, req.VolumeID)
		stateCh <- beginWriteStateResult{state: state, duration: time.Since(stepStart), err: err}
	}()
	if !skipPreIntentLookup {
		idemCh = make(chan beginWriteIdempotencyResult, 1)
		go func() {
			stepStart := time.Now()
			record, err := e.intents.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
			idemCh <- beginWriteIdempotencyResult{record: record, duration: time.Since(stepStart), err: err}
		}()
	}
	go func() {
		var planStats BeginWriteStats
		plan, err := e.planWrite(ctx, req, &planStats)
		planCh <- beginWritePlanResult{plan: plan, stats: planStats, err: err}
	}()

	stateResult := <-stateCh
	planResult := <-planCh

	stats := planResult.stats
	stats.GetVolumeStateDuration = stateResult.duration
	if stateResult.err != nil {
		return nil, stateResult.err
	}
	if skipPreIntentLookup {
		if planResult.err != nil {
			return nil, planResult.err
		}
		if !writePlanIsZeroNoop(planResult.plan, req) && e.canUseMissingWriteIntent(planResult.plan) {
			return e.beginNewWriteWithPlan(ctx, stateResult.state, req, planResult.plan, stats)
		}
		stepStart := time.Now()
		record, err := e.intents.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
		stats.GetIdempotencyDuration = time.Since(stepStart)
		idemCh = make(chan beginWriteIdempotencyResult, 1)
		idemCh <- beginWriteIdempotencyResult{record: record, duration: stats.GetIdempotencyDuration, err: err}
	}

	idemResult := <-idemCh
	stats.GetIdempotencyDuration = idemResult.duration

	switch {
	case idemResult.err == nil:
		existing := idemResult.record
		if existing.AttachmentID != req.AttachmentID || existing.Generation != req.Generation || existing.Operation != "write" {
			return nil, ErrIdempotencyConflict
		}
		switch existing.ResultState {
		case metadata.IdempotencyCommitted:
			stepStart := time.Now()
			operation, opErr := e.intents.GetMutationOperation(ctx, req.VolumeID, writeMutationOperationID(req.IdempotencyKey))
			stats.GetMutationDuration = time.Since(stepStart)
			if opErr == nil && operation.State != metadata.MutationOperationCommitted {
				return nil, ErrIntentPending
			}
			if opErr != nil && !errors.Is(opErr, metadata.ErrNotFound) {
				return nil, opErr
			}
			return &BeginWriteResult{Replay: &existing, Stats: stats}, nil
		case metadata.IdempotencyPending:
			if planResult.err != nil {
				return nil, planResult.err
			}
			return e.beginRecoverablePendingWriteWithPlan(ctx, stateResult.state, req, planResult.plan, stats)
		case metadata.IdempotencyFailed:
			// Retry is allowed by replacing the failed record with a new pending intent below.
		default:
			return nil, fmt.Errorf("unknown idempotency result state %q", existing.ResultState)
		}
	case errors.Is(idemResult.err, metadata.ErrNotFound):
	default:
		return nil, idemResult.err
	}

	if planResult.err != nil {
		return nil, planResult.err
	}
	if writePlanIsZeroNoop(planResult.plan, req) {
		return e.beginWriteZeroNoop(ctx, stateResult.state, req, planResult.plan, stats)
	}
	return e.beginNewWriteWithPlan(ctx, stateResult.state, req, planResult.plan, stats)
}

func (e *Executor) BeginCloneWrite(ctx context.Context, req BeginWriteRequest) (*BeginWriteResult, error) {
	var stats BeginWriteStats
	stepStart := time.Now()
	state, err := e.intents.GetVolumeState(ctx, req.VolumeID)
	stats.GetVolumeStateDuration = time.Since(stepStart)
	if err != nil {
		return nil, err
	}
	stepStart = time.Now()
	if req.CloneID == "" {
		return nil, fmt.Errorf("clone_id is required for clone write")
	}
	clonePlanner, ok := e.planner.(cloneWritePlanner)
	if !ok || clonePlanner == nil {
		return nil, fmt.Errorf("clone write planner is not configured")
	}
	plan, err := clonePlanner.PlanCloneWrite(ctx, req.CloneID, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	stats.PlanDuration = time.Since(stepStart)
	if err != nil {
		return nil, err
	}
	stepStart = time.Now()
	if err := e.prepareWritePlanAllocations(ctx, plan, req); err != nil {
		return nil, err
	}
	stats.AllocationPrepareDuration = time.Since(stepStart)
	exec := NewWriteExecution(plan, req.RequestID, req.AttachmentID, req.Generation, req.IdempotencyKey, state.Epoch, state.Revision)
	exec.MarkValidated()
	exec.MarkIntentPending()
	return &BeginWriteResult{Execution: exec, Stats: stats}, nil
}

func (e *Executor) beginRecoverablePendingWrite(ctx context.Context, state metadata.VolumeState, req BeginWriteRequest, stats BeginWriteStats) (*BeginWriteResult, error) {
	plan, err := e.planWrite(ctx, req, &stats)
	if err != nil {
		return nil, err
	}
	return e.beginRecoverablePendingWriteWithPlan(ctx, state, req, plan, stats)
}

func (e *Executor) beginRecoverablePendingWriteWithPlan(ctx context.Context, state metadata.VolumeState, req BeginWriteRequest, plan *WritePlan, stats BeginWriteStats) (*BeginWriteResult, error) {
	stepStart := time.Now()
	if err := e.prepareWritePlanAllocations(ctx, plan, req); err != nil {
		return nil, err
	}
	stats.AllocationPrepareDuration = time.Since(stepStart)
	exec := NewWriteExecution(plan, req.RequestID, req.AttachmentID, req.Generation, req.IdempotencyKey, state.Epoch, state.Revision)
	exec.MarkValidated()
	exec.MarkIntentPending()
	stepStart = time.Now()
	operation := newWriteMutationOperationRecord(state, req, time.Now().Unix())
	exec.MutationOperation = operation
	if err := e.intents.PutMutationOperation(ctx, operation); err != nil {
		return nil, err
	}
	stats.PutMutationDuration = time.Since(stepStart)
	return &BeginWriteResult{Execution: exec, Stats: stats}, nil
}

func (e *Executor) beginNewWriteWithPlan(ctx context.Context, state metadata.VolumeState, req BeginWriteRequest, plan *WritePlan, stats BeginWriteStats) (*BeginWriteResult, error) {
	if writePlanIsZeroNoop(plan, req) {
		return e.beginWriteZeroNoop(ctx, state, req, plan, stats)
	}
	stepStart := time.Now()
	if err := e.prepareWritePlanAllocations(ctx, plan, req); err != nil {
		return nil, err
	}
	stats.AllocationPrepareDuration = time.Since(stepStart)
	exec := NewWriteExecution(plan, req.RequestID, req.AttachmentID, req.Generation, req.IdempotencyKey, state.Epoch, state.Revision)
	exec.MarkValidated()
	exec.MarkIntentPending()

	record := metadata.IdempotencyRecord{
		IdempotencyKey: req.IdempotencyKey,
		VolumeID:       req.VolumeID,
		AttachmentID:   req.AttachmentID,
		Generation:     req.Generation,
		Epoch:          state.Epoch,
		Revision:       state.Revision,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	operation := newWriteMutationOperationRecord(state, req, time.Now().Unix())
	exec.MutationOperation = operation
	if e.canUseMissingWriteIntent(plan) {
		exec.AllowMissingWriteIntent = true
		return &BeginWriteResult{Execution: exec, Stats: stats}, nil
	}
	if err := e.putWriteIntent(ctx, record, operation, &stats); err != nil {
		return nil, err
	}
	return &BeginWriteResult{Execution: exec, Stats: stats}, nil
}

func (e *Executor) beginWriteZeroNoop(ctx context.Context, state metadata.VolumeState, req BeginWriteRequest, plan *WritePlan, stats BeginWriteStats) (*BeginWriteResult, error) {
	if req.UnsafeZeroNoopSkipIdempotency {
		return &BeginWriteResult{
			Noop:                             beginWriteNoopResult(req, state, plan),
			UnsafeZeroNoopIdempotencySkipped: true,
			Stats:                            stats,
		}, nil
	}
	record := metadata.IdempotencyRecord{
		IdempotencyKey: req.IdempotencyKey,
		VolumeID:       req.VolumeID,
		AttachmentID:   req.AttachmentID,
		Generation:     req.Generation,
		Epoch:          state.Epoch,
		Revision:       state.Revision,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
	}
	stepStart := time.Now()
	if err := e.intents.PutIdempotencyRecord(ctx, record); err != nil {
		return nil, err
	}
	stats.PutIdempotencyDuration = time.Since(stepStart)
	return &BeginWriteResult{Noop: beginWriteNoopResult(req, state, plan), Stats: stats}, nil
}

func (e *Executor) planWrite(ctx context.Context, req BeginWriteRequest, stats *BeginWriteStats) (*WritePlan, error) {
	stepStart := time.Now()
	if planner, ok := e.planner.(writePlannerWithStats); ok {
		plan, planStats, err := planner.PlanWriteWithStats(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
		if stats != nil {
			stats.PlanDuration = time.Since(stepStart)
			stats.PlanResolvePlacementsDuration = planStats.ResolvePlacementsDuration
			stats.PlanResolveAllocationsDuration = planStats.ResolveAllocationsDuration
			stats.PlanSourceCOWDuration = planStats.SourceCOWDuration
			stats.PlanBuildTargetsDuration = planStats.BuildTargetsDuration
			stats.PlanResolvedPlacementCount = planStats.ResolvedPlacementCount
			stats.PlanResolvedAllocationPageCount = planStats.ResolvedAllocationPageCount
			stats.PlanCopyOnWrite = planStats.CopyOnWrite
		}
		return plan, err
	}
	plan, err := e.planner.PlanWrite(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	if stats != nil {
		stats.PlanDuration = time.Since(stepStart)
	}
	return plan, err
}

func (e *Executor) putWriteIntent(ctx context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord, stats *BeginWriteStats) error {
	if writer, ok := e.intents.(writeIntentStore); ok {
		stepStart := time.Now()
		err := writer.PutWriteIntent(ctx, record, operation)
		if stats != nil {
			stats.PutIntentDuration = time.Since(stepStart)
		}
		return err
	}
	stepStart := time.Now()
	if err := e.intents.PutIdempotencyRecord(ctx, record); err != nil {
		return err
	}
	if stats != nil {
		stats.PutIdempotencyDuration = time.Since(stepStart)
	}
	stepStart = time.Now()
	if err := e.intents.PutMutationOperation(ctx, operation); err != nil {
		return err
	}
	if stats != nil {
		stats.PutMutationDuration = time.Since(stepStart)
	}
	return nil
}

func (e *Executor) CommitMetadata(ctx context.Context, exec *WriteExecution, allocationPages []metadata.AllocationPageRecord, retiredPhysicalChunkIDs []uint64, affectedPageChunkRanges []metadata.AllocationPageChunkRangeRecord) (uint64, error) {
	if !exec.CanCommitMetadata() {
		return 0, fmt.Errorf("write execution is not ready for metadata commit")
	}
	if e.committer == nil {
		return 0, fmt.Errorf("write metadata committer is not configured")
	}
	var err error
	expectedEpoch := exec.MetadataEpoch
	expectedRevision := exec.MetadataRevision
	if expectedEpoch == 0 || expectedRevision == 0 {
		state, err := e.intents.GetVolumeState(ctx, exec.VolumeID)
		if err != nil {
			return 0, err
		}
		if expectedEpoch == 0 {
			expectedEpoch = state.Epoch
		}
		if expectedRevision == 0 {
			expectedRevision = state.Revision
		}
	}
	committedRevision := expectedRevision + 1
	commitReq := metadata.CommitWriteMetadataRequest{
		VolumeID:                 exec.VolumeID,
		ExpectedEpoch:            expectedEpoch,
		ExpectedRevision:         expectedRevision,
		IdempotencyKey:           exec.IdempotencyKey,
		ExpectedIdempotencyState: metadata.IdempotencyPending,
		CommittedRevision:        committedRevision,
		AttachmentID:             exec.AttachmentID,
		Generation:               exec.Generation,
		AllowMissingWriteIntent:  exec.AllowMissingWriteIntent,
		AllocationPages:          allocationPages,
		NormalizeExtentMappings:  normalizeExtentIDsForAllocationBackedCommit(exec),
		MutationOperationID:      writeMutationOperationID(exec.IdempotencyKey),
		ExpectedMutationState:    metadata.MutationOperationRunning,
		AffectedExtentIDs:        extentIDsForAllocationBackedCommit(exec),
		AffectedPageNos:          pageNosForAllocationCommit(allocationPages),
		AffectedPageChunkRanges:  append([]metadata.AllocationPageChunkRangeRecord(nil), affectedPageChunkRanges...),
		RetiredPhysicalChunkIDs:  retiredPhysicalChunkIDs,
		MutationOperation:        exec.MutationOperation,
	}
	var record metadata.IdempotencyRecord
	if e.useAppendOnlyServiceWriteEffects(allocationPages) {
		_, record, err = e.commitAppendOnlyWriteStateAndQueueEffects(ctx, commitReq)
	} else if e.useRangeLocalWriteState(allocationPages) {
		_, record, err = e.commitRangeLocalWriteStateAndEffects(ctx, commitReq)
	} else if e.usePageScopedWriteMetadata(allocationPages) {
		_, record, err = e.pageScopedCommitter.CommitPageScopedWriteMetadata(ctx, commitReq)
	} else {
		_, record, err = e.committer.CommitWriteMetadata(ctx, commitReq)
	}
	if err != nil {
		if errors.Is(err, metadata.ErrCASConflict) {
			revision, recovered, recoverErr := e.recoverCommittedWriteAfterCAS(ctx, exec, allocationPages, retiredPhysicalChunkIDs, affectedPageChunkRanges)
			if recoverErr != nil {
				return 0, recoverErr
			}
			if recovered {
				if err := exec.MarkMetadataCommitted(); err != nil {
					return 0, err
				}
				_ = metadata.EnsurePendingPayloadGCMutationOperation(ctx, e.intents, exec.VolumeID, extentIDsForAllocationBackedCommit(exec), pageNosForAllocationCommit(allocationPages), retiredPhysicalChunkIDs, time.Now())
				return revision, nil
			}
		}
		return 0, err
	}
	if err := exec.MarkMetadataCommitted(); err != nil {
		return 0, err
	}
	_ = metadata.EnsurePendingPayloadGCMutationOperation(ctx, e.intents, exec.VolumeID, extentIDsForAllocationBackedCommit(exec), pageNosForAllocationCommit(allocationPages), retiredPhysicalChunkIDs, time.Now())
	return record.Revision, nil
}

func (e *Executor) CommitCloneDeltaMetadata(ctx context.Context, exec *WriteExecution, cloneID string, allocationPages []metadata.AllocationPageRecord) error {
	if !exec.CanCommitMetadata() {
		return fmt.Errorf("write execution is not ready for clone delta metadata commit")
	}
	if e.cloneDeltaCommitter == nil {
		return fmt.Errorf("clone delta metadata committer is not configured")
	}
	if err := e.cloneDeltaCommitter.CommitCloneDeltaAllocationPages(ctx, cloneID, allocationPages); err != nil {
		return err
	}
	return exec.MarkMetadataCommitted()
}

func (e *Executor) metadataCommitMode(allocationPages []metadata.AllocationPageRecord) string {
	if e.useAppendOnlyServiceWriteEffects(allocationPages) {
		return "append_only_service_ordered_effects"
	}
	if e.useRangeLocalWriteState(allocationPages) {
		if e.asyncWriteEffects {
			return "range_local_async_effects"
		}
		return "range_local"
	}
	if e.usePageScopedWriteMetadata(allocationPages) {
		return "page_scoped"
	}
	if e.asyncWriteEffects {
		return "volume_scoped_async_effects"
	}
	return "volume_scoped"
}

func (e *Executor) usePageScopedWriteMetadata(allocationPages []metadata.AllocationPageRecord) bool {
	return e != nil && e.preferPageScoped && e.pageScopedCommitter != nil && len(allocationPages) > 0
}

func (e *Executor) useRangeLocalWriteState(allocationPages []metadata.AllocationPageRecord) bool {
	return e != nil && e.preferRangeLocal && e.rangeLocalCommitter != nil && e.effectsApplier != nil && len(allocationPages) > 0
}

func (e *Executor) useAppendOnlyServiceWriteEffects(allocationPages []metadata.AllocationPageRecord) bool {
	return e != nil && e.preferAppendOnly && e.appendOnlyCommitter != nil && len(allocationPages) > 0
}

func (e *Executor) canUseMissingWriteIntent(plan *WritePlan) bool {
	return e != nil && e.allowMissingWriteIntent && e.preferAppendOnly && e.appendOnlyCommitter != nil && writePlanHasAllocationPages(plan)
}

func writePlanHasAllocationPages(plan *WritePlan) bool {
	if plan == nil {
		return false
	}
	for _, extent := range plan.Extents {
		if len(extent.AllocationPages) > 0 {
			return true
		}
	}
	return false
}

func (e *Executor) commitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	start := time.Now()
	state, record, err := e.appendOnlyCommitter.CommitAppendOnlyWriteStateAndQueueEffects(ctx, req)
	duration := time.Since(start)
	if err != nil {
		structuredlog.Error("sbs.replication", "write_metadata_append_only_service_effects_commit_failed", err,
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("expected_revision", req.ExpectedRevision),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("duration_ms", duration.Milliseconds()),
		)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	committedRevision := req.CommittedRevision
	if record.Revision != 0 {
		committedRevision = record.Revision
	}
	structuredlog.Info("sbs.replication", "write_metadata_append_only_service_effects_committed",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", committedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		structuredlog.F("duration_ms", duration.Milliseconds()),
	)
	return state, record, nil
}

func (e *Executor) commitRangeLocalWriteStateAndEffects(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	stateStart := time.Now()
	state, record, err := e.rangeLocalCommitter.CommitRangeLocalWriteState(ctx, req)
	stateDuration := time.Since(stateStart)
	if err != nil {
		structuredlog.Error("sbs.replication", "write_metadata_range_local_state_commit_failed", err,
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("expected_revision", req.ExpectedRevision),
			structuredlog.F("committed_revision", req.CommittedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("duration_ms", stateDuration.Milliseconds()),
		)
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	committedRevision := req.CommittedRevision
	if record.Revision != 0 {
		committedRevision = record.Revision
	}
	structuredlog.Info("sbs.replication", "write_metadata_range_local_state_committed",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", committedRevision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
		structuredlog.F("allocation_page_count", len(req.AllocationPages)),
		structuredlog.F("duration_ms", stateDuration.Milliseconds()),
	)
	effectsReq := req.EffectsApplyRequest()
	effectsReq.CommittedRevision = committedRevision
	if e.asyncWriteEffects {
		structuredlog.Info("sbs.replication", "write_metadata_range_local_effects_deferred",
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("committed_revision", committedRevision),
			structuredlog.F("idempotency_key", req.IdempotencyKey),
			structuredlog.F("allocation_page_count", len(req.AllocationPages)),
			structuredlog.F("normalize_extent_count", len(req.NormalizeExtentMappings)),
		)
		effectsReq = cloneApplyCommittedWriteEffectsRequest(effectsReq)
		if e.asyncEffectsQueue != nil {
			e.asyncEffectsQueue.Enqueue(ctx, effectsReq)
		} else {
			go func() {
				effectsCtx, cancel := detachedWriteContext(ctx)
				defer cancel()
				_ = applyCommittedWriteEffects(effectsCtx, e.effectsApplier, effectsReq, true)
			}()
		}
		return state, record, nil
	}
	if err := applyCommittedWriteEffects(ctx, e.effectsApplier, effectsReq, false); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (e *Executor) recoverCommittedWriteAfterCAS(ctx context.Context, exec *WriteExecution, allocationPages []metadata.AllocationPageRecord, retiredPhysicalChunkIDs []uint64, affectedPageChunkRanges []metadata.AllocationPageChunkRangeRecord) (uint64, bool, error) {
	record, err := e.intents.GetIdempotencyRecord(ctx, exec.VolumeID, exec.IdempotencyKey)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if record.AttachmentID != exec.AttachmentID || record.Generation != exec.Generation || record.Operation != "write" {
		return 0, false, nil
	}
	if record.ResultState != metadata.IdempotencyCommitted {
		return 0, false, nil
	}

	operationID := writeMutationOperationID(exec.IdempotencyKey)
	operation, err := e.intents.GetMutationOperation(ctx, exec.VolumeID, operationID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return record.Revision, true, nil
		}
		return 0, false, err
	}
	if operation.State == metadata.MutationOperationCommitted {
		return record.Revision, true, nil
	}
	if e.effectsApplier == nil {
		return 0, false, nil
	}
	err = e.effectsApplier.ApplyCommittedWriteEffects(ctx, metadata.ApplyCommittedWriteEffectsRequest{
		VolumeID:                exec.VolumeID,
		CommittedRevision:       record.Revision,
		AllocationPages:         allocationPages,
		NormalizeExtentMappings: normalizeExtentIDsForAllocationBackedCommit(exec),
		MutationOperationID:     operationID,
		ExpectedMutationState:   metadata.MutationOperationRunning,
		AffectedExtentIDs:       extentIDsForAllocationBackedCommit(exec),
		AffectedPageNos:         pageNosForAllocationCommit(allocationPages),
		AffectedPageChunkRanges: append([]metadata.AllocationPageChunkRangeRecord(nil), affectedPageChunkRanges...),
		RetiredPhysicalChunkIDs: retiredPhysicalChunkIDs,
	})
	if err != nil {
		return 0, false, err
	}
	return record.Revision, true, nil
}

func extentIDsForAllocationBackedCommit(exec *WriteExecution) []uint64 {
	if exec == nil {
		return nil
	}
	seen := make(map[uint64]struct{}, len(exec.Extents))
	out := make([]uint64, 0, len(exec.Extents))
	for _, extent := range exec.Extents {
		if len(extent.Plan.AllocationPages) == 0 {
			continue
		}
		extentID := extent.Plan.Extent.ExtentID
		if _, ok := seen[extentID]; ok {
			continue
		}
		seen[extentID] = struct{}{}
		out = append(out, extentID)
	}
	slices.Sort(out)
	return out
}

func normalizeExtentIDsForAllocationBackedCommit(exec *WriteExecution) []uint64 {
	if exec == nil {
		return nil
	}
	seen := make(map[uint64]struct{}, len(exec.Extents))
	out := make([]uint64, 0, len(exec.Extents))
	for _, extent := range exec.Extents {
		if len(extent.Plan.AllocationPages) == 0 || extent.Plan.Extent.ChunkID == 0 {
			continue
		}
		extentID := extent.Plan.Extent.ExtentID
		if _, ok := seen[extentID]; ok {
			continue
		}
		seen[extentID] = struct{}{}
		out = append(out, extentID)
	}
	slices.Sort(out)
	return out
}

func pageNosForAllocationCommit(pages []metadata.AllocationPageRecord) []uint64 {
	if len(pages) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(pages))
	for _, page := range pages {
		out = append(out, page.PageNo)
	}
	slices.Sort(out)
	out = slices.Compact(out)
	return out
}

func beginWriteNoopResult(req BeginWriteRequest, state metadata.VolumeState, plan *WritePlan) *BeginWriteNoopResult {
	extentCount := 0
	if plan != nil {
		extentCount = len(plan.Extents)
	}
	return &BeginWriteNoopResult{
		VolumeID:       req.VolumeID,
		IdempotencyKey: req.IdempotencyKey,
		Revision:       state.Revision,
		ExtentCount:    extentCount,
	}
}

func writePlanIsZeroNoop(plan *WritePlan, req BeginWriteRequest) bool {
	if plan == nil || !req.AllowZeroNoop || !req.ZeroSemantic || req.LengthBytes == 0 || req.ChunkSizeBytes == 0 {
		return false
	}
	touched := false
	for _, extent := range plan.Extents {
		if extent.CopyOnWrite || extent.Extent.ChunkID != 0 || extent.ChunkSizeBytes == 0 || len(extent.AllocationPages) == 0 {
			return false
		}
		writeStart, writeLength, err := overlapRange(extent.Extent.LogicalOffset, extent.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
		if err != nil {
			return false
		}
		if writeLength == 0 {
			continue
		}
		chunkSize := uint64(extent.ChunkSizeBytes)
		writeEnd := writeStart + writeLength
		startChunk := writeStart / chunkSize
		endChunk := (writeEnd - 1) / chunkSize
		for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
			touched = true
			if !resolvedAllocationPagesChunkIsZero(extent.AllocationPages, logicalChunk) {
				return false
			}
		}
	}
	return touched
}

func resolvedAllocationPagesChunkIsZero(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) bool {
	for _, resolved := range pages {
		if logicalChunk < resolved.RangeStartChunk || logicalChunk >= resolved.RangeEndChunk {
			continue
		}
		return allocationPageChunkIsZero(resolved.Page, logicalChunk)
	}
	return false
}

func allocationPageChunkIsZero(page metadata.AllocationPageRecord, logicalChunk uint64) bool {
	for _, extent := range page.Extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if logicalChunk < start || logicalChunk >= end {
			continue
		}
		return extent.Kind == metadata.AllocationKindZero
	}
	return false
}

func (e *Executor) PrepareAllocationCommit(exec *WriteExecution, req WriteRequest) ([]metadata.AllocationPageRecord, []uint64, []metadata.AllocationPageChunkRangeRecord, error) {
	if req.PageBytes == 0 || req.ChunkSizeBytes == 0 || req.PageBytes%req.ChunkSizeBytes != 0 {
		return nil, nil, nil, nil
	}
	pageStates := make(map[uint64]*allocationCommitPageState)
	affectedRanges := make([]metadata.AllocationPageChunkRangeRecord, 0)
	for _, extent := range exec.Extents {
		if extent.Plan.ChunkSizeBytes == 0 || len(extent.Plan.AllocationPages) == 0 {
			continue
		}
		writeStart, writeLength, err := overlapRange(extent.Plan.Extent.LogicalOffset, extent.Plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
		if err != nil || writeLength == 0 {
			continue
		}
		if extent.Plan.ChunkSizeBytes != req.ChunkSizeBytes {
			return nil, nil, nil, fmt.Errorf("extent %d chunk size mismatch: plan=%d req=%d", extent.Plan.Extent.ExtentID, extent.Plan.ChunkSizeBytes, req.ChunkSizeBytes)
		}
		for _, resolvedPage := range extent.Plan.AllocationPages {
			state, ok := pageStates[resolvedPage.Page.PageNo]
			if !ok {
				state, err = newAllocationCommitPageState(resolvedPage.Page)
				if err != nil {
					return nil, nil, nil, err
				}
				pageStates[resolvedPage.Page.PageNo] = state
			}
			rng, overlapsPage, err := allocationCommitAffectedPageChunkRange(resolvedPage.Page, writeStart, writeLength)
			if err != nil {
				return nil, nil, nil, err
			}
			if overlapsPage {
				affectedRanges = append(affectedRanges, rng)
			}
			if err := state.applyWrite(extent.Plan, req, writeStart, writeLength, extent.ChunkEncryptionHeaders); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	if len(pageStates) == 0 {
		return nil, nil, nil, nil
	}
	pageNos := make([]uint64, 0, len(pageStates))
	for pageNo := range pageStates {
		pageNos = append(pageNos, pageNo)
	}
	slices.Sort(pageNos)
	out := make([]metadata.AllocationPageRecord, 0, len(pageNos))
	retired := make([]uint64, 0)
	for _, pageNo := range pageNos {
		state := pageStates[pageNo]
		out = append(out, state.toRecord())
		retired = append(retired, state.retiredPhysicalChunkIDs()...)
	}
	return out, uniqueSortedUint64s(retired), mergeAllocationCommitPageChunkRanges(affectedRanges), nil
}

func (e *Executor) MarkFailed(ctx context.Context, exec *WriteExecution, err error) error {
	exec.MarkFailed(err)
	state, stateErr := e.intents.GetVolumeState(ctx, exec.VolumeID)
	if stateErr != nil {
		return stateErr
	}
	record := metadata.IdempotencyRecord{
		IdempotencyKey: exec.IdempotencyKey,
		VolumeID:       exec.VolumeID,
		AttachmentID:   exec.AttachmentID,
		Generation:     exec.Generation,
		Epoch:          state.Epoch,
		Revision:       state.Revision,
		Operation:      "write",
		ResultState:    metadata.IdempotencyFailed,
	}
	if err := e.intents.PutIdempotencyRecord(ctx, record); err != nil {
		return err
	}
	nowUnix := time.Now().Unix()
	return e.intents.PutMutationOperation(ctx, metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID(exec.IdempotencyKey),
		VolumeID:           exec.VolumeID,
		Kind:               "write",
		State:              metadata.MutationOperationFailed,
		AllocationRevision: state.Revision,
		WriterFencingEpoch: state.Epoch,
		IdempotencyKey:     exec.IdempotencyKey,
		StartedAtUnix:      nowUnix,
		LastUpdatedAtUnix:  nowUnix,
		ErrorMessage:       strings.TrimSpace(err.Error()),
	})
}

func writeMutationOperationID(idempotencyKey string) string {
	return fmt.Sprintf("write-%x", idempotencyKey)
}

func newWriteMutationOperationRecord(state metadata.VolumeState, req BeginWriteRequest, nowUnix int64) metadata.MutationOperationRecord {
	return metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID(req.IdempotencyKey),
		VolumeID:           req.VolumeID,
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: state.Revision,
		WriterFencingEpoch: state.Epoch,
		IdempotencyKey:     req.IdempotencyKey,
		StartedAtUnix:      nowUnix,
		LastUpdatedAtUnix:  nowUnix,
	}
}

func uniqueSortedUint64s(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(values))
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}
