package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakeIntentStore struct {
	volumeState       metadata.VolumeState
	records           map[string]metadata.IdempotencyRecord
	mutationOps       map[string]metadata.MutationOperationRecord
	allocationPages   map[uint64]metadata.AllocationPageRecord
	extentMappings    map[uint64]metadata.ExtentMappingRecord
	nextChunkID       uint64
	volumeStateGets   int
	idempotencyGets   int
	forceCAS          bool
	casFailures       int
	intentPutCalls    int
	commitCalls       int
	pageScopedCalls   int
	rangeLocalCalls   int
	appendOnlyCalls   int
	appendOnlyReq     metadata.CommitWriteMetadataRequest
	stateCommitCalls  int
	effectsApplyCalls int
	effectsStarted    chan struct{}
	effectsRelease    <-chan struct{}
	effectsCompleted  chan struct{}
	cloneID           string
	clonePages        []metadata.AllocationPageRecord
	cloneCommitErr    error
}

type fakeCloneDeltaCommitter struct {
	cloneID string
	pages   []metadata.AllocationPageRecord
	err     error
}

func (f *fakeCloneDeltaCommitter) CommitCloneDeltaAllocationPages(_ context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	if f.err != nil {
		return f.err
	}
	f.cloneID = cloneID
	f.pages = append([]metadata.AllocationPageRecord(nil), pages...)
	return nil
}

func newFakeIntentStore() *fakeIntentStore {
	return &fakeIntentStore{
		volumeState: metadata.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    3,
			Revision: 11,
			Status:   metadata.VolumeStatusHealthy,
		},
		records:         make(map[string]metadata.IdempotencyRecord),
		mutationOps:     make(map[string]metadata.MutationOperationRecord),
		allocationPages: make(map[uint64]metadata.AllocationPageRecord),
		extentMappings:  make(map[uint64]metadata.ExtentMappingRecord),
	}
}

func (f *fakeIntentStore) GetVolumeState(_ context.Context, _ string) (metadata.VolumeState, error) {
	f.volumeStateGets++
	return f.volumeState, nil
}

func (f *fakeIntentStore) PutVolumeState(_ context.Context, rec metadata.VolumeState) error {
	f.volumeState = rec
	return nil
}

func (f *fakeIntentStore) GetIdempotencyRecord(_ context.Context, _ string, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	f.idempotencyGets++
	rec, ok := f.records[idempotencyKey]
	if !ok {
		return metadata.IdempotencyRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func (f *fakeIntentStore) PutIdempotencyRecord(_ context.Context, rec metadata.IdempotencyRecord) error {
	f.records[rec.IdempotencyKey] = rec
	return nil
}

func (f *fakeIntentStore) PutMutationOperation(_ context.Context, rec metadata.MutationOperationRecord) error {
	f.mutationOps[rec.OperationID] = rec
	return nil
}

func (f *fakeIntentStore) PutWriteIntent(_ context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	f.intentPutCalls++
	f.records[record.IdempotencyKey] = record
	f.mutationOps[operation.OperationID] = operation
	return nil
}

func (f *fakeIntentStore) CommitCloneDeltaAllocationPages(_ context.Context, cloneID string, pages []metadata.AllocationPageRecord) error {
	if f.cloneCommitErr != nil {
		return f.cloneCommitErr
	}
	f.cloneID = cloneID
	f.clonePages = append([]metadata.AllocationPageRecord(nil), pages...)
	return nil
}

func (f *fakeIntentStore) AllocateChunkIDs(_ context.Context, _ string, count uint32) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	if f.nextChunkID == 0 {
		f.nextChunkID = 1
	}
	start := f.nextChunkID
	f.nextChunkID += uint64(count)
	return start, nil
}

func (f *fakeIntentStore) GetMutationOperation(_ context.Context, _, operationID string) (metadata.MutationOperationRecord, error) {
	rec, ok := f.mutationOps[operationID]
	if !ok {
		return metadata.MutationOperationRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func (f *fakeIntentStore) CommitWriteMetadata(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	f.commitCalls++
	return f.applyCombinedCommit(req)
}

func (f *fakeIntentStore) CommitPageScopedWriteMetadata(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	f.pageScopedCalls++
	if f.forceCAS {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.casFailures > 0 {
		f.casFailures--
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.volumeState.Epoch != req.ExpectedEpoch {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, ok := f.records[req.IdempotencyKey]
	if !ok {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrNotFound
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	pages := append([]metadata.AllocationPageRecord(nil), req.AllocationPages...)
	var revision uint64
	for i := range pages {
		page := pages[i]
		current := f.allocationPages[page.PageNo]
		if current.PageBytes != 0 && (current.PageBytes != page.PageBytes || current.ChunkSizeBytes != page.ChunkSizeBytes) {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
		}
		if current.Revision != page.Revision {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
		}
		page.Revision = current.Revision + 1
		pages[i] = page
		if page.Revision > revision {
			revision = page.Revision
		}
	}
	record.Revision = revision
	record.ResultState = metadata.IdempotencyCommitted
	f.records[req.IdempotencyKey] = record
	effectsReq := req.EffectsApplyRequest()
	effectsReq.CommittedRevision = revision
	effectsReq.AllocationPages = pages
	if err := f.applyPageScopedEffects(effectsReq); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return f.volumeState, record, nil
}

func (f *fakeIntentStore) CommitRangeLocalWriteState(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	f.rangeLocalCalls++
	if f.forceCAS {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.volumeState.Epoch != req.ExpectedEpoch {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, ok := f.records[req.IdempotencyKey]
	if !ok {
		if !req.AllowMissingWriteIntent {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrNotFound
		}
		if req.ExpectedIdempotencyState != metadata.IdempotencyPending {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
		}
		record = metadata.IdempotencyRecord{
			IdempotencyKey: req.IdempotencyKey,
			VolumeID:       req.VolumeID,
			AttachmentID:   req.AttachmentID,
			Generation:     req.Generation,
			Epoch:          req.ExpectedEpoch,
			Revision:       req.ExpectedRevision,
			Operation:      "write",
			ResultState:    metadata.IdempotencyPending,
		}
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	var revision uint64
	for _, page := range req.AllocationPages {
		current := f.allocationPages[page.PageNo]
		next := current.Revision + 1
		if next > revision {
			revision = next
		}
	}
	record.Revision = revision
	record.ResultState = metadata.IdempotencyCommitted
	f.records[req.IdempotencyKey] = record
	return f.volumeState, record, nil
}

func (f *fakeIntentStore) CommitAppendOnlyWriteStateAndQueueEffects(_ context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	f.appendOnlyCalls++
	f.appendOnlyReq = req
	if f.forceCAS {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.volumeState.Epoch != req.ExpectedEpoch {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, ok := f.records[req.IdempotencyKey]
	if !ok {
		if !req.AllowMissingWriteIntent {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrNotFound
		}
		if req.ExpectedIdempotencyState != metadata.IdempotencyPending {
			return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
		}
		record = metadata.IdempotencyRecord{
			IdempotencyKey: req.IdempotencyKey,
			VolumeID:       req.VolumeID,
			AttachmentID:   req.AttachmentID,
			Generation:     req.Generation,
			Epoch:          req.ExpectedEpoch,
			Revision:       req.ExpectedRevision,
			Operation:      "write",
			ResultState:    metadata.IdempotencyPending,
		}
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record.Revision = req.CommittedRevision + 1000
	record.ResultState = metadata.IdempotencyCommitted
	f.records[req.IdempotencyKey] = record
	return metadata.VolumeState{
		VolumeID: f.volumeState.VolumeID,
		Epoch:    f.volumeState.Epoch,
		Revision: record.Revision,
		Status:   f.volumeState.Status,
	}, record, nil
}

func (f *fakeIntentStore) CommitWriteState(_ context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	f.stateCommitCalls++
	if f.forceCAS {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.casFailures > 0 {
		f.casFailures--
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	if f.volumeState.Epoch != req.ExpectedEpoch || f.volumeState.Revision != req.ExpectedRevision {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, ok := f.records[req.IdempotencyKey]
	if !ok {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrNotFound
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	f.volumeState.Revision = req.CommittedRevision
	record.Revision = req.CommittedRevision
	record.ResultState = metadata.IdempotencyCommitted
	f.records[req.IdempotencyKey] = record
	return f.volumeState, record, nil
}

func (f *fakeIntentStore) ApplyCommittedWriteEffects(_ context.Context, req metadata.ApplyCommittedWriteEffectsRequest) error {
	f.effectsApplyCalls++
	if f.effectsStarted != nil {
		select {
		case f.effectsStarted <- struct{}{}:
		default:
		}
	}
	if f.effectsRelease != nil {
		<-f.effectsRelease
	}
	if req.MutationOperationID != "" {
		op, ok := f.mutationOps[req.MutationOperationID]
		if !ok {
			return metadata.ErrNotFound
		}
		if op.State != req.ExpectedMutationState {
			return metadata.ErrCASConflict
		}
		op.State = metadata.MutationOperationCommitted
		op.AllocationRevision = req.CommittedRevision
		op.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
		op.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
		op.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
		f.mutationOps[req.MutationOperationID] = op
	}
	for _, page := range req.AllocationPages {
		page.Revision = req.CommittedRevision
		f.allocationPages[page.PageNo] = page
	}
	for _, extentID := range req.NormalizeExtentMappings {
		mapping, ok := f.extentMappings[extentID]
		if !ok {
			return metadata.ErrNotFound
		}
		mapping.ChunkID = 0
		mapping.Revision = req.CommittedRevision
		f.extentMappings[extentID] = mapping
	}
	if f.effectsCompleted != nil {
		select {
		case f.effectsCompleted <- struct{}{}:
		default:
		}
	}
	return nil
}

func (f *fakeIntentStore) applyPageScopedEffects(req metadata.ApplyCommittedWriteEffectsRequest) error {
	if req.MutationOperationID != "" {
		op, ok := f.mutationOps[req.MutationOperationID]
		if !ok {
			return metadata.ErrNotFound
		}
		if op.State != req.ExpectedMutationState {
			return metadata.ErrCASConflict
		}
		op.State = metadata.MutationOperationCommitted
		op.AllocationRevision = req.CommittedRevision
		op.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
		op.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
		op.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
		f.mutationOps[req.MutationOperationID] = op
	}
	for _, page := range req.AllocationPages {
		f.allocationPages[page.PageNo] = page
	}
	for _, extentID := range req.NormalizeExtentMappings {
		mapping, ok := f.extentMappings[extentID]
		if !ok {
			return metadata.ErrNotFound
		}
		mapping.ChunkID = 0
		mapping.Revision = req.CommittedRevision
		f.extentMappings[extentID] = mapping
	}
	return nil
}

func (f *fakeIntentStore) applyCombinedCommit(req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	state, record, err := f.CommitWriteState(context.Background(), req.StateCommitRequest())
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if err := f.ApplyCommittedWriteEffects(context.Background(), req.EffectsApplyRequest()); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

type fakePlanner struct {
	plan *WritePlan
	err  error
}

func (f fakePlanner) PlanWrite(_ context.Context, _ string, _, _ uint64, _, _ uint32) (*WritePlan, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.plan, nil
}

func (f fakePlanner) PlanCloneWrite(_ context.Context, _, _ string, _, _ uint64, _, _ uint32) (*WritePlan, error) {
	return f.PlanWrite(context.Background(), "", 0, 0, 0, 0)
}

type blockingBeginIntentStore struct {
	*fakeIntentStore
	release            <-chan struct{}
	volumeStarted      chan struct{}
	idempotencyStarted chan struct{}
}

func (s *blockingBeginIntentStore) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	signalStarted(s.volumeStarted)
	select {
	case <-s.release:
	case <-ctx.Done():
		return metadata.VolumeState{}, ctx.Err()
	}
	return s.fakeIntentStore.GetVolumeState(ctx, volumeID)
}

func (s *blockingBeginIntentStore) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	signalStarted(s.idempotencyStarted)
	select {
	case <-s.release:
	case <-ctx.Done():
		return metadata.IdempotencyRecord{}, ctx.Err()
	}
	return s.fakeIntentStore.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

type signalingPlanner struct {
	plan    *WritePlan
	started chan struct{}
}

func (p signalingPlanner) PlanWrite(_ context.Context, _ string, _, _ uint64, _, _ uint32) (*WritePlan, error) {
	signalStarted(p.started)
	return p.plan, nil
}

func signalStarted(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

type sequencePlanner struct {
	plans []*WritePlan
	err   error
	index int
}

func (p *sequencePlanner) PlanWrite(_ context.Context, _ string, _, _ uint64, _, _ uint32) (*WritePlan, error) {
	if p.err != nil {
		return nil, p.err
	}
	if len(p.plans) == 0 {
		return nil, nil
	}
	if p.index >= len(p.plans) {
		return p.plans[len(p.plans)-1], nil
	}
	plan := p.plans[p.index]
	p.index++
	return plan, nil
}

type trackingEffectsApplier struct {
	mu        sync.Mutex
	active    int
	maxActive int
	order     []uint64
	delay     time.Duration
	done      chan struct{}
}

func (a *trackingEffectsApplier) ApplyCommittedWriteEffects(_ context.Context, req metadata.ApplyCommittedWriteEffectsRequest) error {
	a.mu.Lock()
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	a.order = append(a.order, req.CommittedRevision)
	a.mu.Unlock()

	if a.delay > 0 {
		time.Sleep(a.delay)
	}

	a.mu.Lock()
	a.active--
	a.mu.Unlock()
	if a.done != nil {
		a.done <- struct{}{}
	}
	return nil
}

func (a *trackingEffectsApplier) snapshot() (int, []uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxActive, append([]uint64(nil), a.order...)
}

type flakyEffectsApplier struct {
	failures int
	calls    int
}

func (a *flakyEffectsApplier) ApplyCommittedWriteEffects(_ context.Context, _ metadata.ApplyCommittedWriteEffectsRequest) error {
	a.calls++
	if a.calls <= a.failures {
		return metadata.ErrCASConflict
	}
	return nil
}

func TestDeferredWriteEffectsQueueSerializesByVolume(t *testing.T) {
	const requestCount = 16
	applier := &trackingEffectsApplier{
		delay: 2 * time.Millisecond,
		done:  make(chan struct{}, requestCount),
	}
	queue := newDeferredWriteEffectsQueue(applier)
	for i := 0; i < requestCount; i++ {
		queue.Enqueue(context.Background(), metadata.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: uint64(i + 1),
		})
	}
	for i := 0; i < requestCount; i++ {
		select {
		case <-applier.done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for deferred effects queue to drain")
		}
	}
	maxActive, order := applier.snapshot()
	if maxActive != 1 {
		t.Fatalf("max concurrent effects=%d want=1", maxActive)
	}
	if len(order) != requestCount {
		t.Fatalf("applied effects=%d want=%d", len(order), requestCount)
	}
	for i, revision := range order {
		if revision != uint64(i+1) {
			t.Fatalf("order[%d]=%d want=%d", i, revision, i+1)
		}
	}
}

func TestApplyCommittedWriteEffectsRetriesDeferredCASConflict(t *testing.T) {
	applier := &flakyEffectsApplier{failures: 2}
	err := applyCommittedWriteEffects(context.Background(), applier, metadata.ApplyCommittedWriteEffectsRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 12,
	}, true)
	if err != nil {
		t.Fatalf("applyCommittedWriteEffects: %v", err)
	}
	if applier.calls != 3 {
		t.Fatalf("calls=%d want=3", applier.calls)
	}
}

func TestApplyCommittedWriteEffectsDoesNotRetrySynchronousCASConflict(t *testing.T) {
	applier := &flakyEffectsApplier{failures: 2}
	err := applyCommittedWriteEffects(context.Background(), applier, metadata.ApplyCommittedWriteEffectsRequest{
		VolumeID:          "00a1b2c3",
		CommittedRevision: 12,
	}, false)
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("error=%v want=%v", err, metadata.ErrCASConflict)
	}
	if applier.calls != 1 {
		t.Fatalf("calls=%d want=1", applier.calls)
	}
}

func TestExecutorBeginWriteStoresPendingIntent(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents: []ExtentWritePlan{
				{
					Extent:       zeroExtent(1),
					PlacementRef: "pl-1",
					ReplicaSetID: "rs-1",
					Primary:      ReplicaTarget{ReplicaID: "rep-a"},
					WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
					RequiredAcks: 2,
				},
			},
		},
	})

	result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if result.Execution == nil {
		t.Fatal("BeginWrite execution=nil")
	}
	if result.Execution.State != WriteStateIntentPending {
		t.Fatalf("execution state=%q want=%q", result.Execution.State, WriteStateIntentPending)
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyPending {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyPending)
	}
	if record.Revision != store.volumeState.Revision {
		t.Fatalf("record revision=%d want=%d", record.Revision, store.volumeState.Revision)
	}
	op := store.mutationOps[writeMutationOperationID("idem-1")]
	if op.State != metadata.MutationOperationRunning || op.IdempotencyKey != "idem-1" {
		t.Fatalf("mutation operation=%+v", op)
	}
	if store.intentPutCalls != 1 {
		t.Fatalf("intent put calls=%d want 1", store.intentPutCalls)
	}
	if result.Stats.PutIdempotencyDuration != 0 || result.Stats.PutMutationDuration != 0 {
		t.Fatalf("separate intent durations should stay zero with combined writer: %+v", result.Stats)
	}
}

func TestExecutorBeginWriteAppendOnlyIntentlessCommitSkipsPreIntentBoundary(t *testing.T) {
	store := newFakeIntentStore()
	plan := &WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      1,
					LogicalOffset: 0,
					LengthBytes:   4096,
					Revision:      11,
				},
				PlacementRef:   "pl-1",
				ReplicaSetID:   "rs-1",
				Primary:        ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets:   []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks:   2,
				ChunkSizeBytes: 4096,
				AllocationPages: []metadata.ResolvedAllocationPage{{
					Page: metadata.AllocationPageRecord{
						VolumeID:       "00a1b2c3",
						PageNo:         0,
						PageBytes:      4096,
						ChunkSizeBytes: 4096,
						Revision:       11,
						Extents: []metadata.AllocationExtentRecord{
							{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
						},
					},
					RangeStartChunk: 0,
					RangeEndChunk:   1,
					CoversWholePage: true,
				}},
			},
		},
	}
	executor := NewExecutor(store, fakePlanner{plan: plan}).
		WithAppendOnlyServiceWriteEffects(true).
		WithParallelBeginPlan(true).
		WithAppendOnlyMissingWriteIntent(true)

	result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-intentless",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-intentless",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if result.Execution == nil || !result.Execution.AllowMissingWriteIntent {
		t.Fatalf("execution=%+v want missing-intent execution", result.Execution)
	}
	if store.idempotencyGets != 0 || store.intentPutCalls != 0 {
		t.Fatalf("pre-intent calls idempotency_get=%d put_intent=%d, want 0/0", store.idempotencyGets, store.intentPutCalls)
	}
	if result.Stats.GetIdempotencyDuration != 0 || result.Stats.PutIntentDuration != 0 {
		t.Fatalf("pre-intent durations should be zero: %+v", result.Stats)
	}

	exec := result.Execution
	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(rep-a): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(rep-b): %v", err)
	}
	allocationPages, retiredPhysicalChunkIDs, ranges, err := executor.PrepareAllocationCommit(exec, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-intentless",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-intentless",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
	})
	if err != nil {
		t.Fatalf("PrepareAllocationCommit: %v", err)
	}
	revision, err := executor.CommitMetadata(context.Background(), exec, allocationPages, retiredPhysicalChunkIDs, ranges)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 1012 {
		t.Fatalf("revision=%d want=1012", revision)
	}
	if store.appendOnlyCalls != 1 || !store.appendOnlyReq.AllowMissingWriteIntent {
		t.Fatalf("append-only calls=%d req=%+v, want missing-intent append-only commit", store.appendOnlyCalls, store.appendOnlyReq)
	}
	if store.appendOnlyReq.AttachmentID != "att-1" || store.appendOnlyReq.Generation != 9 {
		t.Fatalf("append-only request attachment/generation=%q/%d", store.appendOnlyReq.AttachmentID, store.appendOnlyReq.Generation)
	}
	record := store.records["idem-intentless"]
	if record.ResultState != metadata.IdempotencyCommitted || record.AttachmentID != "att-1" || record.Generation != 9 {
		t.Fatalf("record=%+v want committed synthesized idempotency", record)
	}
}

func TestExecutorBeginWriteParallelBeginPlanOverlapsPlanWithIntentReads(t *testing.T) {
	base := newFakeIntentStore()
	release := make(chan struct{})
	store := &blockingBeginIntentStore{
		fakeIntentStore:    base,
		release:            release,
		volumeStarted:      make(chan struct{}, 1),
		idempotencyStarted: make(chan struct{}, 1),
	}
	planStarted := make(chan struct{}, 1)
	executor := NewExecutor(store, signalingPlanner{
		started: planStarted,
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents:  []ExtentWritePlan{{Extent: zeroExtent(1)}},
		},
	}).WithParallelBeginPlan(true)

	done := make(chan struct {
		result *BeginWriteResult
		err    error
	}, 1)
	go func() {
		result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
			VolumeID:       "00a1b2c3",
			RequestID:      "req-1",
			AttachmentID:   "att-1",
			Generation:     9,
			IdempotencyKey: "idem-parallel-begin",
			OffsetBytes:    0,
			LengthBytes:    4096,
		})
		done <- struct {
			result *BeginWriteResult
			err    error
		}{result: result, err: err}
	}()

	for _, ch := range []chan struct{}{store.volumeStarted, store.idempotencyStarted, planStarted} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("parallel begin did not start volume/idempotency/plan work concurrently")
		}
	}
	close(release)

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("BeginWrite: %v", got.err)
		}
		if got.result == nil || got.result.Execution == nil {
			t.Fatalf("result=%+v, want execution", got.result)
		}
	case <-time.After(time.Second):
		t.Fatal("BeginWrite did not complete")
	}
	if base.intentPutCalls != 1 {
		t.Fatalf("intent put calls=%d want 1", base.intentPutCalls)
	}
}

func TestExecutorBeginWriteReplaysCommittedIntent(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
		Revision:       15,
	}
	executor := NewExecutor(store, fakePlanner{})

	result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if result.Replay == nil || result.Replay.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("replay=%+v", result.Replay)
	}
}

func TestExecutorBeginWriteBlocksCommittedIntentWithRunningMutation(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
		Revision:       15,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 15,
		IdempotencyKey:     "idem-1",
	}
	executor := NewExecutor(store, fakePlanner{})

	_, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if !errors.Is(err, ErrIntentPending) {
		t.Fatalf("error=%v want=%v", err, ErrIntentPending)
	}
}

func TestExecutorBeginWriteReplaysCommittedIntentWithCommittedMutation(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
		Revision:       15,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationCommitted,
		AllocationRevision: 15,
		IdempotencyKey:     "idem-1",
	}
	executor := NewExecutor(store, fakePlanner{})

	result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if result.Replay == nil || result.Replay.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("replay=%+v", result.Replay)
	}
}

func TestExecutorBeginWriteAdoptsMatchingPendingIntent(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	executor := NewExecutor(store, fakePlanner{
		plan: &WritePlan{
			VolumeID: "00a1b2c3",
			Extents:  []ExtentWritePlan{{Extent: zeroExtent(1)}},
		},
	})

	result, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if result.Execution == nil || result.Execution.IdempotencyKey != "idem-1" {
		t.Fatalf("execution=%+v", result.Execution)
	}
}

func TestExecutorCommitMetadataTransitionsIntentToCommitted(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{})
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-1",
	}
	legacyExtent := metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}
	store.extentMappings[1] = legacyExtent
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       legacyExtent,
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
				AllocationPages: []metadata.ResolvedAllocationPage{
					{
						Page: metadata.AllocationPageRecord{
							VolumeID:       "00a1b2c3",
							PageNo:         0,
							PageBytes:      4096,
							ChunkSizeBytes: 4096,
							Revision:       11,
							Extents: []metadata.AllocationExtentRecord{
								{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
							},
						},
						RangeStartChunk: 0,
						RangeEndChunk:   1,
						CoversWholePage: true,
					},
				},
			},
		},
	}, "req-1", "att-1", 9, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(primary): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(secondary): %v", err)
	}

	revision, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 12 {
		t.Fatalf("revision=%d want=12", revision)
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyCommitted {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyCommitted)
	}
	if record.Revision != 12 {
		t.Fatalf("record revision=%d want=12", record.Revision)
	}
	op := store.mutationOps[writeMutationOperationID("idem-1")]
	if op.State != metadata.MutationOperationCommitted || op.AllocationRevision != 12 {
		t.Fatalf("mutation operation=%+v", op)
	}
	mapping := store.extentMappings[1]
	if mapping.ChunkID != 0 || mapping.Revision != 12 {
		t.Fatalf("extent mapping=%+v", mapping)
	}
}

func TestExecutorCommitMetadataPrefersSplitCommitCapabilities(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutorWithStores(store, fakePlanner{}, store, store, store, store)
	store.records["idem-split"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-split",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-split")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-split"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-split",
	}
	store.extentMappings[1] = metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
				AllocationPages: []metadata.ResolvedAllocationPage{
					{
						Page: metadata.AllocationPageRecord{
							VolumeID:       "00a1b2c3",
							PageNo:         0,
							PageBytes:      4096,
							ChunkSizeBytes: 4096,
						},
						RangeStartChunk: 0,
						RangeEndChunk:   1,
						CoversWholePage: true,
					},
				},
			},
		},
	}, "req-split", "att-1", 9, "idem-split", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(primary): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(secondary): %v", err)
	}

	if _, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil); err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if store.commitCalls != 0 {
		t.Fatalf("combined commit calls=%d want=0", store.commitCalls)
	}
	if store.stateCommitCalls != 1 {
		t.Fatalf("state commit calls=%d want=1", store.stateCommitCalls)
	}
	if store.effectsApplyCalls != 1 {
		t.Fatalf("effects apply calls=%d want=1", store.effectsApplyCalls)
	}
}

func TestExecutorCommitCloneDeltaMetadataCommitsDeltaPagesOnly(t *testing.T) {
	store := newFakeIntentStore()
	cloneCommitter := &fakeCloneDeltaCommitter{}
	executor := NewExecutor(store, fakePlanner{}).WithCloneDeltaMetadataCommitter(cloneCommitter)
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-clone", "att-1", 9, "idem-clone", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(primary): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(secondary): %v", err)
	}
	pages := []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 20},
		},
	}}

	if err := executor.CommitCloneDeltaMetadata(context.Background(), exec, "clone-1", pages); err != nil {
		t.Fatalf("CommitCloneDeltaMetadata: %v", err)
	}
	if exec.State != WriteStateMetadataCommitted {
		t.Fatalf("execution state=%q want %q", exec.State, WriteStateMetadataCommitted)
	}
	if cloneCommitter.cloneID != "clone-1" || len(cloneCommitter.pages) != 1 || cloneCommitter.pages[0].Extents[0].PhysicalChunkStart != 20 {
		t.Fatalf("clone delta commit=%q %+v", cloneCommitter.cloneID, cloneCommitter.pages)
	}
	if store.commitCalls != 0 || store.stateCommitCalls != 0 || store.effectsApplyCalls != 0 {
		t.Fatalf("volume commit path should not run: commit=%d state=%d effects=%d", store.commitCalls, store.stateCommitCalls, store.effectsApplyCalls)
	}
}

func TestNewExecutorInfersCloneDeltaCommitterFromWriteStore(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{})
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-clone", "att-1", 9, "idem-clone", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(primary): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(secondary): %v", err)
	}
	pages := []metadata.AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       12,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 21},
		},
	}}

	if err := executor.CommitCloneDeltaMetadata(context.Background(), exec, "clone-inferred", pages); err != nil {
		t.Fatalf("CommitCloneDeltaMetadata: %v", err)
	}
	if store.cloneID != "clone-inferred" || len(store.clonePages) != 1 || store.clonePages[0].Extents[0].PhysicalChunkStart != 21 {
		t.Fatalf("inferred clone delta commit=%q %+v", store.cloneID, store.clonePages)
	}
}

func TestExecutorCommitMetadataCanDeferSplitWriteEffects(t *testing.T) {
	store := newFakeIntentStore()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	completed := make(chan struct{}, 1)
	store.effectsStarted = started
	store.effectsRelease = release
	store.effectsCompleted = completed
	executor := NewExecutorWithStores(store, fakePlanner{}, store, store, store, store).
		WithAsyncWriteEffects(true)
	store.records["idem-async-effects"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-async-effects",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-async-effects")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-async-effects"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-async-effects",
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-async-effects", "att-1", 9, "idem-async-effects", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 12 {
		t.Fatalf("revision=%d want=12", revision)
	}
	if executor.metadataCommitMode(nil) != "volume_scoped_async_effects" {
		t.Fatalf("metadata commit mode=%q want volume_scoped_async_effects", executor.metadataCommitMode(nil))
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deferred effects apply to start")
	}
	if op := store.mutationOps[writeMutationOperationID("idem-async-effects")]; op.State != metadata.MutationOperationRunning {
		t.Fatalf("mutation operation state before release=%q want running", op.State)
	}
	close(release)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deferred effects apply to complete")
	}
	if op := store.mutationOps[writeMutationOperationID("idem-async-effects")]; op.State != metadata.MutationOperationCommitted {
		t.Fatalf("mutation operation state after release=%q want committed", op.State)
	}
}

func TestExecutorCommitMetadataUsesExecutionRevisionWithoutExtraVolumeStateRead(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{})
	store.records["idem-no-extra-state-read"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-no-extra-state-read",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-no-extra-state-read")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-no-extra-state-read"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-no-extra-state-read",
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-no-extra-state-read", "att-1", 9, "idem-no-extra-state-read", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	volumeStateGetsBefore := store.volumeStateGets
	revision, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 12 {
		t.Fatalf("revision=%d want=12", revision)
	}
	if store.volumeStateGets != volumeStateGetsBefore {
		t.Fatalf("volume state gets=%d want=%d", store.volumeStateGets, volumeStateGetsBefore)
	}
}

func TestExecutorCommitMetadataUsesVolumeScopedCommitterForAllocationPages(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{})
	store.records["idem-volume-scoped"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-volume-scoped",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-volume-scoped")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-volume-scoped"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-volume-scoped",
	}
	store.allocationPages[0] = metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       4,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-volume-scoped", "att-1", 9, "idem-volume-scoped", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Revision:       4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
			},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 12 {
		t.Fatalf("revision=%d want=12", revision)
	}
	if store.pageScopedCalls != 0 || store.commitCalls != 1 || store.stateCommitCalls != 1 {
		t.Fatalf("commit calls page_scoped=%d combined=%d state=%d", store.pageScopedCalls, store.commitCalls, store.stateCommitCalls)
	}
	if store.effectsApplyCalls != 1 {
		t.Fatalf("effects apply calls=%d want=1", store.effectsApplyCalls)
	}
	if store.volumeState.Revision != 12 {
		t.Fatalf("volume revision=%d want=12", store.volumeState.Revision)
	}
	if got := store.allocationPages[0].Revision; got != 12 {
		t.Fatalf("page revision=%d want=12", got)
	}
}

func TestExecutorCommitMetadataCanPreferPageScopedCommitterForAllocationPages(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{}).WithPageScopedWriteMetadata(true)
	store.records["idem-page-scoped"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-page-scoped",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-page-scoped")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-page-scoped"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-page-scoped",
	}
	store.allocationPages[0] = metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       4,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-page-scoped", "att-1", 9, "idem-page-scoped", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Revision:       4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
			},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 5 {
		t.Fatalf("revision=%d want=5", revision)
	}
	if store.pageScopedCalls != 1 || store.commitCalls != 0 || store.stateCommitCalls != 0 {
		t.Fatalf("commit calls page_scoped=%d combined=%d state=%d", store.pageScopedCalls, store.commitCalls, store.stateCommitCalls)
	}
	if store.volumeState.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", store.volumeState.Revision)
	}
	if got := store.allocationPages[0].Revision; got != 5 {
		t.Fatalf("page revision=%d want=5", got)
	}
}

func TestExecutorCommitMetadataCanPreferRangeLocalWriteStateWithAsyncEffects(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{}).
		WithRangeLocalWriteState(true).
		WithAsyncWriteEffects(true)
	store.records["idem-range-local"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-range-local",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-range-local")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-range-local"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-range-local",
	}
	store.allocationPages[0] = metadata.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       4,
		Extents: []metadata.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
		},
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-range-local", "att-1", 9, "idem-range-local", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Revision:       4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
			},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 5 {
		t.Fatalf("revision=%d want=5", revision)
	}
	if store.rangeLocalCalls != 1 || store.pageScopedCalls != 0 || store.commitCalls != 0 || store.stateCommitCalls != 0 {
		t.Fatalf("commit calls range_local=%d page_scoped=%d combined=%d state=%d", store.rangeLocalCalls, store.pageScopedCalls, store.commitCalls, store.stateCommitCalls)
	}
	if store.volumeState.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", store.volumeState.Revision)
	}
	if got := store.records["idem-range-local"].Revision; got != 5 {
		t.Fatalf("idempotency revision=%d want=5", got)
	}
}

func TestExecutorCommitMetadataCanPreferAppendOnlyServiceWriteEffects(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{}).
		WithAppendOnlyServiceWriteEffects(true)
	store.records["idem-append-service"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-append-service",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-append-service")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-append-service"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-append-service",
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-append-service", "att-1", 9, "idem-append-service", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
			Revision:       4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
			},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 1012 {
		t.Fatalf("revision=%d want=1012", revision)
	}
	if store.appendOnlyCalls != 1 || store.rangeLocalCalls != 0 || store.commitCalls != 0 || store.stateCommitCalls != 0 {
		t.Fatalf("commit calls append_only=%d range_local=%d combined=%d state=%d", store.appendOnlyCalls, store.rangeLocalCalls, store.commitCalls, store.stateCommitCalls)
	}
	if store.volumeState.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", store.volumeState.Revision)
	}
}

func TestExecutorCommitMetadataCarriesAffectedPageChunkRangesAndSkipsNormalizedExtentNormalize(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{}).
		WithAppendOnlyServiceWriteEffects(true)
	store.records["idem-append-range"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-append-range",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-append-range")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-append-range"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-append-range",
	}
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent: metadata.ExtentMappingRecord{
					VolumeID:      "00a1b2c3",
					ExtentID:      99,
					LogicalOffset: 0,
					LengthBytes:   4096,
					ChunkID:       0,
					Revision:      11,
				},
				PlacementRef:   "pl-1",
				ReplicaSetID:   "rs-1",
				Primary:        ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets:   []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks:   2,
				ChunkSizeBytes: 1024,
				AllocationPages: []metadata.ResolvedAllocationPage{{
					Page: metadata.AllocationPageRecord{
						VolumeID:       "00a1b2c3",
						PageNo:         0,
						PageBytes:      4096,
						ChunkSizeBytes: 1024,
						Revision:       11,
						Extents: []metadata.AllocationExtentRecord{
							{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
							{LogicalChunkStart: 1, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 201},
							{LogicalChunkStart: 3, ChunkCount: 1, Kind: metadata.AllocationKindZero},
						},
					},
					RangeStartChunk: 0,
					RangeEndChunk:   4,
					CoversWholePage: true,
				}},
			},
		},
	}, "req-append-range", "att-1", 9, "idem-append-range", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	allocationPages, retiredPhysicalChunkIDs, ranges, err := executor.PrepareAllocationCommit(exec, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-append-range",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-append-range",
		OffsetBytes:    1024,
		LengthBytes:    2048,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	})
	if err != nil {
		t.Fatalf("PrepareAllocationCommit: %v", err)
	}
	wantRange := metadata.AllocationPageChunkRangeRecord{PageNo: 0, StartChunk: 1, EndChunk: 3}
	if len(ranges) != 1 || ranges[0] != wantRange {
		t.Fatalf("ranges=%+v want [%+v]", ranges, wantRange)
	}
	revision, err := executor.CommitMetadata(context.Background(), exec, allocationPages, retiredPhysicalChunkIDs, ranges)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 1012 {
		t.Fatalf("revision=%d want=1012", revision)
	}
	if len(store.appendOnlyReq.NormalizeExtentMappings) != 0 {
		t.Fatalf("normalize extents=%v want empty for already-normalized extent", store.appendOnlyReq.NormalizeExtentMappings)
	}
	if len(store.appendOnlyReq.AffectedPageChunkRanges) != 1 || store.appendOnlyReq.AffectedPageChunkRanges[0] != wantRange {
		t.Fatalf("append-only ranges=%+v want [%+v]", store.appendOnlyReq.AffectedPageChunkRanges, wantRange)
	}
	if len(store.appendOnlyReq.AffectedExtentIDs) != 1 || store.appendOnlyReq.AffectedExtentIDs[0] != 99 {
		t.Fatalf("affected extents=%v want [99]", store.appendOnlyReq.AffectedExtentIDs)
	}
}

func TestExecutorMarkFailedTransitionsIntentToFailed(t *testing.T) {
	store := newFakeIntentStore()
	executor := NewExecutor(store, fakePlanner{})
	exec := NewWriteExecution(&WritePlan{VolumeID: "00a1b2c3"}, "req-1", "att-1", 9, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()

	if err := executor.MarkFailed(context.Background(), exec, errors.New("quorum unavailable")); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	record := store.records["idem-1"]
	if record.ResultState != metadata.IdempotencyFailed {
		t.Fatalf("record state=%q want=%q", record.ResultState, metadata.IdempotencyFailed)
	}
	if exec.State != WriteStateFailed {
		t.Fatalf("execution state=%q want=%q", exec.State, WriteStateFailed)
	}
	op := store.mutationOps[writeMutationOperationID("idem-1")]
	if op.State != metadata.MutationOperationFailed || op.ErrorMessage != "quorum unavailable" {
		t.Fatalf("mutation operation=%+v", op)
	}
}

func TestExecutorBeginWriteRejectsStaleWriterContextOnCommittedReplay(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-good",
		Generation:     9,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
		Revision:       15,
	}
	executor := NewExecutor(store, fakePlanner{})

	_, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-stale",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4096,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error=%v want=%v", err, ErrIdempotencyConflict)
	}
}

func TestExecutorCommitMetadataFailsOnCASConflict(t *testing.T) {
	store := newFakeIntentStore()
	store.forceCAS = true
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-1",
	}
	executor := NewExecutor(store, fakePlanner{})
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-1", "att-1", 9, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	_, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("error=%v want=%v", err, metadata.ErrCASConflict)
	}
	if exec.State != WriteStatePayloadQuorumDone {
		t.Fatalf("execution state=%q want=%q", exec.State, WriteStatePayloadQuorumDone)
	}
}

func TestExecutorCommitMetadataCASUsesPlannedRevision(t *testing.T) {
	store := newFakeIntentStore()
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       11,
		Operation:      "write",
		ResultState:    metadata.IdempotencyPending,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-1",
	}
	store.volumeState.Revision = 12
	executor := NewExecutor(store, fakePlanner{})
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-1", "att-1", 9, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	_, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if !errors.Is(err, metadata.ErrCASConflict) {
		t.Fatalf("error=%v want=%v", err, metadata.ErrCASConflict)
	}
	if store.records["idem-1"].ResultState != metadata.IdempotencyPending {
		t.Fatalf("record state=%q want=%q", store.records["idem-1"].ResultState, metadata.IdempotencyPending)
	}
}

func TestExecutorCommitMetadataRecoversCommittedIntentAfterCAS(t *testing.T) {
	store := newFakeIntentStore()
	store.forceCAS = true
	store.records["idem-1"] = metadata.IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          3,
		Revision:       12,
		Operation:      "write",
		ResultState:    metadata.IdempotencyCommitted,
	}
	store.mutationOps[writeMutationOperationID("idem-1")] = metadata.MutationOperationRecord{
		OperationID:        writeMutationOperationID("idem-1"),
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              metadata.MutationOperationCommitted,
		AllocationRevision: 12,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-1",
	}
	executor := NewExecutor(store, fakePlanner{})
	exec := NewWriteExecution(&WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
				RequiredAcks: 2,
			},
		},
	}, "req-1", "att-1", 9, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	_ = exec.MarkReplicaAck(0, "rep-a")
	_ = exec.MarkReplicaAck(0, "rep-b")

	revision, err := executor.CommitMetadata(context.Background(), exec, nil, nil, nil)
	if err != nil {
		t.Fatalf("CommitMetadata: %v", err)
	}
	if revision != 12 {
		t.Fatalf("revision=%d want=12", revision)
	}
	if exec.State != WriteStateMetadataCommitted {
		t.Fatalf("execution state=%q want=%q", exec.State, WriteStateMetadataCommitted)
	}
}

func TestExecutorRebaseWritePlanForRetryPreservesConcurrentChunkMappings(t *testing.T) {
	store := newFakeIntentStore()
	planner := &sequencePlanner{
		plans: []*WritePlan{
			{
				VolumeID: "00a1b2c3",
				Extents: []ExtentWritePlan{
					{
						Extent: metadata.ExtentMappingRecord{
							VolumeID:      "00a1b2c3",
							ExtentID:      1,
							LogicalOffset: 0,
							LengthBytes:   8,
							ChunkID:       0,
							PlacementRef:  "pl-1",
							Revision:      11,
						},
						PlacementRef:   "pl-1",
						ReplicaSetID:   "rs-1",
						Primary:        ReplicaTarget{ReplicaID: "rep-a"},
						WriteTargets:   []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
						RequiredAcks:   2,
						ChunkSizeBytes: 4,
						AllocationPages: []metadata.ResolvedAllocationPage{
							{
								Page: metadata.AllocationPageRecord{
									VolumeID:       "00a1b2c3",
									PageNo:         0,
									PageBytes:      8,
									ChunkSizeBytes: 4,
									Revision:       11,
									Extents: []metadata.AllocationExtentRecord{
										{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
									},
								},
								RangeStartChunk: 0,
								RangeEndChunk:   2,
								CoversWholePage: true,
							},
						},
					},
				},
			},
			{
				VolumeID: "00a1b2c3",
				Extents: []ExtentWritePlan{
					{
						Extent: metadata.ExtentMappingRecord{
							VolumeID:      "00a1b2c3",
							ExtentID:      1,
							LogicalOffset: 0,
							LengthBytes:   8,
							ChunkID:       0,
							PlacementRef:  "pl-1",
							Revision:      12,
						},
						PlacementRef:   "pl-1",
						ReplicaSetID:   "rs-1",
						Primary:        ReplicaTarget{ReplicaID: "rep-a"},
						WriteTargets:   []ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}},
						RequiredAcks:   2,
						ChunkSizeBytes: 4,
						AllocationPages: []metadata.ResolvedAllocationPage{
							{
								Page: metadata.AllocationPageRecord{
									VolumeID:       "00a1b2c3",
									PageNo:         0,
									PageBytes:      8,
									ChunkSizeBytes: 4,
									Revision:       12,
									Extents: []metadata.AllocationExtentRecord{
										{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindZero},
										{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 22},
									},
								},
								RangeStartChunk: 0,
								RangeEndChunk:   2,
								CoversWholePage: true,
							},
						},
					},
				},
			},
		},
	}
	executor := NewExecutor(store, planner)

	begin, err := executor.BeginWrite(context.Background(), BeginWriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4,
		PageBytes:      8,
		ChunkSizeBytes: 4,
	})
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if begin.Execution == nil {
		t.Fatal("BeginWrite execution=nil")
	}

	beforePages, _, _, err := executor.PrepareAllocationCommit(begin.Execution, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Data:           []byte("ABCD"),
	})
	if err != nil {
		t.Fatalf("PrepareAllocationCommit before retry: %v", err)
	}
	if got := beforePages[0].Extents; len(got) != 2 || got[0].Kind != metadata.AllocationKindData || got[1].Kind != metadata.AllocationKindZero {
		t.Fatalf("unexpected initial allocation page extents: %+v", got)
	}

	if err := executor.rebaseWritePlanForRetry(context.Background(), begin.Execution, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Data:           []byte("ABCD"),
	}); err != nil {
		t.Fatalf("rebaseWritePlanForRetry: %v", err)
	}

	afterPages, _, _, err := executor.PrepareAllocationCommit(begin.Execution, WriteRequest{
		VolumeID:       "00a1b2c3",
		RequestID:      "req-1",
		AttachmentID:   "att-1",
		Generation:     9,
		IdempotencyKey: "idem-1",
		OffsetBytes:    0,
		LengthBytes:    4,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Data:           []byte("ABCD"),
	})
	if err != nil {
		t.Fatalf("PrepareAllocationCommit after retry rebase: %v", err)
	}
	got := afterPages[0].Extents
	if len(got) != 2 {
		t.Fatalf("after rebase extents=%+v", got)
	}
	if got[0].Kind != metadata.AllocationKindData || got[0].LogicalChunkStart != 0 || got[0].PhysicalChunkStart == 0 {
		t.Fatalf("unexpected first extent after rebase: %+v", got[0])
	}
	if got[1].Kind != metadata.AllocationKindData || got[1].LogicalChunkStart != 1 || got[1].PhysicalChunkStart != 22 {
		t.Fatalf("unexpected preserved concurrent chunk mapping after rebase: %+v", got[1])
	}
}
