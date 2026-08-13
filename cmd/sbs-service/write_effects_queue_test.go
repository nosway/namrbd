package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type blockingServiceWriteEffectsApplier struct {
	started chan struct{}
	release chan struct{}
	err     error
}

func (a *blockingServiceWriteEffectsApplier) ApplyCommittedWriteEffects(context.Context, clustermeta.ApplyCommittedWriteEffectsRequest) error {
	close(a.started)
	<-a.release
	return a.err
}

func TestServiceWriteEffectsQueueEnqueueAndWaitBlocksUntilApplied(t *testing.T) {
	applier := &blockingServiceWriteEffectsApplier{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier)
	done := make(chan struct {
		stats clustercontrol.WriteSessionEffectsQueueStats
		err   error
	}, 1)
	go func() {
		stats, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{3},
		})
		done <- struct {
			stats clustercontrol.WriteSessionEffectsQueueStats
			err   error
		}{stats: stats, err: err}
	}()

	select {
	case <-applier.started:
	case <-time.After(time.Second):
		t.Fatal("effects apply did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("EnqueueAndWait returned before apply completed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(applier.release)
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("EnqueueAndWait returned error: %v", result.err)
		}
		if result.stats.LaneKey != "volume:00a1b2c3:page:3" {
			t.Fatalf("lane=%q want page lane", result.stats.LaneKey)
		}
		if result.stats.BatchSize != 1 {
			t.Fatalf("batch size=%d want 1", result.stats.BatchSize)
		}
		if result.stats.ApplyDuration <= 0 {
			t.Fatalf("apply duration=%v want >0", result.stats.ApplyDuration)
		}
	case <-time.After(time.Second):
		t.Fatal("EnqueueAndWait did not return after apply completed")
	}
}

func TestServiceWriteEffectsQueueEnqueueAndWaitReturnsApplyError(t *testing.T) {
	wantErr := errors.New("apply failed")
	applier := &blockingServiceWriteEffectsApplier{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     wantErr,
	}
	queue := newServiceWriteEffectsQueue(applier)
	done := make(chan error, 1)
	go func() {
		_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
		})
		done <- err
	}()

	<-applier.started
	close(applier.release)
	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("EnqueueAndWait error=%v want=%v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("EnqueueAndWait did not return after apply failed")
	}
}

type gatedServiceWriteEffectsApplier struct {
	entered chan clustermeta.ApplyCommittedWriteEffectsRequest
	release chan struct{}
}

func (a *gatedServiceWriteEffectsApplier) ApplyCommittedWriteEffects(_ context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) error {
	a.entered <- req
	<-a.release
	return nil
}

func TestServiceWriteEffectsQueueRunsDifferentPagesConcurrently(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier)
	var wg sync.WaitGroup
	for _, pageNo := range []uint64{1, 2} {
		wg.Add(1)
		go func(pageNo uint64) {
			defer wg.Done()
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 7 + pageNo,
				AffectedPageNos:   []uint64{pageNo},
			})
			if err != nil {
				t.Errorf("EnqueueAndWait page=%d: %v", pageNo, err)
			}
		}(pageNo)
	}

	waitEntered := func() {
		t.Helper()
		select {
		case <-applier.entered:
		case <-time.After(time.Second):
			t.Fatal("effects apply did not start")
		}
	}
	waitEntered()
	waitEntered()
	close(applier.release)
	wg.Wait()
}

func TestServiceWriteEffectsQueueSerializesSamePage(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(revision uint64) {
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: revision,
				AffectedPageNos:   []uint64{1},
			})
			done <- err
		}(uint64(7 + i))
	}

	select {
	case <-applier.entered:
	case <-time.After(time.Second):
		t.Fatal("first effects apply did not start")
	}
	select {
	case <-applier.entered:
		t.Fatal("same-page effects apply started concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	close(applier.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("EnqueueAndWait: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("EnqueueAndWait did not return")
		}
	}
}

func TestServiceWriteEffectsQueueSerializesSameNormalizeExtent(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier)
	done := make(chan error, 2)
	for _, pageNo := range []uint64{1, 2} {
		go func(pageNo uint64) {
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:                "00a1b2c3",
				CommittedRevision:       7 + pageNo,
				AffectedPageNos:         []uint64{pageNo},
				NormalizeExtentMappings: []uint64{1},
			})
			done <- err
		}(pageNo)
	}

	select {
	case <-applier.entered:
	case <-time.After(time.Second):
		t.Fatal("first effects apply did not start")
	}
	select {
	case <-applier.entered:
		t.Fatal("same-normalize-extent effects apply started concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	close(applier.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("EnqueueAndWait: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("EnqueueAndWait did not return")
		}
	}
}

type recordingBatchServiceWriteEffectsApplier struct {
	mu          sync.Mutex
	batches     [][]clustermeta.ApplyCommittedWriteEffectsRequest
	singles     []clustermeta.ApplyCommittedWriteEffectsRequest
	batchErr    error
	applyErr    error
	batchErrSeq []error
}

func (a *recordingBatchServiceWriteEffectsApplier) ApplyCommittedWriteEffects(_ context.Context, req clustermeta.ApplyCommittedWriteEffectsRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.singles = append(a.singles, req)
	return a.applyErr
}

func (a *recordingBatchServiceWriteEffectsApplier) ApplyCommittedWriteEffectsBatch(_ context.Context, reqs []clustermeta.ApplyCommittedWriteEffectsRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := append([]clustermeta.ApplyCommittedWriteEffectsRequest(nil), reqs...)
	a.batches = append(a.batches, copied)
	if len(a.batchErrSeq) > 0 {
		err := a.batchErrSeq[0]
		a.batchErrSeq = a.batchErrSeq[1:]
		return err
	}
	return a.batchErr
}

type recordingAppendOnlyCommitApplier struct {
	mu            sync.Mutex
	commitBatches [][]clustermeta.CommitWriteMetadataRequest
	commitSingles []clustermeta.CommitWriteMetadataRequest
	batchErr      error
	singleErr     error
}

func (a *recordingAppendOnlyCommitApplier) ApplyCommittedWriteEffects(context.Context, clustermeta.ApplyCommittedWriteEffectsRequest) error {
	return nil
}

func (a *recordingAppendOnlyCommitApplier) CommitAppendOnlyWriteMetadataBatch(_ context.Context, reqs []clustermeta.CommitWriteMetadataRequest) ([]clustermeta.VolumeState, []clustermeta.IdempotencyRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := append([]clustermeta.CommitWriteMetadataRequest(nil), reqs...)
	a.commitBatches = append(a.commitBatches, copied)
	if a.batchErr != nil {
		return nil, nil, a.batchErr
	}
	states := make([]clustermeta.VolumeState, len(reqs))
	records := make([]clustermeta.IdempotencyRecord, len(reqs))
	for i, req := range reqs {
		revision := uint64(90 + i)
		states[i] = clustermeta.VolumeState{VolumeID: req.VolumeID, Revision: req.ExpectedRevision}
		records[i] = clustermeta.IdempotencyRecord{VolumeID: req.VolumeID, IdempotencyKey: req.IdempotencyKey, Revision: revision, ResultState: clustermeta.IdempotencyCommitted}
	}
	return states, records, nil
}

func (a *recordingAppendOnlyCommitApplier) CommitAppendOnlyWriteStateAndQueueEffects(_ context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.commitSingles = append(a.commitSingles, req)
	if a.singleErr != nil {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, a.singleErr
	}
	return clustermeta.VolumeState{VolumeID: req.VolumeID, Revision: req.ExpectedRevision},
		clustermeta.IdempotencyRecord{VolumeID: req.VolumeID, IdempotencyKey: req.IdempotencyKey, Revision: req.CommittedRevision, ResultState: clustermeta.IdempotencyCommitted},
		nil
}

func TestServiceWriteEffectsQueueApplyItemsUsesBatchApplier(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{}
	queue := newServiceWriteEffectsQueue(applier)
	items := []serviceWriteEffectsRequest{
		{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 7,
				AffectedPageNos:   []uint64{1},
			},
		},
		{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 8,
				AffectedPageNos:   []uint64{1},
			},
		},
	}

	_, _, errs := queue.applyItems(context.Background(), items)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("applyItems returned error: %v", err)
		}
	}
	if len(applier.batches) != 1 {
		t.Fatalf("batch calls=%d want=1", len(applier.batches))
	}
	if len(applier.batches[0]) != 2 {
		t.Fatalf("batch size=%d want=2", len(applier.batches[0]))
	}
	if len(applier.singles) != 0 {
		t.Fatalf("single calls=%d want=0", len(applier.singles))
	}
}

func TestServiceWriteEffectsQueueApplyItemsUsesBatchApplierForSingleItem(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{}
	queue := newServiceWriteEffectsQueue(applier)
	items := []serviceWriteEffectsRequest{
		{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 7,
				AffectedPageNos:   []uint64{1},
			},
		},
	}

	_, _, errs := queue.applyItems(context.Background(), items)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("applyItems returned error: %v", err)
		}
	}
	if len(applier.batches) != 1 {
		t.Fatalf("batch calls=%d want=1", len(applier.batches))
	}
	if len(applier.batches[0]) != 1 {
		t.Fatalf("batch size=%d want=1", len(applier.batches[0]))
	}
	if len(applier.singles) != 0 {
		t.Fatalf("single calls=%d want=0", len(applier.singles))
	}
}

func TestServiceWriteEffectsQueueBatchMaxOptionLimitsDrain(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueBatchMax(2))
	worker := &serviceWriteEffectsWorker{
		requests: make(chan serviceWriteEffectsRequest, 3),
	}
	for revision := uint64(8); revision <= 10; revision++ {
		worker.requests <- serviceWriteEffectsRequest{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: revision,
				AffectedPageNos:   []uint64{1},
			},
		}
	}

	items := queue.drainBatch(worker, serviceWriteEffectsRequest{
		laneKey: "volume:00a1b2c3:page:1",
		req: clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{1},
		},
	})
	if len(items) != 2 {
		t.Fatalf("drained batch size=%d want 2", len(items))
	}
	if len(worker.requests) != 2 {
		t.Fatalf("remaining queued requests=%d want 2", len(worker.requests))
	}
}

func TestServiceWriteEffectsQueueApplyItemsFallsBackAfterBatchFailure(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{batchErr: errors.New("batch failed")}
	queue := newServiceWriteEffectsQueue(applier)
	items := []serviceWriteEffectsRequest{
		{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 7,
				AffectedPageNos:   []uint64{1},
			},
		},
		{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 8,
				AffectedPageNos:   []uint64{1},
			},
		},
	}

	_, _, errs := queue.applyItems(context.Background(), items)
	for _, err := range errs {
		if err != nil {
			t.Fatalf("fallback applyItems returned error: %v", err)
		}
	}
	if len(applier.batches) != 1 {
		t.Fatalf("batch calls=%d want=1", len(applier.batches))
	}
	if len(applier.singles) != 2 {
		t.Fatalf("single fallback calls=%d want=2", len(applier.singles))
	}
}

func TestServiceWriteEffectsQueueEnqueueAppendOnlyCommitUsesBatchCommitter(t *testing.T) {
	applier := &recordingAppendOnlyCommitApplier{}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueNativeAllocationFastPath(true))

	state, record, stats, err := queue.EnqueueAppendOnlyCommitAndWait(context.Background(), clustermeta.CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedRevision:         7,
		IdempotencyKey:           "idem-append-queue",
		ExpectedIdempotencyState: clustermeta.IdempotencyPending,
		CommittedRevision:        8,
		AffectedPageNos:          []uint64{3},
		AllocationPages: []clustermeta.AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         3,
			PageBytes:      4096,
			ChunkSizeBytes: 4096,
		}},
	})
	if err != nil {
		t.Fatalf("EnqueueAppendOnlyCommitAndWait: %v", err)
	}
	if state.VolumeID != "00a1b2c3" || record.IdempotencyKey != "idem-append-queue" || record.Revision == 0 {
		t.Fatalf("state=%+v record=%+v", state, record)
	}
	if stats.LaneKey != "volume:00a1b2c3:page:3" {
		t.Fatalf("lane=%q want page lane", stats.LaneKey)
	}
	if stats.BatchSize != 1 {
		t.Fatalf("batch size=%d want 1", stats.BatchSize)
	}
	if len(applier.commitBatches) != 1 || len(applier.commitBatches[0]) != 1 {
		t.Fatalf("commit batches=%+v want one batch with one request", applier.commitBatches)
	}
	if len(applier.commitSingles) != 0 {
		t.Fatalf("commit singles=%d want 0", len(applier.commitSingles))
	}
}

func TestServiceWriteEffectsQueueApplyBatchRetriesCASConflict(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{
		batchErrSeq: []error{clustermeta.ErrCASConflict, nil},
	}
	queue := newServiceWriteEffectsQueue(applier)
	reqs := []clustermeta.ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{1},
		},
		{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 8,
			AffectedPageNos:   []uint64{1},
		},
	}

	if err := queue.applyBatch(context.Background(), applier, reqs, "volume:00a1b2c3:page:1"); err != nil {
		t.Fatalf("applyBatch returned error: %v", err)
	}
	if len(applier.batches) != 2 {
		t.Fatalf("batch calls=%d want=2", len(applier.batches))
	}
	if len(applier.singles) != 0 {
		t.Fatalf("single calls=%d want=0", len(applier.singles))
	}
}

func TestServiceWriteEffectsQueueDrainBatchWaitsForFollower(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueBatchCoalesceWait(50*time.Millisecond))
	worker := &serviceWriteEffectsWorker{requests: make(chan serviceWriteEffectsRequest, 2)}
	first := serviceWriteEffectsRequest{
		laneKey: "volume:00a1b2c3:page:1",
		req: clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{1},
		},
	}
	go func() {
		time.Sleep(time.Millisecond)
		worker.requests <- serviceWriteEffectsRequest{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 8,
				AffectedPageNos:   []uint64{1},
			},
		}
	}()

	items := queue.drainBatch(worker, first)
	if len(items) != 2 {
		t.Fatalf("drainBatch len=%d want=2", len(items))
	}
	if items[1].req.CommittedRevision != 8 {
		t.Fatalf("drainBatch second revision=%d want=8", items[1].req.CommittedRevision)
	}
}

func TestServiceWriteEffectsQueueDrainBatchCollectsUntilCoalesceWindow(t *testing.T) {
	applier := &recordingBatchServiceWriteEffectsApplier{}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueBatchCoalesceWait(20*time.Millisecond))
	worker := &serviceWriteEffectsWorker{requests: make(chan serviceWriteEffectsRequest, 3)}
	first := serviceWriteEffectsRequest{
		laneKey: "volume:00a1b2c3:page:1",
		req: clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{1},
		},
	}
	go func() {
		time.Sleep(time.Millisecond)
		worker.requests <- serviceWriteEffectsRequest{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 8,
				AffectedPageNos:   []uint64{1},
			},
		}
		time.Sleep(3 * time.Millisecond)
		worker.requests <- serviceWriteEffectsRequest{
			laneKey: "volume:00a1b2c3:page:1",
			req: clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:          "00a1b2c3",
				CommittedRevision: 9,
				AffectedPageNos:   []uint64{1},
			},
		}
	}()

	items := queue.drainBatch(worker, first)
	if len(items) != 3 {
		t.Fatalf("drainBatch len=%d want=3", len(items))
	}
	for i, want := range []uint64{7, 8, 9} {
		if items[i].req.CommittedRevision != want {
			t.Fatalf("drainBatch item %d revision=%d want=%d", i, items[i].req.CommittedRevision, want)
		}
	}
}

func TestServiceWriteEffectsQueueDrainBatchDoesNotWaitWithoutBatchApplier(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 1),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueBatchCoalesceWait(50*time.Millisecond))
	worker := &serviceWriteEffectsWorker{requests: make(chan serviceWriteEffectsRequest, 1)}
	first := serviceWriteEffectsRequest{
		laneKey: "volume:00a1b2c3:page:1",
		req: clustermeta.ApplyCommittedWriteEffectsRequest{
			VolumeID:          "00a1b2c3",
			CommittedRevision: 7,
			AffectedPageNos:   []uint64{1},
		},
	}

	start := time.Now()
	items := queue.drainBatch(worker, first)
	if len(items) != 1 {
		t.Fatalf("drainBatch len=%d want=1", len(items))
	}
	if elapsed := time.Since(start); elapsed >= 25*time.Millisecond {
		t.Fatalf("drainBatch waited %v without batch applier", elapsed)
	}
}

func TestServiceWriteEffectsQueueNativeAllocationFastPathUsesPageLane(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueNativeAllocationFastPath(true))
	var wg sync.WaitGroup
	for _, pageNo := range []uint64{1, 2} {
		wg.Add(1)
		go func(pageNo uint64) {
			defer wg.Done()
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:                "00a1b2c3",
				CommittedRevision:       7 + pageNo,
				AffectedPageNos:         []uint64{pageNo},
				NormalizeExtentMappings: []uint64{1},
				AllocationPages: []clustermeta.AllocationPageRecord{{
					VolumeID:       "00a1b2c3",
					PageNo:         pageNo,
					PageBytes:      4096,
					ChunkSizeBytes: 4096,
				}},
			})
			if err != nil {
				t.Errorf("EnqueueAndWait page=%d: %v", pageNo, err)
			}
		}(pageNo)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-applier.entered:
		case <-time.After(time.Second):
			t.Fatal("effects apply did not start concurrently")
		}
	}
	close(applier.release)
	wg.Wait()
}

func TestServiceWriteEffectsQueueNativeAllocationFastPathCanBucketPageLanes(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier,
		serviceWriteEffectsQueueNativeAllocationFastPath(true),
		serviceWriteEffectsQueueLaneBucketCount(4),
	)
	done := make(chan error, 2)
	for _, pageNo := range []uint64{1, 5} {
		go func(pageNo uint64) {
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:                "00a1b2c3",
				CommittedRevision:       7 + pageNo,
				AffectedPageNos:         []uint64{pageNo},
				NormalizeExtentMappings: []uint64{1},
				AllocationPages: []clustermeta.AllocationPageRecord{{
					VolumeID:       "00a1b2c3",
					PageNo:         pageNo,
					PageBytes:      4096,
					ChunkSizeBytes: 4096,
				}},
			})
			done <- err
		}(pageNo)
	}

	select {
	case <-applier.entered:
	case <-time.After(time.Second):
		t.Fatal("first effects apply did not start")
	}
	select {
	case <-applier.entered:
		t.Fatal("same-bucket page effects started concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	close(applier.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("EnqueueAndWait: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("EnqueueAndWait did not return")
		}
	}
}

func TestServiceWriteEffectsQueueNativeAllocationFastPathKeepsMultiPageEffectsOnAllLane(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueNativeAllocationFastPath(true))
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(revision uint64) {
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:                "00a1b2c3",
				CommittedRevision:       revision,
				AffectedPageNos:         []uint64{1, 2},
				NormalizeExtentMappings: []uint64{1},
				AllocationPages: []clustermeta.AllocationPageRecord{
					{VolumeID: "00a1b2c3", PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 4096},
					{VolumeID: "00a1b2c3", PageNo: 2, PageBytes: 4096, ChunkSizeBytes: 4096},
				},
			})
			done <- err
		}(uint64(7 + i))
	}

	select {
	case <-applier.entered:
	case <-time.After(time.Second):
		t.Fatal("first effects apply did not start")
	}
	select {
	case <-applier.entered:
		t.Fatal("multi-page effects apply started concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	close(applier.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("EnqueueAndWait: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("EnqueueAndWait did not return")
		}
	}
}

func TestServiceWriteEffectsQueueNativeAllocationFastPathStillSerializesWithoutAllocationPage(t *testing.T) {
	applier := &gatedServiceWriteEffectsApplier{
		entered: make(chan clustermeta.ApplyCommittedWriteEffectsRequest, 2),
		release: make(chan struct{}),
	}
	queue := newServiceWriteEffectsQueue(applier, serviceWriteEffectsQueueNativeAllocationFastPath(true))
	done := make(chan error, 2)
	for _, pageNo := range []uint64{1, 2} {
		go func(pageNo uint64) {
			_, err := queue.EnqueueAndWait(context.Background(), clustermeta.ApplyCommittedWriteEffectsRequest{
				VolumeID:                "00a1b2c3",
				CommittedRevision:       7 + pageNo,
				AffectedPageNos:         []uint64{pageNo},
				NormalizeExtentMappings: []uint64{1},
			})
			done <- err
		}(pageNo)
	}

	select {
	case <-applier.entered:
	case <-time.After(time.Second):
		t.Fatal("first effects apply did not start")
	}
	select {
	case <-applier.entered:
		t.Fatal("normalize effects without allocation page started concurrently")
	case <-time.After(10 * time.Millisecond):
	}
	close(applier.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("EnqueueAndWait: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("EnqueueAndWait did not return")
		}
	}
}
