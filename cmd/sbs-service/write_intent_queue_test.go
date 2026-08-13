package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type recordingWriteIntentBatchApplier struct {
	mu      sync.Mutex
	batches [][]clustermeta.WriteIntentRecord
}

func (a *recordingWriteIntentBatchApplier) PutWriteIntent(_ context.Context, record clustermeta.IdempotencyRecord, operation clustermeta.MutationOperationRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batches = append(a.batches, []clustermeta.WriteIntentRecord{{
		IdempotencyRecord: record,
		MutationOperation: operation,
	}})
	return nil
}

func (a *recordingWriteIntentBatchApplier) PutWriteIntentBatch(_ context.Context, intents []clustermeta.WriteIntentRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batches = append(a.batches, append([]clustermeta.WriteIntentRecord(nil), intents...))
	return nil
}

func TestServiceWriteIntentQueueCoalescesConcurrentIntents(t *testing.T) {
	applier := &recordingWriteIntentBatchApplier{}
	queue := newServiceWriteIntentQueue(applier, serviceWriteIntentQueueBatchCoalesceWait(20*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			<-start
			record := clustermeta.IdempotencyRecord{
				VolumeID:       "00a1b2c3",
				IdempotencyKey: fmt.Sprintf("idem-batch-%d", i+1),
				Operation:      "write",
				ResultState:    clustermeta.IdempotencyPending,
				AttachmentID:   "att",
				Generation:     uint64(i + 1),
				Epoch:          5,
				Revision:       11,
			}
			operation := clustermeta.MutationOperationRecord{
				OperationID:    fmt.Sprintf("write-batch-%d", i+1),
				VolumeID:       record.VolumeID,
				Kind:           "write",
				State:          clustermeta.MutationOperationRunning,
				IdempotencyKey: record.IdempotencyKey,
			}
			errs <- queue.EnqueueAndWait(ctx, record, operation)
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("EnqueueAndWait %d: %v", i+1, err)
		}
	}

	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.batches) != 1 {
		t.Fatalf("batch count=%d want 1", len(applier.batches))
	}
	if len(applier.batches[0]) != 2 {
		t.Fatalf("first batch size=%d want 2", len(applier.batches[0]))
	}
}
