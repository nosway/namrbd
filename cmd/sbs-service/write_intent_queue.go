package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const serviceWriteIntentQueueDepth = 4096
const serviceWriteIntentBatchMax = 16
const defaultServiceWriteIntentBatchCoalesceWait = 0

type serviceWriteIntentApplier interface {
	PutWriteIntent(ctx context.Context, record clustermeta.IdempotencyRecord, operation clustermeta.MutationOperationRecord) error
}

type serviceWriteIntentBatchApplier interface {
	PutWriteIntentBatch(ctx context.Context, intents []clustermeta.WriteIntentRecord) error
}

type serviceWriteIntentRequest struct {
	ctx        context.Context
	intent     clustermeta.WriteIntentRecord
	enqueuedAt time.Time
	done       chan error
}

type serviceWriteIntentQueue struct {
	applier           serviceWriteIntentApplier
	batchCoalesceWait time.Duration
	requests          chan serviceWriteIntentRequest
	once              sync.Once
}

type serviceWriteIntentQueueOption func(*serviceWriteIntentQueue)

func serviceWriteIntentQueueBatchCoalesceWait(wait time.Duration) serviceWriteIntentQueueOption {
	return func(q *serviceWriteIntentQueue) {
		if wait < 0 {
			wait = 0
		}
		q.batchCoalesceWait = wait
	}
}

func newServiceWriteIntentQueue(applier serviceWriteIntentApplier, opts ...serviceWriteIntentQueueOption) *serviceWriteIntentQueue {
	q := &serviceWriteIntentQueue{
		applier:           applier,
		batchCoalesceWait: defaultServiceWriteIntentBatchCoalesceWait,
		requests:          make(chan serviceWriteIntentRequest, serviceWriteIntentQueueDepth),
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *serviceWriteIntentQueue) EnqueueAndWait(ctx context.Context, record clustermeta.IdempotencyRecord, operation clustermeta.MutationOperationRecord) error {
	if q == nil || q.applier == nil {
		return nil
	}
	q.once.Do(func() {
		go q.run()
	})
	done := make(chan error, 1)
	item := serviceWriteIntentRequest{
		ctx: ctx,
		intent: clustermeta.WriteIntentRecord{
			IdempotencyRecord: record,
			MutationOperation: operation,
		},
		enqueuedAt: time.Now(),
		done:       done,
	}
	select {
	case q.requests <- item:
	default:
		go func() {
			q.requests <- item
		}()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *serviceWriteIntentQueue) run() {
	for item := range q.requests {
		items := q.drainBatch(item)
		applyStart := time.Now()
		errs := q.applyItems(item.ctx, items)
		applyDuration := time.Since(applyStart)
		for i, item := range items {
			if item.done != nil {
				item.done <- errs[i]
			}
		}
		q.logBatch(items, applyStart, applyDuration, errs)
	}
}

func (q *serviceWriteIntentQueue) drainBatch(first serviceWriteIntentRequest) []serviceWriteIntentRequest {
	items := []serviceWriteIntentRequest{first}
	items = drainReadyServiceWriteIntents(q.requests, items, serviceWriteIntentBatchMax)
	if _, ok := q.applier.(serviceWriteIntentBatchApplier); !ok {
		return items
	}
	if len(items) >= serviceWriteIntentBatchMax || q.batchCoalesceWait <= 0 {
		return items
	}

	timer := time.NewTimer(q.batchCoalesceWait)
	defer timer.Stop()
	for len(items) < serviceWriteIntentBatchMax {
		select {
		case next, ok := <-q.requests:
			if !ok {
				return items
			}
			items = append(items, next)
			items = drainReadyServiceWriteIntents(q.requests, items, serviceWriteIntentBatchMax)
		case <-timer.C:
			return items
		}
	}
	return items
}

func drainReadyServiceWriteIntents(ch <-chan serviceWriteIntentRequest, items []serviceWriteIntentRequest, maxItems int) []serviceWriteIntentRequest {
	for len(items) < maxItems {
		select {
		case next, ok := <-ch:
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

func (q *serviceWriteIntentQueue) applyItems(ctx context.Context, items []serviceWriteIntentRequest) []error {
	errs := make([]error, len(items))
	if len(items) == 0 {
		return errs
	}
	if batcher, ok := q.applier.(serviceWriteIntentBatchApplier); ok && len(items) > 1 {
		intents := make([]clustermeta.WriteIntentRecord, len(items))
		for i, item := range items {
			intents[i] = item.intent
		}
		if err := batcher.PutWriteIntentBatch(ctx, intents); err != nil {
			for i := range errs {
				errs[i] = err
			}
		}
		return errs
	}
	for i, item := range items {
		errs[i] = q.applier.PutWriteIntent(ctx, item.intent.IdempotencyRecord, item.intent.MutationOperation)
	}
	return errs
}

func (q *serviceWriteIntentQueue) logBatch(items []serviceWriteIntentRequest, applyStart time.Time, applyDuration time.Duration, errs []error) {
	if len(items) == 0 {
		return
	}
	maxQueueWait := time.Duration(0)
	for _, item := range items {
		if wait := applyStart.Sub(item.enqueuedAt); wait > maxQueueWait {
			maxQueueWait = wait
		}
	}
	fields := []structuredlog.Field{
		structuredlog.F("volume_id", items[0].intent.IdempotencyRecord.VolumeID),
		structuredlog.F("first_idempotency_key", items[0].intent.IdempotencyRecord.IdempotencyKey),
		structuredlog.F("last_idempotency_key", items[len(items)-1].intent.IdempotencyRecord.IdempotencyKey),
		structuredlog.F("request_count", len(items)),
		structuredlog.F("max_queue_wait_ms", maxQueueWait.Milliseconds()),
		structuredlog.F("duration_ms", applyDuration.Milliseconds()),
	}
	for _, err := range errs {
		if err != nil {
			structuredlog.Error("sbs.service", "write_session_intent_batch_failed", fmt.Errorf("put write intent batch: %w", err), fields...)
			return
		}
	}
	structuredlog.Info("sbs.service", "write_session_intent_batch_applied", fields...)
}
