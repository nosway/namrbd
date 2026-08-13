package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type countingBeginWriteIntentStore struct {
	state metadata.VolumeState

	getVolumeStateCalls   int
	putIdempotencyCalls   int
	putMutationCalls      int
	putWriteIntentCalls   int
	lastIdempotencyRecord metadata.IdempotencyRecord
	lastMutationOperation metadata.MutationOperationRecord
}

func (s *countingBeginWriteIntentStore) GetVolumeState(context.Context, string) (metadata.VolumeState, error) {
	s.getVolumeStateCalls++
	return s.state, nil
}

func (s *countingBeginWriteIntentStore) GetIdempotencyRecord(context.Context, string, string) (metadata.IdempotencyRecord, error) {
	return metadata.IdempotencyRecord{}, metadata.ErrNotFound
}

func (s *countingBeginWriteIntentStore) PutIdempotencyRecord(_ context.Context, rec metadata.IdempotencyRecord) error {
	s.putIdempotencyCalls++
	s.lastIdempotencyRecord = rec
	return nil
}

func (s *countingBeginWriteIntentStore) GetMutationOperation(context.Context, string, string) (metadata.MutationOperationRecord, error) {
	return metadata.MutationOperationRecord{}, metadata.ErrNotFound
}

func (s *countingBeginWriteIntentStore) PutMutationOperation(_ context.Context, rec metadata.MutationOperationRecord) error {
	s.putMutationCalls++
	s.lastMutationOperation = rec
	return nil
}

func (s *countingBeginWriteIntentStore) PutWriteIntent(_ context.Context, record metadata.IdempotencyRecord, operation metadata.MutationOperationRecord) error {
	s.putWriteIntentCalls++
	s.lastIdempotencyRecord = record
	s.lastMutationOperation = operation
	return nil
}

func TestCachedBeginWriteVolumeStateIntentStoreCachesWithinTTL(t *testing.T) {
	base := &countingBeginWriteIntentStore{
		state: metadata.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    7,
			Revision: 11,
			Status:   metadata.VolumeStatusHealthy,
		},
	}
	cache := newCachedBeginWriteVolumeStateIntentStore(base, 250*time.Millisecond)
	now := time.Unix(100, 0)
	cache.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		state, err := cache.GetVolumeState(context.Background(), "00a1b2c3")
		if err != nil {
			t.Fatalf("GetVolumeState cached iteration %d: %v", i, err)
		}
		if state.Revision != 11 || state.Epoch != 7 {
			t.Fatalf("cached state=%+v want epoch=7 revision=11", state)
		}
	}
	if base.getVolumeStateCalls != 1 {
		t.Fatalf("GetVolumeState calls=%d want 1", base.getVolumeStateCalls)
	}

	now = now.Add(251 * time.Millisecond)
	if _, err := cache.GetVolumeState(context.Background(), "00a1b2c3"); err != nil {
		t.Fatalf("GetVolumeState after expiry: %v", err)
	}
	if base.getVolumeStateCalls != 2 {
		t.Fatalf("GetVolumeState calls after expiry=%d want 2", base.getVolumeStateCalls)
	}
}

func TestCachedBeginWriteVolumeStateIntentStorePreservesPutWriteIntent(t *testing.T) {
	base := &countingBeginWriteIntentStore{}
	cache := newCachedBeginWriteVolumeStateIntentStore(base, time.Second)

	record := metadata.IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-1",
		Revision:       12,
	}
	operation := metadata.MutationOperationRecord{
		VolumeID:    "00a1b2c3",
		OperationID: "op-1",
	}
	if err := cache.PutWriteIntent(context.Background(), record, operation); err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	if base.putWriteIntentCalls != 1 {
		t.Fatalf("PutWriteIntent calls=%d want 1", base.putWriteIntentCalls)
	}
	if base.putIdempotencyCalls != 0 || base.putMutationCalls != 0 {
		t.Fatalf("fallback writes used idempotency=%d mutation=%d, want 0/0", base.putIdempotencyCalls, base.putMutationCalls)
	}
	if base.lastIdempotencyRecord.IdempotencyKey != "idem-1" || base.lastMutationOperation.OperationID != "op-1" {
		t.Fatalf("unexpected delegated records idempotency=%+v operation=%+v", base.lastIdempotencyRecord, base.lastMutationOperation)
	}
}
