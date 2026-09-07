package metadata

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

type recordingSnapshotSubjectRegistrar struct {
	calls int
	fail  error
}

func (r *recordingSnapshotSubjectRegistrar) RegisterSnapshotSubject(ctx context.Context, writer ReadWriter, snapshot SnapshotRecord) error {
	r.calls++
	if r.fail != nil {
		return r.fail
	}
	return writer.Set(ctx, fmt.Sprintf("enterprise/test/subjects/%s", snapshot.SnapshotID), []byte(snapshot.SourceVolumeID))
}

func TestSnapshotSubjectRegistrationSharesCreateTransactionAndReplay(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-epi-test")
	registrar := &recordingSnapshotSubjectRegistrar{}
	repo.SetSnapshotSubjectRegistrar(registrar)
	record := SnapshotRecord{
		SnapshotID:      "snap-00a1b2c3-20260826T120000.000000000Z",
		SourceVolumeID:  "00a1b2c3",
		State:           SnapshotStateAvailable,
		IdempotencyKey:  "epi-snapshot-idempotency",
		CreatedAtUnix:   100,
		UpdatedAtUnix:   100,
		SourceSizeBytes: 4096,
	}

	created, replay, err := repo.CreateSnapshotRecord(ctx, record)
	if err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if replay || created.SnapshotID != record.SnapshotID {
		t.Fatalf("created=%+v replay=%t", created, replay)
	}
	if registrar.calls != 1 {
		t.Fatalf("registrar calls=%d want=1", registrar.calls)
	}
	value, found, err := kv.Get(ctx, "enterprise/test/subjects/"+record.SnapshotID)
	if err != nil || !found || string(value) != record.SourceVolumeID {
		t.Fatalf("subject value=%q found=%t err=%v", value, found, err)
	}

	replayed, replay, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:      "snap-00a1b2c3-20260826T120001.000000000Z",
		SourceVolumeID:  record.SourceVolumeID,
		IdempotencyKey:  record.IdempotencyKey,
		CreatedAtUnix:   101,
		UpdatedAtUnix:   101,
		SourceSizeBytes: 8192,
	})
	if err != nil {
		t.Fatalf("CreateSnapshotRecord replay: %v", err)
	}
	if !replay || replayed.SnapshotID != record.SnapshotID {
		t.Fatalf("replayed=%+v replay=%t", replayed, replay)
	}
	if registrar.calls != 2 {
		t.Fatalf("replay must reconcile subject registration: calls=%d want=2", registrar.calls)
	}
}

func TestSnapshotSubjectRegistrationFailureRollsBackSnapshot(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(filepath.Join(t.TempDir(), "metadata"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	repo := NewRepository(kv, "phase-epi-test")
	registrationErr := errors.New("subject authority unavailable")
	repo.SetSnapshotSubjectRegistrar(&recordingSnapshotSubjectRegistrar{fail: registrationErr})
	snapshotID := "snap-00a1b2c3-20260826T120002.000000000Z"

	_, _, err = repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:      snapshotID,
		SourceVolumeID:  "00a1b2c3",
		IdempotencyKey:  "epi-snapshot-failure",
		CreatedAtUnix:   102,
		UpdatedAtUnix:   102,
		SourceSizeBytes: 4096,
	})
	if !errors.Is(err, registrationErr) {
		t.Fatalf("CreateSnapshotRecord error=%v want registration error", err)
	}
	if _, err := repo.GetSnapshotRecord(ctx, snapshotID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed subject registration leaked snapshot: err=%v", err)
	}
}

func TestSnapshotSubjectRegistrationRejectsNonTransactionalMetadata(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-epi-test")
	repo.SetSnapshotSubjectRegistrar(&recordingSnapshotSubjectRegistrar{})
	snapshotID := "snap-00a1b2c3-20260826T120003.000000000Z"

	_, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:      snapshotID,
		SourceVolumeID:  "00a1b2c3",
		CreatedAtUnix:   103,
		UpdatedAtUnix:   103,
		SourceSizeBytes: 4096,
	})
	if !errors.Is(err, ErrSnapshotSubjectRegistrationRequiresTransaction) {
		t.Fatalf("CreateSnapshotRecord error=%v want transactional requirement", err)
	}
	if _, err := repo.GetSnapshotRecord(ctx, snapshotID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("non-transactional rejection leaked snapshot: err=%v", err)
	}
}
