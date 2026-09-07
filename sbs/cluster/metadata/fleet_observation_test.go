package metadata

import (
	"context"
	"strings"
	"testing"
)

func TestFleetObservationIsValidatedAndPointReadable(t *testing.T) {
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "sbs/cluster")
	digest := "sha256:" + strings.Repeat("a", 64)
	observation, err := NewFleetObservation(FleetObservation{
		SourceRevision: 41, ManifestRevision: "manifest-41", ManifestDigest: digest,
		BinaryDigest: digest, ConfigDigest: digest, StoreDigest: digest,
		ApplyState: "paused", ApplyPaused: true, ApplyOperationID: "apply-41",
		ConfigDriftCount: 2, StoreCount: 160, UsableBytes: 1000, FreeBytes: 700,
		ReservedBytes: 100, CapacityObservedAtUnix: 100, ObservedAtUnix: 101,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.PutFleetObservation(context.Background(), observation); err != nil {
		t.Fatal(err)
	}
	kv.resetGetCalls()
	got, err := repo.GetFleetObservation(context.Background())
	if err != nil || got.RecordDigest != observation.RecordDigest || got.ConfigDriftCount != 2 {
		t.Fatalf("observation=%+v err=%v", got, err)
	}
	if kv.getCallCount(fleetObservationKey("sbs/cluster")) != 1 || kv.batchGetCalls != 0 || kv.runTxCalls != 0 {
		t.Fatalf("point read shape get=%d batch=%d tx=%d", kv.getCallCount(fleetObservationKey("sbs/cluster")), kv.batchGetCalls, kv.runTxCalls)
	}

	corrupt := observation
	corrupt.FreeBytes++
	if err := repo.PutFleetObservation(context.Background(), corrupt); err == nil {
		t.Fatal("digest mismatch was accepted")
	}
	invalid := observation
	invalid.ApplyPaused = false
	invalid.RecordDigest = digestFleetObservation(invalid)
	if err := ValidateFleetObservation(invalid); err == nil {
		t.Fatal("paused state mismatch was accepted")
	}
}
