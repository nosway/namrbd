package dr

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type shippingFixtureReader struct {
	data              []byte
	snapshotID        string
	snapshotRootID    string
	failOffset        uint64
	failuresRemaining int
	readOffsets       []uint64
}

func (r *shippingFixtureReader) ReadRecoveryPoint(_ context.Context, snapshotID, snapshotRootID string, offset, length uint64) ([]byte, error) {
	if snapshotID != r.snapshotID || snapshotRootID != r.snapshotRootID {
		return nil, fmt.Errorf("immutable root mismatch")
	}
	r.readOffsets = append(r.readOffsets, offset)
	if offset == r.failOffset && r.failuresRemaining > 0 {
		r.failuresRemaining--
		return nil, fmt.Errorf("injected source interruption")
	}
	if offset+length > uint64(len(r.data)) {
		return nil, fmt.Errorf("read exceeds source")
	}
	return append([]byte(nil), r.data[offset:offset+length]...), nil
}

type shippingFixtureTarget struct {
	identity string
	objects  map[string][]byte
}

func newShippingFixtureTarget(identity string) *shippingFixtureTarget {
	return &shippingFixtureTarget{identity: identity, objects: make(map[string][]byte)}
}

func (t *shippingFixtureTarget) TargetIdentity() string { return t.identity }

func (t *shippingFixtureTarget) PutObject(_ context.Context, key string, data []byte, expectedChecksum string) (ShippingObjectEvidence, error) {
	if shippingSHA256(data) != expectedChecksum {
		return ShippingObjectEvidence{}, fmt.Errorf("put checksum mismatch")
	}
	t.objects[key] = append([]byte(nil), data...)
	return ShippingObjectEvidence{Key: key, Length: uint64(len(data)), Checksum: expectedChecksum}, nil
}

func (t *shippingFixtureTarget) GetObject(_ context.Context, key, expectedChecksum string) ([]byte, error) {
	data, ok := t.objects[key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", key)
	}
	if expectedChecksum != "" && shippingSHA256(data) != expectedChecksum {
		return nil, fmt.Errorf("object checksum mismatch: %s", key)
	}
	return append([]byte(nil), data...), nil
}

type shippingFixtureStandby struct {
	data              []byte
	importCalls       int
	rejectWrites      bool
	corruptReadOffset uint64
}

func (s *shippingFixtureStandby) ImportRange(_ context.Context, offset uint64, data []byte) error {
	if offset+uint64(len(data)) > uint64(len(s.data)) {
		return fmt.Errorf("import exceeds standby")
	}
	copy(s.data[offset:], data)
	s.importCalls++
	return nil
}

func (s *shippingFixtureStandby) ReadRange(_ context.Context, offset, length uint64) ([]byte, error) {
	if offset+length > uint64(len(s.data)) {
		return nil, fmt.Errorf("read exceeds standby")
	}
	out := append([]byte(nil), s.data[offset:offset+length]...)
	if offset == s.corruptReadOffset && len(out) > 0 {
		out[0] ^= 0xff
	}
	return out, nil
}

func (s *shippingFixtureStandby) ProbeExternalWrite(_ context.Context, _ uint64, _ []byte) error {
	if s.rejectWrites {
		return ErrStandbyReadOnly
	}
	return nil
}

func TestShipRecoveryPointResumesAndImportsAcrossAuthorities(t *testing.T) {
	ctx := context.Background()
	payload := []byte("source-authority-immutable-recovery-root")
	spec := shippingFixtureSpec(uint64(len(payload)), 8)
	reader := &shippingFixtureReader{
		data: payload, snapshotID: spec.SourceSnapshotID, snapshotRootID: spec.SourceSnapshotRootID,
		failOffset: 8, failuresRemaining: 1,
	}
	target := newShippingFixtureTarget("target-authority-b")
	var checkpoint ShippingCheckpoint
	result, err := ShipRecoveryPoint(ctx, reader, target, spec, checkpoint, func(_ context.Context, progress ShippingCheckpoint) error {
		checkpoint = progress
		return nil
	})
	if err == nil || !reflect.DeepEqual(result.Checkpoint, checkpoint) {
		t.Fatalf("interrupted result=%+v checkpoint=%+v err=%v", result, checkpoint, err)
	}
	if checkpoint.NextOffset != 8 || len(checkpoint.Objects) != 1 {
		t.Fatalf("checkpoint=%+v", checkpoint)
	}
	readsBeforeResume := len(reader.readOffsets)
	result, err = ShipRecoveryPoint(ctx, reader, target, spec, checkpoint, func(_ context.Context, progress ShippingCheckpoint) error {
		checkpoint = progress
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Resumed || result.Replay || result.Checkpoint.NextOffset != uint64(len(payload)) || uint64(len(result.Manifest.Objects)) != spec.ExpectedObjects || result.TargetVerificationReceipt == "" {
		t.Fatalf("completed result=%+v", result)
	}
	for _, offset := range reader.readOffsets[readsBeforeResume:] {
		if offset == 0 {
			t.Fatalf("resume reread completed source object: offsets=%v", reader.readOffsets)
		}
	}
	replayed, err := ShipRecoveryPoint(ctx, reader, target, spec, checkpoint, nil)
	if err != nil || !replayed.Resumed || !replayed.Replay || replayed.TargetVerificationReceipt != result.TargetVerificationReceipt {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}

	standby := &shippingFixtureStandby{data: make([]byte, len(payload)), rejectWrites: true, corruptReadOffset: ^uint64(0)}
	imported, err := ImportReadOnlyStandby(ctx, target, result.Manifest, shippingFixtureConstraints(spec, result.TargetVerificationReceipt), standby)
	if err != nil {
		t.Fatal(err)
	}
	if imported.ImportedBytes != uint64(len(payload)) || imported.ImportedObjects != spec.ExpectedObjects || !imported.UserspaceReadbackMatched || !imported.StandbyWriteRejected || !reflect.DeepEqual(standby.data, payload) {
		t.Fatalf("imported=%+v data=%q", imported, standby.data)
	}
}

func TestReadShippingManifestRejectsTargetManifestTampering(t *testing.T) {
	ctx := context.Background()
	payload := []byte("target-authority-manifest-readback")
	spec := shippingFixtureSpec(uint64(len(payload)), 8)
	reader := &shippingFixtureReader{data: payload, snapshotID: spec.SourceSnapshotID, snapshotRootID: spec.SourceSnapshotRootID, failOffset: ^uint64(0)}
	target := newShippingFixtureTarget("target-authority-b")
	result, err := ShipRecoveryPoint(ctx, reader, target, spec, ShippingCheckpoint{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadShippingManifest(ctx, target)
	if err != nil || !reflect.DeepEqual(manifest, result.Manifest) {
		t.Fatalf("manifest=%+v err=%v want=%+v", manifest, err, result.Manifest)
	}
	target.objects["manifest.json"][len(target.objects["manifest.json"])-2] ^= 0x01
	if _, err := ReadShippingManifest(ctx, target); !errors.Is(err, ErrShippingIntegrity) {
		t.Fatalf("tampered manifest err=%v want ErrShippingIntegrity", err)
	}
}

func TestImportRejectsCorruptMissingAndIncompatibleTargetBeforeMutation(t *testing.T) {
	ctx := context.Background()
	payload := []byte("target-integrity-before-import")
	spec := shippingFixtureSpec(uint64(len(payload)), 7)
	reader := &shippingFixtureReader{data: payload, snapshotID: spec.SourceSnapshotID, snapshotRootID: spec.SourceSnapshotRootID, failOffset: ^uint64(0)}
	target := newShippingFixtureTarget("target-authority-b")
	result, err := ShipRecoveryPoint(ctx, reader, target, spec, ShippingCheckpoint{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	constraints := shippingFixtureConstraints(spec, result.TargetVerificationReceipt)
	incompatible := constraints
	incompatible.CompatibilityDigest = "sha256:wrong-target-geometry"
	standby := &shippingFixtureStandby{data: make([]byte, len(payload)), rejectWrites: true, corruptReadOffset: ^uint64(0)}
	if _, err := ImportReadOnlyStandby(ctx, target, result.Manifest, incompatible, standby); !errors.Is(err, ErrShippingIntegrity) || standby.importCalls != 0 {
		t.Fatalf("incompatible err=%v import_calls=%d", err, standby.importCalls)
	}

	original := append([]byte(nil), target.objects["objects/000001.bin"]...)
	target.objects["objects/000001.bin"][0] ^= 0xff
	if _, err := ImportReadOnlyStandby(ctx, target, result.Manifest, constraints, standby); !errors.Is(err, ErrShippingIntegrity) || standby.importCalls != 0 {
		t.Fatalf("corrupt err=%v import_calls=%d", err, standby.importCalls)
	}
	target.objects["objects/000001.bin"] = original
	delete(target.objects, "objects/000002.bin")
	if _, err := ImportReadOnlyStandby(ctx, target, result.Manifest, constraints, standby); !errors.Is(err, ErrShippingIntegrity) || standby.importCalls != 0 {
		t.Fatalf("missing err=%v import_calls=%d", err, standby.importCalls)
	}
}

func TestImportFailsWhenReadOnlyOrReadbackContractIsAbsent(t *testing.T) {
	ctx := context.Background()
	payload := []byte("readback-and-read-only")
	spec := shippingFixtureSpec(uint64(len(payload)), 6)
	reader := &shippingFixtureReader{data: payload, snapshotID: spec.SourceSnapshotID, snapshotRootID: spec.SourceSnapshotRootID, failOffset: ^uint64(0)}
	target := newShippingFixtureTarget("target-authority-b")
	result, err := ShipRecoveryPoint(ctx, reader, target, spec, ShippingCheckpoint{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	constraints := shippingFixtureConstraints(spec, result.TargetVerificationReceipt)
	corrupt := &shippingFixtureStandby{data: make([]byte, len(payload)), rejectWrites: true, corruptReadOffset: 0}
	if _, err := ImportReadOnlyStandby(ctx, target, result.Manifest, constraints, corrupt); !errors.Is(err, ErrShippingIntegrity) {
		t.Fatalf("corrupt standby readback err=%v", err)
	}
	writable := &shippingFixtureStandby{data: make([]byte, len(payload)), corruptReadOffset: ^uint64(0)}
	if _, err := ImportReadOnlyStandby(ctx, target, result.Manifest, constraints, writable); err == nil || writable.importCalls != 0 {
		t.Fatalf("writable standby import err=%v import_calls=%d", err, writable.importCalls)
	}
}

func shippingFixtureSpec(expectedBytes, chunkSize uint64) ShippingSpec {
	return ShippingSpec{
		ShippingWorkerID: "worker-a", ShippedManifestID: "manifest-a", RecoveryPointID: "rp-a",
		SourceClusterID: "source-authority-a", TargetClusterID: "target-authority-b",
		SourceVolumeID: "source-volume-a", TargetVolumeID: "standby-volume-b",
		SourceSnapshotID: "snapshot-a", SourceSnapshotRootID: "snapshot-root-a",
		ControlManifestDigest: "sha256:control-manifest-a", PayloadRootsDigest: "sha256:payload-roots-a", ReadViewDigest: "sha256:read-view-a",
		KeyPolicyDigest: "sha256:key-policy-a", GovernanceDigest: "sha256:governance-a",
		GeometryDigest: "sha256:geometry-a", CompatibilityDigest: "sha256:compatibility-a",
		ExpectedBytes: expectedBytes, ExpectedObjects: (expectedBytes + chunkSize - 1) / chunkSize,
		Generation: 3, ChunkSizeBytes: chunkSize, CreatedAt: time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC),
	}
}

func shippingFixtureConstraints(spec ShippingSpec, receipt string) ImportConstraints {
	return ImportConstraints{
		ShippedManifestID: spec.ShippedManifestID, RecoveryPointID: spec.RecoveryPointID,
		SourceSnapshotID: spec.SourceSnapshotID, SourceSnapshotRootID: spec.SourceSnapshotRootID,
		PayloadRootsDigest: spec.PayloadRootsDigest,
		GeometryDigest:     spec.GeometryDigest, CompatibilityDigest: spec.CompatibilityDigest,
		KeyPolicyDigest: spec.KeyPolicyDigest, GovernanceDigest: spec.GovernanceDigest,
		TargetVerificationReceipt: receipt,
	}
}
