package dr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ShippingManifestVersion = "namrbd.dr.shipping.v1"
	ShippingChecksumSHA256  = "SHA-256"
	maxShippingChunkBytes   = 64 * 1024 * 1024
	maxShippingObjectCount  = 1_000_000
)

var (
	ErrShippingIntegrity = errors.New("dr shipping integrity failure")
	ErrStandbyReadOnly   = errors.New("dr standby is read-only")
)

type ImmutableRecoveryReader interface {
	ReadRecoveryPoint(ctx context.Context, snapshotID, snapshotRootID string, offset, length uint64) ([]byte, error)
}

type ShippingObjectRef struct {
	Key      string `json:"key"`
	Offset   uint64 `json:"offset"`
	Length   uint64 `json:"length"`
	Checksum string `json:"checksum"`
}

type ShippingObjectEvidence struct {
	Key      string
	Length   uint64
	Checksum string
}

type ShippingObjectStore interface {
	TargetIdentity() string
	PutObject(ctx context.Context, key string, data []byte, expectedChecksum string) (ShippingObjectEvidence, error)
	GetObject(ctx context.Context, key, expectedChecksum string) ([]byte, error)
}

type ShippingSpec struct {
	ShippingWorkerID      string
	ShippedManifestID     string
	RecoveryPointID       string
	SourceClusterID       string
	TargetClusterID       string
	SourceVolumeID        string
	TargetVolumeID        string
	SourceSnapshotID      string
	SourceSnapshotRootID  string
	ControlManifestDigest string
	PayloadRootsDigest    string
	ReadViewDigest        string
	KeyPolicyDigest       string
	GovernanceDigest      string
	GeometryDigest        string
	CompatibilityDigest   string
	ExpectedBytes         uint64
	ExpectedObjects       uint64
	Generation            uint64
	ChunkSizeBytes        uint64
	CreatedAt             time.Time
}

type ShippingCheckpoint struct {
	ShippingWorkerID     string              `json:"shipping_worker_id"`
	ShippedManifestID    string              `json:"shipped_manifest_id"`
	SourceSnapshotID     string              `json:"source_snapshot_id"`
	SourceSnapshotRootID string              `json:"source_snapshot_root_id"`
	Generation           uint64              `json:"generation"`
	NextOffset           uint64              `json:"next_offset"`
	Objects              []ShippingObjectRef `json:"objects"`
}

type ShippingCheckpointWriter func(context.Context, ShippingCheckpoint) error

type ShippingManifest struct {
	ManifestVersion       string              `json:"manifest_version"`
	ShippingWorkerID      string              `json:"shipping_worker_id"`
	ShippedManifestID     string              `json:"shipped_manifest_id"`
	RecoveryPointID       string              `json:"recovery_point_id"`
	SourceClusterID       string              `json:"source_cluster_id"`
	TargetClusterID       string              `json:"target_cluster_id"`
	SourceVolumeID        string              `json:"source_volume_id"`
	TargetVolumeID        string              `json:"target_volume_id"`
	SourceSnapshotID      string              `json:"source_snapshot_id"`
	SourceSnapshotRootID  string              `json:"source_snapshot_root_id"`
	ControlManifestDigest string              `json:"control_manifest_digest"`
	PayloadRootsDigest    string              `json:"payload_roots_digest"`
	ReadViewDigest        string              `json:"read_view_digest"`
	KeyPolicyDigest       string              `json:"key_policy_digest"`
	GovernanceDigest      string              `json:"governance_digest"`
	GeometryDigest        string              `json:"geometry_digest"`
	CompatibilityDigest   string              `json:"compatibility_digest"`
	ExpectedBytes         uint64              `json:"expected_bytes"`
	ExpectedObjects       uint64              `json:"expected_objects"`
	Generation            uint64              `json:"generation"`
	ChunkSizeBytes        uint64              `json:"chunk_size_bytes"`
	ChecksumAlgorithm     string              `json:"checksum_algorithm"`
	Objects               []ShippingObjectRef `json:"objects"`
	CreatedAt             time.Time           `json:"created_at"`
	ManifestDigest        string              `json:"manifest_digest"`
}

type ShippingResult struct {
	Manifest                  ShippingManifest
	Checkpoint                ShippingCheckpoint
	TargetVerificationReceipt string
	Resumed                   bool
	Replay                    bool
}

type StandbyImporter interface {
	ImportRange(context.Context, uint64, []byte) error
	ReadRange(context.Context, uint64, uint64) ([]byte, error)
	ProbeExternalWrite(context.Context, uint64, []byte) error
}

type ImportConstraints struct {
	ShippedManifestID         string
	RecoveryPointID           string
	SourceSnapshotID          string
	SourceSnapshotRootID      string
	PayloadRootsDigest        string
	GeometryDigest            string
	CompatibilityDigest       string
	KeyPolicyDigest           string
	GovernanceDigest          string
	TargetVerificationReceipt string
}

type ImportResult struct {
	ImportedBytes             uint64
	ImportedObjects           uint64
	UserspaceReadbackMatched  bool
	StandbyWriteRejected      bool
	TargetVerificationReceipt string
}

func ShipRecoveryPoint(ctx context.Context, reader ImmutableRecoveryReader, target ShippingObjectStore, spec ShippingSpec, checkpoint ShippingCheckpoint, persist ShippingCheckpointWriter) (ShippingResult, error) {
	if err := validateShippingSpec(spec); err != nil {
		return ShippingResult{}, err
	}
	if reader == nil || target == nil {
		return ShippingResult{}, fmt.Errorf("immutable recovery reader and shipping target are required")
	}
	if strings.TrimSpace(target.TargetIdentity()) == "" {
		return ShippingResult{}, fmt.Errorf("shipping target identity is required")
	}
	if spec.CreatedAt.IsZero() {
		spec.CreatedAt = time.Now().UTC()
	}
	progress, resumed, err := verifyShippingCheckpoint(ctx, target, spec, checkpoint)
	if err != nil {
		return ShippingResult{}, err
	}
	for progress.NextOffset < spec.ExpectedBytes {
		if err := ctx.Err(); err != nil {
			return ShippingResult{Checkpoint: progress, Resumed: resumed}, err
		}
		length := minShippingUint64(spec.ChunkSizeBytes, spec.ExpectedBytes-progress.NextOffset)
		data, err := reader.ReadRecoveryPoint(ctx, spec.SourceSnapshotID, spec.SourceSnapshotRootID, progress.NextOffset, length)
		if err != nil {
			return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("read immutable recovery point offset=%d length=%d: %w", progress.NextOffset, length, err)
		}
		if uint64(len(data)) != length {
			return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("%w: source read offset=%d bytes=%d want=%d", ErrShippingIntegrity, progress.NextOffset, len(data), length)
		}
		key := fmt.Sprintf("objects/%06d.bin", len(progress.Objects))
		checksum := shippingSHA256(data)
		ref, err := target.PutObject(ctx, key, data, checksum)
		if err != nil {
			return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("put shipping object %s: %w", key, err)
		}
		if ref.Key != key || ref.Length != length || ref.Checksum != checksum {
			return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("%w: target put evidence mismatch object=%s", ErrShippingIntegrity, key)
		}
		progress.Objects = append(progress.Objects, ShippingObjectRef{Key: ref.Key, Offset: progress.NextOffset, Length: ref.Length, Checksum: ref.Checksum})
		progress.NextOffset += length
		if persist != nil {
			if err := persist(ctx, cloneShippingCheckpoint(progress)); err != nil {
				return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("persist shipping checkpoint: %w", err)
			}
		}
	}
	manifest := newShippingManifest(spec, progress.Objects)
	digest, err := shippingManifestDigest(manifest)
	if err != nil {
		return ShippingResult{Checkpoint: progress, Resumed: resumed}, err
	}
	manifest.ManifestDigest = digest
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return ShippingResult{Checkpoint: progress, Resumed: resumed}, err
	}
	manifestRef, err := target.PutObject(ctx, "manifest.json", manifestBytes, shippingSHA256(manifestBytes))
	if err != nil {
		return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("put shipping manifest: %w", err)
	}
	if manifestRef.Key != "manifest.json" || manifestRef.Length != uint64(len(manifestBytes)) || manifestRef.Checksum != shippingSHA256(manifestBytes) {
		return ShippingResult{Checkpoint: progress, Resumed: resumed}, fmt.Errorf("%w: target manifest put evidence mismatch", ErrShippingIntegrity)
	}
	receipt, err := VerifyShippingTarget(ctx, target, manifest)
	if err != nil {
		return ShippingResult{Checkpoint: progress, Resumed: resumed}, err
	}
	return ShippingResult{Manifest: manifest, Checkpoint: progress, TargetVerificationReceipt: receipt, Resumed: resumed, Replay: resumed && checkpoint.NextOffset == spec.ExpectedBytes}, nil
}

func VerifyShippingTarget(ctx context.Context, target ShippingObjectStore, manifest ShippingManifest) (string, error) {
	if target == nil || strings.TrimSpace(target.TargetIdentity()) == "" {
		return "", fmt.Errorf("shipping target and identity are required")
	}
	if err := validateShippingManifest(manifest); err != nil {
		return "", err
	}
	digest, err := shippingManifestDigest(manifest)
	if err != nil || digest != manifest.ManifestDigest {
		return "", fmt.Errorf("%w: shipping manifest digest mismatch", ErrShippingIntegrity)
	}
	var nextOffset uint64
	for index, object := range manifest.Objects {
		wantKey := fmt.Sprintf("objects/%06d.bin", index)
		if object.Key != wantKey || object.Offset != nextOffset || object.Length == 0 || object.Offset > manifest.ExpectedBytes || object.Length > manifest.ExpectedBytes-object.Offset {
			return "", fmt.Errorf("%w: invalid shipping object index=%d key=%s", ErrShippingIntegrity, index, object.Key)
		}
		data, err := target.GetObject(ctx, object.Key, object.Checksum)
		if err != nil {
			return "", fmt.Errorf("%w: verify shipping object %s: %v", ErrShippingIntegrity, object.Key, err)
		}
		if uint64(len(data)) != object.Length || shippingSHA256(data) != object.Checksum {
			return "", fmt.Errorf("%w: shipping object %s content mismatch", ErrShippingIntegrity, object.Key)
		}
		nextOffset += object.Length
	}
	if nextOffset != manifest.ExpectedBytes {
		return "", fmt.Errorf("%w: shipping object coverage ends at %d want=%d", ErrShippingIntegrity, nextOffset, manifest.ExpectedBytes)
	}
	manifestBytes, err := target.GetObject(ctx, "manifest.json", "")
	if err != nil {
		return "", fmt.Errorf("%w: read target manifest: %v", ErrShippingIntegrity, err)
	}
	var stored ShippingManifest
	if err := json.Unmarshal(manifestBytes, &stored); err != nil {
		return "", fmt.Errorf("%w: decode target manifest: %v", ErrShippingIntegrity, err)
	}
	storedDigest, err := shippingManifestDigest(stored)
	if err != nil || storedDigest != manifest.ManifestDigest || stored.ManifestDigest != manifest.ManifestDigest {
		return "", fmt.Errorf("%w: target manifest identity mismatch", ErrShippingIntegrity)
	}
	return shippingReceipt(target.TargetIdentity(), manifest), nil
}

func ReadShippingManifest(ctx context.Context, target ShippingObjectStore) (ShippingManifest, error) {
	if target == nil || strings.TrimSpace(target.TargetIdentity()) == "" {
		return ShippingManifest{}, fmt.Errorf("shipping target and identity are required")
	}
	manifestBytes, err := target.GetObject(ctx, "manifest.json", "")
	if err != nil {
		return ShippingManifest{}, fmt.Errorf("%w: read target manifest: %v", ErrShippingIntegrity, err)
	}
	var manifest ShippingManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return ShippingManifest{}, fmt.Errorf("%w: decode target manifest: %v", ErrShippingIntegrity, err)
	}
	if err := validateShippingManifest(manifest); err != nil {
		return ShippingManifest{}, err
	}
	digest, err := shippingManifestDigest(manifest)
	if err != nil || digest != manifest.ManifestDigest {
		return ShippingManifest{}, fmt.Errorf("%w: shipping manifest digest mismatch", ErrShippingIntegrity)
	}
	return manifest, nil
}

func ImportReadOnlyStandby(ctx context.Context, target ShippingObjectStore, manifest ShippingManifest, constraints ImportConstraints, standby StandbyImporter) (ImportResult, error) {
	if standby == nil {
		return ImportResult{}, fmt.Errorf("standby importer is required")
	}
	receipt, err := VerifyShippingTarget(ctx, target, manifest)
	if err != nil {
		return ImportResult{}, err
	}
	if err := validateImportConstraints(manifest, constraints, receipt); err != nil {
		return ImportResult{}, err
	}
	result := ImportResult{TargetVerificationReceipt: receipt}
	probe := []byte("namrbd-dr-standby-write-probe")
	if err := standby.ProbeExternalWrite(ctx, 0, probe); !errors.Is(err, ErrStandbyReadOnly) {
		return result, fmt.Errorf("standby external write was not rejected as read-only before import: %v", err)
	}
	result.StandbyWriteRejected = true
	for _, object := range manifest.Objects {
		data, err := target.GetObject(ctx, object.Key, object.Checksum)
		if err != nil {
			return result, fmt.Errorf("%w: import shipping object %s: %v", ErrShippingIntegrity, object.Key, err)
		}
		if err := standby.ImportRange(ctx, object.Offset, data); err != nil {
			return result, fmt.Errorf("import standby offset=%d length=%d: %w", object.Offset, object.Length, err)
		}
		result.ImportedBytes += object.Length
		result.ImportedObjects++
	}
	for _, object := range manifest.Objects {
		expected, err := target.GetObject(ctx, object.Key, object.Checksum)
		if err != nil {
			return result, fmt.Errorf("%w: reread target object %s: %v", ErrShippingIntegrity, object.Key, err)
		}
		actual, err := standby.ReadRange(ctx, object.Offset, object.Length)
		if err != nil {
			return result, fmt.Errorf("read imported standby offset=%d length=%d: %w", object.Offset, object.Length, err)
		}
		if len(actual) != len(expected) || shippingSHA256(actual) != shippingSHA256(expected) {
			return result, fmt.Errorf("%w: imported standby readback mismatch offset=%d", ErrShippingIntegrity, object.Offset)
		}
	}
	result.UserspaceReadbackMatched = true
	if err := standby.ProbeExternalWrite(ctx, 0, probe); !errors.Is(err, ErrStandbyReadOnly) {
		return result, fmt.Errorf("standby external write was not rejected as read-only after import: %v", err)
	}
	return result, nil
}

func verifyShippingCheckpoint(ctx context.Context, target ShippingObjectStore, spec ShippingSpec, checkpoint ShippingCheckpoint) (ShippingCheckpoint, bool, error) {
	if checkpoint.ShippingWorkerID == "" {
		return ShippingCheckpoint{ShippingWorkerID: spec.ShippingWorkerID, ShippedManifestID: spec.ShippedManifestID, SourceSnapshotID: spec.SourceSnapshotID, SourceSnapshotRootID: spec.SourceSnapshotRootID, Generation: spec.Generation}, false, nil
	}
	if checkpoint.ShippingWorkerID != spec.ShippingWorkerID || checkpoint.ShippedManifestID != spec.ShippedManifestID || checkpoint.SourceSnapshotID != spec.SourceSnapshotID || checkpoint.SourceSnapshotRootID != spec.SourceSnapshotRootID || checkpoint.Generation != spec.Generation {
		return ShippingCheckpoint{}, false, fmt.Errorf("%w: shipping checkpoint identity mismatch", ErrShippingIntegrity)
	}
	var nextOffset uint64
	for index, object := range checkpoint.Objects {
		wantKey := fmt.Sprintf("objects/%06d.bin", index)
		if object.Key != wantKey || object.Offset != nextOffset || object.Length == 0 || object.Offset > spec.ExpectedBytes || object.Length > spec.ExpectedBytes-object.Offset {
			return ShippingCheckpoint{}, false, fmt.Errorf("%w: invalid checkpoint object index=%d", ErrShippingIntegrity, index)
		}
		data, err := target.GetObject(ctx, object.Key, object.Checksum)
		if err != nil || uint64(len(data)) != object.Length || shippingSHA256(data) != object.Checksum {
			return ShippingCheckpoint{}, false, fmt.Errorf("%w: checkpoint target object %s is missing or corrupt", ErrShippingIntegrity, object.Key)
		}
		nextOffset += object.Length
	}
	if checkpoint.NextOffset != nextOffset || uint64(len(checkpoint.Objects)) > spec.ExpectedObjects {
		return ShippingCheckpoint{}, false, fmt.Errorf("%w: shipping checkpoint progress mismatch", ErrShippingIntegrity)
	}
	return cloneShippingCheckpoint(checkpoint), checkpoint.NextOffset > 0, nil
}

func validateShippingSpec(spec ShippingSpec) error {
	required := []string{spec.ShippingWorkerID, spec.ShippedManifestID, spec.RecoveryPointID, spec.SourceClusterID, spec.TargetClusterID, spec.SourceVolumeID, spec.TargetVolumeID, spec.SourceSnapshotID, spec.SourceSnapshotRootID, spec.ControlManifestDigest, spec.PayloadRootsDigest, spec.ReadViewDigest, spec.KeyPolicyDigest, spec.GovernanceDigest, spec.GeometryDigest, spec.CompatibilityDigest}
	for _, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("shipping spec immutable identity fields are required")
		}
	}
	if spec.ExpectedBytes == 0 || spec.ExpectedObjects == 0 || spec.Generation == 0 || spec.ChunkSizeBytes == 0 || spec.ChunkSizeBytes > maxShippingChunkBytes {
		return fmt.Errorf("shipping spec requires positive bytes, objects, generation, and chunk size up to 64 MiB")
	}
	wantObjects := (spec.ExpectedBytes-1)/spec.ChunkSizeBytes + 1
	if spec.ExpectedObjects != wantObjects || spec.ExpectedObjects > maxShippingObjectCount {
		return fmt.Errorf("shipping expected_objects=%d does not match bytes/chunk object count=%d", spec.ExpectedObjects, wantObjects)
	}
	return nil
}

func validateShippingManifest(manifest ShippingManifest) error {
	spec := ShippingSpec{
		ShippingWorkerID: manifest.ShippingWorkerID, ShippedManifestID: manifest.ShippedManifestID, RecoveryPointID: manifest.RecoveryPointID,
		SourceClusterID: manifest.SourceClusterID, TargetClusterID: manifest.TargetClusterID, SourceVolumeID: manifest.SourceVolumeID, TargetVolumeID: manifest.TargetVolumeID,
		SourceSnapshotID: manifest.SourceSnapshotID, SourceSnapshotRootID: manifest.SourceSnapshotRootID, ControlManifestDigest: manifest.ControlManifestDigest,
		PayloadRootsDigest: manifest.PayloadRootsDigest,
		ReadViewDigest:     manifest.ReadViewDigest, KeyPolicyDigest: manifest.KeyPolicyDigest, GovernanceDigest: manifest.GovernanceDigest,
		GeometryDigest: manifest.GeometryDigest, CompatibilityDigest: manifest.CompatibilityDigest, ExpectedBytes: manifest.ExpectedBytes,
		ExpectedObjects: manifest.ExpectedObjects, Generation: manifest.Generation, ChunkSizeBytes: manifest.ChunkSizeBytes, CreatedAt: manifest.CreatedAt,
	}
	if manifest.ManifestVersion != ShippingManifestVersion || manifest.ChecksumAlgorithm != ShippingChecksumSHA256 || manifest.CreatedAt.IsZero() {
		return fmt.Errorf("%w: unsupported or incomplete shipping manifest", ErrShippingIntegrity)
	}
	if err := validateShippingSpec(spec); err != nil {
		return fmt.Errorf("%w: %v", ErrShippingIntegrity, err)
	}
	if uint64(len(manifest.Objects)) != manifest.ExpectedObjects {
		return fmt.Errorf("%w: manifest object count=%d want=%d", ErrShippingIntegrity, len(manifest.Objects), manifest.ExpectedObjects)
	}
	return nil
}

func validateImportConstraints(manifest ShippingManifest, constraints ImportConstraints, receipt string) error {
	checks := []struct{ got, want, name string }{
		{manifest.ShippedManifestID, constraints.ShippedManifestID, "shipped_manifest_id"},
		{manifest.RecoveryPointID, constraints.RecoveryPointID, "recovery_point_id"},
		{manifest.SourceSnapshotID, constraints.SourceSnapshotID, "source_snapshot_id"},
		{manifest.SourceSnapshotRootID, constraints.SourceSnapshotRootID, "source_snapshot_root_id"},
		{manifest.PayloadRootsDigest, constraints.PayloadRootsDigest, "payload_roots_digest"},
		{manifest.GeometryDigest, constraints.GeometryDigest, "geometry_digest"},
		{manifest.CompatibilityDigest, constraints.CompatibilityDigest, "compatibility_digest"},
		{manifest.KeyPolicyDigest, constraints.KeyPolicyDigest, "key_policy_digest"},
		{manifest.GovernanceDigest, constraints.GovernanceDigest, "governance_digest"},
		{receipt, constraints.TargetVerificationReceipt, "target_verification_receipt"},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.want) == "" || check.got != check.want {
			return fmt.Errorf("%w: standby import %s mismatch", ErrShippingIntegrity, check.name)
		}
	}
	return nil
}

func newShippingManifest(spec ShippingSpec, objects []ShippingObjectRef) ShippingManifest {
	return ShippingManifest{
		ManifestVersion: ShippingManifestVersion, ShippingWorkerID: spec.ShippingWorkerID, ShippedManifestID: spec.ShippedManifestID,
		RecoveryPointID: spec.RecoveryPointID, SourceClusterID: spec.SourceClusterID, TargetClusterID: spec.TargetClusterID,
		SourceVolumeID: spec.SourceVolumeID, TargetVolumeID: spec.TargetVolumeID, SourceSnapshotID: spec.SourceSnapshotID,
		SourceSnapshotRootID: spec.SourceSnapshotRootID, ControlManifestDigest: spec.ControlManifestDigest, PayloadRootsDigest: spec.PayloadRootsDigest,
		ReadViewDigest: spec.ReadViewDigest, KeyPolicyDigest: spec.KeyPolicyDigest, GovernanceDigest: spec.GovernanceDigest,
		GeometryDigest: spec.GeometryDigest, CompatibilityDigest: spec.CompatibilityDigest, ExpectedBytes: spec.ExpectedBytes,
		ExpectedObjects: spec.ExpectedObjects, Generation: spec.Generation, ChunkSizeBytes: spec.ChunkSizeBytes,
		ChecksumAlgorithm: ShippingChecksumSHA256, Objects: append([]ShippingObjectRef(nil), objects...), CreatedAt: spec.CreatedAt.UTC(),
	}
}

func shippingManifestDigest(manifest ShippingManifest) (string, error) {
	manifest.ManifestDigest = ""
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return shippingSHA256(encoded), nil
}

func shippingReceipt(targetIdentity string, manifest ShippingManifest) string {
	payload := fmt.Sprintf("%s\n%s\n%d\n%d\n", strings.TrimSpace(targetIdentity), manifest.ManifestDigest, manifest.ExpectedBytes, manifest.ExpectedObjects)
	return shippingSHA256([]byte(payload))
}

func shippingSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func cloneShippingCheckpoint(checkpoint ShippingCheckpoint) ShippingCheckpoint {
	checkpoint.Objects = append([]ShippingObjectRef(nil), checkpoint.Objects...)
	return checkpoint
}

func minShippingUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
