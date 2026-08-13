package metadata

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type PhysicalObjectState string

const (
	PhysicalObjectStatePreparing PhysicalObjectState = "preparing"
	PhysicalObjectStateCommitted PhysicalObjectState = "committed"
	PhysicalObjectStateRetired   PhysicalObjectState = "retired"
)

type ECStripeState string

const (
	ECStripeStatePreparing ECStripeState = "preparing"
	ECStripeStateCommitted ECStripeState = "committed"
	ECStripeStateRetired   ECStripeState = "retired"
)

type ECShardRole string

const (
	ECShardRoleData   ECShardRole = "data"
	ECShardRoleCoding ECShardRole = "coding"
)

type PhysicalObjectRecord struct {
	VolumeID      string                              `json:"volume_id"`
	ObjectID      string                              `json:"object_id"`
	BackendType   PhysicalObjectBackendType           `json:"backend_type"`
	PlacementRef  string                              `json:"placement_ref,omitempty"`
	LogicalLength uint64                              `json:"logical_length"`
	Generation    uint64                              `json:"generation,omitempty"`
	Checksum      string                              `json:"checksum,omitempty"`
	Encryption    *PayloadEncryptionHeader            `json:"encryption,omitempty"`
	State         PhysicalObjectState                 `json:"state"`
	Replicated    *ReplicatedPhysicalObjectDescriptor `json:"replicated,omitempty"`
	EC            *ECPhysicalObjectDescriptor         `json:"ec,omitempty"`
	CreatedAtUnix int64                               `json:"created_at_unix,omitempty"`
	UpdatedAtUnix int64                               `json:"updated_at_unix,omitempty"`
}

type ECShardRecord struct {
	ShardID       uint32                   `json:"shard_id"`
	Role          ECShardRole              `json:"role"`
	RoleIndex     uint32                   `json:"role_index"`
	Zone          string                   `json:"zone"`
	NodeID        string                   `json:"node_id"`
	StoreID       string                   `json:"store_id"`
	ShardObjectID string                   `json:"shard_object_id,omitempty"`
	Checksum      string                   `json:"checksum,omitempty"`
	SizeBytes     uint32                   `json:"size_bytes,omitempty"`
	Encryption    *PayloadEncryptionHeader `json:"encryption,omitempty"`
}

type ECStripeRecord struct {
	VolumeID         string          `json:"volume_id"`
	ObjectID         string          `json:"object_id"`
	ProfileID        string          `json:"profile_id"`
	StripeID         string          `json:"stripe_id"`
	StripeGeneration uint64          `json:"stripe_generation"`
	StripeUnitBytes  uint32          `json:"stripe_unit_bytes"`
	DataShards       uint32          `json:"data_shards"`
	CodingShards     uint32          `json:"coding_shards"`
	TopologyRevision uint64          `json:"topology_revision,omitempty"`
	State            ECStripeState   `json:"state"`
	Shards           []ECShardRecord `json:"shards"`
	CreatedAtUnix    int64           `json:"created_at_unix,omitempty"`
	UpdatedAtUnix    int64           `json:"updated_at_unix,omitempty"`
}

func NormalizePhysicalObjectRecord(rec PhysicalObjectRecord) PhysicalObjectRecord {
	rec.VolumeID = strings.TrimSpace(rec.VolumeID)
	rec.ObjectID = strings.TrimSpace(rec.ObjectID)
	rec.PlacementRef = strings.TrimSpace(rec.PlacementRef)
	if rec.State == "" {
		rec.State = PhysicalObjectStatePreparing
	}
	return rec
}

func NormalizeECStripeRecord(rec ECStripeRecord) ECStripeRecord {
	rec.VolumeID = strings.TrimSpace(rec.VolumeID)
	rec.ObjectID = strings.TrimSpace(rec.ObjectID)
	rec.ProfileID = strings.TrimSpace(rec.ProfileID)
	rec.StripeID = strings.TrimSpace(rec.StripeID)
	if rec.State == "" {
		rec.State = ECStripeStatePreparing
	}
	for i := range rec.Shards {
		rec.Shards[i].Zone = strings.TrimSpace(rec.Shards[i].Zone)
		rec.Shards[i].NodeID = strings.TrimSpace(rec.Shards[i].NodeID)
		rec.Shards[i].StoreID = strings.TrimSpace(rec.Shards[i].StoreID)
		rec.Shards[i].ShardObjectID = strings.TrimSpace(rec.Shards[i].ShardObjectID)
	}
	return rec
}

func (rec PhysicalObjectRecord) Ref() PhysicalObjectRef {
	rec = NormalizePhysicalObjectRecord(rec)
	return PhysicalObjectRef{
		BackendType:   rec.BackendType,
		ObjectID:      rec.ObjectID,
		PlacementRef:  rec.PlacementRef,
		LogicalLength: rec.LogicalLength,
		Generation:    rec.Generation,
		Checksum:      rec.Checksum,
		Encryption:    clonePayloadEncryptionHeader(rec.Encryption),
		Replicated:    rec.Replicated,
		EC:            rec.EC,
	}
}

func ValidatePhysicalObjectRecord(rec PhysicalObjectRecord) error {
	rec = NormalizePhysicalObjectRecord(rec)
	if _, err := CanonicalVolumeID(rec.VolumeID); err != nil {
		return fmt.Errorf("invalid physical object volume_id %q: %w", rec.VolumeID, err)
	}
	if err := validatePhysicalObjectID(rec.ObjectID); err != nil {
		return err
	}
	switch rec.State {
	case PhysicalObjectStatePreparing, PhysicalObjectStateCommitted, PhysicalObjectStateRetired:
	default:
		return fmt.Errorf("unsupported physical object state %q", rec.State)
	}
	ref := rec.Ref()
	if err := ref.Validate(); err != nil {
		return err
	}
	if ref.ObjectID != rec.ObjectID {
		return fmt.Errorf("physical object ref object_id mismatch")
	}
	if (rec.Encryption == nil) != (ref.Encryption == nil) {
		return fmt.Errorf("physical object ref encryption header mismatch")
	}
	return nil
}

func ValidateECStripeRecord(rec ECStripeRecord) error {
	rec = NormalizeECStripeRecord(rec)
	if _, err := CanonicalVolumeID(rec.VolumeID); err != nil {
		return fmt.Errorf("invalid ec stripe volume_id %q: %w", rec.VolumeID, err)
	}
	if err := validatePhysicalObjectID(rec.ObjectID); err != nil {
		return err
	}
	if err := ValidateECProfileID(rec.ProfileID); err != nil {
		return err
	}
	if err := validateECStripeID(rec.StripeID); err != nil {
		return err
	}
	if rec.StripeGeneration == 0 {
		return fmt.Errorf("stripe_generation is required")
	}
	if rec.StripeUnitBytes == 0 {
		return fmt.Errorf("stripe_unit_bytes is required")
	}
	if rec.DataShards == 0 {
		return fmt.Errorf("data_shards is required")
	}
	if rec.CodingShards == 0 {
		return fmt.Errorf("coding_shards is required")
	}
	switch rec.State {
	case ECStripeStatePreparing, ECStripeStateCommitted, ECStripeStateRetired:
	default:
		return fmt.Errorf("unsupported ec stripe state %q", rec.State)
	}
	totalShards := int(rec.DataShards + rec.CodingShards)
	if len(rec.Shards) != totalShards {
		return fmt.Errorf("ec stripe shard count=%d want=%d", len(rec.Shards), totalShards)
	}
	seenShardIDs := make(map[uint32]struct{}, totalShards)
	seenDataRoleIndexes := make(map[uint32]struct{}, rec.DataShards)
	seenCodingRoleIndexes := make(map[uint32]struct{}, rec.CodingShards)
	dataRoleCount := uint32(0)
	codingRoleCount := uint32(0)
	for idx, shard := range rec.Shards {
		if shard.ShardID >= uint32(totalShards) {
			return fmt.Errorf("ec shard[%d] id=%d out of range", idx, shard.ShardID)
		}
		if _, exists := seenShardIDs[shard.ShardID]; exists {
			return fmt.Errorf("ec shard[%d] duplicate shard_id=%d", idx, shard.ShardID)
		}
		seenShardIDs[shard.ShardID] = struct{}{}
		if shard.Zone == "" {
			return fmt.Errorf("ec shard[%d] missing zone", idx)
		}
		if shard.NodeID == "" {
			return fmt.Errorf("ec shard[%d] missing node_id", idx)
		}
		if shard.StoreID == "" {
			return fmt.Errorf("ec shard[%d] missing store_id", idx)
		}
		if shard.Encryption != nil {
			if err := shard.Encryption.ValidateForECShard(shard.ShardObjectID, rec.StripeID, shard.ShardID, shard.SizeBytes); err != nil {
				return fmt.Errorf("ec shard[%d] encryption header: %w", idx, err)
			}
		}
		switch shard.Role {
		case ECShardRoleData:
			if shard.RoleIndex >= rec.DataShards {
				return fmt.Errorf("ec data shard[%d] role_index=%d out of range", idx, shard.RoleIndex)
			}
			if _, exists := seenDataRoleIndexes[shard.RoleIndex]; exists {
				return fmt.Errorf("ec data shard[%d] duplicate role_index=%d", idx, shard.RoleIndex)
			}
			seenDataRoleIndexes[shard.RoleIndex] = struct{}{}
			dataRoleCount++
		case ECShardRoleCoding:
			if shard.RoleIndex >= rec.CodingShards {
				return fmt.Errorf("ec coding shard[%d] role_index=%d out of range", idx, shard.RoleIndex)
			}
			if _, exists := seenCodingRoleIndexes[shard.RoleIndex]; exists {
				return fmt.Errorf("ec coding shard[%d] duplicate role_index=%d", idx, shard.RoleIndex)
			}
			seenCodingRoleIndexes[shard.RoleIndex] = struct{}{}
			codingRoleCount++
		default:
			return fmt.Errorf("ec shard[%d] unsupported role %q", idx, shard.Role)
		}
	}
	if dataRoleCount != rec.DataShards {
		return fmt.Errorf("ec data shard count=%d want=%d", dataRoleCount, rec.DataShards)
	}
	if codingRoleCount != rec.CodingShards {
		return fmt.Errorf("ec coding shard count=%d want=%d", codingRoleCount, rec.CodingShards)
	}
	return nil
}

func (r *Repository) PutPhysicalObject(ctx context.Context, rec PhysicalObjectRecord) error {
	rec = NormalizePhysicalObjectRecord(rec)
	canon, err := CanonicalVolumeID(rec.VolumeID)
	if err != nil {
		return fmt.Errorf("invalid physical object volume_id %q: %w", rec.VolumeID, err)
	}
	rec.VolumeID = canon
	if err := ValidatePhysicalObjectRecord(rec); err != nil {
		return err
	}
	return r.putJSON(ctx, physicalObjectKey(r.root, rec.VolumeID, rec.ObjectID), rec)
}

func (r *Repository) GetPhysicalObject(ctx context.Context, volumeID, objectID string) (PhysicalObjectRecord, error) {
	canon, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return PhysicalObjectRecord{}, fmt.Errorf("invalid physical object volume_id %q: %w", volumeID, err)
	}
	if err := validatePhysicalObjectID(objectID); err != nil {
		return PhysicalObjectRecord{}, err
	}
	var rec PhysicalObjectRecord
	if err := r.getJSON(ctx, physicalObjectKey(r.root, canon, strings.TrimSpace(objectID)), &rec); err != nil {
		return PhysicalObjectRecord{}, err
	}
	return NormalizePhysicalObjectRecord(rec), nil
}

func (r *Repository) ListPhysicalObjects(ctx context.Context, volumeID string) ([]PhysicalObjectRecord, error) {
	canon, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return nil, fmt.Errorf("invalid physical object volume_id %q: %w", volumeID, err)
	}
	keys, err := r.listAll(ctx, physicalObjectsPrefix(r.root, canon))
	if err != nil {
		return nil, err
	}
	out := make([]PhysicalObjectRecord, 0, len(keys))
	for _, key := range keys {
		var rec PhysicalObjectRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, NormalizePhysicalObjectRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ObjectID < out[j].ObjectID })
	return out, nil
}

func (r *Repository) PutECStripe(ctx context.Context, rec ECStripeRecord) error {
	rec = NormalizeECStripeRecord(rec)
	canon, err := CanonicalVolumeID(rec.VolumeID)
	if err != nil {
		return fmt.Errorf("invalid ec stripe volume_id %q: %w", rec.VolumeID, err)
	}
	rec.VolumeID = canon
	if err := ValidateECStripeRecord(rec); err != nil {
		return err
	}
	return r.putJSON(ctx, ecStripeKey(r.root, rec.VolumeID, rec.StripeID, rec.StripeGeneration), rec)
}

func (r *Repository) GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (ECStripeRecord, error) {
	canon, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return ECStripeRecord{}, fmt.Errorf("invalid ec stripe volume_id %q: %w", volumeID, err)
	}
	if err := validateECStripeID(stripeID); err != nil {
		return ECStripeRecord{}, err
	}
	if stripeGeneration == 0 {
		return ECStripeRecord{}, fmt.Errorf("stripe_generation is required")
	}
	var rec ECStripeRecord
	if err := r.getJSON(ctx, ecStripeKey(r.root, canon, strings.TrimSpace(stripeID), stripeGeneration), &rec); err != nil {
		return ECStripeRecord{}, err
	}
	return NormalizeECStripeRecord(rec), nil
}

func (r *Repository) ListECStripes(ctx context.Context, volumeID string) ([]ECStripeRecord, error) {
	canon, err := CanonicalVolumeID(volumeID)
	if err != nil {
		return nil, fmt.Errorf("invalid ec stripe volume_id %q: %w", volumeID, err)
	}
	keys, err := r.listAll(ctx, ecStripesPrefix(r.root, canon))
	if err != nil {
		return nil, err
	}
	out := make([]ECStripeRecord, 0, len(keys))
	for _, key := range keys {
		var rec ECStripeRecord
		if err := r.getJSON(ctx, key, &rec); err != nil {
			return nil, err
		}
		out = append(out, NormalizeECStripeRecord(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StripeID != out[j].StripeID {
			return out[i].StripeID < out[j].StripeID
		}
		return out[i].StripeGeneration < out[j].StripeGeneration
	})
	return out, nil
}

func validatePhysicalObjectID(objectID string) error {
	objectID = strings.TrimSpace(objectID)
	if objectID == "" {
		return fmt.Errorf("object_id is required")
	}
	if strings.Contains(objectID, "/") {
		return fmt.Errorf("object_id must not contain '/'")
	}
	return nil
}

func validateECStripeID(stripeID string) error {
	stripeID = strings.TrimSpace(stripeID)
	if stripeID == "" {
		return fmt.Errorf("stripe_id is required")
	}
	if strings.Contains(stripeID, "/") {
		return fmt.Errorf("stripe_id must not contain '/'")
	}
	return nil
}

func physicalObjectsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/physical_objects/", root, mustCanonicalVolumeID(volumeID))
}

func physicalObjectKey(root, volumeID, objectID string) string {
	return fmt.Sprintf("%s%s", physicalObjectsPrefix(root, volumeID), strings.TrimSpace(objectID))
}

func ecStripesPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/ec/stripes/", root, mustCanonicalVolumeID(volumeID))
}

func ecStripeKey(root, volumeID, stripeID string, stripeGeneration uint64) string {
	return fmt.Sprintf("%s%s/generations/%020d", ecStripesPrefix(root, volumeID), strings.TrimSpace(stripeID), stripeGeneration)
}
