package metadata

import "fmt"

type PhysicalObjectBackendType string

const (
	PhysicalObjectBackendReplicated PhysicalObjectBackendType = "replicated"
	PhysicalObjectBackendEC         PhysicalObjectBackendType = "ec"
)

type AllocationEntryState string

const (
	AllocationEntryStateZero      AllocationEntryState = "zero"
	AllocationEntryStateAllocated AllocationEntryState = "allocated"
	AllocationEntryStateShared    AllocationEntryState = "shared"
	AllocationEntryStateDeleted   AllocationEntryState = "deleted"
	AllocationEntryStatePending   AllocationEntryState = "pending"
)

type ReplicatedPhysicalObjectDescriptor struct {
	PhysicalChunkStart uint64 `json:"physical_chunk_start"`
	ChunkCount         uint32 `json:"chunk_count"`
}

type ECPhysicalObjectDescriptor struct {
	ProfileID        string   `json:"profile_id"`
	StripeID         string   `json:"stripe_id"`
	StripeGeneration uint64   `json:"stripe_generation"`
	StripeUnitBytes  uint32   `json:"stripe_unit_bytes"`
	DataShards       uint32   `json:"data_shards"`
	CodingShards     uint32   `json:"coding_shards"`
	StripeOffset     uint64   `json:"stripe_offset,omitempty"`
	DataShardRefs    []string `json:"data_shard_refs,omitempty"`
	CodeShardRefs    []string `json:"code_shard_refs,omitempty"`
}

type PhysicalObjectRef struct {
	BackendType   PhysicalObjectBackendType           `json:"backend_type"`
	ObjectID      string                              `json:"object_id"`
	PlacementRef  string                              `json:"placement_ref,omitempty"`
	LogicalLength uint64                              `json:"logical_length"`
	Generation    uint64                              `json:"generation,omitempty"`
	Checksum      string                              `json:"checksum,omitempty"`
	Encryption    *PayloadEncryptionHeader            `json:"encryption,omitempty"`
	Replicated    *ReplicatedPhysicalObjectDescriptor `json:"replicated,omitempty"`
	EC            *ECPhysicalObjectDescriptor         `json:"ec,omitempty"`
}

type AllocationEntry struct {
	LogicalChunkStart uint64               `json:"logical_chunk_start"`
	ChunkCount        uint32               `json:"chunk_count"`
	State             AllocationEntryState `json:"state"`
	PhysicalObjectRef *PhysicalObjectRef   `json:"physical_object_ref,omitempty"`
	Generation        uint64               `json:"generation,omitempty"`
	Checksum          string               `json:"checksum,omitempty"`
}

func (ref PhysicalObjectRef) Validate() error {
	if ref.BackendType == "" {
		return fmt.Errorf("physical object ref missing backend_type")
	}
	if ref.ObjectID == "" {
		return fmt.Errorf("physical object ref missing object_id")
	}
	if ref.LogicalLength == 0 {
		return fmt.Errorf("physical object ref %q has zero logical_length", ref.ObjectID)
	}

	switch ref.BackendType {
	case PhysicalObjectBackendReplicated:
		if ref.Replicated == nil {
			return fmt.Errorf("replicated physical object ref %q missing replicated descriptor", ref.ObjectID)
		}
		if ref.Replicated.PhysicalChunkStart == 0 {
			return fmt.Errorf("replicated physical object ref %q has zero physical_chunk_start", ref.ObjectID)
		}
		if ref.Replicated.ChunkCount == 0 {
			return fmt.Errorf("replicated physical object ref %q has zero chunk_count", ref.ObjectID)
		}
	case PhysicalObjectBackendEC:
		if ref.EC == nil {
			return fmt.Errorf("ec physical object ref %q missing ec descriptor", ref.ObjectID)
		}
		if ref.EC.ProfileID == "" {
			return fmt.Errorf("ec physical object ref %q missing profile_id", ref.ObjectID)
		}
		if ref.EC.StripeID == "" {
			return fmt.Errorf("ec physical object ref %q missing stripe_id", ref.ObjectID)
		}
		if ref.EC.StripeGeneration == 0 {
			return fmt.Errorf("ec physical object ref %q has zero stripe_generation", ref.ObjectID)
		}
		if ref.EC.StripeUnitBytes == 0 {
			return fmt.Errorf("ec physical object ref %q has zero stripe_unit_bytes", ref.ObjectID)
		}
		if ref.EC.DataShards == 0 {
			return fmt.Errorf("ec physical object ref %q has zero data_shards", ref.ObjectID)
		}
		if ref.EC.CodingShards == 0 {
			return fmt.Errorf("ec physical object ref %q has zero coding_shards", ref.ObjectID)
		}
	default:
		return fmt.Errorf("physical object ref %q has unsupported backend_type=%q", ref.ObjectID, ref.BackendType)
	}

	if ref.Encryption != nil {
		if err := ref.Encryption.ValidateForPhysicalObject(ref.ObjectID, ref.BackendType, ref.LogicalLength); err != nil {
			return fmt.Errorf("physical object ref %q encryption header: %w", ref.ObjectID, err)
		}
	}

	return nil
}

func (entry AllocationEntry) ValidateCommittedReadViewEntry() error {
	if entry.ChunkCount == 0 {
		return fmt.Errorf("allocation entry has zero chunk_count")
	}

	switch entry.State {
	case AllocationEntryStateZero:
		if entry.PhysicalObjectRef != nil {
			return fmt.Errorf("zero allocation entry has physical object ref")
		}
	case AllocationEntryStateAllocated, AllocationEntryStateShared:
		if entry.PhysicalObjectRef == nil {
			return fmt.Errorf("%s allocation entry missing physical object ref", entry.State)
		}
		if err := entry.PhysicalObjectRef.Validate(); err != nil {
			return err
		}
	case AllocationEntryStateDeleted, AllocationEntryStatePending:
		return fmt.Errorf("%s allocation entry is not visible in committed read views", entry.State)
	default:
		return fmt.Errorf("allocation entry has unsupported state=%q", entry.State)
	}

	return nil
}

func AllocationEntriesFromPage(page AllocationPageRecord) ([]AllocationEntry, error) {
	out := make([]AllocationEntry, 0, len(page.Extents))
	for idx, extent := range page.Extents {
		if extent.ChunkCount == 0 {
			return nil, fmt.Errorf("allocation extent[%d] has zero chunk_count", idx)
		}

		entry := AllocationEntry{
			LogicalChunkStart: extent.LogicalChunkStart,
			ChunkCount:        extent.ChunkCount,
			Generation:        extent.Generation,
			Checksum:          extent.Checksum,
		}

		switch extent.Kind {
		case AllocationKindZero:
			if extent.PhysicalChunkStart != 0 {
				return nil, fmt.Errorf("zero allocation extent[%d] has physical_chunk_start=%d", idx, extent.PhysicalChunkStart)
			}
			if extent.Encryption != nil {
				return nil, fmt.Errorf("zero allocation extent[%d] has encryption header", idx)
			}
			entry.State = AllocationEntryStateZero
		case AllocationKindData, AllocationKindShared:
			if extent.PhysicalChunkStart == 0 {
				return nil, fmt.Errorf("%s allocation extent[%d] has zero physical_chunk_start", extent.Kind, idx)
			}
			if extent.Kind == AllocationKindShared {
				entry.State = AllocationEntryStateShared
			} else {
				entry.State = AllocationEntryStateAllocated
			}
			entry.PhysicalObjectRef = replicatedPhysicalObjectRef(page, extent)
			if err := entry.PhysicalObjectRef.Validate(); err != nil {
				return nil, fmt.Errorf("%s allocation extent[%d]: %w", extent.Kind, idx, err)
			}
		default:
			return nil, fmt.Errorf("allocation extent[%d] has unsupported kind=%q", idx, extent.Kind)
		}

		out = append(out, entry)
	}
	return out, nil
}

func replicatedPhysicalObjectRef(page AllocationPageRecord, extent AllocationExtentRecord) *PhysicalObjectRef {
	return &PhysicalObjectRef{
		BackendType:   PhysicalObjectBackendReplicated,
		ObjectID:      fmt.Sprintf("replicated:%s:%d", page.VolumeID, extent.PhysicalChunkStart),
		LogicalLength: uint64(extent.ChunkCount) * uint64(page.ChunkSizeBytes),
		Generation:    extent.Generation,
		Checksum:      extent.Checksum,
		Encryption:    clonePayloadEncryptionHeader(extent.Encryption),
		Replicated: &ReplicatedPhysicalObjectDescriptor{
			PhysicalChunkStart: extent.PhysicalChunkStart,
			ChunkCount:         extent.ChunkCount,
		},
	}
}
