package metadata

import (
	"context"
	"fmt"
	"strings"
)

type PhysicalObjectResolverStore interface {
	GetPhysicalObject(ctx context.Context, volumeID, objectID string) (PhysicalObjectRecord, error)
	GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (ECStripeRecord, error)
}

type ResolvedAllocationEntry struct {
	Entry          AllocationEntry       `json:"entry"`
	PhysicalObject *PhysicalObjectRecord `json:"physical_object,omitempty"`
	ECStripe       *ECStripeRecord       `json:"ec_stripe,omitempty"`
}

func ResolveAllocationEntriesFromPage(ctx context.Context, store PhysicalObjectResolverStore, page AllocationPageRecord) ([]ResolvedAllocationEntry, error) {
	out := make([]ResolvedAllocationEntry, 0, len(page.Extents))
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
			if strings.TrimSpace(extent.BackingRef) != "" {
				return nil, fmt.Errorf("zero allocation extent[%d] has backing_ref", idx)
			}
			if extent.PhysicalChunkStart != 0 {
				return nil, fmt.Errorf("zero allocation extent[%d] has physical_chunk_start=%d", idx, extent.PhysicalChunkStart)
			}
			entry.State = AllocationEntryStateZero
			out = append(out, ResolvedAllocationEntry{Entry: entry})
		case AllocationKindData, AllocationKindShared:
			if extent.Kind == AllocationKindShared {
				entry.State = AllocationEntryStateShared
			} else {
				entry.State = AllocationEntryStateAllocated
			}
			resolved, err := resolvePhysicalAllocationEntry(ctx, store, page, extent, entry, idx)
			if err != nil {
				return nil, err
			}
			out = append(out, resolved)
		default:
			return nil, fmt.Errorf("allocation extent[%d] has unsupported kind=%q", idx, extent.Kind)
		}
	}
	return out, nil
}

func resolvePhysicalAllocationEntry(ctx context.Context, store PhysicalObjectResolverStore, page AllocationPageRecord, extent AllocationExtentRecord, entry AllocationEntry, idx int) (ResolvedAllocationEntry, error) {
	backingRef := strings.TrimSpace(extent.BackingRef)
	if backingRef == "" {
		if extent.PhysicalChunkStart == 0 {
			return ResolvedAllocationEntry{}, fmt.Errorf("%s allocation extent[%d] has neither backing_ref nor physical_chunk_start", extent.Kind, idx)
		}
		entry.PhysicalObjectRef = replicatedPhysicalObjectRef(page, extent)
		return ResolvedAllocationEntry{Entry: entry}, nil
	}
	if extent.PhysicalChunkStart != 0 {
		return ResolvedAllocationEntry{}, fmt.Errorf("%s allocation extent[%d] must not mix backing_ref with physical_chunk_start", extent.Kind, idx)
	}
	object, err := store.GetPhysicalObject(ctx, page.VolumeID, backingRef)
	if err != nil {
		return ResolvedAllocationEntry{}, fmt.Errorf("resolve backing_ref %q: %w", backingRef, err)
	}
	ref := object.Ref()
	if err := ref.Validate(); err != nil {
		return ResolvedAllocationEntry{}, fmt.Errorf("validate backing_ref %q: %w", backingRef, err)
	}
	entry.PhysicalObjectRef = &ref
	resolved := ResolvedAllocationEntry{
		Entry:          entry,
		PhysicalObject: &object,
	}
	if ref.BackendType == PhysicalObjectBackendEC {
		stripe, err := store.GetECStripe(ctx, page.VolumeID, ref.EC.StripeID, ref.EC.StripeGeneration)
		if err != nil {
			return ResolvedAllocationEntry{}, fmt.Errorf("resolve ec stripe backing_ref %q: %w", backingRef, err)
		}
		if stripe.ObjectID != object.ObjectID {
			return ResolvedAllocationEntry{}, fmt.Errorf("ec stripe object_id=%q does not match backing_ref %q", stripe.ObjectID, object.ObjectID)
		}
		resolved.ECStripe = &stripe
	}
	return resolved, nil
}
