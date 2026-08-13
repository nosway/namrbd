package metadata

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

type PayloadGCMutationStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (VolumeState, error)
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec MutationOperationRecord) error
}

func PayloadGCMutationOperationID(volumeID string) string {
	return fmt.Sprintf("payload-gc-%s", volumeID)
}

func PayloadGCBatchMutationOperationID(volumeID string, batchIndex int) string {
	return fmt.Sprintf("payload-gc-%s-batch-%06d", volumeID, batchIndex)
}

func EnsurePendingPayloadGCMutationOperation(ctx context.Context, store PayloadGCMutationStore, volumeID string, affectedExtentIDs, affectedPageNos, retiredPhysicalChunkIDs []uint64, now time.Time) error {
	affectedExtentIDs = uniqueSortedNonZeroChunkIDs(affectedExtentIDs)
	affectedPageNos = uniqueSortedUint64s(affectedPageNos)
	retiredPhysicalChunkIDs = uniqueSortedNonZeroChunkIDs(retiredPhysicalChunkIDs)
	if len(retiredPhysicalChunkIDs) == 0 {
		return nil
	}
	volumeState, err := store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return err
	}
	nowUnix := now.Unix()
	operationID := PayloadGCMutationOperationID(volumeID)
	operation, err := store.GetMutationOperation(ctx, volumeID, operationID)
	switch {
	case err == nil:
		operation.OperationID = operationID
		operation.VolumeID = volumeID
		operation.Kind = "payload_gc"
		operation.AllocationRevision = volumeState.Revision
		operation.WriterFencingEpoch = volumeState.Epoch
		operation.IdempotencyKey = volumeID
		operation.LastUpdatedAtUnix = nowUnix
		switch operation.State {
		case MutationOperationPending:
			operation.AffectedExtentIDs = unionSortedChunkIDs(operation.AffectedExtentIDs, affectedExtentIDs)
			operation.AffectedPageNos = unionSortedUint64s(operation.AffectedPageNos, affectedPageNos)
			operation.RetiredPhysicalChunkIDs = unionSortedChunkIDs(operation.RetiredPhysicalChunkIDs, retiredPhysicalChunkIDs)
			if operation.StartedAtUnix == 0 {
				operation.StartedAtUnix = nowUnix
			}
		case MutationOperationRunning:
			operation.AffectedExtentIDs = unionSortedChunkIDs(operation.AffectedExtentIDs, affectedExtentIDs)
			operation.AffectedPageNos = unionSortedUint64s(operation.AffectedPageNos, affectedPageNos)
			operation.RetiredPhysicalChunkIDs = unionSortedChunkIDs(operation.RetiredPhysicalChunkIDs, retiredPhysicalChunkIDs)
			if operation.StartedAtUnix == 0 {
				operation.StartedAtUnix = nowUnix
			}
		default:
			operation.State = MutationOperationPending
			operation.StartedAtUnix = nowUnix
			operation.ErrorMessage = ""
			operation.AffectedExtentIDs = affectedExtentIDs
			operation.AffectedPageNos = affectedPageNos
			operation.RetiredPhysicalChunkIDs = retiredPhysicalChunkIDs
		}
	case errors.Is(err, ErrNotFound):
		operation = MutationOperationRecord{
			OperationID:             operationID,
			VolumeID:                volumeID,
			Kind:                    "payload_gc",
			State:                   MutationOperationPending,
			AllocationRevision:      volumeState.Revision,
			WriterFencingEpoch:      volumeState.Epoch,
			IdempotencyKey:          volumeID,
			AffectedExtentIDs:       affectedExtentIDs,
			AffectedPageNos:         affectedPageNos,
			StartedAtUnix:           nowUnix,
			LastUpdatedAtUnix:       nowUnix,
			RetiredPhysicalChunkIDs: retiredPhysicalChunkIDs,
		}
	default:
		return err
	}
	return store.PutMutationOperation(ctx, operation)
}

func unionSortedChunkIDs(left, right []uint64) []uint64 {
	combined := append(append([]uint64(nil), left...), right...)
	return uniqueSortedNonZeroChunkIDs(combined)
}

func unionSortedUint64s(left, right []uint64) []uint64 {
	combined := append(append([]uint64(nil), left...), right...)
	return uniqueSortedUint64s(combined)
}

func uniqueSortedNonZeroChunkIDs(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(values))
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueSortedUint64s(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(values))
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
