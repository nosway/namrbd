package replication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type LocalPayloadSweepResult struct {
	VolumeID       string
	ReplicaID      string
	CandidateCount int
	DeletedCount   int
	RetainedCount  int
}

type LocalPayloadGarbageCollector struct {
	repo           *metadata.Repository
	resolve        *metadata.Service
	stores         map[string]store.ObjectStore
	chunkBatchSize int
}

type payloadGCChunkBatch struct {
	BatchIndex int
	ChunkIDs   map[uint64]struct{}
}

const defaultPayloadGCChunkBatchSize = 64

func NewLocalPayloadGarbageCollector(repo *metadata.Repository, stores map[string]store.ObjectStore) *LocalPayloadGarbageCollector {
	cloned := make(map[string]store.ObjectStore, len(stores))
	for replicaID, payloadStore := range stores {
		cloned[replicaID] = payloadStore
	}
	return &LocalPayloadGarbageCollector{
		repo:           repo,
		resolve:        metadata.NewService(repo),
		stores:         cloned,
		chunkBatchSize: defaultPayloadGCChunkBatchSize,
	}
}

func (c *LocalPayloadGarbageCollector) SweepAll(ctx context.Context) ([]LocalPayloadSweepResult, error) {
	volumes, err := c.repo.ListVolumeStates(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]LocalPayloadSweepResult, 0)
	for _, volume := range volumes {
		swept, err := c.SweepVolume(ctx, volume.VolumeID)
		if err != nil {
			return nil, err
		}
		results = append(results, swept...)
	}
	return results, nil
}

func (c *LocalPayloadGarbageCollector) SweepVolume(ctx context.Context, volumeID string) ([]LocalPayloadSweepResult, error) {
	if c.repo == nil {
		return nil, fmt.Errorf("metadata repository is required")
	}
	canonicalVolumeID, err := metadata.CanonicalVolumeID(volumeID)
	if err != nil {
		canonicalVolumeID = volumeID
	}
	operation, trackOperation := c.beginSweepMutationOperation(ctx, canonicalVolumeID)
	if trackOperation {
		defer func() {
			if operation.State == metadata.MutationOperationCommitted || operation.State == metadata.MutationOperationFailed {
				return
			}
			operation.State = metadata.MutationOperationCommitted
			operation.LastUpdatedAtUnix = time.Now().Unix()
			operation.ErrorMessage = ""
			_ = c.repo.PutMutationOperation(context.Background(), operation)
		}()
	}
	targetExtentIDs := sliceToUint64Set(operation.AffectedExtentIDs)
	targetPageNos := sliceToUint64SetAllowZero(operation.AffectedPageNos)
	targetChunkIDs := sliceToUint64Set(operation.RetiredPhysicalChunkIDs)
	if len(targetExtentIDs) == 0 && len(targetPageNos) > 0 {
		derived, err := c.targetExtentIDsFromPages(ctx, volumeID, targetPageNos)
		if err != nil {
			if trackOperation {
				operation.State = metadata.MutationOperationFailed
				operation.LastUpdatedAtUnix = time.Now().Unix()
				operation.ErrorMessage = err.Error()
				_ = c.repo.PutMutationOperation(context.Background(), operation)
			}
			return nil, err
		}
		targetExtentIDs = derived
	}
	referencedKeys, activeExtentIDs, nativeAllocation, err := c.referencedPayloadKeys(ctx, volumeID, targetExtentIDs)
	if err != nil {
		if trackOperation {
			operation.State = metadata.MutationOperationFailed
			operation.LastUpdatedAtUnix = time.Now().Unix()
			operation.ErrorMessage = err.Error()
			_ = c.repo.PutMutationOperation(context.Background(), operation)
		}
		return nil, err
	}
	hintedChunkIDs, err := c.retiredPayloadHintChunkIDs(ctx, canonicalVolumeID)
	if err != nil {
		if trackOperation {
			operation.State = metadata.MutationOperationFailed
			operation.LastUpdatedAtUnix = time.Now().Unix()
			operation.ErrorMessage = err.Error()
			_ = c.repo.PutMutationOperation(context.Background(), operation)
		}
		return nil, err
	}
	deletedChunkIDs := make(map[uint64]struct{})

	replicaIDs := make([]string, 0, len(c.stores))
	for replicaID := range c.stores {
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Strings(replicaIDs)

	resultsByReplica := make(map[string]*LocalPayloadSweepResult, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		resultsByReplica[replicaID] = &LocalPayloadSweepResult{VolumeID: canonicalVolumeID, ReplicaID: replicaID}
	}
	for _, chunkBatch := range c.buildChunkBatches(ctx, canonicalVolumeID, targetChunkIDs) {
		batchOperation, trackBatch := c.beginSweepBatchMutationOperation(ctx, canonicalVolumeID, chunkBatch.BatchIndex, operation, chunkBatch.ChunkIDs)
		for _, replicaID := range replicaIDs {
			payloadStore := c.stores[replicaID]
			keys, err := c.listCandidatePayloadKeys(ctx, payloadStore, replicaID, canonicalVolumeID, targetExtentIDs, chunkBatch.ChunkIDs)
			if err != nil {
				if trackBatch {
					batchOperation.State = metadata.MutationOperationFailed
					batchOperation.LastUpdatedAtUnix = time.Now().Unix()
					batchOperation.ErrorMessage = err.Error()
					_ = c.repo.PutMutationOperation(context.Background(), batchOperation)
				}
				if trackOperation {
					operation.State = metadata.MutationOperationFailed
					operation.LastUpdatedAtUnix = time.Now().Unix()
					operation.ErrorMessage = err.Error()
					_ = c.repo.PutMutationOperation(context.Background(), operation)
				}
				return nil, err
			}
			sort.SliceStable(keys, func(i, j int) bool {
				_, _, _, leftChunkID, leftOK := parseLocalReplicaPayloadKey(keys[i])
				_, _, _, rightChunkID, rightOK := parseLocalReplicaPayloadKey(keys[j])
				_, leftHinted := hintedChunkIDs[leftChunkID]
				_, rightHinted := hintedChunkIDs[rightChunkID]
				if !leftOK {
					leftHinted = false
				}
				if !rightOK {
					rightHinted = false
				}
				if leftHinted != rightHinted {
					return leftHinted
				}
				return keys[i] < keys[j]
			})
			result := resultsByReplica[replicaID]
			result.CandidateCount += len(keys)
			for _, key := range keys {
				_, keyVolumeID, extentID, _, ok := parseLocalReplicaPayloadKey(key)
				if !ok || keyVolumeID != canonicalVolumeID {
					continue
				}
				keep := false
				if nativeAllocation {
					_, keep = referencedKeys[key]
				} else {
					_, keep = activeExtentIDs[extentID]
				}
				if keep {
					result.RetainedCount++
					continue
				}
				if err := payloadStore.Delete(ctx, key); err != nil {
					if trackBatch {
						batchOperation.State = metadata.MutationOperationFailed
						batchOperation.LastUpdatedAtUnix = time.Now().Unix()
						batchOperation.ErrorMessage = err.Error()
						_ = c.repo.PutMutationOperation(context.Background(), batchOperation)
					}
					if trackOperation {
						operation.State = metadata.MutationOperationFailed
						operation.LastUpdatedAtUnix = time.Now().Unix()
						operation.ErrorMessage = err.Error()
						_ = c.repo.PutMutationOperation(context.Background(), operation)
					}
					return nil, err
				}
				result.DeletedCount++
				_, _, _, chunkID, ok := parseLocalReplicaPayloadKey(key)
				if ok && chunkID != 0 {
					deletedChunkIDs[chunkID] = struct{}{}
				}
			}
		}
		if trackBatch {
			batchOperation.State = metadata.MutationOperationCommitted
			batchOperation.LastUpdatedAtUnix = time.Now().Unix()
			batchOperation.ErrorMessage = ""
			batchOperation.RetiredPhysicalChunkIDs = sortedUint64SetKeys(chunkBatch.ChunkIDs)
			_ = c.repo.PutMutationOperation(ctx, batchOperation)
		}
	}
	results := make([]LocalPayloadSweepResult, 0, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		if result := resultsByReplica[replicaID]; result != nil {
			results = append(results, *result)
		}
	}
	if trackOperation {
		operation.State = metadata.MutationOperationCommitted
		operation.RetiredPhysicalChunkIDs = sortedUint64SetKeys(deletedChunkIDs)
		operation.LastUpdatedAtUnix = time.Now().Unix()
		operation.ErrorMessage = ""
		if err := c.repo.PutMutationOperation(ctx, operation); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (c *LocalPayloadGarbageCollector) buildChunkBatches(ctx context.Context, volumeID string, targetChunkIDs map[uint64]struct{}) []payloadGCChunkBatch {
	if len(targetChunkIDs) == 0 {
		return []payloadGCChunkBatch{{BatchIndex: 0}}
	}
	batches := make([]payloadGCChunkBatch, 0)
	covered := make(map[uint64]struct{})
	maxBatchIndex := -1
	if operations, err := c.repo.ListMutationOperations(ctx, volumeID); err == nil {
		parentOperationID := metadata.PayloadGCMutationOperationID(volumeID)
		for _, operation := range operations {
			if operation.Kind != "payload_gc_batch" || operation.IdempotencyKey != parentOperationID {
				continue
			}
			batchIndex, ok := payloadGCBatchIndex(volumeID, operation.OperationID)
			if !ok {
				continue
			}
			chunkBatch := make(map[uint64]struct{})
			for _, chunkID := range operation.RetiredPhysicalChunkIDs {
				if chunkID == 0 {
					continue
				}
				if _, ok := targetChunkIDs[chunkID]; !ok {
					continue
				}
				chunkBatch[chunkID] = struct{}{}
				covered[chunkID] = struct{}{}
			}
			if len(chunkBatch) == 0 {
				continue
			}
			if batchIndex > maxBatchIndex {
				maxBatchIndex = batchIndex
			}
			batches = append(batches, payloadGCChunkBatch{
				BatchIndex: batchIndex,
				ChunkIDs:   chunkBatch,
			})
		}
		sort.Slice(batches, func(i, j int) bool { return batches[i].BatchIndex < batches[j].BatchIndex })
	}
	remaining := make(map[uint64]struct{})
	for chunkID := range targetChunkIDs {
		if _, ok := covered[chunkID]; ok {
			continue
		}
		remaining[chunkID] = struct{}{}
	}
	if len(remaining) == 0 {
		return batches
	}
	chunkIDs := sortedUint64SetKeys(remaining)
	batchSize := c.chunkBatchSize
	if batchSize <= 0 {
		batchSize = defaultPayloadGCChunkBatchSize
	}
	nextBatchIndex := maxBatchIndex + 1
	if nextBatchIndex < 0 {
		nextBatchIndex = 0
	}
	for start := 0; start < len(chunkIDs); start += batchSize {
		end := start + batchSize
		if end > len(chunkIDs) {
			end = len(chunkIDs)
		}
		batch := make(map[uint64]struct{}, end-start)
		for _, chunkID := range chunkIDs[start:end] {
			batch[chunkID] = struct{}{}
		}
		batches = append(batches, payloadGCChunkBatch{
			BatchIndex: nextBatchIndex,
			ChunkIDs:   batch,
		})
		nextBatchIndex++
	}
	return batches
}

func payloadGCBatchIndex(volumeID, operationID string) (int, bool) {
	prefix := fmt.Sprintf("%s-batch-", metadata.PayloadGCMutationOperationID(volumeID))
	if !strings.HasPrefix(operationID, prefix) {
		return 0, false
	}
	batchIndex, err := strconv.Atoi(strings.TrimPrefix(operationID, prefix))
	if err != nil {
		return 0, false
	}
	return batchIndex, true
}

func (c *LocalPayloadGarbageCollector) retiredPayloadHintChunkIDs(ctx context.Context, volumeID string) (map[uint64]struct{}, error) {
	operations, err := c.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	chunkIDs := make(map[uint64]struct{})
	for _, operation := range operations {
		if operation.State != metadata.MutationOperationCommitted {
			continue
		}
		if operation.Kind != "write" && operation.Kind != "transition" {
			continue
		}
		for _, chunkID := range operation.RetiredPhysicalChunkIDs {
			if chunkID == 0 {
				continue
			}
			chunkIDs[chunkID] = struct{}{}
		}
	}
	return chunkIDs, nil
}

func sortedUint64SetKeys(values map[uint64]struct{}) []uint64 {
	if len(values) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (c *LocalPayloadGarbageCollector) beginSweepMutationOperation(ctx context.Context, volumeID string) (metadata.MutationOperationRecord, bool) {
	volumeState, err := c.repo.GetVolumeState(ctx, volumeID)
	if err != nil {
		return metadata.MutationOperationRecord{}, false
	}
	nowUnix := time.Now().Unix()
	operationID := metadata.PayloadGCMutationOperationID(volumeID)
	operation, err := c.repo.GetMutationOperation(ctx, volumeID, operationID)
	switch {
	case err == nil:
		operation.OperationID = operationID
		operation.VolumeID = volumeID
		operation.Kind = "payload_gc"
		operation.State = metadata.MutationOperationRunning
		operation.AllocationRevision = volumeState.Revision
		operation.WriterFencingEpoch = volumeState.Epoch
		operation.IdempotencyKey = volumeID
		if operation.StartedAtUnix == 0 {
			operation.StartedAtUnix = nowUnix
		}
		operation.LastUpdatedAtUnix = nowUnix
		operation.ErrorMessage = ""
	case errors.Is(err, metadata.ErrNotFound):
		operation = metadata.MutationOperationRecord{
			OperationID:        operationID,
			VolumeID:           volumeID,
			Kind:               "payload_gc",
			State:              metadata.MutationOperationRunning,
			AllocationRevision: volumeState.Revision,
			WriterFencingEpoch: volumeState.Epoch,
			IdempotencyKey:     volumeID,
			StartedAtUnix:      nowUnix,
			LastUpdatedAtUnix:  nowUnix,
		}
	default:
		return metadata.MutationOperationRecord{}, false
	}
	if err := c.repo.PutMutationOperation(ctx, operation); err != nil {
		return metadata.MutationOperationRecord{}, false
	}
	return operation, true
}

func (c *LocalPayloadGarbageCollector) beginSweepBatchMutationOperation(ctx context.Context, volumeID string, batchIndex int, parent metadata.MutationOperationRecord, chunkBatch map[uint64]struct{}) (metadata.MutationOperationRecord, bool) {
	if len(chunkBatch) == 0 {
		return metadata.MutationOperationRecord{}, false
	}
	nowUnix := time.Now().Unix()
	operationID := metadata.PayloadGCBatchMutationOperationID(volumeID, batchIndex)
	operation, err := c.repo.GetMutationOperation(ctx, volumeID, operationID)
	switch {
	case err == nil:
		if operation.State == metadata.MutationOperationCommitted {
			return operation, false
		}
		operation.OperationID = operationID
		operation.VolumeID = volumeID
		operation.Kind = "payload_gc_batch"
		operation.State = metadata.MutationOperationRunning
		operation.PlacementRevision = parent.PlacementRevision
		operation.AllocationRevision = parent.AllocationRevision
		operation.WriterFencingEpoch = parent.WriterFencingEpoch
		operation.IdempotencyKey = parent.OperationID
		operation.AffectedExtentIDs = append([]uint64(nil), parent.AffectedExtentIDs...)
		operation.AffectedPageNos = append([]uint64(nil), parent.AffectedPageNos...)
		operation.RetiredPhysicalChunkIDs = sortedUint64SetKeys(chunkBatch)
		if operation.StartedAtUnix == 0 {
			operation.StartedAtUnix = nowUnix
		}
		operation.LastUpdatedAtUnix = nowUnix
		operation.ErrorMessage = ""
	case errors.Is(err, metadata.ErrNotFound):
		operation = metadata.MutationOperationRecord{
			OperationID:             operationID,
			VolumeID:                volumeID,
			Kind:                    "payload_gc_batch",
			State:                   metadata.MutationOperationRunning,
			PlacementRevision:       parent.PlacementRevision,
			AllocationRevision:      parent.AllocationRevision,
			WriterFencingEpoch:      parent.WriterFencingEpoch,
			IdempotencyKey:          parent.OperationID,
			AffectedExtentIDs:       append([]uint64(nil), parent.AffectedExtentIDs...),
			AffectedPageNos:         append([]uint64(nil), parent.AffectedPageNos...),
			RetiredPhysicalChunkIDs: sortedUint64SetKeys(chunkBatch),
			StartedAtUnix:           nowUnix,
			LastUpdatedAtUnix:       nowUnix,
		}
	default:
		return metadata.MutationOperationRecord{}, false
	}
	if err := c.repo.PutMutationOperation(ctx, operation); err != nil {
		return metadata.MutationOperationRecord{}, false
	}
	return operation, true
}

func (c *LocalPayloadGarbageCollector) referencedPayloadKeys(ctx context.Context, volumeID string, targetExtentIDs map[uint64]struct{}) (map[string]struct{}, map[uint64]struct{}, bool, error) {
	mappings, err := c.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, nil, false, err
	}
	activeExtentIDs := make(map[uint64]struct{}, len(mappings))
	for _, mapping := range mappings {
		if len(targetExtentIDs) > 0 {
			if _, ok := targetExtentIDs[mapping.ExtentID]; !ok {
				continue
			}
		}
		activeExtentIDs[mapping.ExtentID] = struct{}{}
	}

	pages, err := c.repo.ListAllocationPages(ctx, volumeID)
	if err != nil {
		return nil, nil, false, err
	}
	referenced := make(map[string]struct{})
	nativeAllocation := len(pages) > 0
	if len(pages) > 0 {
		if err := markReferencedPayloadKeysFromAllocationPages(referenced, volumeID, c.stores, mappings, targetExtentIDs, pages); err != nil {
			return nil, nil, false, err
		}
	}
	snapshots, err := c.repo.ListSnapshotRecords(ctx, volumeID, false)
	if err != nil {
		return nil, nil, false, err
	}
	for _, snapshot := range snapshots {
		snapshotPages, err := c.repo.ListSnapshotAllocationPages(ctx, snapshot.SnapshotID)
		if err != nil {
			return nil, nil, false, err
		}
		if len(snapshotPages) > 0 {
			nativeAllocation = true
			if err := markReferencedPayloadKeysFromAllocationPages(referenced, volumeID, c.stores, mappings, targetExtentIDs, snapshotPages); err != nil {
				return nil, nil, false, err
			}
		}
	}
	clones, err := c.repo.ListCloneRecords(ctx, "", volumeID, false)
	if err != nil {
		return nil, nil, false, err
	}
	for _, clone := range clones {
		if clone.State != metadata.CloneStateAvailable && clone.State != metadata.CloneStateMaterializing {
			continue
		}
		cloneDeltaPages, err := c.repo.ListCloneDeltaAllocationPages(ctx, clone.CloneID)
		if err != nil {
			return nil, nil, false, err
		}
		if len(cloneDeltaPages) > 0 {
			nativeAllocation = true
			if err := markReferencedPayloadKeysFromAllocationPages(referenced, volumeID, c.stores, mappings, targetExtentIDs, cloneDeltaPages); err != nil {
				return nil, nil, false, err
			}
		}
	}
	return referenced, activeExtentIDs, nativeAllocation, nil
}

func markReferencedPayloadKeysFromAllocationPages(referenced map[string]struct{}, payloadVolumeID string, stores map[string]store.ObjectStore, mappings []metadata.ExtentMappingRecord, targetExtentIDs map[uint64]struct{}, pages []metadata.AllocationPageRecord) error {
	resolvedPages, err := resolvedAllocationPagesFromRecords(pages)
	if err != nil {
		return err
	}
	if len(resolvedPages) == 0 {
		return nil
	}
	for _, mapping := range mappings {
		if len(targetExtentIDs) > 0 {
			if _, ok := targetExtentIDs[mapping.ExtentID]; !ok {
				continue
			}
		}
		if mapping.LengthBytes == 0 {
			continue
		}
		chunkSize := uint64(resolvedPages[0].Page.ChunkSizeBytes)
		startChunk := mapping.LogicalOffset / chunkSize
		endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / chunkSize
		for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
			covered, zero, physicalChunkID, _ := logicalChunkAllocationState(resolvedPages, logicalChunk)
			if !covered || zero || physicalChunkID == 0 {
				continue
			}
			for replicaID := range stores {
				referenced[localReplicaPayloadKey(replicaID, payloadVolumeID, mapping.ExtentID, physicalChunkID)] = struct{}{}
			}
		}
	}
	return nil
}

func resolvedAllocationPagesFromRecords(pages []metadata.AllocationPageRecord) ([]metadata.ResolvedAllocationPage, error) {
	out := make([]metadata.ResolvedAllocationPage, 0, len(pages))
	var pageBytes uint32
	var chunkSizeBytes uint32
	for _, page := range pages {
		if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
			return nil, fmt.Errorf("invalid allocation page geometry: volume_id=%s page_no=%d page_bytes=%d chunk_size_bytes=%d", page.VolumeID, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
		}
		if pageBytes == 0 && chunkSizeBytes == 0 {
			pageBytes = page.PageBytes
			chunkSizeBytes = page.ChunkSizeBytes
		} else if page.PageBytes != pageBytes || page.ChunkSizeBytes != chunkSizeBytes {
			return nil, fmt.Errorf("mixed allocation page geometry: volume_id=%s page_no=%d page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d", page.VolumeID, page.PageNo, page.PageBytes, page.ChunkSizeBytes, pageBytes, chunkSizeBytes)
		}
		chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
		if chunksPerPage == 0 {
			continue
		}
		startChunk := page.PageNo * chunksPerPage
		out = append(out, metadata.ResolvedAllocationPage{
			Page:            page,
			RangeStartChunk: startChunk,
			RangeEndChunk:   startChunk + chunksPerPage,
			CoversWholePage: true,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RangeStartChunk < out[j].RangeStartChunk })
	return out, nil
}

func (c *LocalPayloadGarbageCollector) listCandidatePayloadKeys(ctx context.Context, payloadStore store.ObjectStore, replicaID, volumeID string, targetExtentIDs, targetChunkIDs map[uint64]struct{}) ([]string, error) {
	if len(targetExtentIDs) == 0 {
		prefix := fmt.Sprintf("replicas/%s/volumes/%s/extents/", replicaID, volumeID)
		if len(targetChunkIDs) == 0 {
			return listAllObjectStoreKeys(ctx, payloadStore, prefix)
		}
		return c.listExactChunkCandidateKeys(ctx, payloadStore, replicaID, volumeID, nil, targetChunkIDs)
	}
	if len(targetChunkIDs) > 0 {
		return c.listExactChunkCandidateKeys(ctx, payloadStore, replicaID, volumeID, targetExtentIDs, targetChunkIDs)
	}
	extentIDs := sortedUint64SetKeys(targetExtentIDs)
	keys := make([]string, 0)
	seen := make(map[string]struct{})
	for _, extentID := range extentIDs {
		prefix := fmt.Sprintf("replicas/%s/volumes/%s/extents/%020d/chunks/", replicaID, volumeID, extentID)
		found, err := listAllObjectStoreKeys(ctx, payloadStore, prefix)
		if err != nil {
			return nil, err
		}
		for _, key := range found {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (c *LocalPayloadGarbageCollector) listExactChunkCandidateKeys(ctx context.Context, payloadStore store.ObjectStore, replicaID, volumeID string, targetExtentIDs, targetChunkIDs map[uint64]struct{}) ([]string, error) {
	extentIDs := sortedUint64SetKeys(targetExtentIDs)
	if len(extentIDs) == 0 {
		extentIDs = []uint64{0}
	}
	chunkIDs := sortedUint64SetKeys(targetChunkIDs)
	keys := make([]string, 0)
	seen := make(map[string]struct{})
	for _, chunkID := range chunkIDs {
		if len(targetExtentIDs) == 0 {
			prefix := fmt.Sprintf("replicas/%s/volumes/%s/extents/", replicaID, volumeID)
			found, err := listAllObjectStoreKeys(ctx, payloadStore, prefix)
			if err != nil {
				return nil, err
			}
			for _, key := range found {
				_, keyVolumeID, _, keyChunkID, ok := parseLocalReplicaPayloadKey(key)
				if !ok || keyVolumeID != volumeID || keyChunkID != chunkID {
					continue
				}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
			continue
		}
		for _, extentID := range extentIDs {
			key := localReplicaPayloadKey(replicaID, volumeID, extentID, chunkID)
			if _, found, err := payloadStore.Get(ctx, key); err != nil {
				return nil, err
			} else if found {
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

func (c *LocalPayloadGarbageCollector) targetExtentIDsFromPages(ctx context.Context, volumeID string, targetPageNos map[uint64]struct{}) (map[uint64]struct{}, error) {
	mappings, err := c.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	pages, err := c.repo.ListAllocationPages(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if len(pages) == 0 {
		return nil, nil
	}
	pageBytes := pages[0].PageBytes
	chunkSizeBytes := pages[0].ChunkSizeBytes
	if pageBytes == 0 || chunkSizeBytes == 0 {
		return nil, nil
	}
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	if chunksPerPage == 0 {
		return nil, nil
	}
	out := make(map[uint64]struct{})
	for _, mapping := range mappings {
		if mapping.LengthBytes == 0 {
			continue
		}
		startChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
		endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / uint64(chunkSizeBytes)
		startPage := startChunk / chunksPerPage
		endPage := endChunk / chunksPerPage
		for pageNo := startPage; pageNo <= endPage; pageNo++ {
			if _, ok := targetPageNos[pageNo]; ok {
				out[mapping.ExtentID] = struct{}{}
				break
			}
		}
	}
	return out, nil
}

func listAllObjectStoreKeys(ctx context.Context, objectStore store.ObjectStore, prefix string) ([]string, error) {
	cursor := ""
	var out []string
	for {
		keys, next, err := objectStore.List(ctx, prefix, cursor, 128)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func sliceToUint64Set(values []uint64) map[uint64]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		if value == 0 {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func sliceToUint64SetAllowZero(values []uint64) map[uint64]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func parseLocalReplicaPayloadKey(key string) (replicaID, volumeID string, extentID, chunkID uint64, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 8 || parts[0] != "replicas" || parts[2] != "volumes" || parts[4] != "extents" || parts[6] != "chunks" {
		return "", "", 0, 0, false
	}
	parsedExtentID, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return "", "", 0, 0, false
	}
	parsedChunkID, err := strconv.ParseUint(parts[7], 10, 64)
	if err != nil {
		return "", "", 0, 0, false
	}
	replicaID = parts[1]
	volumeID = parts[3]
	extentID = parsedExtentID
	chunkID = parsedChunkID
	return replicaID, volumeID, extentID, chunkID, true
}
