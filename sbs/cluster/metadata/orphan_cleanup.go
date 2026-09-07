package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	OrphanMetadataCleanupVolumeMaximum = 32
	orphanMetadataCleanupPageLimit     = 512
	orphanMetadataCleanupBatchLimit    = 128
	orphanMetadataCleanupKeyMaximum    = 100000
)

var ErrOrphanMetadataCleanupRejected = errors.New("orphan metadata cleanup rejected")

type OrphanVolumeMetadataCleanupExpectation struct {
	VolumeID                  string `json:"volume_id"`
	ExactKeyValueCount        int    `json:"exact_key_value_count"`
	ExactKeyValueDigestSHA256 string `json:"exact_key_value_digest_sha256"`
}

type OrphanVolumeMetadataCleanupResult struct {
	VolumeCount                  int `json:"volume_count"`
	VolumeKeyDeleteCount         int `json:"volume_key_delete_count"`
	MutationOperationDeleteCount int `json:"mutation_operation_delete_count"`
	ValidationRangePageCount     int `json:"validation_range_page_count"`
	ValidationBatchGetCount      int `json:"validation_batch_get_count"`
	ValidationBatchGetKeyCount   int `json:"validation_batch_get_key_count"`
	MaximumBatchGetKeyCount      int `json:"maximum_batch_get_key_count"`
	TransactionBatchGetCount     int `json:"transaction_batch_get_count"`
	TransactionBatchGetKeyCount  int `json:"transaction_batch_get_key_count"`
	UniqueTiKVMutationKeyCount   int `json:"unique_tikv_mutation_key_count"`
	TransactionCallCount         int `json:"transaction_call_count"`
	PostDeleteRangePageCount     int `json:"post_delete_range_page_count"`
	PostDeleteRemainingKeyCount  int `json:"post_delete_remaining_key_count"`
}

type orphanMetadataCleanupPlan struct {
	expectation OrphanVolumeMetadataCleanupExpectation
	keys        []string
	values      map[string][]byte
	state       VolumeState
	operations  []MutationOperationRecord
	rangePages  int
	batchGets   int
}

type mutationCountingReadWriter struct {
	base    kvReadWriter
	touched map[string]struct{}
}

func (w *mutationCountingReadWriter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return w.base.Get(ctx, key)
}

func (w *mutationCountingReadWriter) Set(ctx context.Context, key string, value []byte) error {
	if err := w.base.Set(ctx, key, value); err != nil {
		return err
	}
	w.touched[key] = struct{}{}
	return nil
}

func (w *mutationCountingReadWriter) Delete(ctx context.Context, key string) error {
	if err := w.base.Delete(ctx, key); err != nil {
		return err
	}
	w.touched[key] = struct{}{}
	return nil
}

// ValidateOrphanVolumeMetadataCleanupExact recaptures and verifies the complete
// key/value set without writing metadata. The result describes the source rows
// that a subsequent exact cleanup would remove.
func (r *Repository) ValidateOrphanVolumeMetadataCleanupExact(ctx context.Context, expectations []OrphanVolumeMetadataCleanupExpectation) (OrphanVolumeMetadataCleanupResult, error) {
	var result OrphanVolumeMetadataCleanupResult
	plans, err := r.prepareOrphanMetadataCleanup(ctx, expectations)
	if err != nil {
		return result, err
	}
	result.VolumeCount = len(plans)
	for _, plan := range plans {
		result.VolumeKeyDeleteCount += len(plan.keys)
		result.MutationOperationDeleteCount += len(plan.operations)
		result.ValidationRangePageCount += plan.rangePages
		result.ValidationBatchGetCount += plan.batchGets
		result.ValidationBatchGetKeyCount += len(plan.keys)
		result.MaximumBatchGetKeyCount = max(result.MaximumBatchGetKeyCount, min(len(plan.keys), orphanMetadataCleanupBatchLimit))
	}
	return result, nil
}

// DeleteOrphanVolumeMetadataExact removes only terminal metadata-only orphan
// volume subtrees. The complete key/value set is recaptured and matched to the
// caller's digest before one transaction deletes source rows and operation
// indexes. Callers must stop all service writers to exclude prefix phantoms.
func (r *Repository) DeleteOrphanVolumeMetadataExact(ctx context.Context, expectations []OrphanVolumeMetadataCleanupExpectation, servicesStopped bool) (OrphanVolumeMetadataCleanupResult, error) {
	var result OrphanVolumeMetadataCleanupResult
	if r == nil || !servicesStopped {
		return result, fmt.Errorf("%w: stopped sbs-service writers are required", ErrOrphanMetadataCleanupRejected)
	}
	runner, transactionOK := r.kv.(transactionalKV)
	if !transactionOK {
		return result, fmt.Errorf("%w: transactions are required", ErrOrphanMetadataCleanupRejected)
	}
	plans, err := r.prepareOrphanMetadataCleanup(ctx, expectations)
	if err != nil {
		return result, err
	}

	touched := make(map[string]struct{})
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		writer := &mutationCountingReadWriter{base: tx, touched: touched}
		batchReader, ok := tx.(kvBatchReader)
		if !ok {
			return fmt.Errorf("%w: transaction BatchGet is required", ErrOrphanMetadataCleanupRejected)
		}
		for _, plan := range plans {
			if _, found, err := tx.Get(ctx, volumeSpecKey(r.root, plan.expectation.VolumeID)); err != nil {
				return err
			} else if found {
				return fmt.Errorf("%w: volume %s gained a spec", ErrCASConflict, plan.expectation.VolumeID)
			}
			for start := 0; start < len(plan.keys); start += orphanMetadataCleanupBatchLimit {
				end := min(start+orphanMetadataCleanupBatchLimit, len(plan.keys))
				keys := plan.keys[start:end]
				currentValues, err := batchReader.BatchGet(ctx, keys)
				if err != nil {
					return err
				}
				result.TransactionBatchGetCount++
				result.TransactionBatchGetKeyCount += len(keys)
				for _, key := range keys {
					current, found := currentValues[key]
					if !found || !bytes.Equal(current, plan.values[key]) {
						return fmt.Errorf("%w: volume %s key/value set changed", ErrCASConflict, plan.expectation.VolumeID)
					}
				}
			}
		}
		for _, plan := range plans {
			for _, operation := range plan.operations {
				if err := r.deleteMutationOperationWithIndex(ctx, writer, plan.expectation.VolumeID, operation.OperationID); err != nil {
					return err
				}
				result.MutationOperationDeleteCount++
			}
			operationKeys := make(map[string]struct{}, len(plan.operations))
			for _, operation := range plan.operations {
				operationKeys[mutationOperationKey(r.root, plan.expectation.VolumeID, operation.OperationID)] = struct{}{}
			}
			for _, key := range plan.keys {
				if _, deleted := operationKeys[key]; deleted {
					continue
				}
				if err := writer.Delete(ctx, key); err != nil {
					return err
				}
				result.VolumeKeyDeleteCount++
			}
			if err := applySummaryRecordMutation(ctx, writer, r.root, summarySubject("volume", plan.expectation.VolumeID), summaryVolumeContribution(plan.state, true, VolumeSpecRecord{}, false), SummaryCounters{}, r.now()); err != nil {
				return err
			}
			if err := advanceVolumeCatalogRevision(ctx, writer, r.root, r.now()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return OrphanVolumeMetadataCleanupResult{}, err
	}
	result.VolumeCount = len(plans)
	result.VolumeKeyDeleteCount += result.MutationOperationDeleteCount
	for _, plan := range plans {
		result.ValidationRangePageCount += plan.rangePages
		result.ValidationBatchGetCount += plan.batchGets
		result.ValidationBatchGetKeyCount += len(plan.keys)
		result.MaximumBatchGetKeyCount = max(result.MaximumBatchGetKeyCount, min(len(plan.keys), orphanMetadataCleanupBatchLimit))
	}
	result.UniqueTiKVMutationKeyCount = len(touched)
	result.TransactionCallCount = 1

	snapshotter := r.kv.(consistentSnapshotKV)
	err = snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
		for _, plan := range plans {
			keys, pages, err := listOrphanCleanupKeys(ctx, snapshot, r.root+"/volumes/"+plan.expectation.VolumeID+"/")
			result.PostDeleteRangePageCount += pages
			result.PostDeleteRemainingKeyCount += len(keys)
			if err != nil {
				return err
			}
			if len(keys) != 0 {
				return fmt.Errorf("%w: volume %s retains %d keys", ErrCASConflict, plan.expectation.VolumeID, len(keys))
			}
			for _, operation := range plan.operations {
				if _, found, err := snapshot.Get(ctx, operationByIDKey(r.root, operation.OperationID)); err != nil {
					return err
				} else if found {
					return fmt.Errorf("%w: operation index %s remains", ErrCASConflict, operation.OperationID)
				}
			}
		}
		return nil
	})
	return result, err
}

func (r *Repository) prepareOrphanMetadataCleanup(ctx context.Context, expectations []OrphanVolumeMetadataCleanupExpectation) ([]orphanMetadataCleanupPlan, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrOrphanMetadataCleanupRejected)
	}
	if len(expectations) == 0 || len(expectations) > OrphanMetadataCleanupVolumeMaximum {
		return nil, fmt.Errorf("%w: volume batch size must be 1..%d", ErrOrphanMetadataCleanupRejected, OrphanMetadataCleanupVolumeMaximum)
	}
	snapshotter, snapshotOK := r.kv.(consistentSnapshotKV)
	if !snapshotOK {
		return nil, fmt.Errorf("%w: consistent snapshots are required", ErrOrphanMetadataCleanupRejected)
	}
	plans := make([]orphanMetadataCleanupPlan, 0, len(expectations))
	previousVolumeID := ""
	totalKeys := 0
	for _, expectation := range expectations {
		volumeID, err := CanonicalVolumeID(expectation.VolumeID)
		if err != nil || volumeID != expectation.VolumeID || volumeID <= previousVolumeID || expectation.ExactKeyValueCount <= 0 || len(expectation.ExactKeyValueDigestSHA256) != 64 {
			return nil, fmt.Errorf("%w: expectations must be canonical, sorted, unique, and digest-bound", ErrOrphanMetadataCleanupRejected)
		}
		if _, err := hex.DecodeString(expectation.ExactKeyValueDigestSHA256); err != nil {
			return nil, fmt.Errorf("%w: invalid digest for volume %s", ErrOrphanMetadataCleanupRejected, volumeID)
		}
		previousVolumeID = volumeID
	}
	err := snapshotter.RunInReadSnapshot(ctx, func(snapshot kvReadSnapshot) error {
		for _, expectation := range expectations {
			plan, err := r.captureOrphanMetadataCleanupPlan(ctx, snapshot, expectation)
			if err != nil {
				return err
			}
			totalKeys += len(plan.keys)
			if totalKeys > orphanMetadataCleanupKeyMaximum {
				return fmt.Errorf("%w: candidate key count exceeds %d", ErrOrphanMetadataCleanupRejected, orphanMetadataCleanupKeyMaximum)
			}
			plans = append(plans, plan)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plans, nil
}

func (r *Repository) captureOrphanMetadataCleanupPlan(ctx context.Context, snapshot kvReadSnapshot, expectation OrphanVolumeMetadataCleanupExpectation) (orphanMetadataCleanupPlan, error) {
	plan := orphanMetadataCleanupPlan{expectation: expectation}
	if _, found, err := snapshot.Get(ctx, volumeSpecKey(r.root, expectation.VolumeID)); err != nil {
		return plan, err
	} else if found {
		return plan, fmt.Errorf("%w: volume %s has a spec", ErrOrphanMetadataCleanupRejected, expectation.VolumeID)
	}
	prefix := r.root + "/volumes/" + expectation.VolumeID + "/"
	keys, pages, err := listOrphanCleanupKeys(ctx, snapshot, prefix)
	if err != nil {
		return plan, err
	}
	plan.rangePages = pages
	if len(keys) != expectation.ExactKeyValueCount {
		return plan, fmt.Errorf("%w: volume %s key count changed", ErrCASConflict, expectation.VolumeID)
	}
	values := make(map[string][]byte, len(keys))
	for start := 0; start < len(keys); start += orphanMetadataCleanupBatchLimit {
		end := min(start+orphanMetadataCleanupBatchLimit, len(keys))
		batch, err := snapshot.BatchGet(ctx, keys[start:end])
		if err != nil {
			return plan, err
		}
		plan.batchGets++
		for _, key := range keys[start:end] {
			raw, found := batch[key]
			if !found {
				return plan, fmt.Errorf("%w: volume %s key disappeared", ErrCASConflict, expectation.VolumeID)
			}
			values[key] = append([]byte(nil), raw...)
		}
	}
	if digestOrphanMetadataKeyValues(keys, values) != expectation.ExactKeyValueDigestSHA256 {
		return plan, fmt.Errorf("%w: volume %s key/value digest changed", ErrCASConflict, expectation.VolumeID)
	}
	stateFound := false
	for _, key := range keys {
		relative := strings.TrimPrefix(key, prefix)
		switch {
		case relative == "meta/state":
			if stateFound || json.Unmarshal(values[key], &plan.state) != nil || plan.state.VolumeID != expectation.VolumeID {
				return plan, fmt.Errorf("%w: volume %s state is invalid", ErrOrphanMetadataCleanupRejected, expectation.VolumeID)
			}
			stateFound = true
		case relative == "meta/next_chunk_id", strings.HasPrefix(relative, "idem/"), strings.HasPrefix(relative, "idempotency/"):
		case strings.HasPrefix(relative, "operations/"):
			var operation MutationOperationRecord
			if err := json.Unmarshal(values[key], &operation); err != nil || operation.VolumeID != expectation.VolumeID || mutationOperationKey(r.root, expectation.VolumeID, operation.OperationID) != key || operation.State != MutationOperationCommitted && operation.State != MutationOperationRolledBack {
				return plan, fmt.Errorf("%w: volume %s operation is invalid or nonterminal", ErrOrphanMetadataCleanupRejected, expectation.VolumeID)
			}
			plan.operations = append(plan.operations, operation)
		default:
			return plan, fmt.Errorf("%w: volume %s contains unsafe key class", ErrOrphanMetadataCleanupRejected, expectation.VolumeID)
		}
	}
	if !stateFound {
		return plan, fmt.Errorf("%w: volume %s has no authority state", ErrOrphanMetadataCleanupRejected, expectation.VolumeID)
	}
	sort.Slice(plan.operations, func(i, j int) bool { return plan.operations[i].OperationID < plan.operations[j].OperationID })
	plan.keys, plan.values = keys, values
	return plan, nil
}

func listOrphanCleanupKeys(ctx context.Context, snapshot kvReadSnapshot, prefix string) ([]string, int, error) {
	var keys []string
	cursor := ""
	pages := 0
	for {
		page, next, err := snapshot.List(ctx, prefix, cursor, orphanMetadataCleanupPageLimit)
		pages++
		if err != nil {
			return nil, pages, err
		}
		keys = append(keys, page...)
		if next == "" {
			break
		}
		if next <= cursor {
			return nil, pages, fmt.Errorf("%w: metadata cursor did not advance", ErrOrphanMetadataCleanupRejected)
		}
		cursor = next
	}
	sort.Strings(keys)
	return keys, pages, nil
}

func digestOrphanMetadataKeyValues(keys []string, values map[string][]byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("namrbd/phase-ad/orphan-key-value-set/v1\x00"))
	var length [8]byte
	for _, key := range keys {
		raw := values[key]
		binary.BigEndian.PutUint64(length[:], uint64(len(key)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(key))
		binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(raw)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
