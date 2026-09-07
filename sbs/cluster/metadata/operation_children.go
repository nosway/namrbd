package metadata

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

type OperationChildrenSummary struct {
	SchemaVersion int    `json:"schema_version"`
	ParentID      string `json:"parent_id"`
	VolumeID      string `json:"volume_id"`
	ChildKind     string `json:"child_kind"`
	Total         uint64 `json:"total"`
	Running       uint64 `json:"running"`
	Failed        uint64 `json:"failed"`
	Completed     uint64 `json:"completed"`
	Small         uint64 `json:"small,omitempty"`
	StateDigest   string `json:"state_digest"`
}

func (r *Repository) GetOperationChildrenSummary(ctx context.Context, parentID string) (OperationChildrenSummary, error) {
	if r == nil || strings.TrimSpace(parentID) == "" {
		return OperationChildrenSummary{}, ErrOperationListInvalid
	}
	var summary OperationChildrenSummary
	if err := r.getJSON(ctx, operationChildrenSummaryKey(r.root, parentID), &summary); err != nil {
		return OperationChildrenSummary{}, err
	}
	if err := validateOperationChildrenSummary(summary); err != nil {
		return OperationChildrenSummary{}, err
	}
	return summary, nil
}

func updateOperationChildrenSummary(ctx context.Context, store kvReadWriter, root string, before *MutationOperationRecord, after *MutationOperationRecord) error {
	if before != nil && operationChildParent(*before) != "" {
		if after == nil || operationChildParent(*after) != operationChildParent(*before) || after.VolumeID != before.VolumeID || after.Kind != before.Kind {
			if err := applyOperationChildDelta(ctx, store, root, *before, false); err != nil {
				return err
			}
			before = nil
		}
	}
	if before != nil && after != nil && operationChildParent(*after) != "" {
		if err := applyOperationChildDelta(ctx, store, root, *before, false); err != nil {
			return err
		}
		return applyOperationChildDelta(ctx, store, root, *after, true)
	}
	if after != nil && operationChildParent(*after) != "" {
		return applyOperationChildDelta(ctx, store, root, *after, true)
	}
	return nil
}

func applyOperationChildDelta(ctx context.Context, store kvReadWriter, root string, rec MutationOperationRecord, add bool) error {
	parentID := operationChildParent(rec)
	if parentID == "" {
		return nil
	}
	key := operationChildrenSummaryKey(root, parentID)
	var summary OperationChildrenSummary
	found, err := getOptionalJSONStore(ctx, store, key, &summary)
	if err != nil {
		return err
	}
	if found {
		if err := validateOperationChildrenSummary(summary); err != nil {
			return err
		}
		if summary.ParentID != parentID || summary.VolumeID != rec.VolumeID || summary.ChildKind != rec.Kind {
			return fmt.Errorf("%w: operation child summary identity conflict", ErrMaintenanceIndexConflict)
		}
	} else {
		if !add {
			return ErrMaintenanceIndexChanged
		}
		summary = OperationChildrenSummary{SchemaVersion: 1, ParentID: parentID, VolumeID: rec.VolumeID, ChildKind: rec.Kind}
	}
	deltaRunning, deltaFailed, deltaCompleted := operationChildStateCounters(rec.State)
	deltaSmall := uint64(0)
	if rec.Kind == "transition_batch" && len(subtractMutationOperationCompletedPages(rec.AffectedPageNos, rec.CompletedPageNos)) <= 1 {
		deltaSmall = 1
	}
	if add {
		summary.Total++
		summary.Running += deltaRunning
		summary.Failed += deltaFailed
		summary.Completed += deltaCompleted
		summary.Small += deltaSmall
	} else {
		if summary.Total == 0 || summary.Running < deltaRunning || summary.Failed < deltaFailed || summary.Completed < deltaCompleted || summary.Small < deltaSmall {
			return ErrMaintenanceIndexChanged
		}
		summary.Total--
		summary.Running -= deltaRunning
		summary.Failed -= deltaFailed
		summary.Completed -= deltaCompleted
		summary.Small -= deltaSmall
	}
	if summary.Total == 0 {
		return store.Delete(ctx, key)
	}
	summary.StateDigest = digestOperationChildrenSummary(summary)
	return putJSONStore(ctx, store, key, summary)
}

func operationChildParent(rec MutationOperationRecord) string {
	if rec.Kind != "payload_gc_batch" && rec.Kind != "transition_batch" {
		return ""
	}
	return strings.TrimSpace(rec.IdempotencyKey)
}

func operationChildStateCounters(state MutationOperationState) (running, failed, completed uint64) {
	switch state {
	case MutationOperationPending, MutationOperationRunning:
		return 1, 0, 0
	case MutationOperationFailed:
		return 0, 1, 0
	case MutationOperationCommitted:
		return 0, 0, 1
	default:
		return 0, 0, 0
	}
}

func subtractMutationOperationCompletedPages(affected, completed []uint64) []uint64 {
	done := make(map[uint64]struct{}, len(completed))
	for _, page := range completed {
		done[page] = struct{}{}
	}
	remaining := make([]uint64, 0, len(affected))
	for _, page := range affected {
		if _, ok := done[page]; !ok {
			remaining = append(remaining, page)
		}
	}
	return remaining
}

func operationChildrenSummaryKey(root, parentID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/operation-children/%s", root, url.PathEscape(strings.TrimSpace(parentID)))
}

func validateOperationChildrenSummary(summary OperationChildrenSummary) error {
	if summary.SchemaVersion != 1 || strings.TrimSpace(summary.ParentID) == "" || strings.TrimSpace(summary.VolumeID) == "" || summary.ChildKind != "payload_gc_batch" && summary.ChildKind != "transition_batch" || summary.Total == 0 || summary.Running+summary.Failed+summary.Completed > summary.Total || summary.Small > summary.Total || summary.StateDigest != digestOperationChildrenSummary(summary) {
		return ErrMaintenanceIndexInvalid
	}
	return nil
}

func digestOperationChildrenSummary(summary OperationChildrenSummary) string {
	summary.StateDigest = ""
	return digestSummaryValue(summary)
}
