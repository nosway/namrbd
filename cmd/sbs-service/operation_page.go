package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const (
	operationPageTokenVersion  = 1
	operationPagePhaseAdmin    = "admin"
	operationPagePhaseMutation = "mutation"
)

type operationPageToken struct {
	Version            int    `json:"version"`
	Phase              string `json:"phase"`
	Cursor             string `json:"cursor,omitempty"`
	ProjectionRevision string `json:"projection_revision"`
	Kind               string `json:"kind,omitempty"`
	State              int32  `json:"state"`
	HasUpdatedAfter    bool   `json:"has_updated_after,omitempty"`
	UpdatedAfterSec    int64  `json:"updated_after_sec,omitempty"`
	UpdatedAfterNanos  int32  `json:"updated_after_nanos,omitempty"`
	HasUpdatedBefore   bool   `json:"has_updated_before,omitempty"`
	UpdatedBeforeSec   int64  `json:"updated_before_sec,omitempty"`
	UpdatedBeforeNanos int32  `json:"updated_before_nanos,omitempty"`
}

type operationPageFilter struct {
	Kind          string
	State         adminv1.OperationState
	UpdatedAfter  time.Time
	UpdatedBefore time.Time
}

type operationStorePage struct {
	Operations            []*adminv1.OperationStatus
	NextPhase             string
	NextCursor            string
	ScannedCount          int
	RangePageCount        int
	BatchGetCount         int
	BatchGetKeyCount      int
	BackendFullScanCount  int
	FullCompletionCount   int
	NestedCompletionCount int
}

func operationPageFilterFromRequest(req *adminv1.ListOperationsPageRequest) (operationPageFilter, error) {
	filter := operationPageFilter{Kind: strings.TrimSpace(req.GetKind()), State: req.GetState()}
	switch filter.State {
	case adminv1.OperationState_OPERATION_STATE_UNSPECIFIED,
		adminv1.OperationState_OPERATION_STATE_QUEUED,
		adminv1.OperationState_OPERATION_STATE_RUNNING,
		adminv1.OperationState_OPERATION_STATE_COMPLETED,
		adminv1.OperationState_OPERATION_STATE_FAILED,
		adminv1.OperationState_OPERATION_STATE_CANCELED:
	default:
		return filter, fmt.Errorf("unsupported operation state %d", filter.State)
	}
	if req.GetUpdatedAfter() != nil {
		if err := req.GetUpdatedAfter().CheckValid(); err != nil {
			return filter, fmt.Errorf("updated_after: %w", err)
		}
		filter.UpdatedAfter = req.GetUpdatedAfter().AsTime().UTC()
	}
	if req.GetUpdatedBefore() != nil {
		if err := req.GetUpdatedBefore().CheckValid(); err != nil {
			return filter, fmt.Errorf("updated_before: %w", err)
		}
		filter.UpdatedBefore = req.GetUpdatedBefore().AsTime().UTC()
	}
	if !filter.UpdatedAfter.IsZero() && !filter.UpdatedBefore.IsZero() && !filter.UpdatedAfter.Before(filter.UpdatedBefore) {
		return filter, fmt.Errorf("updated_after must be before updated_before")
	}
	return filter, nil
}

func encodeOperationPageToken(phase, cursor, revision string, req *adminv1.ListOperationsPageRequest) (string, error) {
	if phase != operationPagePhaseAdmin && phase != operationPagePhaseMutation {
		return "", fmt.Errorf("invalid operation page phase")
	}
	filter, err := operationPageFilterFromRequest(req)
	if err != nil {
		return "", err
	}
	token := operationPageToken{
		Version: operationPageTokenVersion, Phase: phase, Cursor: strings.TrimSpace(cursor), ProjectionRevision: revision,
		Kind: filter.Kind, State: int32(filter.State),
	}
	if !filter.UpdatedAfter.IsZero() {
		token.HasUpdatedAfter = true
		token.UpdatedAfterSec, token.UpdatedAfterNanos = filter.UpdatedAfter.Unix(), int32(filter.UpdatedAfter.Nanosecond())
	}
	if !filter.UpdatedBefore.IsZero() {
		token.HasUpdatedBefore = true
		token.UpdatedBeforeSec, token.UpdatedBeforeNanos = filter.UpdatedBefore.Unix(), int32(filter.UpdatedBefore.Nanosecond())
	}
	if token.ProjectionRevision == "" || phase == operationPagePhaseAdmin && token.Cursor == "" {
		return "", fmt.Errorf("operation page token requires revision and progress")
	}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeOperationPageToken(raw string, req *adminv1.ListOperationsPageRequest) (operationPageToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return operationPageToken{}, fmt.Errorf("decode operation page token: %w", err)
	}
	var token operationPageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return operationPageToken{}, fmt.Errorf("unmarshal operation page token: %w", err)
	}
	if token.Version != operationPageTokenVersion || token.ProjectionRevision == "" || token.Phase != operationPagePhaseAdmin && token.Phase != operationPagePhaseMutation || token.Phase == operationPagePhaseAdmin && token.Cursor == "" {
		return operationPageToken{}, fmt.Errorf("invalid operation page token payload")
	}
	filter, err := operationPageFilterFromRequest(req)
	if err != nil {
		return operationPageToken{}, err
	}
	afterSec, afterNanos := int64(0), int32(0)
	beforeSec, beforeNanos := int64(0), int32(0)
	hasAfter, hasBefore := !filter.UpdatedAfter.IsZero(), !filter.UpdatedBefore.IsZero()
	if !filter.UpdatedAfter.IsZero() {
		afterSec, afterNanos = filter.UpdatedAfter.Unix(), int32(filter.UpdatedAfter.Nanosecond())
	}
	if !filter.UpdatedBefore.IsZero() {
		beforeSec, beforeNanos = filter.UpdatedBefore.Unix(), int32(filter.UpdatedBefore.Nanosecond())
	}
	if token.Kind != filter.Kind || token.State != int32(filter.State) || token.HasUpdatedAfter != hasAfter || token.UpdatedAfterSec != afterSec || token.UpdatedAfterNanos != afterNanos || token.HasUpdatedBefore != hasBefore || token.UpdatedBeforeSec != beforeSec || token.UpdatedBeforeNanos != beforeNanos {
		return operationPageToken{}, fmt.Errorf("operation page token filter mismatch")
	}
	return token, nil
}

func (s *operationStore) listPage(ctx context.Context, phase, cursor string, limit int, filter operationPageFilter) (operationStorePage, error) {
	result := operationStorePage{}
	if s == nil || limit < 1 || limit > clustermeta.MaintenanceIndexPageMaximum {
		return result, fmt.Errorf("invalid operation page limit %d", limit)
	}
	if phase == "" {
		phase = operationPagePhaseAdmin
	}
	if phase != operationPagePhaseAdmin && phase != operationPagePhaseMutation {
		return result, fmt.Errorf("invalid operation page phase %q", phase)
	}
	remaining := limit
	if phase == operationPagePhaseAdmin {
		operations, next, scanned, batches, batchKeys, err := s.listAdminPage(ctx, cursor, remaining, filter)
		if err != nil {
			return result, err
		}
		result.Operations = append(result.Operations, operations...)
		result.ScannedCount += scanned
		result.RangePageCount++
		result.BatchGetCount += batches
		result.BatchGetKeyCount += batchKeys
		remaining -= scanned
		if next != "" {
			result.NextPhase, result.NextCursor = operationPagePhaseAdmin, next
			return result, nil
		}
		if remaining == 0 {
			result.NextPhase = operationPagePhaseMutation
			return result, nil
		}
		phase, cursor = operationPagePhaseMutation, ""
	}
	if phase == operationPagePhaseMutation {
		page, err := s.repo.ListMutationOperationIndexPage(ctx, cursor, remaining)
		if err != nil {
			return result, err
		}
		result.ScannedCount += page.ScannedCount
		result.RangePageCount += page.RangePageCount
		result.BatchGetCount += page.BatchGetCount
		result.BatchGetKeyCount += page.BatchGetKeyCount
		for _, rec := range page.Operations {
			op := mutationOperationToAdminStatus(rec, nil)
			if operationMatchesPageFilter(op, filter) {
				result.Operations = append(result.Operations, op)
			}
		}
		if page.NextCursor != "" {
			result.NextPhase, result.NextCursor = operationPagePhaseMutation, page.NextCursor
		}
	}
	return result, nil
}

func (s *operationStore) listAdminPage(ctx context.Context, cursor string, limit int, filter operationPageFilter) ([]*adminv1.OperationStatus, string, int, int, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys, next, err := s.kv.List(ctx, operationsPrefix(s.root), strings.TrimSpace(cursor), limit)
	if err != nil {
		return nil, "", 0, 0, 0, err
	}
	values, err := batchGetOperationStoreValues(ctx, s.kv, keys)
	if err != nil {
		return nil, "", 0, 0, 0, err
	}
	out := make([]*adminv1.OperationStatus, 0, len(keys))
	for _, key := range keys {
		raw, found := values[key]
		if !found {
			return nil, "", 0, 0, 0, clustermeta.ErrOperationListChanged
		}
		var record storedOperation
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, "", 0, 0, 0, err
		}
		if operationKey(s.root, record.OperationID) != key {
			return nil, "", 0, 0, 0, clustermeta.ErrOperationListInvalid
		}
		op := record.toProto()
		if operationMatchesPageFilter(op, filter) {
			out = append(out, op)
		}
	}
	return out, next, len(keys), operationStoreBatchCount(len(keys)), len(keys), nil
}

func operationMatchesPageFilter(op *adminv1.OperationStatus, filter operationPageFilter) bool {
	if filter.Kind != "" && op.GetKind() != filter.Kind {
		return false
	}
	if filter.State != adminv1.OperationState_OPERATION_STATE_UNSPECIFIED && op.GetState() != filter.State {
		return false
	}
	updated := op.GetLastProgressAt()
	if updated == nil {
		updated = op.GetStartedAt()
	}
	if !filter.UpdatedAfter.IsZero() && (updated == nil || !updated.AsTime().After(filter.UpdatedAfter)) {
		return false
	}
	if !filter.UpdatedBefore.IsZero() && (updated == nil || !updated.AsTime().Before(filter.UpdatedBefore)) {
		return false
	}
	return true
}

func batchGetOperationStoreValues(ctx context.Context, reader interface {
	Get(context.Context, string) ([]byte, bool, error)
}, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	batcher, canBatch := reader.(interface {
		BatchGet(context.Context, []string) (map[string][]byte, error)
	})
	for start := 0; start < len(keys); start += clustermeta.MaintenanceIndexBatchMaximum {
		end := min(start+clustermeta.MaintenanceIndexBatchMaximum, len(keys))
		if canBatch {
			values, err := batcher.BatchGet(ctx, keys[start:end])
			if err != nil {
				return nil, err
			}
			for key, raw := range values {
				out[key] = raw
			}
			continue
		}
		for _, key := range keys[start:end] {
			raw, found, err := reader.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			if found {
				out[key] = raw
			}
		}
	}
	return out, nil
}

func operationStoreBatchCount(keys int) int {
	if keys == 0 {
		return 0
	}
	return (keys + clustermeta.MaintenanceIndexBatchMaximum - 1) / clustermeta.MaintenanceIndexBatchMaximum
}
