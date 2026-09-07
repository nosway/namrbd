package metadata

import (
	"context"
	"fmt"
	"strings"
)

const NodeHealthAffectedSchemaVersion = 1

type NodeHealthAffectedProgressRecord struct {
	SchemaVersion      int             `json:"schema_version"`
	NodeID             string          `json:"node_id"`
	MembershipRevision uint64          `json:"membership_revision"`
	HealthState        NodeHealthState `json:"health_state"`
	SourceCursor       string          `json:"source_cursor"`
	Completed          bool            `json:"completed"`
	UpdatedAtUnix      int64           `json:"updated_at_unix"`
	ProgressDigest     string          `json:"progress_digest"`
}

type NodeHealthAffectedProgressPointRead struct {
	Progress              NodeHealthAffectedProgressRecord `json:"progress"`
	PointGetCount         int                              `json:"point_get_count"`
	BackendFullScanCount  int                              `json:"backend_full_scan_count"`
	FullCompletionCount   int                              `json:"full_completion_count"`
	NestedCompletionCount int                              `json:"nested_completion_count"`
}

func (r *Repository) BeginNodeHealthAffectedProgress(ctx context.Context, node NodeMembershipRecord) (NodeHealthAffectedProgressRecord, error) {
	node.NodeID = strings.TrimSpace(node.NodeID)
	if r == nil || node.NodeID == "" || node.MembershipRevision == 0 || (node.HealthState != NodeHealthSuspect && node.HealthState != NodeHealthDown) {
		return NodeHealthAffectedProgressRecord{}, fmt.Errorf("%w: node health affected progress identity", ErrMaintenanceIndexInvalid)
	}
	var result NodeHealthAffectedProgressRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		key := nodeHealthAffectedProgressKey(r.root, node.NodeID)
		var existing NodeHealthAffectedProgressRecord
		found, err := getOptionalJSONStore(ctx, store, key, &existing)
		if err != nil {
			return err
		}
		if found {
			if err := validateNodeHealthAffectedProgress(existing); err != nil {
				return err
			}
			if existing.MembershipRevision == node.MembershipRevision {
				if existing.HealthState != node.HealthState {
					return fmt.Errorf("%w: membership revision changed health identity", ErrMaintenanceIndexConflict)
				}
				result = existing
				return nil
			}
			if existing.MembershipRevision > node.MembershipRevision {
				return ErrCASConflict
			}
		}
		result = NodeHealthAffectedProgressRecord{
			SchemaVersion: NodeHealthAffectedSchemaVersion,
			NodeID:        node.NodeID, MembershipRevision: node.MembershipRevision,
			HealthState: node.HealthState, UpdatedAtUnix: r.now().UTC().Unix(),
		}
		result.ProgressDigest = digestNodeHealthAffectedProgress(result)
		return putJSONStore(ctx, store, key, result)
	})
	return result, err
}

func (r *Repository) GetNodeHealthAffectedProgress(ctx context.Context, nodeID string) (NodeHealthAffectedProgressRecord, error) {
	point, err := r.GetNodeHealthAffectedProgressPoint(ctx, nodeID)
	return point.Progress, err
}

func (r *Repository) GetNodeHealthAffectedProgressPoint(ctx context.Context, nodeID string) (NodeHealthAffectedProgressPointRead, error) {
	nodeID = strings.TrimSpace(nodeID)
	result := NodeHealthAffectedProgressPointRead{}
	if r == nil || nodeID == "" {
		return result, fmt.Errorf("%w: node health affected progress node", ErrMaintenanceIndexInvalid)
	}
	result.PointGetCount = 1
	var progress NodeHealthAffectedProgressRecord
	found, err := getOptionalJSONStore(ctx, r.kv, nodeHealthAffectedProgressKey(r.root, nodeID), &progress)
	if err != nil {
		return result, err
	}
	if !found {
		return result, ErrNotFound
	}
	if err := validateNodeHealthAffectedProgress(progress); err != nil {
		return result, err
	}
	result.Progress = progress
	return result, nil
}

func (r *Repository) AdvanceNodeHealthAffectedProgress(ctx context.Context, before NodeHealthAffectedProgressRecord, sourceCursor string, completed bool) (NodeHealthAffectedProgressRecord, error) {
	if err := validateNodeHealthAffectedProgress(before); err != nil {
		return NodeHealthAffectedProgressRecord{}, err
	}
	if completed && sourceCursor != "" {
		return NodeHealthAffectedProgressRecord{}, fmt.Errorf("%w: completed node health cursor is not empty", ErrMaintenanceIndexInvalid)
	}
	if !completed && sourceCursor == "" {
		return NodeHealthAffectedProgressRecord{}, fmt.Errorf("%w: incomplete node health cursor is empty", ErrMaintenanceIndexInvalid)
	}
	var result NodeHealthAffectedProgressRecord
	err := r.applyIndexedWrite(ctx, func(store kvReadWriter) error {
		key := nodeHealthAffectedProgressKey(r.root, before.NodeID)
		var current NodeHealthAffectedProgressRecord
		found, err := getOptionalJSONStore(ctx, store, key, &current)
		if err != nil {
			return err
		}
		if !found || current.ProgressDigest != before.ProgressDigest {
			return ErrCASConflict
		}
		current.SourceCursor = sourceCursor
		current.Completed = completed
		current.UpdatedAtUnix = r.now().UTC().Unix()
		current.ProgressDigest = digestNodeHealthAffectedProgress(current)
		result = current
		return putJSONStore(ctx, store, key, current)
	})
	return result, err
}

func validateNodeHealthAffectedProgress(progress NodeHealthAffectedProgressRecord) error {
	validHealth := progress.HealthState == NodeHealthSuspect || progress.HealthState == NodeHealthDown
	if progress.SchemaVersion != NodeHealthAffectedSchemaVersion || progress.NodeID == "" || progress.MembershipRevision == 0 || !validHealth || progress.Completed && progress.SourceCursor != "" || progress.UpdatedAtUnix <= 0 || progress.ProgressDigest != digestNodeHealthAffectedProgress(progress) {
		return fmt.Errorf("%w: node health affected progress record", ErrMaintenanceIndexInvalid)
	}
	return nil
}

func digestNodeHealthAffectedProgress(progress NodeHealthAffectedProgressRecord) string {
	progress.ProgressDigest = ""
	return digestSummaryValue(progress)
}

func nodeHealthAffectedProgressKey(root, nodeID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/node-health-affected/%s", root, escapeMaintenanceIndexPart(nodeID))
}
