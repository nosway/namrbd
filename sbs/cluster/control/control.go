package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/maintenance"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type metadataStore interface {
	GetNodeMembership(ctx context.Context, nodeID string) (metadata.NodeMembershipRecord, error)
	PutNodeMembership(ctx context.Context, rec metadata.NodeMembershipRecord) error
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error)
	ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error)
	ListVolumeStates(ctx context.Context) ([]metadata.VolumeState, error)
	ListPlacementTransitions(ctx context.Context, volumeID string) ([]metadata.PlacementTransitionRecord, error)
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
}

type nodeHealthDetailStore interface {
	GetNodeHealthDetail(ctx context.Context, nodeID string) (metadata.NodeHealthDetailRecord, error)
}

type repairScanner interface {
	ScanAndFailoverPrimaries(ctx context.Context, volumeID string) (int, error)
	ScanAndEnqueueRepairs(ctx context.Context, volumeID string) (int, error)
}

type nodeHealthAffectedStore interface {
	GetMaintenanceIndexState(ctx context.Context) (metadata.MaintenanceIndexState, error)
	BeginNodeHealthAffectedProgress(ctx context.Context, node metadata.NodeMembershipRecord) (metadata.NodeHealthAffectedProgressRecord, error)
	AdvanceNodeHealthAffectedProgress(ctx context.Context, before metadata.NodeHealthAffectedProgressRecord, sourceCursor string, completed bool) (metadata.NodeHealthAffectedProgressRecord, error)
	ListPlacementByNodePage(ctx context.Context, nodeID, cursor string, limit int) (metadata.PlacementByNodePage, error)
}

type nodePlacementHealthReconciler interface {
	ReconcileNodePlacementHealth(ctx context.Context, nodeID string, index metadata.PlacementByNodeRecord) (maintenance.NodePlacementHealthResult, error)
}

type Controller struct {
	store   metadataStore
	repairs repairScanner
	now     func() time.Time
}

type VolumeSnapshot struct {
	Volume      metadata.VolumeState           `json:"volume"`
	Extents     []metadata.ExtentMappingRecord `json:"extents"`
	ReplicaSets []metadata.ReplicaSetState     `json:"replica_sets"`
}

type NodeSnapshot struct {
	Node   metadata.NodeMembershipRecord    `json:"node"`
	Detail *metadata.NodeHealthDetailRecord `json:"detail,omitempty"`
}

type MetricsSnapshot struct {
	Volumes map[string]int `json:"volumes"`
	Nodes   map[string]int `json:"nodes"`
	Backlog map[string]int `json:"backlog"`
}

type NodeHealthAffectedPageResult struct {
	NodeID                  string `json:"node_id"`
	MembershipRevision      uint64 `json:"membership_revision"`
	RequestedLimit          int    `json:"requested_limit"`
	InputCount              int    `json:"input_count"`
	ProcessedCount          int    `json:"processed_count"`
	PrimaryFailoverCount    int    `json:"primary_failover_count"`
	RepairEnqueuedCount     int    `json:"repair_enqueued_count"`
	ExistingTransitionCount int    `json:"existing_transition_count"`
	NextCursor              string `json:"next_cursor"`
	Completed               bool   `json:"completed"`
	PointGetCount           int    `json:"point_get_count"`
	BatchGetCount           int    `json:"batch_get_count"`
	BatchGetKeyCount        int    `json:"batch_get_key_count"`
	RangePageCount          int    `json:"range_page_count"`
	BackendFullScanCount    int    `json:"backend_full_scan_count"`
	FullCompletionCount     int    `json:"full_completion_count"`
	NestedCompletionCount   int    `json:"nested_completion_count"`
}

func NewController(store metadataStore, repairs repairScanner) *Controller {
	return &Controller{
		store:   store,
		repairs: repairs,
		now:     time.Now,
	}
}

func NewFromRepository(repo *metadata.Repository) *Controller {
	placementApply := NewServiceBackedPlacementApplyAdapter(NewRepositoryBackedPlacementApplyInternalService(repo))
	maintenanceSvc := maintenance.NewServiceWithPlacementApply(repo, placementApply)
	return NewController(repo, maintenanceSvc)
}

func (c *Controller) GetNode(ctx context.Context, nodeID string) (metadata.NodeMembershipRecord, error) {
	return c.store.GetNodeMembership(ctx, nodeID)
}

func (c *Controller) GetNodeSnapshot(ctx context.Context, nodeID string) (NodeSnapshot, error) {
	rec, err := c.store.GetNodeMembership(ctx, nodeID)
	if err != nil {
		return NodeSnapshot{}, err
	}
	snapshot := NodeSnapshot{Node: rec}
	detailStore, ok := c.store.(nodeHealthDetailStore)
	if !ok {
		return snapshot, nil
	}
	detail, err := detailStore.GetNodeHealthDetail(ctx, nodeID)
	if err != nil {
		if err == metadata.ErrNotFound {
			return snapshot, nil
		}
		return NodeSnapshot{}, err
	}
	snapshot.Detail = &detail
	return snapshot, nil
}

func (c *Controller) GetVolume(ctx context.Context, volumeID string) (VolumeSnapshot, error) {
	volume, err := c.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return VolumeSnapshot{}, err
	}
	extents, err := c.store.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return VolumeSnapshot{}, err
	}
	replicaSets, err := c.store.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return VolumeSnapshot{}, err
	}
	return VolumeSnapshot{
		Volume:      volume,
		Extents:     extents,
		ReplicaSets: replicaSets,
	}, nil
}

func (c *Controller) SetNodeHealth(ctx context.Context, nodeID string, next metadata.NodeHealthState) (metadata.NodeMembershipRecord, int, int, error) {
	rec, err := c.SetNodeHealthOnly(ctx, nodeID, next)
	if err != nil {
		return metadata.NodeMembershipRecord{}, 0, 0, err
	}
	ready, err := c.nodeHealthAffectedReady(ctx)
	if err != nil {
		return rec, 0, 0, err
	}
	if ready {
		if rec.HealthState == metadata.NodeHealthSuspect || rec.HealthState == metadata.NodeHealthDown {
			result, err := c.ReconcileNodeHealthTransitionsForNode(ctx, rec.NodeID, metadata.MaintenanceIndexPageDefault)
			return rec, result.PrimaryFailoverCount, result.RepairEnqueuedCount, err
		}
		return rec, 0, 0, nil
	}
	failovers, enqueued, err := c.ReconcileNodeHealthTransitions(ctx)
	return rec, failovers, enqueued, err
}

// SetNodeHealthOnly changes one node without scanning every volume. The
// sharded health reconciler batches these short authority commits and invokes
// ReconcileNodeHealthTransitions once after all state changes in the run.
func (c *Controller) SetNodeHealthOnly(ctx context.Context, nodeID string, next metadata.NodeHealthState) (metadata.NodeMembershipRecord, error) {
	rec, err := c.store.GetNodeMembership(ctx, nodeID)
	if err != nil {
		return metadata.NodeMembershipRecord{}, err
	}
	rec.HealthState = next
	rec.ObservedState = string(next)
	rec.LastHeartbeatUnix = c.now().Unix()
	if err := c.store.PutNodeMembership(ctx, rec); err != nil {
		return metadata.NodeMembershipRecord{}, err
	}
	return c.store.GetNodeMembership(ctx, nodeID)
}

func (c *Controller) nodeHealthAffectedReady(ctx context.Context) (bool, error) {
	store, storeOK := c.store.(nodeHealthAffectedStore)
	_, repairOK := c.repairs.(nodePlacementHealthReconciler)
	if !storeOK || !repairOK {
		return false, nil
	}
	state, err := store.GetMaintenanceIndexState(ctx)
	if errors.Is(err, metadata.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return state.HealthProjectionReady, nil
}

// ReconcileNodeHealthTransitionsForNode processes at most one placement-index
// page for one unhealthy membership revision. Progress advances only after all
// placements in the page have been reconciled successfully.
func (c *Controller) ReconcileNodeHealthTransitionsForNode(ctx context.Context, nodeID string, limit int) (NodeHealthAffectedPageResult, error) {
	if limit == 0 {
		limit = metadata.MaintenanceIndexPageDefault
	}
	result := NodeHealthAffectedPageResult{NodeID: nodeID, RequestedLimit: limit}
	ready, err := c.nodeHealthAffectedReady(ctx)
	if err != nil {
		return result, err
	}
	if !ready {
		return result, fmt.Errorf("%w: node health affected-set projection is not ready", metadata.ErrMaintenanceIndexInvalid)
	}
	store := c.store.(nodeHealthAffectedStore)
	reconciler := c.repairs.(nodePlacementHealthReconciler)
	node, err := c.store.GetNodeMembership(ctx, nodeID)
	if err != nil {
		return result, err
	}
	progress, err := store.BeginNodeHealthAffectedProgress(ctx, node)
	if err != nil {
		return result, err
	}
	result.MembershipRevision = progress.MembershipRevision
	if progress.Completed {
		result.Completed = true
		return result, nil
	}
	page, err := store.ListPlacementByNodePage(ctx, node.NodeID, progress.SourceCursor, limit)
	if err != nil {
		return result, err
	}
	result.InputCount = len(page.Records)
	result.NextCursor = page.NextCursor
	result.PointGetCount = page.PointGetCount
	result.BatchGetCount = page.BatchGetCount
	result.BatchGetKeyCount = page.BatchGetKeyCount
	result.RangePageCount = page.RangePageCount
	result.BackendFullScanCount = page.BackendFullScanCount
	result.FullCompletionCount = page.FullCompletionCount
	result.NestedCompletionCount = page.NestedCompletionCount
	for _, index := range page.Records {
		placement, err := reconciler.ReconcileNodePlacementHealth(ctx, node.NodeID, index)
		if err != nil {
			return result, err
		}
		result.ProcessedCount++
		if placement.PrimaryFailoverCommitted {
			result.PrimaryFailoverCount++
		}
		if placement.RepairEnqueued {
			result.RepairEnqueuedCount++
		}
		if placement.SkippedExistingTransition {
			result.ExistingTransitionCount++
		}
	}
	completed := page.NextCursor == ""
	progress, err = store.AdvanceNodeHealthAffectedProgress(ctx, progress, page.NextCursor, completed)
	if err != nil {
		return result, err
	}
	result.NextCursor = progress.SourceCursor
	result.Completed = progress.Completed
	return result, nil
}

func (c *Controller) ReconcileNodeHealthTransitions(ctx context.Context) (int, int, error) {
	volumes, err := c.store.ListVolumeStates(ctx)
	if err != nil {
		return 0, 0, err
	}
	failovers := 0
	enqueued := 0
	for _, volume := range volumes {
		failoverCount, err := c.repairs.ScanAndFailoverPrimaries(ctx, volume.VolumeID)
		if err != nil {
			return failovers, enqueued, err
		}
		failovers += failoverCount
		count, err := c.repairs.ScanAndEnqueueRepairs(ctx, volume.VolumeID)
		if err != nil {
			return failovers, enqueued, err
		}
		enqueued += count
	}
	return failovers, enqueued, nil
}

func (c *Controller) GetMetrics(ctx context.Context) (MetricsSnapshot, error) {
	volumes, err := c.store.ListVolumeStates(ctx)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	nodes, err := c.store.ListNodeMemberships(ctx)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	snapshot := MetricsSnapshot{
		Volumes: map[string]int{
			"total":       len(volumes),
			"healthy":     0,
			"degraded":    0,
			"repairing":   0,
			"rebalancing": 0,
			"blocked":     0,
		},
		Nodes: map[string]int{
			"total":    len(nodes),
			"healthy":  0,
			"suspect":  0,
			"down":     0,
			"active":   0,
			"draining": 0,
			"removed":  0,
			"joining":  0,
		},
		Backlog: map[string]int{
			"queued":      0,
			"running":     0,
			"failed":      0,
			"completed":   0,
			"repair_like": 0,
			"rebalance":   0,
		},
	}
	for _, volume := range volumes {
		switch volume.Status {
		case metadata.VolumeStatusHealthy:
			snapshot.Volumes["healthy"]++
		case metadata.VolumeStatusDegraded:
			snapshot.Volumes["degraded"]++
		case metadata.VolumeStatusRepairing:
			snapshot.Volumes["repairing"]++
		case metadata.VolumeStatusRebalancing:
			snapshot.Volumes["rebalancing"]++
		case metadata.VolumeStatusBlocked:
			snapshot.Volumes["blocked"]++
		}
		transitions, err := c.store.ListPlacementTransitions(ctx, volume.VolumeID)
		if err != nil {
			return MetricsSnapshot{}, err
		}
		for _, transition := range transitions {
			switch transition.State {
			case metadata.PlacementTransitionQueued:
				snapshot.Backlog["queued"]++
			case metadata.PlacementTransitionRunning:
				snapshot.Backlog["running"]++
			case metadata.PlacementTransitionFailed:
				snapshot.Backlog["failed"]++
			case metadata.PlacementTransitionCompleted:
				snapshot.Backlog["completed"]++
			}
			if transition.Reason == "rebalance" {
				snapshot.Backlog["rebalance"]++
			} else {
				snapshot.Backlog["repair_like"]++
			}
		}
	}
	for _, node := range nodes {
		switch node.HealthState {
		case metadata.NodeHealthHealthy:
			snapshot.Nodes["healthy"]++
		case metadata.NodeHealthSuspect:
			snapshot.Nodes["suspect"]++
		case metadata.NodeHealthDown:
			snapshot.Nodes["down"]++
		}
		switch node.LifecycleState {
		case metadata.NodeLifecycleActive:
			snapshot.Nodes["active"]++
		case metadata.NodeLifecycleDraining:
			snapshot.Nodes["draining"]++
		case metadata.NodeLifecycleRemoved:
			snapshot.Nodes["removed"]++
		case metadata.NodeLifecycleJoining:
			snapshot.Nodes["joining"]++
		}
	}
	return snapshot, nil
}

func (c *Controller) GetHealthDetailMetrics(ctx context.Context) (map[string]uint64, map[string]uint64, error) {
	healthProbe := map[string]uint64{
		"nodes_with_probe_failures":      0,
		"max_consecutive_probe_failures": 0,
	}
	recovery := map[string]uint64{
		"nodes_in_recovery_cooldown":              0,
		"max_recovery_cooldown_remaining_seconds": 0,
	}
	detailStore, ok := c.store.(nodeHealthDetailStore)
	if !ok {
		return healthProbe, recovery, nil
	}
	nodes, err := c.store.ListNodeMemberships(ctx)
	if err != nil {
		return healthProbe, recovery, err
	}
	nowUnix := time.Now().Unix()
	for _, node := range nodes {
		detail, err := detailStore.GetNodeHealthDetail(ctx, node.NodeID)
		if err != nil {
			if err == metadata.ErrNotFound {
				continue
			}
			return healthProbe, recovery, err
		}
		if detail.ConsecutiveProbeFailures > 0 {
			healthProbe["nodes_with_probe_failures"]++
			if uint64(detail.ConsecutiveProbeFailures) > healthProbe["max_consecutive_probe_failures"] {
				healthProbe["max_consecutive_probe_failures"] = uint64(detail.ConsecutiveProbeFailures)
			}
		}
		if detail.RecoveryEligibleAtUnix > nowUnix {
			recovery["nodes_in_recovery_cooldown"]++
			remaining := uint64(detail.RecoveryEligibleAtUnix - nowUnix)
			if remaining > recovery["max_recovery_cooldown_remaining_seconds"] {
				recovery["max_recovery_cooldown_remaining_seconds"] = remaining
			}
		}
	}
	return healthProbe, recovery, nil
}
