package control

import (
	"context"
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
	return rec, nil
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
