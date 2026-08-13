package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type placementResolver interface {
	ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error)
}

type allocationResolver interface {
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type cloneAllocationResolver interface {
	ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type snapshotAllocationResolver interface {
	ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type sourceSnapshotLister interface {
	ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error)
}

type Coordinator struct {
	placements      placementResolver
	allocations     allocationResolver
	sourceSnapshots sourceSnapshotLister
	cacheTTL        time.Duration
	cacheMu         sync.Mutex
	placementCache  map[writePlanPlacementCacheKey]writePlanPlacementCacheEntry
	cowCache        map[string]writePlanCOWCacheEntry
}

type PlanWriteStats struct {
	ResolvePlacementsDuration   time.Duration
	ResolveAllocationsDuration  time.Duration
	SourceCOWDuration           time.Duration
	BuildTargetsDuration        time.Duration
	ResolvedPlacementCount      int
	ResolvedAllocationPageCount int
	CopyOnWrite                 bool
}

func NewCoordinator(placements placementResolver, allocations ...allocationResolver) *Coordinator {
	var allocation allocationResolver
	var sourceSnapshots sourceSnapshotLister
	if len(allocations) > 0 {
		allocation = allocations[0]
		sourceSnapshots, _ = allocation.(sourceSnapshotLister)
	}
	return &Coordinator{placements: placements, allocations: allocation, sourceSnapshots: sourceSnapshots}
}

// WithWritePlanCacheTTL enables a short-lived lab-only cache for read-only
// placement/COW planning inputs. Allocation pages are deliberately not cached.
func (c *Coordinator) WithWritePlanCacheTTL(ttl time.Duration) *Coordinator {
	if c == nil {
		return c
	}
	if ttl < 0 {
		ttl = 0
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.cacheTTL = ttl
	if ttl <= 0 {
		c.placementCache = nil
		c.cowCache = nil
		return c
	}
	if c.placementCache == nil {
		c.placementCache = make(map[writePlanPlacementCacheKey]writePlanPlacementCacheEntry)
	}
	if c.cowCache == nil {
		c.cowCache = make(map[string]writePlanCOWCacheEntry)
	}
	return c
}

func (c *Coordinator) WithSourceSnapshotLister(lister sourceSnapshotLister) *Coordinator {
	c.sourceSnapshots = lister
	return c
}

type writePlanPlacementCacheKey struct {
	volumeID    string
	offsetBytes uint64
	lengthBytes uint64
}

type writePlanPlacementCacheEntry struct {
	placements []metadata.ResolvedExtentPlacement
	expiresAt  time.Time
}

type writePlanCOWCacheEntry struct {
	copyOnWrite bool
	expiresAt   time.Time
}

type ReplicaTarget struct {
	NodeID        string
	ReplicaID     string
	Role          metadata.ReplicaRole
	FailureDomain string
}

type ExtentWritePlan struct {
	Extent           metadata.ExtentMappingRecord
	PlacementRef     string
	ReplicaSetID     string
	Primary          ReplicaTarget
	WriteTargets     []ReplicaTarget
	RequiredAcks     uint32
	ReplicaSetEpoch  uint64
	MetadataRevision uint64
	AllocationPages  []metadata.ResolvedAllocationPage
	ChunkSizeBytes   uint32
	CopyOnWrite      bool
	BaseAllocations  []metadata.ResolvedAllocationPage
}

type WritePlan struct {
	VolumeID string
	Extents  []ExtentWritePlan
}

type ExtentReadPlan struct {
	Extent            metadata.ExtentMappingRecord
	PlacementRef      string
	ReplicaSetID      string
	Preferred         ReplicaTarget
	Fallbacks         []ReplicaTarget
	ReplicaSetEpoch   uint64
	CommittedRevision uint64
	AllocationPages   []metadata.ResolvedAllocationPage
	ChunkSizeBytes    uint32
}

type ReadPlan struct {
	VolumeID string
	Extents  []ExtentReadPlan
}

func (c *Coordinator) PlanWrite(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, error) {
	plan, _, err := c.PlanWriteWithStats(ctx, volumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
	return plan, err
}

func (c *Coordinator) PlanWriteWithStats(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, PlanWriteStats, error) {
	var stats PlanWriteStats
	stepStart := time.Now()
	resolved, err := c.resolveExtentPlacementsForWrite(ctx, volumeID, offsetBytes, lengthBytes)
	stats.ResolvePlacementsDuration = time.Since(stepStart)
	stats.ResolvedPlacementCount = len(resolved)
	if err != nil {
		return nil, stats, err
	}
	var allocationPages []metadata.ResolvedAllocationPage
	if pageBytes > 0 && chunkSizeBytes > 0 {
		if c.allocations != nil {
			stepStart = time.Now()
			allocationPages, err = c.allocations.ResolveAllocationPages(ctx, volumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
			stats.ResolveAllocationsDuration = time.Since(stepStart)
			stats.ResolvedAllocationPageCount = len(allocationPages)
			if err != nil {
				return nil, stats, err
			}
		}
	}
	stepStart = time.Now()
	copyOnWrite, err := c.sourceWriteRequiresCopyOnWrite(ctx, volumeID)
	stats.SourceCOWDuration = time.Since(stepStart)
	stats.CopyOnWrite = copyOnWrite
	if err != nil {
		return nil, stats, err
	}
	stepStart = time.Now()
	plan := &WritePlan{
		VolumeID: volumeID,
		Extents:  make([]ExtentWritePlan, 0, len(resolved)),
	}
	for _, placement := range resolved {
		primary, targets, err := buildWriteTargets(placement.ReplicaSet, placement.Nodes)
		if err != nil {
			return nil, stats, fmt.Errorf("extent %d: %w", placement.ExtentMapping.ExtentID, err)
		}
		if uint32(len(targets)) < placement.ReplicaSet.WriteQuorum {
			return nil, stats, fmt.Errorf("extent %d: insufficient replicas for write quorum", placement.ExtentMapping.ExtentID)
		}
		extentAllocationPages := overlapAllocationPagesForExtent(allocationPages, placement.ExtentMapping)
		extentPlan := ExtentWritePlan{
			Extent:           placement.ExtentMapping,
			PlacementRef:     placement.ExtentMapping.PlacementRef,
			ReplicaSetID:     placement.ReplicaSet.ReplicaSetID,
			Primary:          primary,
			WriteTargets:     targets,
			RequiredAcks:     placement.ReplicaSet.WriteQuorum,
			ReplicaSetEpoch:  placement.ReplicaSet.Epoch,
			MetadataRevision: placement.ExtentMapping.Revision,
			AllocationPages:  extentAllocationPages,
			ChunkSizeBytes:   chunkSizeBytes,
		}
		if copyOnWrite {
			extentPlan.CopyOnWrite = true
			extentPlan.BaseAllocations = cloneResolvedAllocationPages(extentAllocationPages)
		}
		plan.Extents = append(plan.Extents, extentPlan)
	}
	stats.BuildTargetsDuration = time.Since(stepStart)
	return plan, stats, nil
}

func (c *Coordinator) resolveExtentPlacementsForWrite(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error) {
	if c == nil || c.placements == nil {
		return nil, fmt.Errorf("extent placement resolver is not configured")
	}
	if c.cacheTTL <= 0 {
		return c.placements.ResolveExtentPlacements(ctx, volumeID, offsetBytes, lengthBytes)
	}
	key := writePlanPlacementCacheKey{volumeID: volumeID, offsetBytes: offsetBytes, lengthBytes: lengthBytes}
	now := time.Now()
	c.cacheMu.Lock()
	if entry, ok := c.placementCache[key]; ok && now.Before(entry.expiresAt) {
		out := cloneResolvedExtentPlacements(entry.placements)
		c.cacheMu.Unlock()
		return out, nil
	}
	c.cacheMu.Unlock()

	resolved, err := c.placements.ResolveExtentPlacements(ctx, volumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	c.cacheMu.Lock()
	if c.cacheTTL > 0 {
		if c.placementCache == nil {
			c.placementCache = make(map[writePlanPlacementCacheKey]writePlanPlacementCacheEntry)
		}
		c.placementCache[key] = writePlanPlacementCacheEntry{
			placements: cloneResolvedExtentPlacements(resolved),
			expiresAt:  now.Add(c.cacheTTL),
		}
	}
	c.cacheMu.Unlock()
	return resolved, nil
}

func (c *Coordinator) sourceWriteRequiresCopyOnWrite(ctx context.Context, volumeID string) (bool, error) {
	snapshots := c.sourceSnapshots
	if snapshots == nil {
		return false, nil
	}
	if c.cacheTTL > 0 {
		now := time.Now()
		c.cacheMu.Lock()
		if entry, ok := c.cowCache[volumeID]; ok && now.Before(entry.expiresAt) {
			copyOnWrite := entry.copyOnWrite
			c.cacheMu.Unlock()
			return copyOnWrite, nil
		}
		c.cacheMu.Unlock()
	}
	records, err := snapshots.ListSnapshotRecords(ctx, volumeID, false)
	if err != nil {
		if errors.Is(err, metadata.ErrSnapshotRecordListerNotConfigured) {
			return false, nil
		}
		return false, err
	}
	copyOnWrite := false
	for _, record := range records {
		switch record.State {
		case metadata.SnapshotStateCreating, metadata.SnapshotStateAvailable, metadata.SnapshotStateDeleting:
			copyOnWrite = true
		}
	}
	if c.cacheTTL > 0 {
		c.cacheMu.Lock()
		if c.cowCache == nil {
			c.cowCache = make(map[string]writePlanCOWCacheEntry)
		}
		c.cowCache[volumeID] = writePlanCOWCacheEntry{
			copyOnWrite: copyOnWrite,
			expiresAt:   time.Now().Add(c.cacheTTL),
		}
		c.cacheMu.Unlock()
	}
	return copyOnWrite, nil
}

func cloneResolvedExtentPlacements(in []metadata.ResolvedExtentPlacement) []metadata.ResolvedExtentPlacement {
	if len(in) == 0 {
		return nil
	}
	out := make([]metadata.ResolvedExtentPlacement, len(in))
	for i, placement := range in {
		placement.ReplicaSet.Replicas = slices.Clone(placement.ReplicaSet.Replicas)
		placement.ReplicaSet.FailureDomains = slices.Clone(placement.ReplicaSet.FailureDomains)
		if placement.Nodes != nil {
			nodes := make(map[string]metadata.NodeMembershipRecord, len(placement.Nodes))
			for key, node := range placement.Nodes {
				node.Capabilities = slices.Clone(node.Capabilities)
				node.SBSEndpoints = slices.Clone(node.SBSEndpoints)
				nodes[key] = node
			}
			placement.Nodes = nodes
		}
		out[i] = placement
	}
	return out
}

func (c *Coordinator) PlanCloneWrite(ctx context.Context, cloneID, sourceVolumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*WritePlan, error) {
	resolved, err := c.placements.ResolveExtentPlacements(ctx, sourceVolumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	var allocationPages []metadata.ResolvedAllocationPage
	if pageBytes > 0 && chunkSizeBytes > 0 {
		cloneAllocations, ok := c.allocations.(cloneAllocationResolver)
		if !ok || cloneAllocations == nil {
			return nil, fmt.Errorf("clone allocation resolver is not configured")
		}
		allocationPages, err = cloneAllocations.ResolveCloneAllocationPages(ctx, cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		if err != nil {
			return nil, err
		}
	}
	plan, err := c.buildWritePlanFromResolved(sourceVolumeID, resolved, allocationPages, chunkSizeBytes)
	if err != nil {
		return nil, err
	}
	for i := range plan.Extents {
		plan.Extents[i].CopyOnWrite = true
		plan.Extents[i].BaseAllocations = cloneResolvedAllocationPages(plan.Extents[i].AllocationPages)
	}
	return plan, nil
}

func (c *Coordinator) buildWritePlanFromResolved(volumeID string, resolved []metadata.ResolvedExtentPlacement, allocationPages []metadata.ResolvedAllocationPage, chunkSizeBytes uint32) (*WritePlan, error) {
	plan := &WritePlan{
		VolumeID: volumeID,
		Extents:  make([]ExtentWritePlan, 0, len(resolved)),
	}
	for _, placement := range resolved {
		primary, targets, err := buildWriteTargets(placement.ReplicaSet, placement.Nodes)
		if err != nil {
			return nil, fmt.Errorf("extent %d: %w", placement.ExtentMapping.ExtentID, err)
		}
		if uint32(len(targets)) < placement.ReplicaSet.WriteQuorum {
			return nil, fmt.Errorf("extent %d: insufficient replicas for write quorum", placement.ExtentMapping.ExtentID)
		}
		extentAllocationPages := overlapAllocationPagesForExtent(allocationPages, placement.ExtentMapping)
		plan.Extents = append(plan.Extents, ExtentWritePlan{
			Extent:           placement.ExtentMapping,
			PlacementRef:     placement.ExtentMapping.PlacementRef,
			ReplicaSetID:     placement.ReplicaSet.ReplicaSetID,
			Primary:          primary,
			WriteTargets:     targets,
			RequiredAcks:     placement.ReplicaSet.WriteQuorum,
			ReplicaSetEpoch:  placement.ReplicaSet.Epoch,
			MetadataRevision: placement.ExtentMapping.Revision,
			AllocationPages:  extentAllocationPages,
			ChunkSizeBytes:   chunkSizeBytes,
		})
	}
	return plan, nil
}

func (c *Coordinator) PlanRead(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*ReadPlan, error) {
	resolved, err := c.placements.ResolveExtentPlacements(ctx, volumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	var allocationPages []metadata.ResolvedAllocationPage
	if pageBytes > 0 && chunkSizeBytes > 0 {
		if c.allocations != nil {
			allocationPages, err = c.allocations.ResolveAllocationPages(ctx, volumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
			if err != nil {
				return nil, err
			}
		}
	}
	return c.buildReadPlanFromResolved(volumeID, resolved, allocationPages, chunkSizeBytes)
}

func (c *Coordinator) PlanCloneRead(ctx context.Context, cloneID, sourceVolumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*ReadPlan, error) {
	resolved, err := c.placements.ResolveExtentPlacements(ctx, sourceVolumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	var allocationPages []metadata.ResolvedAllocationPage
	if pageBytes > 0 && chunkSizeBytes > 0 {
		cloneAllocations, ok := c.allocations.(cloneAllocationResolver)
		if !ok || cloneAllocations == nil {
			return nil, fmt.Errorf("clone allocation resolver is not configured")
		}
		allocationPages, err = cloneAllocations.ResolveCloneAllocationPages(ctx, cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		if err != nil {
			return nil, err
		}
	}
	return c.buildReadPlanFromResolved(sourceVolumeID, resolved, allocationPages, chunkSizeBytes)
}

func (c *Coordinator) PlanSnapshotRead(ctx context.Context, snapshotID, sourceVolumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) (*ReadPlan, error) {
	resolved, err := c.placements.ResolveExtentPlacements(ctx, sourceVolumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	var allocationPages []metadata.ResolvedAllocationPage
	if pageBytes > 0 && chunkSizeBytes > 0 {
		snapshotAllocations, ok := c.allocations.(snapshotAllocationResolver)
		if !ok || snapshotAllocations == nil {
			return nil, fmt.Errorf("snapshot allocation resolver is not configured")
		}
		allocationPages, err = snapshotAllocations.ResolveSnapshotAllocationPages(ctx, snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
		if err != nil {
			return nil, err
		}
	}
	return c.buildReadPlanFromResolved(sourceVolumeID, resolved, allocationPages, chunkSizeBytes)
}

func (c *Coordinator) buildReadPlanFromResolved(volumeID string, resolved []metadata.ResolvedExtentPlacement, allocationPages []metadata.ResolvedAllocationPage, chunkSizeBytes uint32) (*ReadPlan, error) {
	plan := &ReadPlan{
		VolumeID: volumeID,
		Extents:  make([]ExtentReadPlan, 0, len(resolved)),
	}
	for _, placement := range resolved {
		primary, fallbacks, err := buildReadTargets(placement.ReplicaSet, placement.Nodes)
		if err != nil {
			return nil, fmt.Errorf("extent %d: %w", placement.ExtentMapping.ExtentID, err)
		}
		extentAllocationPages := overlapAllocationPagesForExtent(allocationPages, placement.ExtentMapping)
		plan.Extents = append(plan.Extents, ExtentReadPlan{
			Extent:            placement.ExtentMapping,
			PlacementRef:      placement.ExtentMapping.PlacementRef,
			ReplicaSetID:      placement.ReplicaSet.ReplicaSetID,
			Preferred:         primary,
			Fallbacks:         fallbacks,
			ReplicaSetEpoch:   placement.ReplicaSet.Epoch,
			CommittedRevision: placement.ExtentMapping.Revision,
			AllocationPages:   extentAllocationPages,
			ChunkSizeBytes:    chunkSizeBytes,
		})
	}
	return plan, nil
}

func overlapAllocationPagesForExtent(pages []metadata.ResolvedAllocationPage, extent metadata.ExtentMappingRecord) []metadata.ResolvedAllocationPage {
	if len(pages) == 0 || extent.LengthBytes == 0 {
		return nil
	}
	extentStart := extent.LogicalOffset
	extentEnd := extent.LogicalOffset + extent.LengthBytes
	out := make([]metadata.ResolvedAllocationPage, 0, len(pages))
	for _, page := range pages {
		pageStart := page.RangeStartChunk * uint64(page.Page.ChunkSizeBytes)
		pageEnd := page.RangeEndChunk * uint64(page.Page.ChunkSizeBytes)
		if pageEnd <= extentStart || pageStart >= extentEnd {
			continue
		}
		out = append(out, page)
	}
	return out
}

func buildWriteTargets(replicaSet metadata.ReplicaSetState, nodes map[string]metadata.NodeMembershipRecord) (ReplicaTarget, []ReplicaTarget, error) {
	if len(replicaSet.Replicas) == 0 {
		return ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no replicas", replicaSet.ReplicaSetID)
	}
	candidates := writableReplicas(replicaSet.Replicas, nodes)
	if len(candidates) == 0 {
		return ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no writable replicas", replicaSet.ReplicaSetID)
	}
	primary, err := findPrimary(metadata.ReplicaSetState{ReplicaSetID: replicaSet.ReplicaSetID, PrimaryReplicaID: replicaSet.PrimaryReplicaID, Replicas: candidates})
	if err != nil {
		primary = toTarget(candidates[0])
	}
	targets := make([]ReplicaTarget, 0, len(candidates))
	targets = append(targets, primary)
	for _, replica := range candidates {
		if replica.ReplicaID == primary.ReplicaID {
			continue
		}
		targets = append(targets, toTarget(replica))
	}
	return primary, targets, nil
}

func buildReadTargets(replicaSet metadata.ReplicaSetState, nodes map[string]metadata.NodeMembershipRecord) (ReplicaTarget, []ReplicaTarget, error) {
	candidates := readableReplicas(replicaSet.Replicas, nodes)
	if len(candidates) == 0 {
		return ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no readable replicas", replicaSet.ReplicaSetID)
	}
	primary, targets, err := buildWriteTargets(metadata.ReplicaSetState{
		ReplicaSetID:     replicaSet.ReplicaSetID,
		PrimaryReplicaID: replicaSet.PrimaryReplicaID,
		WriteQuorum:      replicaSet.WriteQuorum,
		Replicas:         candidates,
	}, nil)
	if err != nil {
		return ReplicaTarget{}, nil, err
	}
	if len(targets) == 0 {
		return ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no readable targets", replicaSet.ReplicaSetID)
	}
	return primary, targets[1:], nil
}

func writableReplicas(replicas []metadata.ReplicaDescriptor, nodes map[string]metadata.NodeMembershipRecord) []metadata.ReplicaDescriptor {
	out := make([]metadata.ReplicaDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		if replicaWritable(replica, nodes) {
			out = append(out, replica)
		}
	}
	return out
}

func readableReplicas(replicas []metadata.ReplicaDescriptor, nodes map[string]metadata.NodeMembershipRecord) []metadata.ReplicaDescriptor {
	out := make([]metadata.ReplicaDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		if replicaReadable(replica, nodes) {
			out = append(out, replica)
		}
	}
	return out
}

func replicaWritable(replica metadata.ReplicaDescriptor, nodes map[string]metadata.NodeMembershipRecord) bool {
	node, ok := nodes[replica.NodeID]
	if !ok {
		return true
	}
	if node.LifecycleState != "" && node.LifecycleState != metadata.NodeLifecycleActive {
		return false
	}
	return node.HealthState == "" || node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect
}

func replicaReadable(replica metadata.ReplicaDescriptor, nodes map[string]metadata.NodeMembershipRecord) bool {
	node, ok := nodes[replica.NodeID]
	if !ok {
		return true
	}
	if node.LifecycleState == metadata.NodeLifecycleRemoved || node.LifecycleState == metadata.NodeLifecycleJoining {
		return false
	}
	return node.HealthState == "" || node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect
}

func findPrimary(replicaSet metadata.ReplicaSetState) (ReplicaTarget, error) {
	if replicaSet.PrimaryReplicaID == "" {
		return ReplicaTarget{}, fmt.Errorf("replica set %q has no primary replica id", replicaSet.ReplicaSetID)
	}
	for _, replica := range replicaSet.Replicas {
		if replica.ReplicaID == replicaSet.PrimaryReplicaID || replica.Role == metadata.ReplicaRolePrimary {
			return toTarget(replica), nil
		}
	}
	return ReplicaTarget{}, fmt.Errorf("replica set %q primary %q not found", replicaSet.ReplicaSetID, replicaSet.PrimaryReplicaID)
}

func toTarget(replica metadata.ReplicaDescriptor) ReplicaTarget {
	return ReplicaTarget{
		NodeID:        replica.NodeID,
		ReplicaID:     replica.ReplicaID,
		Role:          replica.Role,
		FailureDomain: replica.FailureDomain,
	}
}
