package cluster

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type cachedPlanMetadataResolver struct {
	mappingResolver        metadataExtentMappingResolver
	replicaSetResolver     metadataReplicaSetResolver
	nodeResolver           metadataNodeMembershipResolver
	sourceSnapshotLister   metadataSourceSnapshotLister
	ttl                    time.Duration
	mu                     sync.Mutex
	extentMappingsByVolume map[string]cachedExtentMappings
	replicaSetsByVolume    map[string]cachedReplicaSets
	nodeMemberships        cachedNodeMemberships
	snapshotsByVolume      map[string]cachedSnapshotRecords
}

type cachedExtentMappings struct {
	records   []metadata.ExtentMappingRecord
	expiresAt time.Time
}

type cachedReplicaSets struct {
	records   []metadata.ReplicaSetState
	expiresAt time.Time
}

type cachedNodeMemberships struct {
	records   []metadata.NodeMembershipRecord
	expiresAt time.Time
}

type cachedSnapshotRecords struct {
	records   []metadata.SnapshotRecord
	expiresAt time.Time
}

func newCachedPlanMetadataResolver(mappingResolver metadataExtentMappingResolver, replicaSetResolver metadataReplicaSetResolver, nodeResolver metadataNodeMembershipResolver, sourceSnapshotLister metadataSourceSnapshotLister, ttl time.Duration) *cachedPlanMetadataResolver {
	if ttl < 0 {
		ttl = 0
	}
	return &cachedPlanMetadataResolver{
		mappingResolver:        mappingResolver,
		replicaSetResolver:     replicaSetResolver,
		nodeResolver:           nodeResolver,
		sourceSnapshotLister:   sourceSnapshotLister,
		ttl:                    ttl,
		extentMappingsByVolume: make(map[string]cachedExtentMappings),
		replicaSetsByVolume:    make(map[string]cachedReplicaSets),
		snapshotsByVolume:      make(map[string]cachedSnapshotRecords),
	}
}

func (r *cachedPlanMetadataResolver) ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	if r == nil || r.ttl <= 0 {
		return r.mappingResolver.ListExtentMappings(ctx, volumeID)
	}
	now := time.Now()
	r.mu.Lock()
	if entry, ok := r.extentMappingsByVolume[volumeID]; ok && now.Before(entry.expiresAt) {
		records := slices.Clone(entry.records)
		r.mu.Unlock()
		return records, nil
	}
	r.mu.Unlock()

	records, err := r.mappingResolver.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.extentMappingsByVolume[volumeID] = cachedExtentMappings{
		records:   slices.Clone(records),
		expiresAt: now.Add(r.ttl),
	}
	r.mu.Unlock()
	return records, nil
}

func (r *cachedPlanMetadataResolver) ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	if r == nil || r.ttl <= 0 {
		return r.replicaSetResolver.ListReplicaSets(ctx, volumeID)
	}
	now := time.Now()
	r.mu.Lock()
	if entry, ok := r.replicaSetsByVolume[volumeID]; ok && now.Before(entry.expiresAt) {
		records := cloneReplicaSetStates(entry.records)
		r.mu.Unlock()
		return records, nil
	}
	r.mu.Unlock()

	records, err := r.replicaSetResolver.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.replicaSetsByVolume[volumeID] = cachedReplicaSets{
		records:   cloneReplicaSetStates(records),
		expiresAt: now.Add(r.ttl),
	}
	r.mu.Unlock()
	return records, nil
}

func (r *cachedPlanMetadataResolver) ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	if r == nil || r.ttl <= 0 {
		return r.nodeResolver.ListNodeMemberships(ctx)
	}
	now := time.Now()
	r.mu.Lock()
	if now.Before(r.nodeMemberships.expiresAt) {
		records := cloneNodeMembershipRecords(r.nodeMemberships.records)
		r.mu.Unlock()
		return records, nil
	}
	r.mu.Unlock()

	records, err := r.nodeResolver.ListNodeMemberships(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.nodeMemberships = cachedNodeMemberships{
		records:   cloneNodeMembershipRecords(records),
		expiresAt: now.Add(r.ttl),
	}
	r.mu.Unlock()
	return records, nil
}

func (r *cachedPlanMetadataResolver) ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error) {
	if r == nil || r.ttl <= 0 || includeDeleted {
		return r.sourceSnapshotLister.ListSnapshotRecords(ctx, sourceVolumeID, includeDeleted)
	}
	now := time.Now()
	r.mu.Lock()
	if entry, ok := r.snapshotsByVolume[sourceVolumeID]; ok && now.Before(entry.expiresAt) {
		records := slices.Clone(entry.records)
		r.mu.Unlock()
		return records, nil
	}
	r.mu.Unlock()

	records, err := r.sourceSnapshotLister.ListSnapshotRecords(ctx, sourceVolumeID, includeDeleted)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.snapshotsByVolume[sourceVolumeID] = cachedSnapshotRecords{
		records:   slices.Clone(records),
		expiresAt: now.Add(r.ttl),
	}
	r.mu.Unlock()
	return records, nil
}

func cloneReplicaSetStates(in []metadata.ReplicaSetState) []metadata.ReplicaSetState {
	if len(in) == 0 {
		return nil
	}
	out := make([]metadata.ReplicaSetState, len(in))
	for i, record := range in {
		record.Replicas = slices.Clone(record.Replicas)
		record.FailureDomains = slices.Clone(record.FailureDomains)
		out[i] = record
	}
	return out
}

func cloneNodeMembershipRecords(in []metadata.NodeMembershipRecord) []metadata.NodeMembershipRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]metadata.NodeMembershipRecord, len(in))
	for i, record := range in {
		record.Capabilities = slices.Clone(record.Capabilities)
		record.SBSEndpoints = slices.Clone(record.SBSEndpoints)
		out[i] = record
	}
	return out
}
