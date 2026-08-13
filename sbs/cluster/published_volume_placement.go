package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type ExtentMappingResolver interface {
	ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error)
}

type ReplicaSetResolver interface {
	ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error)
}

type PublishedVolumePlacementOptions struct {
	Endpoint         string
	ClusterID        string
	SBSClusterID     string
	TTL              time.Duration
	FallbackMappings ExtentMappingResolver
	FallbackSets     ReplicaSetResolver
	AllowRawFallback bool
}

type publishedVolumePlacementResolver struct {
	endpoint         string
	clusterRef       *adminv1.ClusterRef
	ttl              time.Duration
	fallbackMappings ExtentMappingResolver
	fallbackSets     ReplicaSetResolver
	allowRawFallback bool

	mu       sync.Mutex
	cache    map[string]publishedVolumePlacementCacheEntry
	warnOnce sync.Once
}

type publishedVolumePlacementCacheEntry struct {
	mappings    []metadata.ExtentMappingRecord
	replicaSets []metadata.ReplicaSetState
	expiresAt   time.Time
}

func NewPublishedVolumePlacementResolvers(opts PublishedVolumePlacementOptions) (ExtentMappingResolver, ReplicaSetResolver) {
	adminEndpoint := strings.TrimSpace(opts.Endpoint)
	if adminEndpoint == "" {
		return opts.FallbackMappings, opts.FallbackSets
	}
	ttl := opts.TTL
	if ttl == 0 {
		ttl = DefaultVolumeCacheTTL
	}
	resolver := &publishedVolumePlacementResolver{
		endpoint: adminEndpoint,
		clusterRef: &adminv1.ClusterRef{
			ClusterId:    strings.TrimSpace(opts.ClusterID),
			SbsClusterId: strings.TrimSpace(opts.SBSClusterID),
		},
		ttl:              ttl,
		fallbackMappings: opts.FallbackMappings,
		fallbackSets:     opts.FallbackSets,
		allowRawFallback: opts.AllowRawFallback,
		cache:            make(map[string]publishedVolumePlacementCacheEntry),
	}
	return resolver, resolver
}

func (r *publishedVolumePlacementResolver) ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	if r == nil {
		return nil, fmt.Errorf("volume placement resolver is not configured")
	}
	canonical := strings.TrimSpace(volumeID)
	if canonical == "" {
		return nil, fmt.Errorf("volume id is required")
	}
	entry, err := r.lookupAndCache(ctx, canonical)
	if err != nil {
		if r.allowRawFallback && r.fallbackMappings != nil {
			r.warnOnce.Do(func() {
				log.Printf("gateway admin volume placement lookup unavailable via sbs-admin endpoint %q: %v; activating legacy raw cluster placement lookup fallback", r.endpoint, err)
			})
			return r.fallbackMappings.ListExtentMappings(ctx, canonical)
		}
		return nil, err
	}
	return append([]metadata.ExtentMappingRecord(nil), entry.mappings...), nil
}

func (r *publishedVolumePlacementResolver) ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	if r == nil {
		return nil, fmt.Errorf("volume placement resolver is not configured")
	}
	canonical := strings.TrimSpace(volumeID)
	if canonical == "" {
		return nil, fmt.Errorf("volume id is required")
	}
	entry, err := r.lookupAndCache(ctx, canonical)
	if err != nil {
		if r.allowRawFallback && r.fallbackSets != nil {
			r.warnOnce.Do(func() {
				log.Printf("gateway admin volume placement lookup unavailable via sbs-admin endpoint %q: %v; activating legacy raw cluster placement lookup fallback", r.endpoint, err)
			})
			return r.fallbackSets.ListReplicaSets(ctx, canonical)
		}
		return nil, err
	}
	return append([]metadata.ReplicaSetState(nil), entry.replicaSets...), nil
}

func (r *publishedVolumePlacementResolver) lookupAndCache(ctx context.Context, volumeID string) (publishedVolumePlacementCacheEntry, error) {
	now := time.Now()
	r.mu.Lock()
	if entry, ok := r.cache[volumeID]; ok && now.Before(entry.expiresAt) {
		r.mu.Unlock()
		return clonePublishedVolumePlacementCacheEntry(entry), nil
	}
	r.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(dialCtx, r.endpoint)
	if err != nil {
		return publishedVolumePlacementCacheEntry{}, err
	}
	defer client.Close()
	resp, err := client.Admin.GetVolumePlacementView(ctx, &adminv1.GetVolumePlacementViewRequest{
		Cluster:  r.clusterRef,
		VolumeId: volumeID,
	})
	if err != nil {
		return publishedVolumePlacementCacheEntry{}, err
	}
	entry := publishedVolumePlacementCacheEntry{
		mappings:    make([]metadata.ExtentMappingRecord, 0, len(resp.GetExtentMappings())),
		replicaSets: make([]metadata.ReplicaSetState, 0, len(resp.GetReplicaSets())),
		expiresAt:   time.Now().Add(r.ttl),
	}
	for _, mapping := range resp.GetExtentMappings() {
		if mapping == nil {
			continue
		}
		entry.mappings = append(entry.mappings, ExtentMappingRecordFromAdmin(mapping))
	}
	for _, replicaSet := range resp.GetReplicaSets() {
		if replicaSet == nil {
			continue
		}
		entry.replicaSets = append(entry.replicaSets, ReplicaSetStateFromAdmin(replicaSet))
	}
	r.mu.Lock()
	r.cache[volumeID] = clonePublishedVolumePlacementCacheEntry(entry)
	r.mu.Unlock()
	return entry, nil
}

func clonePublishedVolumePlacementCacheEntry(entry publishedVolumePlacementCacheEntry) publishedVolumePlacementCacheEntry {
	return publishedVolumePlacementCacheEntry{
		mappings:    append([]metadata.ExtentMappingRecord(nil), entry.mappings...),
		replicaSets: append([]metadata.ReplicaSetState(nil), entry.replicaSets...),
		expiresAt:   entry.expiresAt,
	}
}

func ExtentMappingRecordFromAdmin(mapping *adminv1.ExtentPlacementSummary) metadata.ExtentMappingRecord {
	return metadata.ExtentMappingRecord{
		VolumeID:      strings.TrimSpace(mapping.GetVolumeId()),
		ExtentID:      mapping.GetExtentId(),
		LogicalOffset: mapping.GetLogicalOffset(),
		LengthBytes:   mapping.GetLengthBytes(),
		ChunkID:       mapping.GetChunkId(),
		PlacementRef:  strings.TrimSpace(mapping.GetPlacementRef()),
		Revision:      mapping.GetRevision(),
	}
}

func ReplicaSetStateFromAdmin(replicaSet *adminv1.ReplicaSetSummary) metadata.ReplicaSetState {
	out := metadata.ReplicaSetState{
		ReplicaSetID:     strings.TrimSpace(replicaSet.GetReplicaSetId()),
		VolumeID:         strings.TrimSpace(replicaSet.GetVolumeId()),
		PlacementRef:     strings.TrimSpace(replicaSet.GetPlacementRef()),
		Epoch:            replicaSet.GetEpoch(),
		PrimaryReplicaID: strings.TrimSpace(replicaSet.GetPrimaryReplicaId()),
		WriteQuorum:      replicaSet.GetWriteQuorum(),
		ReadQuorum:       replicaSet.GetReadQuorum(),
		FailureDomains:   append([]string(nil), replicaSet.GetFailureDomains()...),
	}
	for _, replica := range replicaSet.GetReplicas() {
		if replica == nil {
			continue
		}
		out.Replicas = append(out.Replicas, metadata.ReplicaDescriptor{
			NodeID:        strings.TrimSpace(replica.GetNodeId()),
			ReplicaID:     strings.TrimSpace(replica.GetReplicaId()),
			Role:          metadata.ReplicaRole(strings.TrimSpace(replica.GetRole())),
			FailureDomain: strings.TrimSpace(replica.GetFailureDomain()),
		})
	}
	return out
}
