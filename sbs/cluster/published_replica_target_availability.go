package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

type PublishedReplicaTargetAvailabilityOptions struct {
	Endpoint     string
	ClusterID    string
	SBSClusterID string
	TTL          time.Duration
}

func NewPublishedReplicaTargetAvailabilityProvider(opts PublishedReplicaTargetAvailabilityOptions) ReplicaTargetAvailabilityProvider {
	adminEndpoint := strings.TrimSpace(opts.Endpoint)
	if adminEndpoint == "" {
		return nil
	}
	ttl := opts.TTL
	if ttl == 0 {
		ttl = DefaultVolumeCacheTTL
	}
	clusterRef := &adminv1.ClusterRef{
		ClusterId:    strings.TrimSpace(opts.ClusterID),
		SbsClusterId: strings.TrimSpace(opts.SBSClusterID),
	}
	type cachedAvailability struct {
		targets   map[string]struct{}
		expiresAt time.Time
	}
	var mu sync.Mutex
	cache := make(map[string]cachedAvailability)
	return ReplicaTargetAvailabilityFunc(func(ctx context.Context, volumeID string) (map[string]struct{}, error) {
		cacheKey := strings.TrimSpace(volumeID)
		now := time.Now()
		mu.Lock()
		if entry, ok := cache[cacheKey]; ok && now.Before(entry.expiresAt) {
			targets := cloneStringSet(entry.targets)
			mu.Unlock()
			return targets, nil
		}
		mu.Unlock()

		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		client, err := adminclient.Dial(dialCtx, adminEndpoint)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		resp, err := client.Admin.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{
			Cluster:  clusterRef,
			VolumeId: cacheKey,
		})
		if err != nil {
			return nil, err
		}
		available := make(map[string]struct{}, len(resp.GetTargets()))
		for _, target := range resp.GetTargets() {
			if target == nil || !target.GetUsable() {
				continue
			}
			targetID := strings.TrimSpace(target.GetTargetId())
			if targetID == "" {
				continue
			}
			available[targetID] = struct{}{}
		}
		if len(available) == 0 {
			return nil, fmt.Errorf("no usable replica targets returned by published view for volume %q", cacheKey)
		}
		cacheTTL := ttl
		if resp.GetCacheTtlSeconds() > 0 {
			publishedTTL := time.Duration(resp.GetCacheTtlSeconds()) * time.Second
			if publishedTTL < cacheTTL {
				cacheTTL = publishedTTL
			}
		}
		mu.Lock()
		cache[cacheKey] = cachedAvailability{
			targets:   cloneStringSet(available),
			expiresAt: time.Now().Add(cacheTTL),
		}
		mu.Unlock()
		return available, nil
	})
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
}
