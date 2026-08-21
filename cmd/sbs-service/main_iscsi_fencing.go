package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

const iscsiWriterFenceProjectionConcurrency = 16
const iscsiWriterFenceProjectionTimeout = 10 * time.Second

// projectISCSIWriterFence requires an acknowledgement from every SBS receiver
// currently registered with sbs-service before TiKV publishes the new writer.
// A partial projection is safe to retry because epochs are monotonic and equal
// fence values are idempotent at sbs-data.
func (s *server) projectISCSIWriterFence(ctx context.Context, fence service.ISCSIWriterFence) error {
	if s.iscsiWriterFenceProjector != nil {
		return s.iscsiWriterFenceProjector(ctx, fence)
	}
	if err := fence.Validate(); err != nil {
		return err
	}
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return fmt.Errorf("list SBS receivers: %w", err)
	}
	endpoints := make([]string, 0, len(nodes))
	seen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if node.LifecycleState == clustermeta.NodeLifecycleRemoved {
			continue
		}
		endpoint := strings.TrimSpace(endpointString(node.SBSEndpoints))
		if endpoint == "" || seen[endpoint] {
			continue
		}
		seen[endpoint] = true
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	if len(endpoints) == 0 {
		return fmt.Errorf("no registered SBS receiver endpoints")
	}

	semaphore := make(chan struct{}, iscsiWriterFenceProjectionConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errorsByEndpoint := make([]string, 0)
	for _, endpoint := range endpoints {
		endpoint := endpoint
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				mu.Lock()
				errorsByEndpoint = append(errorsByEndpoint, endpoint+": "+ctx.Err().Error())
				mu.Unlock()
				return
			case semaphore <- struct{}{}:
			}
			defer func() { <-semaphore }()
			client, err := s.cache.GetISCSIWriterFenceClient(endpoint)
			if err == nil {
				var resp *service.ApplyISCSIWriterFenceResponse
				projectionCtx, cancel := context.WithTimeout(ctx, iscsiWriterFenceProjectionTimeout)
				resp, err = client.ApplyISCSIWriterFence(projectionCtx, &service.ApplyISCSIWriterFenceRequest{Fence: fence})
				cancel()
				if err == nil && (resp == nil || resp.Fence != fence) {
					err = fmt.Errorf("receiver acknowledged a different writer fence")
				}
			}
			if err != nil {
				mu.Lock()
				errorsByEndpoint = append(errorsByEndpoint, endpoint+": "+err.Error())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(errorsByEndpoint) > 0 {
		sort.Strings(errorsByEndpoint)
		return fmt.Errorf("%d/%d SBS receiver fence projections failed: %s", len(errorsByEndpoint), len(endpoints), strings.Join(errorsByEndpoint, "; "))
	}
	return nil
}
