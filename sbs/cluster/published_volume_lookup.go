package cluster

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/adminclient"
	"github.com/nosway/namrbd/internal/structuredlog"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	grpcmetadata "google.golang.org/grpc/metadata"
)

const (
	publishedVolumeSummaryModeMetadataKey = "namrbd-volume-summary-mode"
	publishedVolumeSummaryModeSpecOnly    = "spec-only"
)

type PublishedVolumeLookupOptions struct {
	Endpoint         string
	ClusterID        string
	SBSClusterID     string
	Fallback         VolumeLookup
	AllowRawFallback bool
	TTL              time.Duration
}

type publishedVolumeLookup struct {
	endpoint         string
	clusterRef       *adminv1.ClusterRef
	ttl              time.Duration
	fallback         VolumeLookup
	allowRawFallback bool

	mu       sync.Mutex
	cache    map[uint64]publishedVolumeCacheEntry
	warnOnce sync.Once
}

type publishedVolumeCacheEntry struct {
	spec      service.VolumeSpec
	expiresAt time.Time
}

func NewPublishedVolumeLookup(opts PublishedVolumeLookupOptions) VolumeLookup {
	adminEndpoint := strings.TrimSpace(opts.Endpoint)
	if adminEndpoint == "" {
		return opts.Fallback
	}
	ttl := opts.TTL
	if ttl == 0 {
		ttl = DefaultVolumeCacheTTL
	}
	return &publishedVolumeLookup{
		endpoint: adminEndpoint,
		clusterRef: &adminv1.ClusterRef{
			ClusterId:    strings.TrimSpace(opts.ClusterID),
			SbsClusterId: strings.TrimSpace(opts.SBSClusterID),
		},
		ttl:              ttl,
		fallback:         opts.Fallback,
		allowRawFallback: opts.AllowRawFallback,
		cache:            make(map[uint64]publishedVolumeCacheEntry),
	}
}

func (l *publishedVolumeLookup) GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if l == nil {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	now := time.Now()
	l.mu.Lock()
	if entry, ok := l.cache[volumeID]; ok && now.Before(entry.expiresAt) {
		spec := entry.spec
		l.mu.Unlock()
		return spec, nil
	}
	l.mu.Unlock()

	lookupErr := l.lookupAndCache(ctx, volumeID)
	if lookupErr == nil {
		l.mu.Lock()
		entry := l.cache[volumeID]
		spec := entry.spec
		l.mu.Unlock()
		return spec, nil
	}
	if l.allowRawFallback && l.fallback != nil {
		l.warnOnce.Do(func() {
			log.Printf("gateway admin volume lookup unavailable via sbs-admin endpoint %q: %v; activating legacy raw cluster metadata lookup fallback", l.endpoint, lookupErr)
		})
		return l.fallback.GetVolume(ctx, volumeID)
	}
	return service.VolumeSpec{}, lookupErr
}

func (l *publishedVolumeLookup) RefreshVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if l == nil {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	lookupErr := l.lookupAndCache(ctx, volumeID)
	if lookupErr == nil {
		l.mu.Lock()
		entry := l.cache[volumeID]
		spec := entry.spec
		l.mu.Unlock()
		return spec, nil
	}
	if l.allowRawFallback && l.fallback != nil {
		if fallback, ok := l.fallback.(interface {
			RefreshVolume(context.Context, uint64) (service.VolumeSpec, error)
		}); ok {
			return fallback.RefreshVolume(ctx, volumeID)
		}
		return l.fallback.GetVolume(ctx, volumeID)
	}
	return service.VolumeSpec{}, lookupErr
}

func (l *publishedVolumeLookup) lookupAndCache(ctx context.Context, volumeID uint64) error {
	started := time.Now()
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(lookupCtx, l.endpoint)
	if err != nil {
		structuredlog.Error("gateway.admin_volume_lookup", "get_volume_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("endpoint", l.endpoint),
			structuredlog.F("phase", "dial"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		)
		return err
	}
	defer client.Close()
	rpcCtx := grpcmetadata.AppendToOutgoingContext(lookupCtx, publishedVolumeSummaryModeMetadataKey, publishedVolumeSummaryModeSpecOnly)
	resp, err := client.Admin.GetVolume(rpcCtx, &adminv1.GetVolumeRequest{
		Cluster:  l.clusterRef,
		VolumeId: service.CanonicalVolumeID(volumeID),
	})
	if err != nil {
		structuredlog.Error("gateway.admin_volume_lookup", "get_volume_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("endpoint", l.endpoint),
			structuredlog.F("phase", "rpc"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		)
		return err
	}
	vol := resp.GetVolume()
	if vol == nil {
		structuredlog.Error("gateway.admin_volume_lookup", "get_volume_failed", service.ErrVolumeNotFound,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("endpoint", l.endpoint),
			structuredlog.F("phase", "empty_response"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		)
		return service.ErrVolumeNotFound
	}
	spec := publishedVolumeSpecFromSummary(volumeID, vol)
	l.mu.Lock()
	l.cache[volumeID] = publishedVolumeCacheEntry{
		spec:      spec,
		expiresAt: time.Now().Add(l.ttl),
	}
	l.mu.Unlock()
	structuredlog.Info("gateway.admin_volume_lookup", "get_volume_completed",
		structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
		structuredlog.F("endpoint", l.endpoint),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("cache_ttl_ms", l.ttl.Milliseconds()),
	)
	return nil
}

func publishedVolumeSpecFromSummary(volumeID uint64, vol *adminv1.VolumeSummary) service.VolumeSpec {
	allocationChunkSizeBytes, allocationPageBytes := normalizePublishedVolumeGeometry(uint32(vol.GetChunkSizeBytes()), uint32(vol.GetExtentPageBytes()))
	return service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:                   service.HexVolumeID(volumeID),
		Name:                 "sbs-" + vol.GetVolumeId(),
		Prefix:               "sbs-" + vol.GetVolumeId(),
		SizeBytes:            vol.GetSizeBytes(),
		BlockSize:            vol.GetBlockSize(),
		ChunkSizeBytes:       allocationChunkSizeBytes,
		ExtentPageBytes:      allocationPageBytes,
		State:                service.VolumeStateAvailable,
		RedundancyBackend:    strings.TrimSpace(vol.GetRedundancyBackend()),
		TopologyMode:         strings.TrimSpace(vol.GetTopologyMode()),
		ECProfileID:          strings.TrimSpace(vol.GetEcProfileId()),
		ECCodecID:            strings.TrimSpace(vol.GetEcCodecId()),
		ECDataShards:         vol.GetEcDataShards(),
		ECParityShards:       vol.GetEcParityShards(),
		ECStripeUnitBytes:    vol.GetEcStripeUnitBytes(),
		ECFailureDomain:      strings.TrimSpace(vol.GetEcFailureDomain()),
		WeakPlacementAllowed: vol.GetWeakPlacementAllowed(),
		ProtectedState:       protectedStateFromPublishedVolume(vol.GetProtectedState()),
	})
}

func protectedStateFromPublishedVolume(in *adminv1.VolumeProtectedState) *service.VolumeProtectedState {
	if in == nil {
		return nil
	}
	protectedState := service.VolumeProtectedState{
		State:            service.VolumeProtectedStateKind(strings.TrimSpace(in.GetState())),
		ReasonCode:       strings.TrimSpace(in.GetReasonCode()),
		SealedObjectID:   strings.TrimSpace(in.GetSealedObjectId()),
		SealOperationID:  strings.TrimSpace(in.GetSealOperationId()),
		PolicySnapshotID: strings.TrimSpace(in.GetPolicySnapshotId()),
		LifecycleState:   strings.TrimSpace(in.GetLifecycleState()),
		SourceVolumeID:   strings.TrimSpace(in.GetSourceVolumeId()),
	}.Normalize()
	if protectedState.IsZero() {
		return nil
	}
	return &protectedState
}

func normalizePublishedVolumeGeometry(allocationChunkSizeBytes, allocationPageBytes uint32) (uint32, uint32) {
	if allocationChunkSizeBytes == 0 {
		allocationChunkSizeBytes = service.DefaultAllocationChunkSize
	}
	if allocationPageBytes == 0 {
		allocationPageBytes = service.DefaultAllocationPageSize
	}
	return allocationChunkSizeBytes, allocationPageBytes
}
