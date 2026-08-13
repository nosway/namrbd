package control

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

const DefaultPlacementResolverCacheTTL = time.Second

type PlacementResolverAdapter interface {
	ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error)
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
	ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
	ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type PlacementResolverInternalService interface {
	ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error)
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
	ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
	ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type PlacementResolverInternalServiceWithStats interface {
	PlacementResolverInternalService
	ResolveExtentPlacementsWithStats(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, metadata.ResolveExtentPlacementsStats, error)
}

type RepositoryBackedPlacementResolverInternalService struct {
	service *metadata.Service
}

type placementResolverRepository interface {
	ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error)
	ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error)
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (metadata.AllocationPageRecord, error)
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]metadata.AllocationPageRecord, error)
	GetSnapshotAllocationPage(ctx context.Context, snapshotID string, pageNo uint64) (metadata.AllocationPageRecord, error)
	GetSnapshotRecord(ctx context.Context, snapshotID string) (metadata.SnapshotRecord, error)
	GetCloneRecord(ctx context.Context, cloneID string) (metadata.CloneRecord, error)
	GetCloneDeltaAllocationPage(ctx context.Context, cloneID string, pageNo uint64) (metadata.AllocationPageRecord, error)
}

func NewRepositoryBackedPlacementResolverInternalService(repo placementResolverRepository) *RepositoryBackedPlacementResolverInternalService {
	return &RepositoryBackedPlacementResolverInternalService{service: metadata.NewService(repo)}
}

func NewRepositoryBackedPlacementResolverInternalServiceWithCacheTTL(repo placementResolverRepository, ttl time.Duration) *RepositoryBackedPlacementResolverInternalService {
	if ttl <= 0 {
		return NewRepositoryBackedPlacementResolverInternalService(repo)
	}
	return &RepositoryBackedPlacementResolverInternalService{service: metadata.NewService(newCachedPlacementResolverRepository(repo, ttl, time.Now))}
}

func (s *RepositoryBackedPlacementResolverInternalService) ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error) {
	if s.service == nil {
		return nil, fmt.Errorf("placement resolver service is required")
	}
	return s.service.ResolveExtentPlacements(ctx, volumeID, offsetBytes, lengthBytes)
}

func (s *RepositoryBackedPlacementResolverInternalService) ResolveExtentPlacementsWithStats(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, metadata.ResolveExtentPlacementsStats, error) {
	if s.service == nil {
		return nil, metadata.ResolveExtentPlacementsStats{}, fmt.Errorf("placement resolver service is required")
	}
	return s.service.ResolveExtentPlacementsDetailed(ctx, volumeID, offsetBytes, lengthBytes)
}

func (s *RepositoryBackedPlacementResolverInternalService) ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if s.service == nil {
		return nil, fmt.Errorf("placement resolver service is required")
	}
	return s.service.ResolveAllocationPages(ctx, volumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func (s *RepositoryBackedPlacementResolverInternalService) ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if s.service == nil {
		return nil, fmt.Errorf("placement resolver service is required")
	}
	return s.service.ResolveSnapshotAllocationPages(ctx, snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func (s *RepositoryBackedPlacementResolverInternalService) ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if s.service == nil {
		return nil, fmt.Errorf("placement resolver service is required")
	}
	return s.service.ResolveCloneAllocationPages(ctx, cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

type ServiceBackedPlacementResolverAdapter struct {
	service PlacementResolverInternalService
}

func NewServiceBackedPlacementResolverAdapter(service PlacementResolverInternalService) *ServiceBackedPlacementResolverAdapter {
	return &ServiceBackedPlacementResolverAdapter{service: service}
}

func (a *ServiceBackedPlacementResolverAdapter) ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error) {
	if a.service == nil {
		return nil, fmt.Errorf("placement resolver internal service is required")
	}
	if err := ValidatePlacementResolverRange(volumeID, offsetBytes, lengthBytes); err != nil {
		return nil, err
	}
	return a.service.ResolveExtentPlacements(ctx, volumeID, offsetBytes, lengthBytes)
}

func (a *ServiceBackedPlacementResolverAdapter) ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if a.service == nil {
		return nil, fmt.Errorf("placement resolver internal service is required")
	}
	if err := ValidatePlacementResolverRange(volumeID, offsetBytes, lengthBytes); err != nil {
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		return nil, err
	}
	return a.service.ResolveAllocationPages(ctx, volumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func (a *ServiceBackedPlacementResolverAdapter) ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if a.service == nil {
		return nil, fmt.Errorf("placement resolver internal service is required")
	}
	if err := ValidatePlacementResolverSnapshotRange(snapshotID, offsetBytes, lengthBytes); err != nil {
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		return nil, err
	}
	return a.service.ResolveSnapshotAllocationPages(ctx, snapshotID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func (a *ServiceBackedPlacementResolverAdapter) ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error) {
	if a.service == nil {
		return nil, fmt.Errorf("placement resolver internal service is required")
	}
	if err := ValidatePlacementResolverCloneRange(cloneID, offsetBytes, lengthBytes); err != nil {
		return nil, err
	}
	if err := ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes); err != nil {
		return nil, err
	}
	return a.service.ResolveCloneAllocationPages(ctx, cloneID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func ValidatePlacementResolverRange(volumeID string, offsetBytes, lengthBytes uint64) error {
	if _, err := metadata.CanonicalVolumeID(volumeID); err != nil {
		return InvalidPlacementResolverRequestError("invalid placement resolver volume_id %q: %v", volumeID, err)
	}
	if lengthBytes == 0 {
		return nil
	}
	if offsetBytes+lengthBytes < offsetBytes {
		return InvalidPlacementResolverRequestError("placement resolver byte range overflows: offset_bytes=%d length_bytes=%d", offsetBytes, lengthBytes)
	}
	return nil
}

func ValidatePlacementResolverSnapshotRange(snapshotID string, offsetBytes, lengthBytes uint64) error {
	if strings.TrimSpace(snapshotID) == "" {
		return InvalidPlacementResolverRequestError("snapshot_id is required")
	}
	if lengthBytes == 0 {
		return nil
	}
	if offsetBytes+lengthBytes < offsetBytes {
		return InvalidPlacementResolverRequestError("placement resolver byte range overflows: offset_bytes=%d length_bytes=%d", offsetBytes, lengthBytes)
	}
	return nil
}

func ValidatePlacementResolverCloneRange(cloneID string, offsetBytes, lengthBytes uint64) error {
	if strings.TrimSpace(cloneID) == "" {
		return InvalidPlacementResolverRequestError("clone_id is required")
	}
	if lengthBytes == 0 {
		return nil
	}
	if offsetBytes+lengthBytes < offsetBytes {
		return InvalidPlacementResolverRequestError("placement resolver byte range overflows: offset_bytes=%d length_bytes=%d", offsetBytes, lengthBytes)
	}
	return nil
}

func ValidatePlacementResolverGeometry(pageBytes, chunkSizeBytes uint32) error {
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return InvalidPlacementResolverRequestError("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	return nil
}

type cachedPlacementResolverRepository struct {
	repo placementResolverRepository
	ttl  time.Duration
	now  func() time.Time

	mu          sync.RWMutex
	mappings    map[string]cachedExtentMappings
	replicaSets map[string]cachedReplicaSets
	nodes       cachedNodeMemberships
}

type cachedExtentMappings struct {
	records    []metadata.ExtentMappingRecord
	expiresAt  time.Time
	refreshing bool
}

type cachedReplicaSets struct {
	records    []metadata.ReplicaSetState
	expiresAt  time.Time
	refreshing bool
}

type cachedNodeMemberships struct {
	records    []metadata.NodeMembershipRecord
	expiresAt  time.Time
	refreshing bool
}

func newCachedPlacementResolverRepository(repo placementResolverRepository, ttl time.Duration, now func() time.Time) *cachedPlacementResolverRepository {
	if now == nil {
		now = time.Now
	}
	return &cachedPlacementResolverRepository{
		repo:        repo,
		ttl:         ttl,
		now:         now,
		mappings:    make(map[string]cachedExtentMappings),
		replicaSets: make(map[string]cachedReplicaSets),
	}
}

func (r *cachedPlacementResolverRepository) ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	now := r.now()
	r.mu.RLock()
	entry, ok := r.mappings[volumeID]
	if ok && now.Before(entry.expiresAt) {
		out := cloneExtentMappingRecords(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	if ok && len(entry.records) > 0 && entry.refreshing {
		out := cloneExtentMappingRecords(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	entry, ok = r.mappings[volumeID]
	now = r.now()
	if ok && now.Before(entry.expiresAt) {
		out := cloneExtentMappingRecords(entry.records)
		r.mu.Unlock()
		return out, nil
	}
	if ok && len(entry.records) > 0 {
		out := cloneExtentMappingRecords(entry.records)
		if entry.refreshing {
			r.mu.Unlock()
			return out, nil
		}
		if now.Before(entry.expiresAt.Add(r.ttl)) {
			entry.refreshing = true
			r.mappings[volumeID] = entry
			r.mu.Unlock()
			r.refreshExtentMappingsAsync(volumeID)
			return out, nil
		}
		entry.refreshing = true
		r.mappings[volumeID] = entry
	}
	r.mu.Unlock()

	return r.refreshExtentMappings(ctx, volumeID)
}

func (r *cachedPlacementResolverRepository) refreshExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	records, err := r.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		r.mu.Lock()
		entry, ok := r.mappings[volumeID]
		if ok {
			entry.refreshing = false
			r.mappings[volumeID] = entry
		}
		r.mu.Unlock()
		return nil, err
	}
	entry := cachedExtentMappings{
		records:   cloneExtentMappingRecords(records),
		expiresAt: r.now().Add(r.ttl),
	}
	r.mu.Lock()
	r.mappings[volumeID] = entry
	r.mu.Unlock()
	return cloneExtentMappingRecords(records), nil
}

func (r *cachedPlacementResolverRepository) ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	now := r.now()
	r.mu.RLock()
	entry, ok := r.replicaSets[volumeID]
	if ok && now.Before(entry.expiresAt) {
		out := cloneReplicaSetStates(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	if ok && len(entry.records) > 0 && entry.refreshing {
		out := cloneReplicaSetStates(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	entry, ok = r.replicaSets[volumeID]
	now = r.now()
	if ok && now.Before(entry.expiresAt) {
		out := cloneReplicaSetStates(entry.records)
		r.mu.Unlock()
		return out, nil
	}
	if ok && len(entry.records) > 0 {
		out := cloneReplicaSetStates(entry.records)
		if entry.refreshing {
			r.mu.Unlock()
			return out, nil
		}
		if now.Before(entry.expiresAt.Add(r.ttl)) {
			entry.refreshing = true
			r.replicaSets[volumeID] = entry
			r.mu.Unlock()
			r.refreshReplicaSetsAsync(volumeID)
			return out, nil
		}
		entry.refreshing = true
		r.replicaSets[volumeID] = entry
	}
	r.mu.Unlock()

	return r.refreshReplicaSets(ctx, volumeID)
}

func (r *cachedPlacementResolverRepository) refreshReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	records, err := r.repo.ListReplicaSets(ctx, volumeID)
	if err != nil {
		r.mu.Lock()
		entry, ok := r.replicaSets[volumeID]
		if ok {
			entry.refreshing = false
			r.replicaSets[volumeID] = entry
		}
		r.mu.Unlock()
		return nil, err
	}
	entry := cachedReplicaSets{
		records:   cloneReplicaSetStates(records),
		expiresAt: r.now().Add(r.ttl),
	}
	r.mu.Lock()
	r.replicaSets[volumeID] = entry
	r.mu.Unlock()
	return cloneReplicaSetStates(records), nil
}

func (r *cachedPlacementResolverRepository) ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	now := r.now()
	r.mu.RLock()
	entry := r.nodes
	if !entry.expiresAt.IsZero() && now.Before(entry.expiresAt) {
		out := cloneNodeMembershipRecords(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	if !entry.expiresAt.IsZero() && len(entry.records) > 0 && entry.refreshing {
		out := cloneNodeMembershipRecords(entry.records)
		r.mu.RUnlock()
		return out, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	entry = r.nodes
	now = r.now()
	if !entry.expiresAt.IsZero() && now.Before(entry.expiresAt) {
		out := cloneNodeMembershipRecords(entry.records)
		r.mu.Unlock()
		return out, nil
	}
	if !entry.expiresAt.IsZero() && len(entry.records) > 0 {
		out := cloneNodeMembershipRecords(entry.records)
		if entry.refreshing {
			r.mu.Unlock()
			return out, nil
		}
		if now.Before(entry.expiresAt.Add(r.ttl)) {
			entry.refreshing = true
			r.nodes = entry
			r.mu.Unlock()
			r.refreshNodeMembershipsAsync()
			return out, nil
		}
	}
	entry.refreshing = true
	r.nodes = entry
	r.mu.Unlock()

	return r.refreshNodeMemberships(ctx)
}

func (r *cachedPlacementResolverRepository) refreshNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	records, err := r.repo.ListNodeMemberships(ctx)
	if err != nil {
		r.mu.Lock()
		entry := r.nodes
		entry.refreshing = false
		r.nodes = entry
		r.mu.Unlock()
		return nil, err
	}
	entry := cachedNodeMemberships{
		records:   cloneNodeMembershipRecords(records),
		expiresAt: r.now().Add(r.ttl),
	}
	r.mu.Lock()
	r.nodes = entry
	r.mu.Unlock()
	return cloneNodeMembershipRecords(records), nil
}

func (r *cachedPlacementResolverRepository) refreshExtentMappingsAsync(volumeID string) {
	go func() {
		ctx, cancel := r.backgroundRefreshContext()
		defer cancel()
		_, _ = r.refreshExtentMappings(ctx, volumeID)
	}()
}

func (r *cachedPlacementResolverRepository) refreshReplicaSetsAsync(volumeID string) {
	go func() {
		ctx, cancel := r.backgroundRefreshContext()
		defer cancel()
		_, _ = r.refreshReplicaSets(ctx, volumeID)
	}()
}

func (r *cachedPlacementResolverRepository) refreshNodeMembershipsAsync() {
	go func() {
		ctx, cancel := r.backgroundRefreshContext()
		defer cancel()
		_, _ = r.refreshNodeMemberships(ctx)
	}()
}

func (r *cachedPlacementResolverRepository) backgroundRefreshContext() (context.Context, context.CancelFunc) {
	timeout := 5 * time.Second
	if r.ttl > timeout {
		timeout = r.ttl
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (r *cachedPlacementResolverRepository) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (metadata.AllocationPageRecord, error) {
	return r.repo.GetCompatibleAllocationPage(ctx, volumeID, pageNo, pageBytes, chunkSizeBytes)
}

func (r *cachedPlacementResolverRepository) ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]metadata.AllocationPageRecord, error) {
	return r.repo.ListCompatibleAllocationPages(ctx, volumeID, pageBytes, chunkSizeBytes)
}

func (r *cachedPlacementResolverRepository) GetSnapshotAllocationPage(ctx context.Context, snapshotID string, pageNo uint64) (metadata.AllocationPageRecord, error) {
	return r.repo.GetSnapshotAllocationPage(ctx, snapshotID, pageNo)
}

func (r *cachedPlacementResolverRepository) GetSnapshotRecord(ctx context.Context, snapshotID string) (metadata.SnapshotRecord, error) {
	return r.repo.GetSnapshotRecord(ctx, snapshotID)
}

func (r *cachedPlacementResolverRepository) GetCloneRecord(ctx context.Context, cloneID string) (metadata.CloneRecord, error) {
	return r.repo.GetCloneRecord(ctx, cloneID)
}

func (r *cachedPlacementResolverRepository) GetCloneDeltaAllocationPage(ctx context.Context, cloneID string, pageNo uint64) (metadata.AllocationPageRecord, error) {
	return r.repo.GetCloneDeltaAllocationPage(ctx, cloneID, pageNo)
}

func cloneReplicaSetStates(in []metadata.ReplicaSetState) []metadata.ReplicaSetState {
	out := make([]metadata.ReplicaSetState, len(in))
	for i, rec := range in {
		rec.Replicas = append([]metadata.ReplicaDescriptor(nil), rec.Replicas...)
		rec.FailureDomains = append([]string(nil), rec.FailureDomains...)
		out[i] = rec
	}
	return out
}

func cloneExtentMappingRecords(in []metadata.ExtentMappingRecord) []metadata.ExtentMappingRecord {
	return append([]metadata.ExtentMappingRecord(nil), in...)
}

func cloneNodeMembershipRecords(in []metadata.NodeMembershipRecord) []metadata.NodeMembershipRecord {
	out := make([]metadata.NodeMembershipRecord, len(in))
	for i, rec := range in {
		rec.Capabilities = append([]string(nil), rec.Capabilities...)
		rec.SBSEndpoints = append([]metadata.SBSEndpoint(nil), rec.SBSEndpoints...)
		out[i] = rec
	}
	return out
}
