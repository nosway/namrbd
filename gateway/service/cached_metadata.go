package service

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const defaultExtentPageCacheEntries = 4096

type cachedMetadataRepository struct {
	next            MetadataRepository
	ttl             time.Duration
	maxExtentPages  int
	mu              sync.RWMutex
	volumeCache     map[uint64]cachedVolumeSpec
	extentPageCache map[extentPageCacheKey]*list.Element
	extentPageLRU   *list.List
	volumePages     map[uint64]map[uint64]struct{}
}

type cachedVolumeSpec struct {
	spec      VolumeSpec
	expiresAt time.Time
}

type extentPageCacheKey struct {
	volumeID uint64
	pageNo   uint64
}

type cachedExtentPage struct {
	key       extentPageCacheKey
	page      AllocationPageRecord
	expiresAt time.Time
}

func NewCachedMetadataRepository(next MetadataRepository, ttl time.Duration) MetadataRepository {
	if ttl <= 0 {
		return next
	}
	return &cachedMetadataRepository{
		next:            next,
		ttl:             ttl,
		maxExtentPages:  defaultExtentPageCacheEntries,
		volumeCache:     make(map[uint64]cachedVolumeSpec),
		extentPageCache: make(map[extentPageCacheKey]*list.Element),
		extentPageLRU:   list.New(),
		volumePages:     make(map[uint64]map[uint64]struct{}),
	}
}

func (r *cachedMetadataRepository) EnsureVolume(ctx context.Context, spec VolumeSpec) error {
	if err := r.next.EnsureVolume(ctx, spec); err != nil {
		return err
	}
	r.invalidate(uint64(spec.ID))
	return nil
}

func (r *cachedMetadataRepository) SyncVolumeSpec(ctx context.Context, spec VolumeSpec) error {
	if next, ok := r.next.(VolumeSpecSyncRepository); ok {
		if err := next.SyncVolumeSpec(ctx, spec); err != nil {
			return err
		}
	} else if err := r.next.EnsureVolume(ctx, spec); err != nil {
		return err
	}
	r.store(NormalizeVolumeSpec(spec))
	return nil
}

func (r *cachedMetadataRepository) CreateVolume(ctx context.Context, req VolumeCreateRequest) (VolumeSpec, error) {
	spec, err := r.next.CreateVolume(ctx, req)
	if err != nil {
		return VolumeSpec{}, err
	}
	r.store(spec)
	return spec, nil
}

func (r *cachedMetadataRepository) UpdateVolume(ctx context.Context, volumeID uint64, req VolumeUpdateRequest) (VolumeSpec, error) {
	spec, err := r.next.UpdateVolume(ctx, volumeID, req)
	if err != nil {
		return VolumeSpec{}, err
	}
	r.store(spec)
	return spec, nil
}

func (r *cachedMetadataRepository) DeleteVolume(ctx context.Context, volumeID uint64) error {
	if err := r.next.DeleteVolume(ctx, volumeID); err != nil {
		return err
	}
	r.invalidate(volumeID)
	return nil
}

func (r *cachedMetadataRepository) GetVolume(ctx context.Context, volumeID uint64) (VolumeSpec, error) {
	if spec, ok := r.load(volumeID); ok {
		return spec, nil
	}
	spec, err := r.next.GetVolume(ctx, volumeID)
	if err != nil {
		return VolumeSpec{}, err
	}
	r.store(spec)
	return spec, nil
}

func (r *cachedMetadataRepository) RefreshVolume(ctx context.Context, volumeID uint64) (VolumeSpec, error) {
	var (
		spec VolumeSpec
		err  error
	)
	if next, ok := r.next.(FreshVolumeMetadataRepository); ok {
		spec, err = next.RefreshVolume(ctx, volumeID)
	} else {
		r.invalidate(volumeID)
		spec, err = r.next.GetVolume(ctx, volumeID)
	}
	if err != nil {
		return VolumeSpec{}, err
	}
	r.store(spec)
	return spec, nil
}

func (r *cachedMetadataRepository) GetVolumeStatus(ctx context.Context, volumeID uint64) (VolumeStatusRecord, error) {
	return r.next.GetVolumeStatus(ctx, volumeID)
}

func (r *cachedMetadataRepository) PutVolumeStatus(ctx context.Context, status VolumeStatusRecord) error {
	if err := r.next.PutVolumeStatus(ctx, status); err != nil {
		return err
	}
	r.invalidate(uint64(status.VolumeID))
	return nil
}

func (r *cachedMetadataRepository) ListVolumes(ctx context.Context) ([]VolumeSpec, error) {
	return r.next.ListVolumes(ctx)
}

func (r *cachedMetadataRepository) SetVolumeState(ctx context.Context, volumeID uint64, state VolumeLifecycleState) (VolumeSpec, error) {
	spec, err := r.next.SetVolumeState(ctx, volumeID, state)
	if err != nil {
		return VolumeSpec{}, err
	}
	r.store(spec)
	return spec, nil
}

func (r *cachedMetadataRepository) GetAttachment(ctx context.Context, volumeID uint64) (AttachmentRecord, error) {
	return r.next.GetAttachment(ctx, volumeID)
}

func (r *cachedMetadataRepository) GetGeneration(ctx context.Context, volumeID uint64) (uint64, error) {
	return r.next.GetGeneration(ctx, volumeID)
}

func (r *cachedMetadataRepository) UnsafeClearAttachment(ctx context.Context, volumeID uint64) (AttachmentRecord, error) {
	rec, err := r.next.UnsafeClearAttachment(ctx, volumeID)
	if err != nil {
		return AttachmentRecord{}, err
	}
	r.invalidate(volumeID)
	return rec, nil
}

func (r *cachedMetadataRepository) UnsafeSetGeneration(ctx context.Context, volumeID uint64, generation uint64) (uint64, error) {
	updated, err := r.next.UnsafeSetGeneration(ctx, volumeID, generation)
	if err != nil {
		return 0, err
	}
	r.invalidate(volumeID)
	return updated, nil
}

func (r *cachedMetadataRepository) Attach(ctx context.Context, req AttachRequest) (AttachmentRecord, error) {
	rec, err := r.next.Attach(ctx, req)
	if err != nil {
		return AttachmentRecord{}, err
	}
	r.invalidate(req.VolumeID)
	return rec, nil
}

func (r *cachedMetadataRepository) Detach(ctx context.Context, req DetachRequest) (AttachmentRecord, error) {
	rec, err := r.next.Detach(ctx, req)
	if err != nil {
		return AttachmentRecord{}, err
	}
	r.invalidate(req.VolumeID)
	return rec, nil
}

func (r *cachedMetadataRepository) GetGateway(ctx context.Context, gatewayID string) (GatewayRecord, error) {
	return r.next.GetGateway(ctx, gatewayID)
}

func (r *cachedMetadataRepository) ListGateways(ctx context.Context) ([]GatewayRecord, error) {
	return r.next.ListGateways(ctx)
}

func (r *cachedMetadataRepository) PutGateway(ctx context.Context, rec GatewayRecord) error {
	return r.next.PutGateway(ctx, rec)
}

func (r *cachedMetadataRepository) GetExtentPage(ctx context.Context, volumeID, pageNo uint64) (AllocationPageRecord, error) {
	if page, ok := r.loadExtentPage(volumeID, pageNo); ok {
		return page, nil
	}
	page, err := r.next.GetExtentPage(ctx, volumeID, pageNo)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	r.storeExtentPage(page)
	return page, nil
}

func (r *cachedMetadataRepository) ListExtentPages(ctx context.Context, volumeID uint64) ([]AllocationPageRecord, error) {
	pages, err := r.next.ListExtentPages(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	for _, page := range pages {
		r.storeExtentPage(page)
	}
	return pages, nil
}

func (r *cachedMetadataRepository) PutExtentPage(ctx context.Context, rec AllocationPageRecord, expectedRevision int64) (AllocationPageRecord, error) {
	page, err := r.next.PutExtentPage(ctx, rec, expectedRevision)
	if err != nil {
		return AllocationPageRecord{}, err
	}
	r.storeExtentPage(page)
	return page, nil
}

func (r *cachedMetadataRepository) AllocateChunkIDs(ctx context.Context, volumeID uint64, count uint32) (uint64, error) {
	return r.next.AllocateChunkIDs(ctx, volumeID, count)
}

func (r *cachedMetadataRepository) PutChunkGarbage(ctx context.Context, rec AllocationChunkGarbageRecord) error {
	return r.next.PutChunkGarbage(ctx, rec)
}

func (r *cachedMetadataRepository) ListChunkGarbage(ctx context.Context, volumeID uint64, limit int) ([]AllocationChunkGarbageRecord, error) {
	return r.next.ListChunkGarbage(ctx, volumeID, limit)
}

func (r *cachedMetadataRepository) DeleteChunkGarbage(ctx context.Context, volumeID, chunkID uint64) error {
	return r.next.DeleteChunkGarbage(ctx, volumeID, chunkID)
}

func (r *cachedMetadataRepository) load(volumeID uint64) (VolumeSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.volumeCache[volumeID]
	if !ok || time.Now().After(entry.expiresAt) {
		return VolumeSpec{}, false
	}
	return entry.spec, true
}

func (r *cachedMetadataRepository) store(spec VolumeSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.volumeCache[uint64(spec.ID)] = cachedVolumeSpec{
		spec:      spec,
		expiresAt: time.Now().Add(r.ttl),
	}
}

func (r *cachedMetadataRepository) invalidate(volumeID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.volumeCache, volumeID)
	r.invalidateExtentPagesLocked(volumeID)
}

func (r *cachedMetadataRepository) loadExtentPage(volumeID, pageNo uint64) (AllocationPageRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := extentPageCacheKey{volumeID: volumeID, pageNo: pageNo}
	elem, ok := r.extentPageCache[key]
	if !ok {
		return AllocationPageRecord{}, false
	}
	entry := elem.Value.(cachedExtentPage)
	if time.Now().After(entry.expiresAt) {
		r.removeExtentPageLocked(elem)
		return AllocationPageRecord{}, false
	}
	r.extentPageLRU.MoveToFront(elem)
	return cloneAllocationPage(entry.page), true
}

func (r *cachedMetadataRepository) storeExtentPage(page AllocationPageRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := extentPageCacheKey{volumeID: uint64(page.VolumeID), pageNo: page.PageNo}
	if elem, ok := r.extentPageCache[key]; ok {
		elem.Value = cachedExtentPage{
			key:       key,
			page:      cloneAllocationPage(page),
			expiresAt: time.Now().Add(r.ttl),
		}
		r.extentPageLRU.MoveToFront(elem)
		return
	}

	elem := r.extentPageLRU.PushFront(cachedExtentPage{
		key:       key,
		page:      cloneAllocationPage(page),
		expiresAt: time.Now().Add(r.ttl),
	})
	r.extentPageCache[key] = elem
	if _, ok := r.volumePages[uint64(page.VolumeID)]; !ok {
		r.volumePages[uint64(page.VolumeID)] = make(map[uint64]struct{})
	}
	r.volumePages[uint64(page.VolumeID)][page.PageNo] = struct{}{}

	for r.maxExtentPages > 0 && r.extentPageLRU.Len() > r.maxExtentPages {
		back := r.extentPageLRU.Back()
		if back == nil {
			break
		}
		r.removeExtentPageLocked(back)
	}
}

func (r *cachedMetadataRepository) invalidateExtentPagesLocked(volumeID uint64) {
	pageNos, ok := r.volumePages[volumeID]
	if !ok {
		return
	}
	for pageNo := range pageNos {
		key := extentPageCacheKey{volumeID: volumeID, pageNo: pageNo}
		if elem, ok := r.extentPageCache[key]; ok {
			r.removeExtentPageLocked(elem)
		}
	}
	delete(r.volumePages, volumeID)
}

func (r *cachedMetadataRepository) removeExtentPageLocked(elem *list.Element) {
	entry := elem.Value.(cachedExtentPage)
	delete(r.extentPageCache, entry.key)
	r.extentPageLRU.Remove(elem)
	if pages, ok := r.volumePages[entry.key.volumeID]; ok {
		delete(pages, entry.key.pageNo)
		if len(pages) == 0 {
			delete(r.volumePages, entry.key.volumeID)
		}
	}
}
