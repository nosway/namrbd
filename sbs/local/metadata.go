package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/service"
)

type metadataRepository struct {
	db                       *pebble.DB
	mu                       sync.Mutex
	observedRevisionCounters sync.Map
	idempotencyWriteOptions  *pebble.WriteOptions
}

type volumeObservabilityCounts struct {
	Volumes            int
	ExtentPages        int
	GarbageChunks      int
	IdempotencyRecords int
}

type persistedVolumeState struct {
	VolumeID       string `json:"volume_id"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type revisionBumpStats struct {
	LockWaitDuration        time.Duration
	CriticalSectionDuration time.Duration
	StateGetDuration        time.Duration
	StatePutDuration        time.Duration
	Mode                    string
}

type observedVolumeRevisionCounter struct {
	initMu      sync.Mutex
	initialized atomic.Bool
	value       atomic.Uint64
}

func newMetadataRepository(db *pebble.DB) *metadataRepository {
	return &metadataRepository{db: db, idempotencyWriteOptions: pebble.Sync}
}

func (r *metadataRepository) setIdempotencySync(enabled bool) {
	if enabled {
		r.idempotencyWriteOptions = pebble.Sync
		return
	}
	r.idempotencyWriteOptions = pebble.NoSync
}

func isNotFound(err error) bool {
	var sbsErr *service.SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == service.SBSErrorCodeNotFound
}

func (r *metadataRepository) EnsureVolume(_ context.Context, spec service.VolumeSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec = normalizeVolumeSpec(spec)
	existing, err := r.getVolumeSpec(context.Background(), service.CanonicalVolumeID(uint64(spec.ID)))
	if err == nil {
		if existing.ChunkSizeBytes != spec.ChunkSizeBytes || existing.ExtentPageBytes != spec.ExtentPageBytes {
			return fmt.Errorf("%w: volume_id=%s current_page_bytes=%d current_chunk_size_bytes=%d page_bytes=%d chunk_size_bytes=%d",
				service.ErrVolumeGeometryChange,
				service.CanonicalVolumeID(uint64(spec.ID)),
				existing.ExtentPageBytes,
				existing.ChunkSizeBytes,
				spec.ExtentPageBytes,
				spec.ChunkSizeBytes)
		}
	} else if !isNotFound(err) {
		return err
	}
	if err := r.putJSON(specKey(service.CanonicalVolumeID(uint64(spec.ID))), spec, pebble.Sync); err != nil {
		return err
	}
	_, closer, err := r.db.Get([]byte(stateKey(service.CanonicalVolumeID(uint64(spec.ID)))))
	if err == pebble.ErrNotFound {
		return r.putJSON(stateKey(service.CanonicalVolumeID(uint64(spec.ID))), persistedVolumeState{
			VolumeID:       service.CanonicalVolumeID(uint64(spec.ID)),
			VolumeRevision: 1,
		}, pebble.Sync)
	}
	if err != nil {
		return err
	}
	closer.Close()
	return nil
}

func (r *metadataRepository) getVolumeSpec(_ context.Context, volumeID string) (service.VolumeSpec, error) {
	var spec service.VolumeSpec
	if err := r.getJSON(specKey(volumeID), &spec); err != nil {
		if err == pebble.ErrNotFound {
			return service.VolumeSpec{}, notFound("volume not found")
		}
		return service.VolumeSpec{}, err
	}
	return spec, nil
}

func (r *metadataRepository) getVolumeState(_ context.Context, volumeID string) (persistedVolumeState, error) {
	var state persistedVolumeState
	if err := r.getJSON(stateKey(volumeID), &state); err != nil {
		if err == pebble.ErrNotFound {
			return persistedVolumeState{}, notFound("volume not found")
		}
		return persistedVolumeState{}, err
	}
	return state, nil
}

func (r *metadataRepository) bumpVolumeRevision(ctx context.Context, volumeID string) (persistedVolumeState, error) {
	state, _, err := r.bumpVolumeRevisionWithStats(ctx, volumeID)
	return state, err
}

func (r *metadataRepository) bumpVolumeRevisionWithStats(ctx context.Context, volumeID string) (persistedVolumeState, revisionBumpStats, error) {
	stats := revisionBumpStats{Mode: "persisted_state"}
	lockWaitStart := time.Now()
	r.mu.Lock()
	stats.LockWaitDuration = time.Since(lockWaitStart)
	defer r.mu.Unlock()
	criticalSectionStart := time.Now()
	stateGetStart := time.Now()
	state, err := r.getVolumeState(ctx, volumeID)
	stats.StateGetDuration = time.Since(stateGetStart)
	if err != nil {
		stats.CriticalSectionDuration = time.Since(criticalSectionStart)
		return persistedVolumeState{}, stats, err
	}
	state.VolumeRevision = r.reserveObservedVolumeRevisionAtLeast(volumeID, state.VolumeRevision+1)
	statePutStart := time.Now()
	if err := r.putJSON(stateKey(volumeID), state, pebble.Sync); err != nil {
		stats.StatePutDuration = time.Since(statePutStart)
		stats.CriticalSectionDuration = time.Since(criticalSectionStart)
		return persistedVolumeState{}, stats, err
	}
	stats.StatePutDuration = time.Since(statePutStart)
	stats.CriticalSectionDuration = time.Since(criticalSectionStart)
	return state, stats, nil
}

func (r *metadataRepository) observePhysicalWriteRevisionWithStats(ctx context.Context, volumeID string) (persistedVolumeState, revisionBumpStats, error) {
	stats := revisionBumpStats{Mode: "observed_in_memory"}
	counter := r.observedRevisionCounter(volumeID)
	stateGetStart := time.Now()
	if err := counter.ensureInitialized(ctx, r, volumeID); err != nil {
		stats.StateGetDuration = time.Since(stateGetStart)
		return persistedVolumeState{}, stats, err
	}
	stats.StateGetDuration = time.Since(stateGetStart)
	revision := counter.value.Add(1)
	return persistedVolumeState{
		VolumeID:       volumeID,
		VolumeRevision: revision,
	}, stats, nil
}

func (r *metadataRepository) observedRevisionCounter(volumeID string) *observedVolumeRevisionCounter {
	actual, _ := r.observedRevisionCounters.LoadOrStore(volumeID, &observedVolumeRevisionCounter{})
	return actual.(*observedVolumeRevisionCounter)
}

func (r *metadataRepository) reserveObservedVolumeRevisionAtLeast(volumeID string, floor uint64) uint64 {
	counter := r.observedRevisionCounter(volumeID)
	for {
		current := counter.value.Load()
		next := current + 1
		if next < floor {
			next = floor
		}
		if counter.value.CompareAndSwap(current, next) {
			counter.initialized.Store(true)
			return next
		}
	}
}

func (r *metadataRepository) initializeObservedVolumeRevisionAtLeast(volumeID string, floor uint64) {
	counter := r.observedRevisionCounter(volumeID)
	counter.initMu.Lock()
	defer counter.initMu.Unlock()
	counter.raiseAtLeast(floor)
	counter.initialized.Store(true)
}

func (c *observedVolumeRevisionCounter) ensureInitialized(ctx context.Context, r *metadataRepository, volumeID string) error {
	if c.initialized.Load() {
		return nil
	}
	c.initMu.Lock()
	defer c.initMu.Unlock()
	if c.initialized.Load() {
		return nil
	}
	state, err := r.getVolumeState(ctx, volumeID)
	if err != nil {
		return err
	}
	c.raiseAtLeast(state.VolumeRevision)
	c.initialized.Store(true)
	return nil
}

func (c *observedVolumeRevisionCounter) raiseAtLeast(floor uint64) {
	for {
		current := c.value.Load()
		if current >= floor {
			return
		}
		if c.value.CompareAndSwap(current, floor) {
			return
		}
	}
}

func (r *metadataRepository) loadIdempotency(_ context.Context, volumeID, attachmentID string, generation uint64, key string) ([]byte, bool, error) {
	raw, closer, err := r.db.Get([]byte(idempotencyKey(volumeID, attachmentID, generation, key)))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	out := append([]byte(nil), raw...)
	return out, true, nil
}

func (r *metadataRepository) storeIdempotency(_ context.Context, volumeID, attachmentID string, generation uint64, key string, value []byte) error {
	opts := r.idempotencyWriteOptions
	if opts == nil {
		opts = pebble.Sync
	}
	return r.db.Set([]byte(idempotencyKey(volumeID, attachmentID, generation, key)), value, opts)
}

func (r *metadataRepository) GetExtentPage(_ context.Context, volumeID, pageNo uint64) (service.AllocationPageRecord, error) {
	vid := service.CanonicalVolumeID(volumeID)
	var page service.AllocationPageRecord
	if err := r.getJSON(extentPageKey(vid, pageNo), &page); err != nil {
		if err == pebble.ErrNotFound {
			spec, specErr := r.getVolumeSpec(context.Background(), vid)
			if specErr != nil {
				return service.AllocationPageRecord{}, specErr
			}
			spec = service.NormalizeVolumeSpec(spec)
			return service.AllocationPageRecord{
				VolumeID:       service.HexVolumeID(volumeID),
				PageNo:         pageNo,
				PageBytes:      spec.ExtentPageBytes,
				ChunkSizeBytes: spec.ChunkSizeBytes,
				Revision:       0,
				Extents: []service.AllocationChunkRecord{{
					LogicalChunkStart: pageNo * (uint64(spec.ExtentPageBytes) / uint64(spec.ChunkSizeBytes)),
					ChunkCount:        uint32(logicalChunkCountInPage(spec, pageNo)),
					Kind:              service.AllocationChunkKindZero,
				}},
			}, nil
		}
		return service.AllocationPageRecord{}, err
	}
	return page, nil
}

func (r *metadataRepository) PutExtentPage(_ context.Context, rec service.AllocationPageRecord, expectedRevision int64) (service.AllocationPageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vid := service.CanonicalVolumeID(uint64(rec.VolumeID))
	current, err := r.GetExtentPage(context.Background(), uint64(rec.VolumeID), rec.PageNo)
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	if current.Revision != expectedRevision {
		return service.AllocationPageRecord{}, service.ErrMetadataCASConflict
	}
	rec.Revision = current.Revision + 1
	if err := r.putJSON(extentPageKey(vid, rec.PageNo), rec, pebble.Sync); err != nil {
		return service.AllocationPageRecord{}, err
	}
	return rec, nil
}

func (r *metadataRepository) ListExtentPages(_ context.Context, volumeID uint64) ([]service.AllocationPageRecord, error) {
	prefix := extentPagesPrefix(service.CanonicalVolumeID(volumeID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + "\xff")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var pages []service.AllocationPageRecord
	for iter.First(); iter.Valid(); iter.Next() {
		var page service.AllocationPageRecord
		if err := json.Unmarshal(iter.Value(), &page); err != nil {
			return nil, err
		}
		pages = append(pages, page)
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	return pages, nil
}

func (r *metadataRepository) AllocateChunkIDs(_ context.Context, volumeID uint64, count uint32) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vid := service.CanonicalVolumeID(volumeID)
	key := nextChunkIDKey(vid)
	next := uint64(1)
	if raw, closer, err := r.db.Get([]byte(key)); err == nil {
		next, _ = strconv.ParseUint(string(raw), 10, 64)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return 0, err
	}
	start := next
	next += uint64(count)
	if err := r.db.Set([]byte(key), []byte(strconv.FormatUint(next, 10)), pebble.Sync); err != nil {
		return 0, err
	}
	return start, nil
}

func (r *metadataRepository) PutChunkGarbage(_ context.Context, rec service.AllocationChunkGarbageRecord) error {
	return r.putJSON(gcKey(service.CanonicalVolumeID(uint64(rec.VolumeID)), rec.ChunkID), rec, pebble.Sync)
}

func (r *metadataRepository) ListChunkGarbage(_ context.Context, volumeID uint64, limit int) ([]service.AllocationChunkGarbageRecord, error) {
	prefix := gcPrefix(service.CanonicalVolumeID(volumeID))
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + "\xff")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []service.AllocationChunkGarbageRecord
	for iter.First(); iter.Valid(); iter.Next() {
		var rec service.AllocationChunkGarbageRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *metadataRepository) DeleteChunkGarbage(_ context.Context, volumeID, chunkID uint64) error {
	return r.db.Delete([]byte(gcKey(service.CanonicalVolumeID(volumeID), chunkID)), pebble.Sync)
}

func (r *metadataRepository) putJSON(key string, v any, sync *pebble.WriteOptions) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.db.Set([]byte(key), raw, sync)
}

func (r *metadataRepository) countPrefix(prefix string) (int, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + "\xff")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count, nil
}

func (r *metadataRepository) countPrefixSuffix(prefix, suffix string) (int, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + "\xff")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if suffix != "" && !strings.HasSuffix(key, suffix) {
			continue
		}
		count++
	}
	return count, nil
}

func (r *metadataRepository) countPrefixContains(prefix, needle string) (int, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: []byte(prefix + "\xff")})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if needle != "" && !strings.Contains(key, needle) {
			continue
		}
		count++
	}
	return count, nil
}

func (r *metadataRepository) countVolumeObservability() (volumeObservabilityCounts, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte("volumes/"), UpperBound: []byte("volumes/" + "\xff")})
	if err != nil {
		return volumeObservabilityCounts{}, err
	}
	defer iter.Close()
	var counts volumeObservabilityCounts
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		switch {
		case strings.HasSuffix(key, "/spec"):
			counts.Volumes++
		case strings.Contains(key, "/extents/pages/"):
			counts.ExtentPages++
		case strings.Contains(key, "/gc/"):
			counts.GarbageChunks++
		case strings.Contains(key, "/idem/"):
			counts.IdempotencyRecords++
		}
	}
	return counts, nil
}

func (r *metadataRepository) getJSON(key string, out any) error {
	raw, closer, err := r.db.Get([]byte(key))
	if err != nil {
		return err
	}
	defer closer.Close()
	return json.Unmarshal(raw, out)
}

func specKey(volumeID string) string  { return fmt.Sprintf("volumes/%s/spec", volumeID) }
func stateKey(volumeID string) string { return fmt.Sprintf("volumes/%s/state", volumeID) }
func extentPagesPrefix(volumeID string) string {
	return fmt.Sprintf("volumes/%s/extents/pages/", volumeID)
}
func extentPageKey(volumeID string, pageNo uint64) string {
	return fmt.Sprintf("%s%d", extentPagesPrefix(volumeID), pageNo)
}
func nextChunkIDKey(volumeID string) string { return fmt.Sprintf("volumes/%s/chunks/next", volumeID) }
func gcPrefix(volumeID string) string       { return fmt.Sprintf("volumes/%s/gc/", volumeID) }
func gcKey(volumeID string, chunkID uint64) string {
	return fmt.Sprintf("%s%d", gcPrefix(volumeID), chunkID)
}
func idempotencyKey(volumeID, attachmentID string, generation uint64, key string) string {
	return fmt.Sprintf("volumes/%s/idem/%s/%d/%s", volumeID, attachmentID, generation, key)
}

func logicalChunkCountInPage(volume service.VolumeSpec, pageNo uint64) int {
	pageBytes := uint64(volume.ExtentPageBytes)
	chunkSize := uint64(volume.ChunkSizeBytes)
	if pageBytes == 0 || chunkSize == 0 {
		return 0
	}
	pageStartOffset := pageNo * pageBytes
	if pageStartOffset >= volume.SizeBytes {
		return 0
	}
	remaining := volume.SizeBytes - pageStartOffset
	covered := pageBytes
	if remaining < covered {
		covered = remaining
	}
	count := covered / chunkSize
	if covered%chunkSize != 0 {
		count++
	}
	return int(count)
}

func (r *metadataRepository) CreateVolume(context.Context, service.VolumeCreateRequest) (service.VolumeSpec, error) {
	return service.VolumeSpec{}, service.ErrNotSupported
}
func (r *metadataRepository) UpdateVolume(context.Context, uint64, service.VolumeUpdateRequest) (service.VolumeSpec, error) {
	return service.VolumeSpec{}, service.ErrNotSupported
}
func (r *metadataRepository) DeleteVolume(context.Context, uint64) error {
	return service.ErrNotSupported
}
func (r *metadataRepository) GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	return r.getVolumeSpec(ctx, service.CanonicalVolumeID(volumeID))
}
func (r *metadataRepository) GetVolumeStatus(context.Context, uint64) (service.VolumeStatusRecord, error) {
	return service.VolumeStatusRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) PutVolumeStatus(context.Context, service.VolumeStatusRecord) error {
	return service.ErrNotSupported
}
func (r *metadataRepository) ListVolumes(_ context.Context) ([]service.VolumeSpec, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: []byte("volumes/"), UpperBound: []byte("volumes/" + "\xff")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var volumes []service.VolumeSpec
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if !strings.HasSuffix(key, "/spec") {
			continue
		}
		var spec service.VolumeSpec
		if err := json.Unmarshal(iter.Value(), &spec); err != nil {
			return nil, err
		}
		volumes = append(volumes, spec)
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].ID < volumes[j].ID })
	return volumes, nil
}
func (r *metadataRepository) SetVolumeState(context.Context, uint64, service.VolumeLifecycleState) (service.VolumeSpec, error) {
	return service.VolumeSpec{}, service.ErrNotSupported
}
func (r *metadataRepository) GetAttachment(context.Context, uint64) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) GetGeneration(context.Context, uint64) (uint64, error) {
	return 0, service.ErrNotSupported
}
func (r *metadataRepository) UnsafeClearAttachment(context.Context, uint64) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) UnsafeSetGeneration(context.Context, uint64, uint64) (uint64, error) {
	return 0, service.ErrNotSupported
}
func (r *metadataRepository) Attach(context.Context, service.AttachRequest) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) Detach(context.Context, service.DetachRequest) (service.AttachmentRecord, error) {
	return service.AttachmentRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) GetGateway(context.Context, string) (service.GatewayRecord, error) {
	return service.GatewayRecord{}, service.ErrNotSupported
}
func (r *metadataRepository) ListGateways(context.Context) ([]service.GatewayRecord, error) {
	return nil, service.ErrNotSupported
}
func (r *metadataRepository) PutGateway(context.Context, service.GatewayRecord) error {
	return service.ErrNotSupported
}

func parseVolumeIDFromPrefix(prefix string) string {
	return strings.TrimPrefix(prefix, "volumes/")
}
