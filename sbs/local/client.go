package local

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/structuredlog"
	namrbdversion "github.com/nosway/namrbd/version"
)

type Config struct {
	Path                            string
	Stores                          []StoreSpec
	BuildVersion                    string
	DisableIdempotencySync          bool
	CacheOpenVolumeSpec             bool
	DisablePhysicalWriteIdempotency bool
	TraceDataOperations             bool
}

type Client struct {
	db                              *pebble.DB
	meta                            *metadataRepository
	objects                         *objectStore
	data                            service.DataRepository
	version                         string
	stores                          []StoreSpec
	cacheOpenVolumeSpec             bool
	disablePhysicalWriteIdempotency bool
	traceDataOperations             bool

	mu          sync.Mutex
	open        map[string]openSession
	storeStates map[string]string
}

type ObservabilitySnapshot struct {
	BuildVersion       string               `json:"build_version"`
	Volumes            int                  `json:"volumes"`
	OpenSessions       int                  `json:"open_sessions"`
	ExtentPages        int                  `json:"extent_pages"`
	GarbageChunks      int                  `json:"garbage_chunks"`
	IdempotencyRecords int                  `json:"idempotency_records"`
	Stores             []StoreSnapshot      `json:"stores"`
	Timings            ObservabilityTimings `json:"timings"`
}

type ObservabilityTimings struct {
	MetadataScanMs int64 `json:"metadata_scan_ms"`
	StoreRuntimeMs int64 `json:"store_runtime_ms"`
	TotalMs        int64 `json:"total_ms"`
}

type StoreHealthSnapshot struct {
	BuildVersion string             `json:"build_version"`
	Stores       []StoreSnapshot    `json:"stores"`
	Timings      StoreHealthTimings `json:"timings"`
}

type StoreHealthTimings struct {
	StoreRuntimeMs int64 `json:"store_runtime_ms"`
	TotalMs        int64 `json:"total_ms"`
}

type VolumePurgeResult struct {
	VolumeID       string `json:"volume_id"`
	KeyCount       uint64 `json:"key_count"`
	ReclaimedBytes uint64 `json:"reclaimed_bytes"`
}

type openSession struct {
	handle       string
	attachmentID string
	generation   uint64
	spec         service.VolumeSpec
}

type idempotencyRecord struct {
	Hash     []byte          `json:"hash"`
	Response json.RawMessage `json:"response"`
}

type physicalChunkDataRepository interface {
	ReadPhysicalChunkAt(ctx context.Context, volume service.VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64) ([]byte, error)
	WritePhysicalChunkAt(ctx context.Context, volume service.VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64, data []byte) error
}

type instrumentedPhysicalChunkDataRepository interface {
	WritePhysicalChunkAtWithStats(ctx context.Context, volume service.VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64, data []byte) (service.PhysicalChunkWriteStats, error)
}

func Open(cfg Config) (*Client, error) {
	stores, err := normalizeStoreSpecs(cfg.Path, cfg.Stores)
	if err != nil {
		return nil, err
	}
	if cfg.BuildVersion == "" {
		cfg.BuildVersion = namrbdversion.ProductVersion()
	}
	db, err := pebble.Open(cfg.Path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	meta := newMetadataRepository(db)
	meta.setIdempotencySync(!cfg.DisableIdempotencySync)
	objects, err := newObjectStore(cfg.Path, db, stores)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	version := strings.TrimSpace(cfg.BuildVersion)
	if version == "" {
		version = namrbdversion.ProductVersion()
	}
	storeStates := func() map[string]string {
		out := make(map[string]string, len(stores))
		for _, spec := range stores {
			out[spec.ID] = StoreStateHealthy
		}
		return out
	}()
	client := &Client{
		db:                              db,
		meta:                            meta,
		objects:                         objects,
		version:                         version,
		stores:                          stores,
		cacheOpenVolumeSpec:             cfg.CacheOpenVolumeSpec,
		disablePhysicalWriteIdempotency: cfg.DisablePhysicalWriteIdempotency,
		traceDataOperations:             cfg.TraceDataOperations,
		open:                            make(map[string]openSession),
		storeStates:                     storeStates,
	}
	planner := newStorePlanner(client.currentStoreSpecs, client.currentStoreStates)
	client.data = service.NewChunkExtentDataRepositoryWithPlanner(meta, objects, planner)
	return client, nil
}

func (c *Client) logDataOperation(event string, fields ...structuredlog.Field) {
	if !c.traceDataOperations {
		return
	}
	structuredlog.Info("sbs.data", event, fields...)
}

func (c *Client) Close() error {
	if c.objects != nil {
		if err := c.objects.Close(); err != nil {
			_ = c.db.Close()
			return err
		}
	}
	return c.db.Close()
}

func (c *Client) ObservabilitySnapshot() (ObservabilitySnapshot, error) {
	startedAt := time.Now()
	metadataScanStartedAt := time.Now()
	counts, err := c.meta.countVolumeObservability()
	if err != nil {
		return ObservabilitySnapshot{}, err
	}
	metadataScanDuration := time.Since(metadataScanStartedAt)
	openSessions, stores, storeStates := c.snapshotStoreView()
	storeSnapshots := make([]StoreSnapshot, 0, len(stores))
	storeRuntimeSnapshots := map[string]storeRuntimeSnapshot{}
	storeRuntimeStartedAt := time.Now()
	if c.objects != nil {
		storeRuntimeSnapshots, err = c.objects.StoreRuntimeSnapshots(stores)
		if err != nil {
			return ObservabilitySnapshot{}, err
		}
	}
	storeRuntimeDuration := time.Since(storeRuntimeStartedAt)
	for _, spec := range stores {
		state := storeStates[spec.ID]
		if state == "" {
			state = StoreStateHealthy
		}
		runtime := storeRuntimeSnapshots[spec.ID]
		storeSnapshots = append(storeSnapshots, StoreSnapshot{
			ID:                        spec.ID,
			Path:                      spec.Path,
			Shards:                    spec.Shards,
			Weight:                    spec.Weight,
			AllocationWeight:          spec.Weight,
			State:                     state,
			CapacityBytes:             runtime.CapacityBytes,
			AvailableBytes:            runtime.AvailableBytes,
			UsedBytes:                 runtime.UsedBytes,
			PebbleDiskUsageBytes:      runtime.PebbleDiskUsageBytes,
			CompactionPendingBytes:    runtime.CompactionPendingBytes,
			CompactionInProgressBytes: runtime.CompactionInProgressBytes,
		})
	}
	return ObservabilitySnapshot{
		BuildVersion:       c.version,
		Volumes:            counts.Volumes,
		OpenSessions:       openSessions,
		ExtentPages:        counts.ExtentPages,
		GarbageChunks:      counts.GarbageChunks,
		IdempotencyRecords: counts.IdempotencyRecords,
		Stores:             storeSnapshots,
		Timings: ObservabilityTimings{
			MetadataScanMs: metadataScanDuration.Milliseconds(),
			StoreRuntimeMs: storeRuntimeDuration.Milliseconds(),
			TotalMs:        time.Since(startedAt).Milliseconds(),
		},
	}, nil
}

func (c *Client) StoreHealthSnapshot() (StoreHealthSnapshot, error) {
	startedAt := time.Now()
	_, stores, storeStates := c.snapshotStoreView()

	storeRuntimeStartedAt := time.Now()
	storeRuntimeSnapshots := map[string]storeRuntimeSnapshot{}
	var err error
	if c.objects != nil {
		storeRuntimeSnapshots, err = c.objects.StoreRuntimeSnapshots(stores)
		if err != nil {
			return StoreHealthSnapshot{}, err
		}
	}
	storeRuntimeDuration := time.Since(storeRuntimeStartedAt)

	storeSnapshots := make([]StoreSnapshot, 0, len(stores))
	for _, spec := range stores {
		state := storeStates[spec.ID]
		if state == "" {
			state = StoreStateHealthy
		}
		runtime := storeRuntimeSnapshots[spec.ID]
		storeSnapshots = append(storeSnapshots, StoreSnapshot{
			ID:                        spec.ID,
			Path:                      spec.Path,
			Shards:                    spec.Shards,
			Weight:                    spec.Weight,
			AllocationWeight:          spec.Weight,
			State:                     state,
			CapacityBytes:             runtime.CapacityBytes,
			AvailableBytes:            runtime.AvailableBytes,
			UsedBytes:                 runtime.UsedBytes,
			PebbleDiskUsageBytes:      runtime.PebbleDiskUsageBytes,
			CompactionPendingBytes:    runtime.CompactionPendingBytes,
			CompactionInProgressBytes: runtime.CompactionInProgressBytes,
		})
	}

	return StoreHealthSnapshot{
		BuildVersion: c.version,
		Stores:       storeSnapshots,
		Timings: StoreHealthTimings{
			StoreRuntimeMs: storeRuntimeDuration.Milliseconds(),
			TotalMs:        time.Since(startedAt).Milliseconds(),
		},
	}, nil
}

func (c *Client) SweepChunkGarbage(ctx context.Context, volumeID string, limit int, protectedRefs []service.PhysicalChunkRef) (service.ChunkGarbageSweepResult, error) {
	parsedVolumeID, err := service.ParseVolumeID(volumeID)
	if err != nil {
		return service.ChunkGarbageSweepResult{}, fmt.Errorf("parse volume_id: %w", err)
	}
	collector := service.NewChunkGarbageCollector(c.meta, c.objects)
	return collector.SweepVolumeWithProtectedRefs(ctx, parsedVolumeID, limit, protectedRefs)
}

func (c *Client) PurgeVolume(ctx context.Context, volumeID string) (VolumePurgeResult, error) {
	parsedVolumeID, err := service.ParseVolumeID(volumeID)
	if err != nil {
		return VolumePurgeResult{}, fmt.Errorf("parse volume_id: %w", err)
	}
	canonical := service.CanonicalVolumeID(parsedVolumeID)
	spec, err := c.meta.GetVolume(ctx, parsedVolumeID)
	if err != nil {
		return VolumePurgeResult{}, fmt.Errorf("get local volume spec: %w", err)
	}
	c.mu.Lock()
	for _, session := range c.open {
		if service.CanonicalVolumeID(uint64(session.spec.ID)) == canonical {
			c.mu.Unlock()
			return VolumePurgeResult{}, fmt.Errorf("volume %s still has an open session", canonical)
		}
	}
	c.mu.Unlock()
	result, err := c.objects.PurgeVolume(spec.Prefix)
	if err != nil {
		return VolumePurgeResult{}, err
	}
	if spec.Prefix != canonical {
		metadataResult, err := c.objects.PurgeVolume(canonical)
		if err != nil {
			return VolumePurgeResult{}, err
		}
		result.KeyCount += metadataResult.KeyCount
		result.Bytes += metadataResult.Bytes
	}
	return VolumePurgeResult{VolumeID: canonical, KeyCount: result.KeyCount, ReclaimedBytes: result.Bytes}, nil
}

func (c *Client) currentStoreSpecs() []StoreSpec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]StoreSpec(nil), c.stores...)
}

func (c *Client) currentStoreStates() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string, len(c.storeStates))
	for id, state := range c.storeStates {
		out[id] = state
	}
	return out
}

func (c *Client) snapshotStoreView() (int, []StoreSpec, map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stores := append([]StoreSpec(nil), c.stores...)
	states := make(map[string]string, len(c.storeStates))
	for id, state := range c.storeStates {
		states[id] = state
	}
	return len(c.open), stores, states
}

func (c *Client) ReloadStoreConfig(stores []StoreSpec) error {
	normalized, err := normalizeStoreSpecs("", stores)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(normalized) != len(c.stores) {
		return fmt.Errorf("store config reload must keep the same store set: current=%d requested=%d", len(c.stores), len(normalized))
	}

	requestedByID := make(map[string]StoreSpec, len(normalized))
	for _, spec := range normalized {
		requestedByID[spec.ID] = spec
	}

	reloaded := make([]StoreSpec, 0, len(c.stores))
	for _, current := range c.stores {
		requested, ok := requestedByID[current.ID]
		if !ok {
			return fmt.Errorf("store config reload cannot remove store %q", current.ID)
		}
		if requested.Path != current.Path {
			return fmt.Errorf("store config reload cannot change path for store %q", current.ID)
		}
		if requested.Shards != current.Shards {
			return fmt.Errorf("store config reload cannot change shards for store %q", current.ID)
		}
		reloaded = append(reloaded, StoreSpec{
			ID:     current.ID,
			Path:   current.Path,
			Shards: current.Shards,
			Weight: requested.Weight,
		})
		delete(requestedByID, current.ID)
	}
	for storeID := range requestedByID {
		return fmt.Errorf("store config reload cannot add store %q", storeID)
	}
	c.stores = reloaded
	return nil
}

func (c *Client) ReloadStoreConfigFile(path string) error {
	stores, err := LoadStoreConfigFile(path)
	if err != nil {
		return err
	}
	return c.ReloadStoreConfig(stores)
}

func (c *Client) ReloadStoreWeights(updates []StoreWeightUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reloaded, err := ApplyStoreWeightUpdates(c.stores, updates)
	if err != nil {
		return err
	}
	c.stores = reloaded
	return nil
}

func (c *Client) ReloadStoreTuning(updates []StoreTuningUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reloaded, err := ApplyStoreTuningUpdates(c.stores, updates)
	if err != nil {
		return err
	}
	c.stores = reloaded
	return nil
}

func (c *Client) SetStoreState(storeID, state string) error {
	if err := ValidateStoreState(state); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.storeStates[storeID]; !ok {
		return fmt.Errorf("unknown store id %q", storeID)
	}
	c.storeStates[storeID] = state
	return nil
}

func (c *Client) ListExtentPages(ctx context.Context, volumeID string) ([]service.AllocationPageRecord, error) {
	parsedID, err := service.ParseVolumeID(volumeID)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	return c.meta.ListExtentPages(ctx, parsedID)
}

func (c *Client) ShardSnapshots() ([]shardSnapshot, error) {
	if c.objects == nil {
		return nil, nil
	}
	return c.objects.ShardSnapshots()
}

func (c *Client) CreateVolume(ctx context.Context, spec service.VolumeSpec) (service.VolumeSpec, error) {
	spec = normalizeVolumeSpec(spec)
	if err := c.meta.EnsureVolume(ctx, spec); err != nil {
		return service.VolumeSpec{}, err
	}
	return spec, nil
}

func (c *Client) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}

	handle := volumeHandle(req.VolumeID, req.Context.AttachmentID, req.Context.Generation)
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.open[req.VolumeID]; ok {
		if current.attachmentID != req.Context.AttachmentID || current.generation != req.Context.Generation {
			return nil, attachmentMismatch("volume already opened by different writer context")
		}
		handle = current.handle
		current.spec = spec
		c.open[req.VolumeID] = current
	} else {
		c.open[req.VolumeID] = openSession{
			handle:       handle,
			attachmentID: req.Context.AttachmentID,
			generation:   req.Context.Generation,
			spec:         spec,
		}
	}

	state, err := c.meta.getVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	c.meta.initializeObservedVolumeRevisionAtLeast(req.VolumeID, state.VolumeRevision)
	return &service.OpenVolumeResponse{
		Status:         "ok",
		VolumeHandle:   handle,
		VolumeID:       req.VolumeID,
		VolumeRevision: state.VolumeRevision,
		Profile:        profileFromSpec(spec),
		ServerVersion:  c.version,
	}, nil
}

func (c *Client) CloseVolume(_ context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.open[req.VolumeID]
	if !ok {
		return &service.CloseVolumeResponse{Status: "ok"}, nil
	}
	if current.attachmentID != req.Context.AttachmentID || current.generation != req.Context.Generation {
		return nil, attachmentMismatch("attachment mismatch")
	}
	delete(c.open, req.VolumeID)
	return &service.CloseVolumeResponse{Status: "ok"}, nil
}

func (c *Client) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return &service.GetVolumeProfileResponse{
		VolumeID: req.VolumeID,
		Profile:  profileFromSpec(spec),
	}, nil
}

func (c *Client) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, err := c.lookupVolume(ctx, req.VolumeID); err != nil {
		return nil, err
	}
	state, err := c.meta.getVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return &service.GetVolumeStatusResponse{
		VolumeID:       req.VolumeID,
		State:          service.SBSVolumeStateReady,
		Readable:       true,
		Writable:       true,
		VolumeRevision: state.VolumeRevision,
	}, nil
}

func (c *Client) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, session, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		return nil, err
	}
	_ = session
	data, err := c.data.ReadAt(ctx, spec, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, translateServiceError(err)
	}
	state, err := c.meta.getVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	c.logDataOperation("local_read_completed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("payload_sha256", shortPayloadHash(data)),
		structuredlog.F("payload_all_zero", payloadAllZero(data)),
		structuredlog.F("volume_revision", state.VolumeRevision),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
	)
	return &service.ReadResponse{
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		Data:           data,
		VolumeRevision: state.VolumeRevision,
	}, nil
}

func (c *Client) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		return nil, err
	}
	requireOpenDuration := time.Since(start)
	hash := writeHash("write", req.OffsetBytes, req.LengthBytes, req.Data)
	var cached service.WriteResponse
	idempotencyLookupStart := time.Now()
	ok, err := c.loadIdempotent(ctx, req, hash, &cached)
	idempotencyLookupDuration := time.Since(idempotencyLookupStart)
	if err != nil {
		return nil, err
	}
	if ok {
		c.logDataOperation("local_write_replayed",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("payload_sha256", shortPayloadHash(req.Data)),
			structuredlog.F("payload_all_zero", payloadAllZero(req.Data)),
			structuredlog.F("volume_revision", cached.VolumeRevision),
			structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
			structuredlog.F("idempotency_lookup_duration_ms", idempotencyLookupDuration.Milliseconds()),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		)
		return &cached, nil
	}
	dataWriteStart := time.Now()
	dataWriteStats, err := writeAtWithStats(ctx, c.data, spec, req.OffsetBytes, req.LengthBytes, req.Data)
	if err != nil {
		return nil, translateServiceError(err)
	}
	dataWriteDuration := time.Since(dataWriteStart)
	revisionBumpStart := time.Now()
	state, revisionBumpStats, err := c.meta.bumpVolumeRevisionWithStats(ctx, req.VolumeID)
	revisionBumpDuration := time.Since(revisionBumpStart)
	if err != nil {
		return nil, err
	}
	resp := &service.WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("commit-%s-%d", req.VolumeID, state.VolumeRevision),
		VolumeRevision: state.VolumeRevision,
	}
	idempotencyStoreStart := time.Now()
	if err := c.storeIdempotent(ctx, req.VolumeID, req.Context, "write", hash, resp); err != nil {
		return nil, err
	}
	idempotencyStoreDuration := time.Since(idempotencyStoreStart)
	c.logDataOperation("local_write_completed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("payload_sha256", shortPayloadHash(req.Data)),
		structuredlog.F("payload_all_zero", payloadAllZero(req.Data)),
		structuredlog.F("volume_revision", state.VolumeRevision),
		structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
		structuredlog.F("idempotency_lookup_duration_ms", idempotencyLookupDuration.Milliseconds()),
		structuredlog.F("data_write_duration_ms", dataWriteDuration.Milliseconds()),
		structuredlog.F("data_write_allocation_chunk_size_bytes", spec.ChunkSizeBytes),
		structuredlog.F("data_write_allocation_page_bytes", spec.ExtentPageBytes),
		structuredlog.F("data_write_allocation_page_index", allocationPageIndex(req.OffsetBytes, spec.ExtentPageBytes)),
		structuredlog.F("data_write_chunk_size_bytes", spec.ChunkSizeBytes),
		structuredlog.F("data_write_extent_page_bytes", spec.ExtentPageBytes),
		structuredlog.F("data_write_extent_page_index", allocationPageIndex(req.OffsetBytes, spec.ExtentPageBytes)),
		structuredlog.F("data_write_page_lock_wait_duration_ms", dataWriteStats.PageLockWaitDuration.Milliseconds()),
		structuredlog.F("data_write_allocation_page_get_duration_ms", dataWriteStats.ExtentPageGetDuration.Milliseconds()),
		structuredlog.F("data_write_extent_page_get_duration_ms", dataWriteStats.ExtentPageGetDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_read_duration_ms", dataWriteStats.ChunkReadDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_allocate_duration_ms", dataWriteStats.ChunkAllocateDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_payload_duration_ms", dataWriteStats.ChunkPayloadDuration.Milliseconds()),
		structuredlog.F("data_write_allocation_page_put_duration_ms", dataWriteStats.ExtentPagePutDuration.Milliseconds()),
		structuredlog.F("data_write_extent_page_put_duration_ms", dataWriteStats.ExtentPagePutDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_garbage_duration_ms", dataWriteStats.ChunkGarbageDuration.Milliseconds()),
		structuredlog.F("data_write_pages", dataWriteStats.Pages),
		structuredlog.F("data_write_attempts", dataWriteStats.Attempts),
		structuredlog.F("data_write_chunks_read", dataWriteStats.ChunksRead),
		structuredlog.F("data_write_chunks_written", dataWriteStats.ChunksWritten),
		structuredlog.F("data_write_full_chunk_overwrites", dataWriteStats.FullChunkOverwrites),
		structuredlog.F("data_write_chunk_garbage_records_put", dataWriteStats.ChunkGarbageRecordsPut),
		structuredlog.F("revision_bump_mode", revisionBumpStats.Mode),
		structuredlog.F("revision_bump_duration_ms", revisionBumpDuration.Milliseconds()),
		structuredlog.F("revision_bump_lock_wait_duration_ms", revisionBumpStats.LockWaitDuration.Milliseconds()),
		structuredlog.F("revision_bump_state_get_duration_ms", revisionBumpStats.StateGetDuration.Milliseconds()),
		structuredlog.F("revision_bump_state_put_duration_ms", revisionBumpStats.StatePutDuration.Milliseconds()),
		structuredlog.F("revision_bump_critical_section_duration_ms", revisionBumpStats.CriticalSectionDuration.Milliseconds()),
		structuredlog.F("idempotency_store_duration_ms", idempotencyStoreDuration.Milliseconds()),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
	)
	return resp, nil
}

func allocationPageIndex(offsetBytes uint64, extentPageBytes uint32) uint64 {
	if extentPageBytes == 0 {
		return 0
	}
	return offsetBytes / uint64(extentPageBytes)
}

func writeAtWithStats(ctx context.Context, repo service.DataRepository, spec service.VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) (service.DataWriteStats, error) {
	if instrumented, ok := repo.(service.InstrumentedDataRepository); ok {
		return instrumented.WriteAtWithStats(ctx, spec, offsetBytes, lengthBytes, data)
	}
	err := repo.WriteAt(ctx, spec, offsetBytes, lengthBytes, data)
	return service.DataWriteStats{}, err
}

func (c *Client) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		if req.VolumeHandle != "" || req.Context.AttachmentID != "" || req.Context.Generation != 0 {
			return nil, err
		}
		spec, err = c.lookupVolume(ctx, req.VolumeID)
		if err != nil {
			return nil, err
		}
	}
	physical, ok := c.data.(physicalChunkDataRepository)
	if !ok {
		return nil, service.ErrNotSupported
	}
	data, err := physical.ReadPhysicalChunkAt(ctx, spec, req.PhysicalChunkID, req.ChunkOffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, translateServiceError(err)
	}
	state, err := c.meta.getVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return &service.ReadPhysicalChunkResponse{
		VolumeID:         req.VolumeID,
		PhysicalChunkID:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		Data:             data,
		VolumeRevision:   state.VolumeRevision,
	}, nil
}

func (c *Client) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		return nil, err
	}
	requireOpenDuration := time.Since(start)
	physicalOffset := req.PhysicalChunkID*uint64(spec.ChunkSizeBytes) + req.ChunkOffsetBytes
	hash := writeHash("write-physical-chunk", physicalOffset, req.LengthBytes, req.Data)
	var cached service.WritePhysicalChunkResponse
	idempotencyFastPath := c.disablePhysicalWriteIdempotency
	idempotencyLookupDuration := time.Duration(0)
	if !idempotencyFastPath {
		idempotencyLookupStart := time.Now()
		ok, err := c.loadIdempotent(ctx, req, hash, &cached)
		idempotencyLookupDuration = time.Since(idempotencyLookupStart)
		if err != nil {
			return nil, err
		}
		if ok {
			c.logDataOperation("local_physical_chunk_write_replayed",
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("gateway_id", req.Context.GatewayID),
				structuredlog.F("host_id", req.Context.HostID),
				structuredlog.F("session_id", req.Context.SessionID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
				structuredlog.F("idempotency_fast_path", false),
				structuredlog.F("physical_chunk_id", req.PhysicalChunkID),
				structuredlog.F("physical_chunk_offset_bytes", req.ChunkOffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("payload_sha256", shortPayloadHash(req.Data)),
				structuredlog.F("payload_all_zero", payloadAllZero(req.Data)),
				structuredlog.F("volume_revision", cached.VolumeRevision),
				structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
				structuredlog.F("idempotency_lookup_duration_ms", idempotencyLookupDuration.Milliseconds()),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			)
			return &cached, nil
		}
	}
	physical, ok := c.data.(physicalChunkDataRepository)
	if !ok {
		return nil, service.ErrNotSupported
	}
	dataWriteStart := time.Now()
	dataWriteStats, err := writePhysicalChunkAtWithStats(ctx, physical, spec, req.PhysicalChunkID, req.ChunkOffsetBytes, req.LengthBytes, req.Data)
	dataWriteDuration := time.Since(dataWriteStart)
	if err != nil {
		return nil, translateServiceError(err)
	}
	revisionBumpStart := time.Now()
	state, revisionBumpStats, err := c.meta.observePhysicalWriteRevisionWithStats(ctx, req.VolumeID)
	revisionBumpDuration := time.Since(revisionBumpStart)
	if err != nil {
		return nil, err
	}
	resp := &service.WritePhysicalChunkResponse{
		Status:           "ok",
		VolumeID:         req.VolumeID,
		PhysicalChunkID:  req.PhysicalChunkID,
		ChunkOffsetBytes: req.ChunkOffsetBytes,
		LengthBytes:      req.LengthBytes,
		CommitID:         fmt.Sprintf("physical-commit-%s-%d", req.VolumeID, state.VolumeRevision),
		VolumeRevision:   state.VolumeRevision,
	}
	idempotencyStoreDuration := time.Duration(0)
	if !idempotencyFastPath {
		idempotencyStoreStart := time.Now()
		if err := c.storeIdempotent(ctx, req.VolumeID, req.Context, "write-physical-chunk", hash, resp); err != nil {
			return nil, err
		}
		idempotencyStoreDuration = time.Since(idempotencyStoreStart)
	}
	c.logDataOperation("local_physical_chunk_write_completed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("gateway_id", req.Context.GatewayID),
		structuredlog.F("host_id", req.Context.HostID),
		structuredlog.F("session_id", req.Context.SessionID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("idempotency_fast_path", idempotencyFastPath),
		structuredlog.F("physical_chunk_id", req.PhysicalChunkID),
		structuredlog.F("physical_chunk_offset_bytes", req.ChunkOffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("payload_sha256", shortPayloadHash(req.Data)),
		structuredlog.F("payload_all_zero", payloadAllZero(req.Data)),
		structuredlog.F("volume_revision", state.VolumeRevision),
		structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
		structuredlog.F("idempotency_lookup_duration_ms", idempotencyLookupDuration.Milliseconds()),
		structuredlog.F("data_write_duration_ms", dataWriteDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_size_bytes", spec.ChunkSizeBytes),
		structuredlog.F("data_write_physical_chunk_id", req.PhysicalChunkID),
		structuredlog.F("data_write_physical_chunk_offset_bytes", req.ChunkOffsetBytes),
		structuredlog.F("data_write_chunk_read_duration_ms", dataWriteStats.ChunkReadDuration.Milliseconds()),
		structuredlog.F("data_write_chunk_payload_duration_ms", dataWriteStats.ChunkPayloadDuration.Milliseconds()),
		structuredlog.F("data_write_chunks_read", dataWriteStats.ChunksRead),
		structuredlog.F("data_write_chunks_written", dataWriteStats.ChunksWritten),
		structuredlog.F("data_write_full_chunk_overwrites", dataWriteStats.FullChunkOverwrites),
		structuredlog.F("revision_bump_mode", revisionBumpStats.Mode),
		structuredlog.F("revision_bump_duration_ms", revisionBumpDuration.Milliseconds()),
		structuredlog.F("revision_bump_lock_wait_duration_ms", revisionBumpStats.LockWaitDuration.Milliseconds()),
		structuredlog.F("revision_bump_state_get_duration_ms", revisionBumpStats.StateGetDuration.Milliseconds()),
		structuredlog.F("revision_bump_state_put_duration_ms", revisionBumpStats.StatePutDuration.Milliseconds()),
		structuredlog.F("revision_bump_critical_section_duration_ms", revisionBumpStats.CriticalSectionDuration.Milliseconds()),
		structuredlog.F("idempotency_store_duration_ms", idempotencyStoreDuration.Milliseconds()),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
	)
	return resp, nil
}

func writePhysicalChunkAtWithStats(ctx context.Context, repo physicalChunkDataRepository, spec service.VolumeSpec, physicalChunkID, chunkOffsetBytes, lengthBytes uint64, data []byte) (service.PhysicalChunkWriteStats, error) {
	if instrumented, ok := repo.(instrumentedPhysicalChunkDataRepository); ok {
		return instrumented.WritePhysicalChunkAtWithStats(ctx, spec, physicalChunkID, chunkOffsetBytes, lengthBytes, data)
	}
	err := repo.WritePhysicalChunkAt(ctx, spec, physicalChunkID, chunkOffsetBytes, lengthBytes, data)
	return service.PhysicalChunkWriteStats{}, err
}

func (c *Client) WriteECShard(ctx context.Context, req *service.WriteECShardRequest) (*service.WriteECShardResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	requireOpenStart := time.Now()
	if _, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context); err != nil {
		return nil, err
	}
	requireOpenDuration := time.Since(requireOpenStart)
	checksum := ecShardChecksum(req.Data)
	if req.Checksum != "" && req.Checksum != checksum {
		return nil, badRequest("ec shard checksum mismatch")
	}
	key := ecShardPayloadKey(req.VolumeID, req.ObjectID, req.StripeID, req.StripeGeneration, req.ShardID, req.StoreID)
	dataWriteStart := time.Now()
	if err := c.objects.Put(ctx, key, append([]byte(nil), req.Data...)); err != nil {
		return nil, err
	}
	dataWriteDuration := time.Since(dataWriteStart)
	c.logDataOperation("ec_shard_write_completed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("gateway_id", req.Context.GatewayID),
		structuredlog.F("host_id", req.Context.HostID),
		structuredlog.F("session_id", req.Context.SessionID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("object_id", req.ObjectID),
		structuredlog.F("stripe_id", req.StripeID),
		structuredlog.F("stripe_generation", req.StripeGeneration),
		structuredlog.F("shard_id", req.ShardID),
		structuredlog.F("role", req.Role),
		structuredlog.F("role_index", req.RoleIndex),
		structuredlog.F("store_id", req.StoreID),
		structuredlog.F("length_bytes", len(req.Data)),
		structuredlog.F("payload_sha256", shortPayloadHash(req.Data)),
		structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
		structuredlog.F("data_write_duration_ms", dataWriteDuration.Milliseconds()),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
	)
	return &service.WriteECShardResponse{
		Status:           "ok",
		VolumeID:         req.VolumeID,
		ObjectID:         req.ObjectID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardID:          req.ShardID,
		Role:             req.Role,
		RoleIndex:        req.RoleIndex,
		StoreID:          req.StoreID,
		LengthBytes:      uint64(len(req.Data)),
		Checksum:         checksum,
	}, nil
}

func (c *Client) ReadECShard(ctx context.Context, req *service.ReadECShardRequest) (*service.ReadECShardResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context); err != nil {
		if req.VolumeHandle != "" || req.Context.AttachmentID != "" || req.Context.Generation != 0 {
			return nil, err
		}
		if _, lookupErr := c.lookupVolume(ctx, req.VolumeID); lookupErr != nil {
			return nil, lookupErr
		}
	}
	key := ecShardPayloadKey(req.VolumeID, req.ObjectID, req.StripeID, req.StripeGeneration, req.ShardID, req.StoreID)
	payload, found, err := c.objects.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("ec shard payload not found")
	}
	end := req.OffsetBytes + req.LengthBytes
	if end < req.OffsetBytes || end > uint64(len(payload)) {
		return nil, badRequest("ec shard read range exceeds payload length")
	}
	data := append([]byte(nil), payload[req.OffsetBytes:end]...)
	return &service.ReadECShardResponse{
		VolumeID:         req.VolumeID,
		ObjectID:         req.ObjectID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardID:          req.ShardID,
		StoreID:          req.StoreID,
		OffsetBytes:      req.OffsetBytes,
		LengthBytes:      req.LengthBytes,
		Data:             data,
		Checksum:         ecShardChecksum(data),
	}, nil
}

func (c *Client) DeleteECShard(ctx context.Context, req *service.DeleteECShardRequest) (*service.DeleteECShardResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context); err != nil {
		return nil, err
	}
	key := ecShardPayloadKey(req.VolumeID, req.ObjectID, req.StripeID, req.StripeGeneration, req.ShardID, req.StoreID)
	if err := c.objects.Delete(ctx, key); err != nil {
		return nil, err
	}
	return &service.DeleteECShardResponse{
		Status:           "ok",
		VolumeID:         req.VolumeID,
		ObjectID:         req.ObjectID,
		StripeID:         req.StripeID,
		StripeGeneration: req.StripeGeneration,
		ShardID:          req.ShardID,
		StoreID:          req.StoreID,
	}, nil
}

func shortPayloadHash(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:8])
}

func payloadAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func (c *Client) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, _, err := c.requireOpen(ctx, req.VolumeID, req.VolumeHandle, req.Context); err != nil {
		return nil, err
	}
	hash := writeHash("flush", 0, 0, nil)
	var cached service.FlushResponse
	ok, err := c.loadIdempotent(ctx, req, hash, &cached)
	if err != nil {
		return nil, err
	}
	if ok {
		return &cached, nil
	}
	state, err := c.meta.getVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	resp := &service.FlushResponse{Status: "ok", VolumeRevision: state.VolumeRevision}
	if err := c.storeIdempotent(ctx, req.VolumeID, req.Context, "flush", hash, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, _, err := c.requireOpen(ctx, req.VolumeID, "", req.Context); err != nil {
		return nil, err
	}
	resp, err := c.zeroLike(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.Context, "discard")
	if err != nil {
		return nil, err
	}
	return &service.DiscardResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
}

func (c *Client) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	resp, err := c.zeroLike(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.Context, "zero")
	if err != nil {
		return nil, err
	}
	return &service.ZeroResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
}

func (c *Client) zeroLike(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, reqCtx service.SBSRequestContext, op string) (*service.WriteResponse, error) {
	spec, _, err := c.requireOpen(ctx, volumeID, "", reqCtx)
	if err != nil {
		return nil, err
	}
	hash := writeHash(op, offsetBytes, lengthBytes, nil)
	var cached service.WriteResponse
	ok, err := c.loadIdempotent(ctx, &service.WriteRequest{Context: reqCtx}, hash, &cached)
	if err != nil {
		return nil, err
	}
	if ok {
		return &cached, nil
	}
	data := make([]byte, lengthBytes)
	if err := c.data.WriteAt(ctx, spec, offsetBytes, lengthBytes, data); err != nil {
		return nil, translateServiceError(err)
	}
	state, err := c.meta.bumpVolumeRevision(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	resp := &service.WriteResponse{
		Status:         "ok",
		VolumeID:       volumeID,
		OffsetBytes:    offsetBytes,
		LengthBytes:    lengthBytes,
		CommitID:       fmt.Sprintf("commit-%s-%d", volumeID, state.VolumeRevision),
		VolumeRevision: state.VolumeRevision,
	}
	if err := c.storeIdempotent(ctx, volumeID, reqCtx, op, hash, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) lookupVolume(ctx context.Context, volumeID string) (service.VolumeSpec, error) {
	spec, err := c.meta.getVolumeSpec(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	return normalizeVolumeSpec(spec), nil
}

func (c *Client) requireOpen(ctx context.Context, volumeID, handle string, reqCtx service.SBSRequestContext) (service.VolumeSpec, openSession, error) {
	if c.cacheOpenVolumeSpec {
		c.mu.Lock()
		current, ok := c.open[volumeID]
		c.mu.Unlock()
		if err := validateOpenSession(current, ok, handle, reqCtx); err != nil {
			return service.VolumeSpec{}, openSession{}, err
		}
		return current.spec, current, nil
	}
	spec, err := c.lookupVolume(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, openSession{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.open[volumeID]
	if err := validateOpenSession(current, ok, handle, reqCtx); err != nil {
		return service.VolumeSpec{}, openSession{}, err
	}
	return spec, current, nil
}

func validateOpenSession(current openSession, ok bool, handle string, reqCtx service.SBSRequestContext) error {
	if !ok {
		return attachmentMismatch("volume is not opened")
	}
	if current.attachmentID != reqCtx.AttachmentID {
		return attachmentMismatch("attachment mismatch")
	}
	if current.generation != reqCtx.Generation {
		return staleGeneration("generation mismatch")
	}
	if handle != "" && current.handle != handle {
		return attachmentMismatch("volume handle mismatch")
	}
	return nil
}

func (c *Client) loadIdempotent(ctx context.Context, req any, hash []byte, out any) (bool, error) {
	var volumeID string
	var reqCtx service.SBSRequestContext
	switch r := req.(type) {
	case *service.WriteRequest:
		volumeID, reqCtx = r.VolumeID, r.Context
	case *service.WritePhysicalChunkRequest:
		volumeID, reqCtx = r.VolumeID, r.Context
	case *service.FlushRequest:
		volumeID, reqCtx = r.VolumeID, r.Context
	default:
		return false, nil
	}
	raw, found, err := c.meta.loadIdempotency(ctx, volumeID, reqCtx.AttachmentID, reqCtx.Generation, reqCtx.IdempotencyKey)
	if err != nil || !found {
		return false, err
	}
	var rec idempotencyRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return false, err
	}
	if string(rec.Hash) != string(hash) {
		return false, &service.SBSError{
			Code:    service.SBSErrorCodeIdempotencyConflict,
			Message: "same idempotency key used with different request body",
		}
	}
	if err := json.Unmarshal(rec.Response, out); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) storeIdempotent(ctx context.Context, volumeID string, reqCtx service.SBSRequestContext, op string, hash []byte, response any) error {
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(idempotencyRecord{
		Hash:     hash,
		Response: body,
	})
	if err != nil {
		return err
	}
	return c.meta.storeIdempotency(ctx, volumeID, reqCtx.AttachmentID, reqCtx.Generation, reqCtx.IdempotencyKey, raw)
}

func profileFromSpec(spec service.VolumeSpec) service.SBSVolumeProfile {
	return service.SBSVolumeProfile{
		SizeBytes:       spec.SizeBytes,
		BlockSize:       spec.BlockSize,
		MaxIOSize:       spec.ExtentPageBytes,
		SupportsFlush:   true,
		SupportsDiscard: true,
		SupportsZero:    true,
		ConsistencyMode: "single-writer-linearized",
	}
}

func normalizeVolumeSpec(spec service.VolumeSpec) service.VolumeSpec {
	spec = service.NormalizeVolumeSpec(spec)
	if spec.Prefix == "" {
		spec.Prefix = service.CanonicalVolumeID(uint64(spec.ID))
	}
	return spec
}

func volumeHandle(volumeID, attachmentID string, generation uint64) string {
	return fmt.Sprintf("vh-%s-%s-%d", volumeID, attachmentID, generation)
}

func writeHash(op string, offsetBytes, lengthBytes uint64, data []byte) []byte {
	sum := sha256.Sum256(append([]byte(fmt.Sprintf("%s:%d:%d:", op, offsetBytes, lengthBytes)), data...))
	return sum[:]
}

func ecShardChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum[:])
}

func ecShardPayloadKey(volumeID, objectID, stripeID string, stripeGeneration uint64, shardID uint32, storeID string) string {
	encodedObject := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%s|%s|%d|%d", volumeID, objectID, stripeID, stripeGeneration, shardID)))
	return fmt.Sprintf("ec-%s:store:%s:shard:0:obj:%s", volumeID, ecShardPayloadStoreID(storeID), encodedObject)
}

func ecShardPayloadStoreID(storeID string) string {
	storeID = strings.TrimSpace(storeID)
	if idx := strings.LastIndex(storeID, "/"); idx >= 0 && idx+1 < len(storeID) {
		return storeID[idx+1:]
	}
	return storeID
}

func badRequest(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeBadRequest, Message: msg}
}

func attachmentMismatch(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeAttachmentMismatch, Message: msg}
}

func staleGeneration(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeStaleGeneration, Message: msg}
}

func notFound(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeNotFound, Message: msg}
}

func translateServiceError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, service.ErrVolumeNotFound), errors.Is(err, pebble.ErrNotFound):
		return notFound(err.Error())
	case errors.Is(err, service.ErrBadAlignment), errors.Is(err, service.ErrBadDataLength), errors.Is(err, service.ErrOutOfRange):
		return badRequest(err.Error())
	default:
		return err
	}
}

var _ service.SBSClient = (*Client)(nil)
var _ service.ECShardSBSClient = (*Client)(nil)
var _ store.ObjectStore = (*objectStore)(nil)
