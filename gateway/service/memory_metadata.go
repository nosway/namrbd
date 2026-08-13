package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"time"
)

type inMemoryMetadataRepository struct {
	mu          sync.Mutex
	volumes     map[uint64]VolumeSpec
	state       map[uint64]AttachmentRecord
	status      map[uint64]VolumeStatusRecord
	gateway     map[string]GatewayRecord
	extentPages map[uint64]map[uint64]AllocationPageRecord
	nextChunkID map[uint64]uint64
	chunkGC     map[uint64]map[uint64]AllocationChunkGarbageRecord
}

func NewInMemoryMetadataRepository(volumes []VolumeSpec) MetadataRepository {
	volumeMap := make(map[uint64]VolumeSpec, len(volumes))
	stateMap := make(map[uint64]AttachmentRecord, len(volumes))
	statusMap := make(map[uint64]VolumeStatusRecord, len(volumes))
	for _, volume := range volumes {
		volume = NormalizeVolumeSpec(volume)
		volumeMap[uint64(volume.ID)] = cloneVolumeSpec(volume)
		stateMap[uint64(volume.ID)] = AttachmentRecord{Generation: 1}
		statusMap[uint64(volume.ID)] = VolumeStatusRecord{
			VolumeID:                 volume.ID,
			InUse:                    false,
			GatewayConnectionState:   GatewayStateUnknown,
			DesiredActiveGatewaySet:  nil,
			ObservedActiveGatewaySet: nil,
		}
	}
	repo := &inMemoryMetadataRepository{
		volumes:     volumeMap,
		state:       stateMap,
		status:      statusMap,
		gateway:     make(map[string]GatewayRecord),
		extentPages: make(map[uint64]map[uint64]AllocationPageRecord),
		nextChunkID: make(map[uint64]uint64),
		chunkGC:     make(map[uint64]map[uint64]AllocationChunkGarbageRecord),
	}
	for volumeID := range volumeMap {
		repo.extentPages[volumeID] = make(map[uint64]AllocationPageRecord)
		repo.nextChunkID[volumeID] = 1
		repo.chunkGC[volumeID] = make(map[uint64]AllocationChunkGarbageRecord)
	}
	return repo
}

func (r *inMemoryMetadataRepository) EnsureVolume(_ context.Context, volume VolumeSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume = NormalizeVolumeSpec(volume)
	if _, ok := r.volumes[uint64(volume.ID)]; ok {
		r.volumes[uint64(volume.ID)] = cloneVolumeSpec(volume)
		return nil
	}
	r.volumes[uint64(volume.ID)] = cloneVolumeSpec(volume)
	r.state[uint64(volume.ID)] = AttachmentRecord{Generation: 1}
	r.status[uint64(volume.ID)] = VolumeStatusRecord{
		VolumeID:                 volume.ID,
		GatewayConnectionState:   GatewayStateUnknown,
		DesiredActiveGatewaySet:  nil,
		ObservedActiveGatewaySet: nil,
	}
	r.extentPages[uint64(volume.ID)] = make(map[uint64]AllocationPageRecord)
	r.nextChunkID[uint64(volume.ID)] = 1
	r.chunkGC[uint64(volume.ID)] = make(map[uint64]AllocationChunkGarbageRecord)
	return nil
}

func (r *inMemoryMetadataRepository) SyncVolumeSpec(_ context.Context, volume VolumeSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume = NormalizeVolumeSpec(volume)
	if _, ok := r.volumes[uint64(volume.ID)]; !ok {
		return ErrVolumeNotFound
	}
	r.volumes[uint64(volume.ID)] = cloneVolumeSpec(volume)
	return nil
}

func (r *inMemoryMetadataRepository) CreateVolume(_ context.Context, req VolumeCreateRequest) (VolumeSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return VolumeSpec{}, ErrVolumeNameRequired
	}
	for _, volume := range r.volumes {
		if volume.Name == name {
			return VolumeSpec{}, ErrVolumeNameConflict
		}
	}
	volumeID, err := generateUniqueVolumeIDMemory(r.volumes)
	if err != nil {
		return VolumeSpec{}, err
	}
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:              HexVolumeID(volumeID),
		Name:            name,
		Prefix:          BuildVolumePrefix(name, volumeID),
		SizeBytes:       req.SizeBytes,
		BlockSize:       req.BlockSize,
		ChunkSizeBytes:  req.ChunkSizeBytes,
		ExtentPageBytes: req.ExtentPageBytes,
		AccessMode:      req.AccessMode,
		State:           req.State,
	})
	r.volumes[volumeID] = cloneVolumeSpec(spec)
	r.state[volumeID] = AttachmentRecord{Generation: 1}
	r.status[volumeID] = VolumeStatusRecord{
		VolumeID:                 HexVolumeID(volumeID),
		GatewayConnectionState:   GatewayStateUnknown,
		DesiredActiveGatewaySet:  nil,
		ObservedActiveGatewaySet: nil,
	}
	r.extentPages[volumeID] = make(map[uint64]AllocationPageRecord)
	r.nextChunkID[volumeID] = 1
	r.chunkGC[volumeID] = make(map[uint64]AllocationChunkGarbageRecord)
	return spec, nil
}

func (r *inMemoryMetadataRepository) UpdateVolume(_ context.Context, volumeID uint64, req VolumeUpdateRequest) (VolumeSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[volumeID]
	if !ok {
		return VolumeSpec{}, ErrVolumeNotFound
	}
	isAttached := r.state[volumeID].AttachmentID != ""
	if err := ValidateImmutableVolumeGeometry(volume, req); err != nil {
		return VolumeSpec{}, err
	}

	if req.Name != nil {
		nextName := strings.TrimSpace(*req.Name)
		if nextName == "" {
			return VolumeSpec{}, ErrVolumeNameRequired
		}
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
		for existingID, existing := range r.volumes {
			if existingID != volumeID && existing.Name == nextName {
				return VolumeSpec{}, ErrVolumeNameConflict
			}
		}
		volume.Name = nextName
		volume.Prefix = BuildVolumePrefix(nextName, volumeID)
	}
	if req.SizeBytes != nil {
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
		volume.SizeBytes = *req.SizeBytes
	}
	if req.BlockSize != nil {
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
	}
	if req.ChunkSizeBytes != nil {
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
	}
	if req.ExtentPageBytes != nil {
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
	}
	if req.AccessMode != nil {
		if isAttached {
			return VolumeSpec{}, ErrVolumeNotDetached
		}
		volume.AccessMode = *req.AccessMode
	}
	if req.State != nil {
		volume.State = *req.State
	}
	volume = NormalizeVolumeSpec(volume)
	r.volumes[volumeID] = cloneVolumeSpec(volume)
	return cloneVolumeSpec(volume), nil
}

func (r *inMemoryMetadataRepository) DeleteVolume(_ context.Context, volumeID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[volumeID]
	if !ok {
		return ErrVolumeNotFound
	}
	if r.state[volumeID].AttachmentID != "" {
		return ErrVolumeNotDetached
	}
	if volume.State != VolumeStateAvailable && volume.State != VolumeStateDisabled {
		return ErrVolumeNotDetached
	}
	if pages := r.extentPages[volumeID]; len(pages) > 0 {
		return ErrVolumeHasObjects
	}
	if candidates := r.chunkGC[volumeID]; len(candidates) > 0 {
		return ErrVolumeHasObjects
	}
	delete(r.volumes, volumeID)
	delete(r.state, volumeID)
	delete(r.status, volumeID)
	delete(r.extentPages, volumeID)
	delete(r.nextChunkID, volumeID)
	delete(r.chunkGC, volumeID)
	return nil
}

func (r *inMemoryMetadataRepository) GetVolume(_ context.Context, volumeID uint64) (VolumeSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[volumeID]
	if !ok {
		return VolumeSpec{}, ErrVolumeNotFound
	}
	return cloneVolumeSpec(volume), nil
}

func (r *inMemoryMetadataRepository) GetVolumeStatus(_ context.Context, volumeID uint64) (VolumeStatusRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return VolumeStatusRecord{}, ErrVolumeNotFound
	}
	return r.status[volumeID], nil
}

func (r *inMemoryMetadataRepository) PutVolumeStatus(_ context.Context, status VolumeStatusRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	volumeID := uint64(status.VolumeID)
	if _, ok := r.volumes[volumeID]; !ok {
		return ErrVolumeNotFound
	}
	r.status[volumeID] = status
	return nil
}

func (r *inMemoryMetadataRepository) ListVolumes(_ context.Context) ([]VolumeSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]VolumeSpec, 0, len(r.volumes))
	for _, volume := range r.volumes {
		out = append(out, cloneVolumeSpec(volume))
	}
	return out, nil
}

func (r *inMemoryMetadataRepository) SetVolumeState(_ context.Context, volumeID uint64, state VolumeLifecycleState) (VolumeSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[volumeID]
	if !ok {
		return VolumeSpec{}, ErrVolumeNotFound
	}
	volume.State = state
	r.volumes[volumeID] = cloneVolumeSpec(volume)
	return cloneVolumeSpec(volume), nil
}

func (r *inMemoryMetadataRepository) GetAttachment(_ context.Context, volumeID uint64) (AttachmentRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return AttachmentRecord{}, ErrVolumeNotFound
	}
	return r.state[volumeID], nil
}

func (r *inMemoryMetadataRepository) GetGeneration(_ context.Context, volumeID uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return 0, ErrVolumeNotFound
	}
	gen := r.state[volumeID].Generation
	if gen == 0 {
		gen = 1
	}
	return gen, nil
}

func (r *inMemoryMetadataRepository) UnsafeClearAttachment(_ context.Context, volumeID uint64) (AttachmentRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return AttachmentRecord{}, ErrVolumeNotFound
	}
	state := r.state[volumeID]
	if state.Generation == 0 {
		state.Generation = 1
	}
	if state.AttachmentID != "" {
		state.Generation++
	}
	state.HostID = ""
	state.AttachmentID = ""
	state.DeviceID = 0
	state.OwnerGatewayID = ""
	state.LeaseID = ""
	state.AttachedAtUnix = 0
	r.state[volumeID] = state

	status := r.status[volumeID]
	status.InUse = false
	status.CurrentAttachmentID = ""
	status.CurrentHostID = ""
	status.CurrentGatewayID = ""
	status.GatewayConnectionState = GatewayStateDetached
	status.AttachmentGeneration = state.Generation
	r.status[volumeID] = status

	volume := r.volumes[volumeID]
	volume.State = VolumeStateAvailable
	r.volumes[volumeID] = volume
	return state, nil
}

func (r *inMemoryMetadataRepository) UnsafeSetGeneration(_ context.Context, volumeID uint64, generation uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return 0, ErrVolumeNotFound
	}
	if generation == 0 {
		generation = 1
	}
	state := r.state[volumeID]
	state.Generation = generation
	r.state[volumeID] = state
	return generation, nil
}

func (r *inMemoryMetadataRepository) Attach(_ context.Context, req AttachRequest) (AttachmentRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[req.VolumeID]; !ok {
		return AttachmentRecord{}, ErrVolumeNotFound
	}
	if r.volumes[req.VolumeID].State == VolumeStateDisabled {
		return AttachmentRecord{}, ErrVolumeDisabled
	}
	state := r.state[req.VolumeID]
	if state.HostID != "" && (state.HostID != req.HostID || state.DeviceID != req.DeviceID) {
		return AttachmentRecord{}, ErrAttachConflict
	}
	if state.AttachmentID == "" {
		state.AttachmentID = FormatAttachmentID(req.VolumeID, state.Generation)
	} else if req.GatewayID != "" && state.OwnerGatewayID != "" && state.OwnerGatewayID != req.GatewayID {
		state.Generation++
		state.AttachmentID = FormatAttachmentID(req.VolumeID, state.Generation)
		state.LeaseID = ""
		state.AttachedAtUnix = 0
	}
	state.HostID = req.HostID
	state.DeviceID = req.DeviceID
	state.OwnerGatewayID = req.GatewayID
	state.AttachedAtUnix = time.Now().Unix()
	r.state[req.VolumeID] = state
	status := r.status[req.VolumeID]
	status.InUse = true
	status.CurrentAttachmentID = state.AttachmentID
	status.CurrentHostID = req.HostID
	status.CurrentGatewayID = req.GatewayID
	status.AttachmentGeneration = state.Generation
	if req.GatewayID != "" {
		status.GatewayConnectionState = GatewayStateUp
	}
	r.status[req.VolumeID] = status
	volume := r.volumes[req.VolumeID]
	volume.State = VolumeStateInUse
	r.volumes[req.VolumeID] = volume
	return state, nil
}

func (r *inMemoryMetadataRepository) Detach(_ context.Context, req DetachRequest) (AttachmentRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[req.VolumeID]; !ok {
		return AttachmentRecord{}, ErrVolumeNotFound
	}
	state := r.state[req.VolumeID]
	if state.HostID != "" && (state.HostID != req.HostID || state.AttachmentID != req.AttachmentID) {
		return AttachmentRecord{}, ErrDetachConflict
	}
	if state.HostID == req.HostID && state.AttachmentID == req.AttachmentID {
		state.HostID = ""
		state.AttachmentID = ""
		state.DeviceID = 0
		state.OwnerGatewayID = ""
		state.LeaseID = ""
		state.Generation++
		r.state[req.VolumeID] = state
		status := r.status[req.VolumeID]
		status.InUse = false
		status.CurrentAttachmentID = ""
		status.CurrentHostID = ""
		status.CurrentGatewayID = ""
		status.GatewayConnectionState = GatewayStateDetached
		status.AttachmentGeneration = state.Generation
		r.status[req.VolumeID] = status
		volume := r.volumes[req.VolumeID]
		volume.State = VolumeStateAvailable
		r.volumes[req.VolumeID] = volume
	}
	return state, nil
}

func (r *inMemoryMetadataRepository) GetGateway(_ context.Context, gatewayID string) (GatewayRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.gateway[gatewayID]
	if !ok {
		return GatewayRecord{}, ErrGatewayNotFound
	}
	return rec, nil
}

func (r *inMemoryMetadataRepository) ListGateways(_ context.Context) ([]GatewayRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]GatewayRecord, 0, len(r.gateway))
	for _, rec := range r.gateway {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GatewayID < out[j].GatewayID })
	return out, nil
}

func (r *inMemoryMetadataRepository) PutGateway(_ context.Context, rec GatewayRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.gateway[rec.GatewayID] = rec
	return nil
}

func (r *inMemoryMetadataRepository) GetExtentPage(_ context.Context, volumeID, pageNo uint64) (AllocationPageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[volumeID]
	if !ok {
		return AllocationPageRecord{}, ErrVolumeNotFound
	}
	pages := r.extentPages[volumeID]
	rec, ok := pages[pageNo]
	if !ok {
		return AllocationPageRecord{
			VolumeID:       HexVolumeID(volumeID),
			PageNo:         pageNo,
			PageBytes:      volume.ExtentPageBytes,
			ChunkSizeBytes: volume.ChunkSizeBytes,
			Extents:        nil,
		}, nil
	}
	return cloneAllocationPage(rec), nil
}

func (r *inMemoryMetadataRepository) ListExtentPages(_ context.Context, volumeID uint64) ([]AllocationPageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return nil, ErrVolumeNotFound
	}
	pages := r.extentPages[volumeID]
	out := make([]AllocationPageRecord, 0, len(pages))
	for _, rec := range pages {
		out = append(out, cloneAllocationPage(rec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return out, nil
}

func (r *inMemoryMetadataRepository) PutExtentPage(_ context.Context, rec AllocationPageRecord, expectedRevision int64) (AllocationPageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	volume, ok := r.volumes[uint64(rec.VolumeID)]
	if !ok {
		return AllocationPageRecord{}, ErrVolumeNotFound
	}
	if rec.PageBytes == 0 {
		rec.PageBytes = volume.ExtentPageBytes
	}
	if rec.ChunkSizeBytes == 0 {
		rec.ChunkSizeBytes = volume.ChunkSizeBytes
	}
	if _, ok := r.extentPages[uint64(rec.VolumeID)]; !ok {
		r.extentPages[uint64(rec.VolumeID)] = make(map[uint64]AllocationPageRecord)
	}
	current, ok := r.extentPages[uint64(rec.VolumeID)][rec.PageNo]
	currentRevision := int64(0)
	if ok {
		currentRevision = current.Revision
	}
	if currentRevision != expectedRevision {
		return AllocationPageRecord{}, ErrMetadataCASConflict
	}
	rec.Revision = currentRevision + 1
	rec.Extents = append([]AllocationChunkRecord(nil), rec.Extents...)
	r.extentPages[uint64(rec.VolumeID)][rec.PageNo] = rec
	return cloneAllocationPage(rec), nil
}

func (r *inMemoryMetadataRepository) AllocateChunkIDs(_ context.Context, volumeID uint64, count uint32) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return 0, ErrVolumeNotFound
	}
	if count == 0 {
		return 0, nil
	}
	next := r.nextChunkID[volumeID]
	if next == 0 {
		next = 1
	}
	r.nextChunkID[volumeID] = next + uint64(count)
	return next, nil
}

func (r *inMemoryMetadataRepository) PutChunkGarbage(_ context.Context, rec AllocationChunkGarbageRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[uint64(rec.VolumeID)]; !ok {
		return ErrVolumeNotFound
	}
	if rec.EnqueuedAtUnix == 0 {
		rec.EnqueuedAtUnix = time.Now().Unix()
	}
	if _, ok := r.chunkGC[uint64(rec.VolumeID)]; !ok {
		r.chunkGC[uint64(rec.VolumeID)] = make(map[uint64]AllocationChunkGarbageRecord)
	}
	r.chunkGC[uint64(rec.VolumeID)][rec.ChunkID] = rec
	return nil
}

func (r *inMemoryMetadataRepository) ListChunkGarbage(_ context.Context, volumeID uint64, limit int) ([]AllocationChunkGarbageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return nil, ErrVolumeNotFound
	}
	records := make([]AllocationChunkGarbageRecord, 0, len(r.chunkGC[volumeID]))
	for _, rec := range r.chunkGC[volumeID] {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].EnqueuedAtUnix == records[j].EnqueuedAtUnix {
			return records[i].ChunkID < records[j].ChunkID
		}
		return records[i].EnqueuedAtUnix < records[j].EnqueuedAtUnix
	})
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records, nil
}

func (r *inMemoryMetadataRepository) DeleteChunkGarbage(_ context.Context, volumeID, chunkID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.volumes[volumeID]; !ok {
		return ErrVolumeNotFound
	}
	delete(r.chunkGC[volumeID], chunkID)
	return nil
}

func generateUniqueVolumeIDMemory(existing map[uint64]VolumeSpec) (uint64, error) {
	for i := 0; i < 64; i++ {
		var raw [4]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		id := uint64(binary.BigEndian.Uint32(raw[:]))
		if id == 0 {
			continue
		}
		if _, ok := existing[id]; ok {
			continue
		}
		return id, nil
	}
	return 0, ErrVolumeNameConflict
}

func cloneAllocationPage(rec AllocationPageRecord) AllocationPageRecord {
	rec.Extents = append([]AllocationChunkRecord(nil), rec.Extents...)
	for i := range rec.Extents {
		rec.Extents[i].PayloadEncryption = cloneChunkPayloadEncryptionHeader(rec.Extents[i].PayloadEncryption)
	}
	return rec
}
