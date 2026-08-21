package metadata

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nosway/namrbd/gateway/service"
)

const defaultLeaseTTL = 15
const extentCASRetryLimit = 8

type EtcdRepository struct {
	pressure *EtcdPressure
	client   *clientv3.Client
	rootPath string

	// onEtcdOutcome receives the result of etcd calls this repository already
	// makes. AA-IMPL-004B feeds the dependency availability tracker from it.
	//
	// It is a function rather than a depavail dependency so this package keeps
	// knowing nothing about availability policy: it reports what happened, and
	// what that means is decided elsewhere.
	onEtcdOutcome func(error)
}

// SetEtcdOutcomeObserver installs the outcome observer. Calling it with nil
// removes the observer.
//
// Reporting from calls the repository already makes is the whole point. A
// dedicated liveness probe would add a standing read per process per
// dependency, which is a fraction of the load AA-IMPL-003 spent three slices
// removing, to learn something the lease renewal already knows.
func (r *EtcdRepository) SetEtcdOutcomeObserver(f func(error)) {
	if r != nil {
		r.onEtcdOutcome = f
	}
}

func (r *EtcdRepository) observeEtcd(err error) {
	if r != nil && r.onEtcdOutcome != nil {
		r.onEtcdOutcome(err)
	}
}

func NewEtcdRepository(client *clientv3.Client, rootPath string) *EtcdRepository {
	rootPath = strings.TrimSpace(rootPath)
	if rootPath == "" {
		rootPath = "/namrbd"
	}
	rootPath = strings.TrimRight(rootPath, "/")
	return &EtcdRepository{pressure: &EtcdPressure{}, client: client, rootPath: rootPath}
}

func NewEtcdClient(endpoints []string, timeout time.Duration) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("etcd endpoints are required")
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
}

func (r *EtcdRepository) EnsureVolume(ctx context.Context, spec service.VolumeSpec) error {
	spec = service.NormalizeVolumeSpec(spec)
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	status := service.VolumeStatusRecord{
		VolumeID:               spec.ID,
		GatewayConnectionState: service.GatewayStateUnknown,
	}
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return err
	}
	genBytes, err := json.Marshal(uint64(1))
	if err != nil {
		return err
	}
	_, err = r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(r.volumeSpecKey(uint64(spec.ID))), "=", 0)).
		Then(
			clientv3.OpPut(r.volumeSpecKey(uint64(spec.ID)), string(specBytes)),
			clientv3.OpPut(r.volumeStatusKey(uint64(spec.ID)), string(statusBytes)),
			clientv3.OpPut(r.generationKey(uint64(spec.ID)), string(genBytes)),
		).
		Commit()
	return err
}

func (r *EtcdRepository) SyncVolumeSpec(ctx context.Context, spec service.VolumeSpec) error {
	spec = service.NormalizeVolumeSpec(spec)
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	resp, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(r.volumeSpecKey(uint64(spec.ID))), ">", 0)).
		Then(clientv3.OpPut(r.volumeSpecKey(uint64(spec.ID)), string(specBytes))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return service.ErrVolumeNotFound
	}
	return nil
}

func (r *EtcdRepository) CreateVolume(ctx context.Context, req service.VolumeCreateRequest) (service.VolumeSpec, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return service.VolumeSpec{}, service.ErrVolumeNameRequired
	}
	volumes, err := r.ListVolumes(ctx)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	existing := make(map[uint64]service.VolumeSpec, len(volumes))
	for _, volume := range volumes {
		existing[uint64(volume.ID)] = volume
		if volume.Name == name {
			return service.VolumeSpec{}, service.ErrVolumeNameConflict
		}
	}
	volumeID, err := generateUniqueVolumeID(existing)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(volumeID),
		Name:            name,
		Prefix:          service.BuildVolumePrefix(name, volumeID),
		SizeBytes:       req.SizeBytes,
		BlockSize:       req.BlockSize,
		ChunkSizeBytes:  req.ChunkSizeBytes,
		ExtentPageBytes: req.ExtentPageBytes,
		AccessMode:      req.AccessMode,
		State:           req.State,
	})
	if err := r.EnsureVolume(ctx, spec); err != nil {
		if _, getErr := r.GetVolume(ctx, volumeID); getErr == nil {
			return service.VolumeSpec{}, service.ErrVolumeNameConflict
		}
		return service.VolumeSpec{}, err
	}
	return spec, nil
}

func (r *EtcdRepository) UpdateVolume(ctx context.Context, volumeID uint64, req service.VolumeUpdateRequest) (service.VolumeSpec, error) {
	spec, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	attachment, err := r.GetAttachment(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	isAttached := attachment.AttachmentID != ""
	if err := service.ValidateImmutableVolumeGeometry(spec, req); err != nil {
		return service.VolumeSpec{}, err
	}

	if req.Name != nil {
		nextName := strings.TrimSpace(*req.Name)
		if nextName == "" {
			return service.VolumeSpec{}, service.ErrVolumeNameRequired
		}
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
		volumes, err := r.ListVolumes(ctx)
		if err != nil {
			return service.VolumeSpec{}, err
		}
		for _, volume := range volumes {
			if uint64(volume.ID) != volumeID && volume.Name == nextName {
				return service.VolumeSpec{}, service.ErrVolumeNameConflict
			}
		}
		spec.Name = nextName
		spec.Prefix = service.BuildVolumePrefix(nextName, volumeID)
	}
	if req.SizeBytes != nil {
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
		spec.SizeBytes = *req.SizeBytes
	}
	if req.BlockSize != nil {
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
	}
	if req.ChunkSizeBytes != nil {
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
	}
	if req.ExtentPageBytes != nil {
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
	}
	if req.AccessMode != nil {
		if isAttached {
			return service.VolumeSpec{}, service.ErrVolumeNotDetached
		}
		spec.AccessMode = *req.AccessMode
	}
	if req.State != nil {
		spec.State = *req.State
	}
	spec = service.NormalizeVolumeSpec(spec)
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	if _, err := r.client.Put(ctx, r.volumeSpecKey(volumeID), string(specBytes)); err != nil {
		return service.VolumeSpec{}, err
	}
	return spec, nil
}

func (r *EtcdRepository) DeleteVolume(ctx context.Context, volumeID uint64) error {
	spec, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return err
	}
	attachment, err := r.GetAttachment(ctx, volumeID)
	if err != nil {
		return err
	}
	if attachment.AttachmentID != "" {
		return service.ErrVolumeNotDetached
	}
	if spec.State != service.VolumeStateAvailable && spec.State != service.VolumeStateDisabled {
		return service.ErrVolumeNotDetached
	}
	pages, err := r.ListExtentPages(ctx, volumeID)
	if err != nil {
		return err
	}
	if len(pages) > 0 {
		return service.ErrVolumeHasObjects
	}
	garbage, err := r.ListChunkGarbage(ctx, volumeID, 1)
	if err != nil {
		return err
	}
	if len(garbage) > 0 {
		return service.ErrVolumeHasObjects
	}
	_, err = r.client.Txn(ctx).Then(
		clientv3.OpDelete(r.volumeSpecKey(volumeID)),
		clientv3.OpDelete(r.volumeStatusKey(volumeID)),
		clientv3.OpDelete(r.generationKey(volumeID)),
		clientv3.OpDelete(r.attachmentKey(volumeID)),
		clientv3.OpDelete(r.extentPagePrefix(volumeID), clientv3.WithPrefix()),
		clientv3.OpDelete(r.chunkGarbagePrefix(volumeID), clientv3.WithPrefix()),
		clientv3.OpDelete(r.chunkNextIDKey(volumeID)),
	).Commit()
	return err
}

func (r *EtcdRepository) GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	resp, err := r.client.Get(ctx, r.volumeSpecKey(volumeID))
	if err != nil {
		return service.VolumeSpec{}, err
	}
	if len(resp.Kvs) == 0 {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	var spec service.VolumeSpec
	if err := json.Unmarshal(resp.Kvs[0].Value, &spec); err != nil {
		return service.VolumeSpec{}, err
	}
	return service.NormalizeVolumeSpec(spec), nil
}

func (r *EtcdRepository) GetVolumeStatus(ctx context.Context, volumeID uint64) (service.VolumeStatusRecord, error) {
	resp, err := r.client.Get(ctx, r.volumeStatusKey(volumeID))
	if err != nil {
		return service.VolumeStatusRecord{}, err
	}
	if len(resp.Kvs) == 0 {
		if _, err := r.GetVolume(ctx, volumeID); err != nil {
			return service.VolumeStatusRecord{}, err
		}
		return service.VolumeStatusRecord{
			VolumeID:               service.HexVolumeID(volumeID),
			GatewayConnectionState: service.GatewayStateUnknown,
		}, nil
	}
	var status service.VolumeStatusRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &status); err != nil {
		return service.VolumeStatusRecord{}, err
	}
	return status, nil
}

func (r *EtcdRepository) PutVolumeStatus(ctx context.Context, status service.VolumeStatusRecord) error {
	volumeID := uint64(status.VolumeID)
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return err
	}
	payload, err := json.Marshal(status)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, r.volumeStatusKey(volumeID), string(payload))
	return err
}

func (r *EtcdRepository) ListVolumes(ctx context.Context) ([]service.VolumeSpec, error) {
	r.pressure.countPrefixScan()
	resp, err := r.client.Get(ctx, r.volumeSpecPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]service.VolumeSpec, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if !strings.HasSuffix(string(kv.Key), "/spec") {
			continue
		}
		var spec service.VolumeSpec
		if err := json.Unmarshal(kv.Value, &spec); err != nil {
			return nil, err
		}
		out = append(out, service.NormalizeVolumeSpec(spec))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *EtcdRepository) SetVolumeState(ctx context.Context, volumeID uint64, state service.VolumeLifecycleState) (service.VolumeSpec, error) {
	spec, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	spec.State = state
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	if _, err := r.client.Put(ctx, r.volumeSpecKey(volumeID), string(specBytes)); err != nil {
		return service.VolumeSpec{}, err
	}
	return spec, nil
}

func (r *EtcdRepository) GetAttachment(ctx context.Context, volumeID uint64) (service.AttachmentRecord, error) {
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return service.AttachmentRecord{}, err
	}
	resp, err := r.client.Get(ctx, r.attachmentKey(volumeID))
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if len(resp.Kvs) == 0 {
		gen, err := r.getGeneration(ctx, volumeID)
		if err != nil {
			return service.AttachmentRecord{}, err
		}
		return service.AttachmentRecord{Generation: gen}, nil
	}
	var rec service.AttachmentRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return service.AttachmentRecord{}, err
	}
	return rec, nil
}

func (r *EtcdRepository) GetGeneration(ctx context.Context, volumeID uint64) (uint64, error) {
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return 0, err
	}
	return r.getGeneration(ctx, volumeID)
}

func (r *EtcdRepository) UnsafeClearAttachment(ctx context.Context, volumeID uint64) (service.AttachmentRecord, error) {
	spec, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	current, err := r.GetAttachment(ctx, volumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	gen, err := r.getGeneration(ctx, volumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if gen == 0 {
		gen = 1
	}
	if current.AttachmentID != "" {
		gen++
	}
	nextGenBytes, err := json.Marshal(gen)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	spec.State = service.VolumeStateAvailable
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	status, err := r.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	status.VolumeID = service.HexVolumeID(volumeID)
	status.GatewayConnectionState = service.GatewayStateDetached
	status.InUse = false
	status.CurrentAttachmentID = ""
	status.CurrentHostID = ""
	status.CurrentGatewayID = ""
	status.AttachmentGeneration = gen
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	cleared := service.AttachmentRecord{Generation: gen}
	ops := []clientv3.Op{
		clientv3.OpDelete(r.attachmentKey(volumeID)),
		clientv3.OpPut(r.generationKey(volumeID), string(nextGenBytes)),
		clientv3.OpPut(r.volumeStatusKey(volumeID), string(statusBytes)),
		clientv3.OpPut(r.volumeSpecKey(volumeID), string(specBytes)),
	}
	if current.HostID != "" && current.AttachmentID != "" {
		ops = append(ops, clientv3.OpDelete(r.hostAttachmentKey(current.HostID, current.AttachmentID)))
	}
	if _, err := r.client.Txn(ctx).Then(ops...).Commit(); err != nil {
		return service.AttachmentRecord{}, err
	}
	return cleared, nil
}

func (r *EtcdRepository) UnsafeSetGeneration(ctx context.Context, volumeID uint64, generation uint64) (uint64, error) {
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return 0, err
	}
	if generation == 0 {
		generation = 1
	}
	genBytes, err := json.Marshal(generation)
	if err != nil {
		return 0, err
	}
	if _, err := r.client.Put(ctx, r.generationKey(volumeID), string(genBytes)); err != nil {
		return 0, err
	}
	state, err := r.GetAttachment(ctx, volumeID)
	if err == nil {
		state.Generation = generation
		if state.AttachmentID != "" {
			stateBytes, marshalErr := json.Marshal(state)
			if marshalErr != nil {
				return 0, marshalErr
			}
			if _, putErr := r.client.Put(ctx, r.attachmentKey(volumeID), string(stateBytes)); putErr != nil {
				return 0, putErr
			}
		}
	}
	return generation, nil
}

func (r *EtcdRepository) Attach(ctx context.Context, req service.AttachRequest) (service.AttachmentRecord, error) {
	if _, err := r.GetVolume(ctx, req.VolumeID); err != nil {
		return service.AttachmentRecord{}, err
	}
	spec, err := r.GetVolume(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if spec.State == service.VolumeStateDisabled {
		return service.AttachmentRecord{}, service.ErrVolumeDisabled
	}
	status, err := r.GetVolumeStatus(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	gen, err := r.getGeneration(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	existing, err := r.GetAttachment(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if existing.AttachmentID != "" {
		if existing.HostID == req.HostID && existing.DeviceID == req.DeviceID {
			if existing.OwnerGatewayID == req.GatewayID || req.GatewayID == "" || existing.OwnerGatewayID == "" {
				return existing, nil
			}
			record := existing
			record.Generation++
			record.AttachmentID = service.FormatAttachmentID(req.VolumeID, record.Generation)
			record.OwnerGatewayID = req.GatewayID
			record.LeaseID = ""
			record.AttachedAtUnix = time.Now().Unix()
			recordBytes, err := json.Marshal(record)
			if err != nil {
				return service.AttachmentRecord{}, err
			}
			status.InUse = true
			status.CurrentAttachmentID = record.AttachmentID
			status.CurrentHostID = req.HostID
			status.CurrentGatewayID = req.GatewayID
			status.GatewayConnectionState = service.GatewayStateUp
			status.AttachmentGeneration = record.Generation
			statusBytes, err := json.Marshal(status)
			if err != nil {
				return service.AttachmentRecord{}, err
			}
			spec.State = service.VolumeStateInUse
			specBytes, err := json.Marshal(spec)
			if err != nil {
				return service.AttachmentRecord{}, err
			}
			resp, err := r.client.Txn(ctx).
				If(clientv3.Compare(clientv3.Version(r.attachmentKey(req.VolumeID)), ">", 0)).
				Then(
					clientv3.OpPut(r.attachmentKey(req.VolumeID), string(recordBytes)),
					clientv3.OpPut(r.volumeStatusKey(req.VolumeID), string(statusBytes)),
					clientv3.OpPut(r.volumeSpecKey(req.VolumeID), string(specBytes)),
					clientv3.OpDelete(r.hostAttachmentKey(req.HostID, existing.AttachmentID)),
					clientv3.OpPut(r.hostAttachmentKey(req.HostID, record.AttachmentID), string(recordBytes)),
				).
				Commit()
			if err != nil {
				return service.AttachmentRecord{}, err
			}
			if !resp.Succeeded {
				return service.AttachmentRecord{}, service.ErrAttachConflict
			}
			return record, nil
		}
		return service.AttachmentRecord{}, service.ErrAttachConflict
	}

	record := service.AttachmentRecord{
		Generation:     gen,
		HostID:         req.HostID,
		AttachmentID:   service.FormatAttachmentID(req.VolumeID, gen),
		DeviceID:       req.DeviceID,
		OwnerGatewayID: req.GatewayID,
		AttachedAtUnix: time.Now().Unix(),
	}
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	status.InUse = true
	status.CurrentAttachmentID = record.AttachmentID
	status.CurrentHostID = req.HostID
	status.CurrentGatewayID = req.GatewayID
	status.GatewayConnectionState = service.GatewayStateUp
	status.AttachmentGeneration = record.Generation
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	spec.State = service.VolumeStateInUse
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	resp, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(r.attachmentKey(req.VolumeID)), "=", 0)).
		Then(
			clientv3.OpPut(r.attachmentKey(req.VolumeID), string(recordBytes)),
			clientv3.OpPut(r.volumeStatusKey(req.VolumeID), string(statusBytes)),
			clientv3.OpPut(r.volumeSpecKey(req.VolumeID), string(specBytes)),
			clientv3.OpPut(r.hostAttachmentKey(req.HostID, record.AttachmentID), string(recordBytes)),
		).
		Commit()
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if !resp.Succeeded {
		return service.AttachmentRecord{}, service.ErrAttachConflict
	}
	return record, nil
}

func (r *EtcdRepository) Detach(ctx context.Context, req service.DetachRequest) (service.AttachmentRecord, error) {
	spec, err := r.GetVolume(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	current, err := r.GetAttachment(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if current.AttachmentID == "" {
		return current, nil
	}
	if current.HostID != req.HostID || current.AttachmentID != req.AttachmentID {
		return service.AttachmentRecord{}, service.ErrDetachConflict
	}
	next := current
	next.HostID = ""
	next.AttachmentID = ""
	next.DeviceID = 0
	next.OwnerGatewayID = ""
	next.LeaseID = ""
	next.AttachedAtUnix = 0
	next.Generation++
	nextGenBytes, err := json.Marshal(next.Generation)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	status, err := r.GetVolumeStatus(ctx, req.VolumeID)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	status.VolumeID = service.HexVolumeID(req.VolumeID)
	status.GatewayConnectionState = service.GatewayStateDetached
	status.InUse = false
	status.CurrentAttachmentID = ""
	status.CurrentHostID = ""
	status.CurrentGatewayID = ""
	status.AttachmentGeneration = next.Generation
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	spec.State = service.VolumeStateAvailable
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	resp, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(r.attachmentKey(req.VolumeID)), "=", mustJSON(current))).
		Then(
			clientv3.OpDelete(r.attachmentKey(req.VolumeID)),
			clientv3.OpDelete(r.hostAttachmentKey(req.HostID, req.AttachmentID)),
			clientv3.OpPut(r.generationKey(req.VolumeID), string(nextGenBytes)),
			clientv3.OpPut(r.volumeStatusKey(req.VolumeID), string(statusBytes)),
			clientv3.OpPut(r.volumeSpecKey(req.VolumeID), string(specBytes)),
		).
		Commit()
	if err != nil {
		return service.AttachmentRecord{}, err
	}
	if !resp.Succeeded {
		return service.AttachmentRecord{}, service.ErrDetachConflict
	}
	return next, nil
}

func (r *EtcdRepository) GetGateway(ctx context.Context, gatewayID string) (service.GatewayRecord, error) {
	resp, err := r.client.Get(ctx, r.gatewayStatusKey(gatewayID))
	if err != nil {
		return service.GatewayRecord{}, err
	}
	if len(resp.Kvs) == 0 {
		return service.GatewayRecord{}, service.ErrGatewayNotFound
	}
	var rec service.GatewayRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return service.GatewayRecord{}, err
	}
	rec = service.NormalizeGatewayFleetRecord(rec)
	rec.RegistryRevision = resp.Kvs[0].ModRevision
	if err := service.ValidateGatewayFleetRecord(rec); err != nil {
		return service.GatewayRecord{}, err
	}
	return rec, nil
}

func (r *EtcdRepository) ListGateways(ctx context.Context) ([]service.GatewayRecord, error) {
	out := []service.GatewayRecord{}
	opts := GatewayFleetListOptions{}
	for {
		page, err := r.ListGatewayFleetPage(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Records...)
		if page.NextCursor == "" {
			break
		}
		opts.Cursor = page.NextCursor
		opts.Revision = page.Revision
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GatewayID < out[j].GatewayID })
	return out, nil
}

func (r *EtcdRepository) PutGateway(ctx context.Context, rec service.GatewayRecord) error {
	rec = service.NormalizeGatewayFleetRecord(rec)
	if err := service.ValidateGatewayFleetRecord(rec); err != nil {
		return err
	}
	if err := r.validateGatewayRecordAgainstRegistry(ctx, rec); err != nil {
		return err
	}
	rec.RegistryRevision = 0
	recBytes, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, r.gatewayStatusKey(rec.GatewayID), string(recBytes))
	return err
}

func (r *EtcdRepository) validateGatewayRecordAgainstRegistry(ctx context.Context, rec service.GatewayRecord) error {
	rec = service.NormalizeGatewayFleetRecord(rec)
	if err := service.ValidateGatewayFleetRecord(rec); err != nil {
		return err
	}
	opts := GatewayFleetListOptions{}
	for {
		page, err := r.ListGatewayFleetPage(ctx, opts)
		if err != nil {
			return err
		}
		for _, existing := range page.Records {
			if existing.GatewayID == "" || existing.GatewayID == rec.GatewayID {
				continue
			}
			if err := validateGatewayRecordCompatibility(existing, rec); err != nil {
				return err
			}
		}
		if page.NextCursor == "" {
			return nil
		}
		opts.Cursor = page.NextCursor
		opts.Revision = page.Revision
	}
}

func validateGatewayRecordCompatibility(existing, incoming service.GatewayRecord) error {
	checks := []struct {
		field    string
		existing string
		incoming string
	}{
		{field: "product", existing: string(existing.Product), incoming: string(incoming.Product)},
		{field: "cluster_id", existing: existing.ClusterID, incoming: incoming.ClusterID},
		{field: "sbs_cluster_id", existing: existing.SBSClusterID, incoming: incoming.SBSClusterID},
		{field: "metadata_backend", existing: existing.MetadataBackend, incoming: incoming.MetadataBackend},
		{field: "metadata_root", existing: existing.MetadataRoot, incoming: incoming.MetadataRoot},
		{field: "sbs_cluster_metadata_backend", existing: existing.SBSClusterMetadataBackend, incoming: incoming.SBSClusterMetadataBackend},
		{field: "sbs_cluster_metadata_root", existing: existing.SBSClusterMetadataRoot, incoming: incoming.SBSClusterMetadataRoot},
	}
	for _, check := range checks {
		if check.existing == "" || check.incoming == "" {
			continue
		}
		if check.existing != check.incoming {
			return fmt.Errorf("gateway registry identity mismatch for %s: existing=%q incoming=%q", check.field, check.existing, check.incoming)
		}
	}
	return nil
}

func (r *EtcdRepository) GetExtentPage(ctx context.Context, volumeID, pageNo uint64) (service.AllocationPageRecord, error) {
	volume, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	resp, err := r.client.Get(ctx, r.extentPageKey(volumeID, pageNo))
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	if len(resp.Kvs) == 0 {
		return service.AllocationPageRecord{
			VolumeID:       service.HexVolumeID(volumeID),
			PageNo:         pageNo,
			PageBytes:      volume.ExtentPageBytes,
			ChunkSizeBytes: volume.ChunkSizeBytes,
		}, nil
	}
	var rec service.AllocationPageRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return service.AllocationPageRecord{}, err
	}
	rec.VolumeID = service.HexVolumeID(volumeID)
	rec.PageNo = pageNo
	if rec.PageBytes == 0 {
		rec.PageBytes = volume.ExtentPageBytes
	}
	if rec.ChunkSizeBytes == 0 {
		rec.ChunkSizeBytes = volume.ChunkSizeBytes
	}
	rec.Revision = resp.Kvs[0].ModRevision
	return rec, nil
}

func (r *EtcdRepository) ListExtentPages(ctx context.Context, volumeID uint64) ([]service.AllocationPageRecord, error) {
	r.pressure.countPrefixScan()
	volume, err := r.GetVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Get(ctx, r.extentPagePrefix(volumeID), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]service.AllocationPageRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var rec service.AllocationPageRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, err
		}
		rec.VolumeID = service.HexVolumeID(volumeID)
		if rec.PageBytes == 0 {
			rec.PageBytes = volume.ExtentPageBytes
		}
		if rec.ChunkSizeBytes == 0 {
			rec.ChunkSizeBytes = volume.ChunkSizeBytes
		}
		rec.Revision = kv.ModRevision
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PageNo < out[j].PageNo })
	return out, nil
}

func (r *EtcdRepository) PutExtentPage(ctx context.Context, rec service.AllocationPageRecord, expectedRevision int64) (service.AllocationPageRecord, error) {
	volume, err := r.GetVolume(ctx, uint64(rec.VolumeID))
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	if rec.PageBytes == 0 {
		rec.PageBytes = volume.ExtentPageBytes
	}
	if rec.ChunkSizeBytes == 0 {
		rec.ChunkSizeBytes = volume.ChunkSizeBytes
	}
	rec.Revision = 0
	payload, err := json.Marshal(rec)
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	resp, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(r.extentPageKey(uint64(rec.VolumeID), rec.PageNo)), "=", expectedRevision)).
		Then(clientv3.OpPut(r.extentPageKey(uint64(rec.VolumeID), rec.PageNo), string(payload))).
		Commit()
	if err != nil {
		return service.AllocationPageRecord{}, err
	}
	if !resp.Succeeded {
		return service.AllocationPageRecord{}, service.ErrMetadataCASConflict
	}
	return r.GetExtentPage(ctx, uint64(rec.VolumeID), rec.PageNo)
}

func (r *EtcdRepository) AllocateChunkIDs(ctx context.Context, volumeID uint64, count uint32) (uint64, error) {
	if count == 0 {
		return 0, nil
	}
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return 0, err
	}
	key := r.chunkNextIDKey(volumeID)
	for attempt := 0; attempt < extentCASRetryLimit; attempt++ {
		resp, err := r.client.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		nextID := uint64(1)
		expectedRevision := int64(0)
		if len(resp.Kvs) > 0 {
			expectedRevision = resp.Kvs[0].ModRevision
			if err := json.Unmarshal(resp.Kvs[0].Value, &nextID); err != nil {
				return 0, err
			}
			if nextID == 0 {
				nextID = 1
			}
		}
		startID := nextID
		updatedNextID := startID + uint64(count)
		payload, err := json.Marshal(updatedNextID)
		if err != nil {
			return 0, err
		}
		txnResp, err := r.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision)).
			Then(clientv3.OpPut(key, string(payload))).
			Commit()
		if err != nil {
			return 0, err
		}
		if txnResp.Succeeded {
			return startID, nil
		}
	}
	return 0, service.ErrMetadataCASConflict
}

func (r *EtcdRepository) PutChunkGarbage(ctx context.Context, rec service.AllocationChunkGarbageRecord) error {
	if _, err := r.GetVolume(ctx, uint64(rec.VolumeID)); err != nil {
		return err
	}
	if rec.EnqueuedAtUnix == 0 {
		rec.EnqueuedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, r.chunkGarbageKey(uint64(rec.VolumeID), rec.ChunkID), string(payload))
	return err
}

func (r *EtcdRepository) ListChunkGarbage(ctx context.Context, volumeID uint64, limit int) ([]service.AllocationChunkGarbageRecord, error) {
	r.pressure.countPrefixScan()
	if _, err := r.GetVolume(ctx, volumeID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 128
	}
	resp, err := r.client.Get(ctx, r.chunkGarbagePrefix(volumeID), clientv3.WithPrefix(), clientv3.WithLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	records := make([]service.AllocationChunkGarbageRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var rec service.AllocationChunkGarbageRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].EnqueuedAtUnix == records[j].EnqueuedAtUnix {
			return records[i].ChunkID < records[j].ChunkID
		}
		return records[i].EnqueuedAtUnix < records[j].EnqueuedAtUnix
	})
	return records, nil
}

func (r *EtcdRepository) DeleteChunkGarbage(ctx context.Context, volumeID, chunkID uint64) error {
	_, err := r.client.Delete(ctx, r.chunkGarbageKey(volumeID, chunkID))
	return err
}

func (r *EtcdRepository) getGeneration(ctx context.Context, volumeID uint64) (uint64, error) {
	resp, err := r.client.Get(ctx, r.generationKey(volumeID))
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 1, nil
	}
	var gen uint64
	if err := json.Unmarshal(resp.Kvs[0].Value, &gen); err != nil {
		return 0, err
	}
	if gen == 0 {
		gen = 1
	}
	return gen, nil
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (r *EtcdRepository) volumeSpecPrefix() string { return r.rootPath + "/volumes/" }
func (r *EtcdRepository) volumeSpecKey(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/spec", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) volumeStatusKey(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/status", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) attachmentKey(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/attachments/current", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) generationKey(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/generations/current", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) hostAttachmentKey(hostID, attachmentID string) string {
	return fmt.Sprintf("%s/hosts/%s/attachments/%s", r.rootPath, hostID, attachmentID)
}
func (r *EtcdRepository) gatewayStatusKey(gatewayID string) string {
	return fmt.Sprintf("%s/gateways/%s/status", r.rootPath, gatewayID)
}
func (r *EtcdRepository) gatewayPrefix() string { return r.rootPath + "/gateways/" }
func (r *EtcdRepository) extentPageKey(volumeID, pageNo uint64) string {
	return fmt.Sprintf("%s/volumes/%s/extents/pages/%d", r.rootPath, service.CanonicalVolumeID(volumeID), pageNo)
}
func (r *EtcdRepository) extentPagePrefix(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/extents/pages/", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) chunkNextIDKey(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/chunks/next_id", r.rootPath, service.CanonicalVolumeID(volumeID))
}
func (r *EtcdRepository) chunkGarbageKey(volumeID, chunkID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/chunks/garbage/%d", r.rootPath, service.CanonicalVolumeID(volumeID), chunkID)
}
func (r *EtcdRepository) chunkGarbagePrefix(volumeID uint64) string {
	return fmt.Sprintf("%s/volumes/%s/chunks/garbage/", r.rootPath, service.CanonicalVolumeID(volumeID))
}

func generateUniqueVolumeID(existing map[uint64]service.VolumeSpec) (uint64, error) {
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
	return 0, service.ErrVolumeNameConflict
}

func parsePageNoFromKey(key, prefix string) (uint64, error) {
	raw := strings.TrimPrefix(key, prefix)
	if raw == key || raw == "" {
		return 0, fmt.Errorf("invalid extent page key %q", key)
	}
	return strconv.ParseUint(raw, 10, 64)
}
