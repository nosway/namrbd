package driver

import (
	"context"
	"errors"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

type fakeBackend struct {
	createVolumeReq             *adminv1.CreateVolumeRequest
	createVolumeFromSnapshotReq *adminv1.CreateVolumeFromSnapshotRequest
	deleteVolumeReq             *adminv1.DeleteVolumeRequest
	createSnapshotReq           *adminv1.CreateSnapshotRequest
	deleteSnapshotReq           *adminv1.DeleteSnapshotRequest
	expandVolumeReq             *adminv1.ExpandVolumeRequest
	volumes                     map[string]*adminv1.VolumeSummary
	snapshots                   map[string]*adminv1.SnapshotSummary
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		volumes:   make(map[string]*adminv1.VolumeSummary),
		snapshots: make(map[string]*adminv1.SnapshotSummary),
	}
}

func (f *fakeBackend) CreateVolume(_ context.Context, req *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
	f.createVolumeReq = req
	if _, ok := f.volumes[req.GetVolumeId()]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "volume exists")
	}
	f.volumes[req.GetVolumeId()] = &adminv1.VolumeSummary{VolumeId: req.GetVolumeId(), SizeBytes: req.GetSizeBytes(), BlockSize: req.GetBlockSize()}
	return &adminv1.CreateVolumeResponse{Operation: &adminv1.OperationHandle{Accepted: true}}, nil
}

func (f *fakeBackend) CreateVolumeFromSnapshot(_ context.Context, req *adminv1.CreateVolumeFromSnapshotRequest) (*adminv1.CreateVolumeFromSnapshotResponse, error) {
	f.createVolumeFromSnapshotReq = req
	f.volumes[req.GetVolumeId()] = &adminv1.VolumeSummary{VolumeId: req.GetVolumeId(), SizeBytes: req.GetSizeBytes(), BlockSize: 4096}
	return &adminv1.CreateVolumeFromSnapshotResponse{
		Operation:        &adminv1.OperationHandle{Accepted: true},
		VolumeId:         req.GetVolumeId(),
		SourceSnapshotId: req.GetSourceSnapshotId(),
		CloneId:          "clone-1",
		SizeBytes:        req.GetSizeBytes(),
	}, nil
}

func (f *fakeBackend) DeleteVolume(_ context.Context, req *adminv1.DeleteVolumeRequest) (*adminv1.DeleteVolumeResponse, error) {
	f.deleteVolumeReq = req
	delete(f.volumes, req.GetVolumeId())
	return &adminv1.DeleteVolumeResponse{}, nil
}

func (f *fakeBackend) GetVolume(_ context.Context, req *adminv1.GetVolumeRequest) (*adminv1.GetVolumeResponse, error) {
	volume, ok := f.volumes[req.GetVolumeId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "volume not found")
	}
	return &adminv1.GetVolumeResponse{Volume: volume}, nil
}

func (f *fakeBackend) ListVolumes(context.Context, *adminv1.ListVolumesRequest) (*adminv1.ListVolumesResponse, error) {
	out := &adminv1.ListVolumesResponse{}
	for _, volume := range f.volumes {
		out.Volumes = append(out.Volumes, volume)
	}
	return out, nil
}

func (f *fakeBackend) CreateSnapshot(_ context.Context, req *adminv1.CreateSnapshotRequest) (*adminv1.CreateSnapshotResponse, error) {
	f.createSnapshotReq = req
	snapshotID := "snap-" + req.GetSourceVolumeId() + "-fixture"
	f.snapshots[snapshotID] = &adminv1.SnapshotSummary{
		SnapshotId:      snapshotID,
		SourceVolumeId:  req.GetSourceVolumeId(),
		State:           adminv1.SnapshotState_SNAPSHOT_STATE_AVAILABLE,
		CreatedAt:       timestamppb.Now(),
		SourceSizeBytes: 8 << 20,
	}
	return &adminv1.CreateSnapshotResponse{SnapshotId: snapshotID, SnapshotRootId: snapshotID}, nil
}

func (f *fakeBackend) GetSnapshot(_ context.Context, req *adminv1.GetSnapshotRequest) (*adminv1.GetSnapshotResponse, error) {
	snapshot, ok := f.snapshots[req.GetSnapshotId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "snapshot not found")
	}
	return &adminv1.GetSnapshotResponse{Snapshot: snapshot}, nil
}

func (f *fakeBackend) ListSnapshots(_ context.Context, req *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error) {
	out := &adminv1.ListSnapshotsResponse{}
	for _, snapshot := range f.snapshots {
		if req.GetSourceVolumeId() == "" || snapshot.GetSourceVolumeId() == req.GetSourceVolumeId() {
			out.Snapshots = append(out.Snapshots, snapshot)
		}
	}
	return out, nil
}

func (f *fakeBackend) DeleteSnapshot(_ context.Context, req *adminv1.DeleteSnapshotRequest) (*adminv1.DeleteSnapshotResponse, error) {
	f.deleteSnapshotReq = req
	delete(f.snapshots, req.GetSnapshotId())
	return &adminv1.DeleteSnapshotResponse{}, nil
}

func (f *fakeBackend) ExpandVolume(_ context.Context, req *adminv1.ExpandVolumeRequest) (*adminv1.ExpandVolumeResponse, error) {
	f.expandVolumeReq = req
	if volume, ok := f.volumes[req.GetVolumeId()]; ok {
		volume.SizeBytes = req.GetTargetSizeBytes()
	}
	return &adminv1.ExpandVolumeResponse{VolumeId: req.GetVolumeId(), SizeBytes: req.GetTargetSizeBytes()}, nil
}

func newTestServer(t *testing.T, backend *fakeBackend) *Server {
	t.Helper()
	srv, err := New(Config{
		ClusterID:    "test-cluster",
		SBSClusterID: "test-sbs",
		Backend:      backend,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

func blockRWO() *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Block{Block: &csipb.VolumeCapability_BlockVolume{}},
		AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func mountRWOP() *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Mount{Mount: &csipb.VolumeCapability_MountVolume{FsType: "ext4"}},
		AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
	}
}

func TestIdentityAndControllerCapabilities(t *testing.T) {
	srv := newTestServer(t, newFakeBackend())
	info, err := srv.GetPluginInfo(context.Background(), &csipb.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo: %v", err)
	}
	if info.GetName() != DefaultDriverName || info.GetVendorVersion() == "" {
		t.Fatalf("plugin info=%+v", info)
	}
	pluginCaps, err := srv.GetPluginCapabilities(context.Background(), &csipb.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities: %v", err)
	}
	if len(pluginCaps.GetCapabilities()) != 2 {
		t.Fatalf("plugin capabilities=%+v", pluginCaps.GetCapabilities())
	}
	controllerCaps, err := srv.ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities: %v", err)
	}
	got := map[csipb.ControllerServiceCapability_RPC_Type]bool{}
	for _, cap := range controllerCaps.GetCapabilities() {
		got[cap.GetRpc().GetType()] = true
	}
	for _, want := range []csipb.ControllerServiceCapability_RPC_Type{
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csipb.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csipb.ControllerServiceCapability_RPC_GET_SNAPSHOT,
		csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	} {
		if !got[want] {
			t.Fatalf("missing controller capability %v in %+v", want, got)
		}
	}
	if got[csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME] {
		t.Fatalf("publish capability must not be advertised before L-SLICE-007")
	}
}

func TestCreateVolumeMapsParametersAndIdempotentName(t *testing.T) {
	backend := newFakeBackend()
	srv := newTestServer(t, backend)
	resp, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "pvc-alpha",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 16 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
		Parameters: map[string]string{
			"redundancy_backend":    "replicated",
			"replication_factor":    "3",
			"topology_mode":         "prefer",
			"block_size":            "4K",
			"allocation_chunk_size": "128K",
			"allocation_page_size":  "4M",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	wantVolumeID := volumeIDFromName("pvc-alpha")
	if resp.GetVolume().GetVolumeId() != wantVolumeID {
		t.Fatalf("volume_id=%q want %q", resp.GetVolume().GetVolumeId(), wantVolumeID)
	}
	req := backend.createVolumeReq
	if req.GetVolumeId() != wantVolumeID || req.GetSizeBytes() != 16<<20 || req.GetRedundancyBackend() != "replicated" || req.GetReplicationFactor() != 3 || req.GetTopologyMode() != "prefer" {
		t.Fatalf("backend CreateVolume req=%+v", req)
	}
	if req.GetBlockSize() != 4096 || req.GetAllocationChunkSizeBytes() != 128<<10 || req.GetAllocationPageSizeBytes() != 4<<20 {
		t.Fatalf("backend geometry req=%+v", req)
	}
}

func TestCreateVolumeNormalizesReplicatedSpreadAlias(t *testing.T) {
	backend := newFakeBackend()
	srv := newTestServer(t, backend)
	_, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "replicated-pvc",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 8 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
		Parameters: map[string]string{
			"redundancy_backend": "replicated",
			"replication_factor": "3",
			"topology_mode":      "spread",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	req := backend.createVolumeReq
	if req.GetRedundancyBackend() != "replicated" || req.GetReplicationFactor() != 3 || req.GetTopologyMode() != "prefer" {
		t.Fatalf("backend CreateVolume req=%+v", req)
	}
}

func TestCreateVolumeFromSnapshotUsesRestoreWrapper(t *testing.T) {
	backend := newFakeBackend()
	backend.snapshots["snap-123"] = &adminv1.SnapshotSummary{SnapshotId: "snap-123", SourceVolumeId: "source", SourceSizeBytes: 16 << 20}
	srv := newTestServer(t, backend)
	source := &csipb.VolumeContentSource{
		Type: &csipb.VolumeContentSource_Snapshot{
			Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: "snap-123"},
		},
	}
	resp, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:                "restore-pvc",
		CapacityRange:       &csipb.CapacityRange{RequiredBytes: 32 << 20},
		VolumeCapabilities:  []*csipb.VolumeCapability{blockRWO()},
		VolumeContentSource: source,
		Parameters:          map[string]string{"volume_id": "00a1b2c4"},
	})
	if err != nil {
		t.Fatalf("CreateVolume restore: %v", err)
	}
	if resp.GetVolume().GetVolumeId() != "00a1b2c4" || resp.GetVolume().GetContentSource() == nil {
		t.Fatalf("restore response=%+v", resp.GetVolume())
	}
	req := backend.createVolumeFromSnapshotReq
	if req.GetSourceSnapshotId() != "snap-123" || req.GetVolumeId() != "00a1b2c4" || req.GetSizeBytes() != 32<<20 {
		t.Fatalf("restore req=%+v", req)
	}
	if req.GetIdempotencyKey() != "csi-restore:restore-pvc:snap-123" {
		t.Fatalf("restore idempotency=%q", req.GetIdempotencyKey())
	}
}

func TestCreateVolumeFromSnapshotRejectsSmallerAndAcceptsEqual(t *testing.T) {
	backend := newFakeBackend()
	backend.snapshots["snap-source"] = &adminv1.SnapshotSummary{SnapshotId: "snap-source", SourceVolumeId: "source", SourceSizeBytes: 8 << 20}
	srv := newTestServer(t, backend)
	request := func(name string, size int64) *csipb.CreateVolumeRequest {
		return &csipb.CreateVolumeRequest{
			Name:               name,
			CapacityRange:      &csipb.CapacityRange{RequiredBytes: size},
			VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
			VolumeContentSource: &csipb.VolumeContentSource{Type: &csipb.VolumeContentSource_Snapshot{
				Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: "snap-source"},
			}},
		}
	}
	if _, err := srv.CreateVolume(context.Background(), request("too-small", 4<<20)); status.Code(err) != codes.OutOfRange {
		t.Fatalf("smaller restore err=%v want OutOfRange", err)
	}
	if backend.createVolumeFromSnapshotReq != nil {
		t.Fatalf("smaller restore reached backend: %+v", backend.createVolumeFromSnapshotReq)
	}
	resp, err := srv.CreateVolume(context.Background(), request("equal", 8<<20))
	if err != nil {
		t.Fatalf("equal restore: %v", err)
	}
	if resp.GetVolume().GetCapacityBytes() != 8<<20 {
		t.Fatalf("equal restore response=%+v", resp.GetVolume())
	}
}

func TestCreateVolumeRejectsTopologyRequirements(t *testing.T) {
	srv := newTestServer(t, newFakeBackend())
	_, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "topology-pvc",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 8 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
		AccessibilityRequirements: &csipb.TopologyRequirement{
			Requisite: []*csipb.Topology{{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-a"}}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("topology requirement err=%v want InvalidArgument", err)
	}
}

func TestListVolumesUsesStablePaginationAndAbortedToken(t *testing.T) {
	backend := newFakeBackend()
	for _, id := range []string{"vol-c", "vol-a", "vol-b"} {
		backend.volumes[id] = &adminv1.VolumeSummary{VolumeId: id, SizeBytes: 8 << 20}
	}
	srv := newTestServer(t, backend)
	first, err := srv.ListVolumes(context.Background(), &csipb.ListVolumesRequest{MaxEntries: 2})
	if err != nil {
		t.Fatalf("ListVolumes first: %v", err)
	}
	if len(first.GetEntries()) != 2 || first.GetEntries()[0].GetVolume().GetVolumeId() != "vol-a" || first.GetEntries()[1].GetVolume().GetVolumeId() != "vol-b" || first.GetNextToken() != "2" {
		t.Fatalf("ListVolumes first=%+v", first)
	}
	second, err := srv.ListVolumes(context.Background(), &csipb.ListVolumesRequest{StartingToken: first.GetNextToken(), MaxEntries: 2})
	if err != nil {
		t.Fatalf("ListVolumes second: %v", err)
	}
	if len(second.GetEntries()) != 1 || second.GetEntries()[0].GetVolume().GetVolumeId() != "vol-c" || second.GetNextToken() != "" {
		t.Fatalf("ListVolumes second=%+v", second)
	}
	if _, err := srv.ListVolumes(context.Background(), &csipb.ListVolumesRequest{StartingToken: "bad"}); status.Code(err) != codes.Aborted {
		t.Fatalf("invalid ListVolumes token err=%v want Aborted", err)
	}
	if _, err := srv.ListVolumes(context.Background(), &csipb.ListVolumesRequest{MaxEntries: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative ListVolumes max_entries err=%v want InvalidArgument", err)
	}
}

func TestSnapshotListDeleteAndExpand(t *testing.T) {
	backend := newFakeBackend()
	backend.volumes["00a1b2c3"] = &adminv1.VolumeSummary{VolumeId: "00a1b2c3", SizeBytes: 8 << 20}
	srv := newTestServer(t, backend)
	snapResp, err := srv.CreateSnapshot(context.Background(), &csipb.CreateSnapshotRequest{
		Name:           "snap-alpha",
		SourceVolumeId: "00a1b2c3",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snapResp.GetSnapshot().GetSnapshotId() == "" || !snapResp.GetSnapshot().GetReadyToUse() {
		t.Fatalf("snapshot response=%+v", snapResp.GetSnapshot())
	}
	if backend.createSnapshotReq.GetIdempotencyKey() != "csi-create-snapshot:snap-alpha" {
		t.Fatalf("snapshot idempotency=%q", backend.createSnapshotReq.GetIdempotencyKey())
	}
	listResp, err := srv.ListSnapshots(context.Background(), &csipb.ListSnapshotsRequest{SourceVolumeId: "00a1b2c3"})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(listResp.GetEntries()) != 1 || listResp.GetEntries()[0].GetSnapshot().GetSnapshotId() != snapResp.GetSnapshot().GetSnapshotId() {
		t.Fatalf("snapshot list=%+v", listResp.GetEntries())
	}
	expandResp, err := srv.ControllerExpandVolume(context.Background(), &csipb.ControllerExpandVolumeRequest{
		VolumeId:         "00a1b2c3",
		CapacityRange:    &csipb.CapacityRange{RequiredBytes: 12 << 20},
		VolumeCapability: mountRWOP(),
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if expandResp.GetCapacityBytes() != 12<<20 || !expandResp.GetNodeExpansionRequired() {
		t.Fatalf("expand response=%+v", expandResp)
	}
	if backend.expandVolumeReq.GetIdempotencyKey() != "csi-expand:00a1b2c3:12582912" {
		t.Fatalf("expand idempotency=%q", backend.expandVolumeReq.GetIdempotencyKey())
	}
	if _, err := srv.DeleteSnapshot(context.Background(), &csipb.DeleteSnapshotRequest{SnapshotId: snapResp.GetSnapshot().GetSnapshotId()}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if backend.deleteSnapshotReq.GetSnapshotId() != snapResp.GetSnapshot().GetSnapshotId() {
		t.Fatalf("delete snapshot req=%+v", backend.deleteSnapshotReq)
	}
}

func TestControllerExpandVolumeReplayDoesNotReachBackend(t *testing.T) {
	backend := newFakeBackend()
	backend.volumes["00a1b2c3"] = &adminv1.VolumeSummary{VolumeId: "00a1b2c3", SizeBytes: 16 << 20}
	srv := newTestServer(t, backend)
	resp, err := srv.ControllerExpandVolume(context.Background(), &csipb.ControllerExpandVolumeRequest{
		VolumeId:      "00a1b2c3",
		CapacityRange: &csipb.CapacityRange{RequiredBytes: 12 << 20, LimitBytes: 20 << 20},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume replay: %v", err)
	}
	if resp.GetCapacityBytes() != 16<<20 || backend.expandVolumeReq != nil {
		t.Fatalf("replay response=%+v backend=%+v", resp, backend.expandVolumeReq)
	}
	if _, err := srv.ControllerExpandVolume(context.Background(), &csipb.ControllerExpandVolumeRequest{
		VolumeId:      "00a1b2c3",
		CapacityRange: &csipb.CapacityRange{RequiredBytes: 12 << 20, LimitBytes: 14 << 20},
	}); status.Code(err) != codes.OutOfRange {
		t.Fatalf("incompatible limit err=%v want OutOfRange", err)
	}
}

func TestControllerReplaySurvivesServerRestart(t *testing.T) {
	backend := newFakeBackend()
	first := newTestServer(t, backend)
	createReq := &csipb.CreateVolumeRequest{Name: "restart-pvc", CapacityRange: &csipb.CapacityRange{RequiredBytes: 8 << 20}, VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()}}
	created, err := first.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatalf("first CreateVolume: %v", err)
	}
	second := newTestServer(t, backend)
	replayed, err := second.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatalf("replayed CreateVolume: %v", err)
	}
	if replayed.GetVolume().GetVolumeId() != created.GetVolume().GetVolumeId() {
		t.Fatalf("replayed volume=%+v created=%+v", replayed.GetVolume(), created.GetVolume())
	}
	snapshotReq := &csipb.CreateSnapshotRequest{Name: "restart-snapshot", SourceVolumeId: created.GetVolume().GetVolumeId()}
	snapshot, err := first.CreateSnapshot(context.Background(), snapshotReq)
	if err != nil {
		t.Fatalf("first CreateSnapshot: %v", err)
	}
	replayedSnapshot, err := second.CreateSnapshot(context.Background(), snapshotReq)
	if err != nil {
		t.Fatalf("replayed CreateSnapshot: %v", err)
	}
	if replayedSnapshot.GetSnapshot().GetSnapshotId() != snapshot.GetSnapshot().GetSnapshotId() {
		t.Fatalf("replayed snapshot=%+v created=%+v", replayedSnapshot.GetSnapshot(), snapshot.GetSnapshot())
	}
	if _, err := second.DeleteSnapshot(context.Background(), &csipb.DeleteSnapshotRequest{SnapshotId: snapshot.GetSnapshot().GetSnapshotId()}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if _, err := second.DeleteSnapshot(context.Background(), &csipb.DeleteSnapshotRequest{SnapshotId: snapshot.GetSnapshot().GetSnapshotId()}); err != nil {
		t.Fatalf("DeleteSnapshot replay: %v", err)
	}
	if _, err := second.DeleteVolume(context.Background(), &csipb.DeleteVolumeRequest{VolumeId: created.GetVolume().GetVolumeId()}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if _, err := second.DeleteVolume(context.Background(), &csipb.DeleteVolumeRequest{VolumeId: created.GetVolume().GetVolumeId()}); err != nil {
		t.Fatalf("DeleteVolume replay: %v", err)
	}
}

func TestMapBackendErrorPreservesContextStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{status.Error(codes.Unavailable, "leader transition"), codes.Unavailable},
		{errors.New("backend failed"), codes.Internal},
	} {
		if got := status.Code(mapBackendError(tc.err)); got != tc.code {
			t.Fatalf("mapBackendError(%v)=%v want %v", tc.err, got, tc.code)
		}
	}
}

func TestRejectsUnsupportedParametersAndAccessModes(t *testing.T) {
	srv := newTestServer(t, newFakeBackend())
	if _, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "bad-param",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
		Parameters:         map[string]string{"unknown": "true"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown parameter err=%v want InvalidArgument", err)
	}
	if _, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "bad-ec-topology",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{blockRWO()},
		Parameters: map[string]string{
			"redundancy_backend": "ec",
			"ec_profile":         "ec-6-3",
			"topology_mode":      "prefer",
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported ec topology err=%v want InvalidArgument", err)
	}
	if _, err := srv.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:          "bad-mode",
		CapacityRange: &csipb.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csipb.VolumeCapability{{
			AccessType: &csipb.VolumeCapability_Block{Block: &csipb.VolumeCapability_BlockVolume{}},
			AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported access mode err=%v want InvalidArgument", err)
	}
}
