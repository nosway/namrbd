package driver

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const (
	DefaultDriverName    = "block.namrbd.io"
	DefaultVendorVersion = "dev"
)

type Backend interface {
	CreateVolume(context.Context, *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error)
	CreateVolumeFromSnapshot(context.Context, *adminv1.CreateVolumeFromSnapshotRequest) (*adminv1.CreateVolumeFromSnapshotResponse, error)
	DeleteVolume(context.Context, *adminv1.DeleteVolumeRequest) (*adminv1.DeleteVolumeResponse, error)
	GetVolume(context.Context, *adminv1.GetVolumeRequest) (*adminv1.GetVolumeResponse, error)
	CreateSnapshot(context.Context, *adminv1.CreateSnapshotRequest) (*adminv1.CreateSnapshotResponse, error)
	GetSnapshot(context.Context, *adminv1.GetSnapshotRequest) (*adminv1.GetSnapshotResponse, error)
	ListSnapshots(context.Context, *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error)
	DeleteSnapshot(context.Context, *adminv1.DeleteSnapshotRequest) (*adminv1.DeleteSnapshotResponse, error)
	ExpandVolume(context.Context, *adminv1.ExpandVolumeRequest) (*adminv1.ExpandVolumeResponse, error)
}

type Config struct {
	DriverName    string
	VendorVersion string
	ClusterID     string
	SBSClusterID  string
	Actor         string
	Backend       Backend
	NodeID        string
	GatewayURL    string
	NamrbdctlPath string
	NodeHelper    NodeHelper
}

type Server struct {
	csipb.UnimplementedIdentityServer
	csipb.UnimplementedControllerServer
	csipb.UnimplementedNodeServer

	driverName    string
	vendorVersion string
	cluster       *adminv1.ClusterRef
	actor         string
	backend       Backend
	nodeID        string
	nodeHelper    NodeHelper
	mu            sync.Mutex
	staged        map[string]nodeStageRecord
	published     map[string]nodePublishRecord
}

func New(cfg Config) (*Server, error) {
	driverName := strings.TrimSpace(cfg.DriverName)
	if driverName == "" {
		driverName = DefaultDriverName
	}
	vendorVersion := strings.TrimSpace(cfg.VendorVersion)
	if vendorVersion == "" {
		vendorVersion = DefaultVendorVersion
	}
	if cfg.Backend == nil {
		return nil, errors.New("backend is required")
	}
	nodeID := strings.TrimSpace(cfg.NodeID)
	if nodeID == "" {
		if host, err := os.Hostname(); err == nil {
			nodeID = host
		}
	}
	if nodeID == "" {
		nodeID = "namrbd-csi-node"
	}
	actor := strings.TrimSpace(cfg.Actor)
	if actor == "" {
		actor = "namrbd-csi-driver"
	}
	nodeHelper := cfg.NodeHelper
	if nodeHelper == nil {
		nodeHelper = NewCommandNodeHelper(CommandNodeHelperConfig{
			NodeID:        nodeID,
			GatewayURL:    cfg.GatewayURL,
			NamrbdctlPath: cfg.NamrbdctlPath,
		})
	}
	return &Server{
		driverName:    driverName,
		vendorVersion: vendorVersion,
		cluster: &adminv1.ClusterRef{
			ClusterId:    strings.TrimSpace(cfg.ClusterID),
			SbsClusterId: strings.TrimSpace(cfg.SBSClusterID),
		},
		actor:      actor,
		backend:    cfg.Backend,
		nodeID:     nodeID,
		nodeHelper: nodeHelper,
		staged:     make(map[string]nodeStageRecord),
		published:  make(map[string]nodePublishRecord),
	}, nil
}

func (s *Server) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          s.driverName,
		VendorVersion: s.vendorVersion,
		Manifest: map[string]string{
			"phase": "L",
			"scope": "identity-controller",
		},
	}, nil
}

func (s *Server) GetPluginCapabilities(context.Context, *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	return &csipb.GetPluginCapabilitiesResponse{Capabilities: []*csipb.PluginCapability{
		{
			Type: &csipb.PluginCapability_Service_{
				Service: &csipb.PluginCapability_Service{Type: csipb.PluginCapability_Service_CONTROLLER_SERVICE},
			},
		},
		{
			Type: &csipb.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csipb.PluginCapability_VolumeExpansion{Type: csipb.PluginCapability_VolumeExpansion_ONLINE},
			},
		},
	}}, nil
}

func (s *Server) Probe(context.Context, *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

func (s *Server) ControllerGetCapabilities(context.Context, *csipb.ControllerGetCapabilitiesRequest) (*csipb.ControllerGetCapabilitiesResponse, error) {
	return &csipb.ControllerGetCapabilitiesResponse{Capabilities: []*csipb.ControllerServiceCapability{
		controllerCapability(csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		controllerCapability(csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
		controllerCapability(csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
		controllerCapability(csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME),
	}}, nil
}

func (s *Server) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	params, err := parseVolumeParameters(req.GetParameters())
	if err != nil {
		return nil, err
	}
	sizeBytes, err := requestedCapacityBytes(req.GetCapacityRange())
	if err != nil {
		return nil, err
	}
	volumeID := params.VolumeID
	if volumeID == "" {
		volumeID = volumeIDFromName(name)
	}

	source := req.GetVolumeContentSource()
	if source.GetVolume() != nil {
		return nil, status.Error(codes.Unimplemented, "volume clone source is not supported in L-SLICE-006")
	}
	if snapshot := source.GetSnapshot(); snapshot != nil {
		if strings.TrimSpace(snapshot.GetSnapshotId()) == "" {
			return nil, status.Error(codes.InvalidArgument, "snapshot source snapshot_id is required")
		}
		resp, err := s.backend.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
			Cluster:          s.cluster,
			Meta:             s.meta("csi-create-volume-from-snapshot"),
			SourceSnapshotId: strings.TrimSpace(snapshot.GetSnapshotId()),
			VolumeId:         volumeID,
			SizeBytes:        sizeBytes,
			IdempotencyKey:   restoreIdempotencyKey(name, snapshot.GetSnapshotId()),
		})
		if err != nil {
			return nil, mapBackendError(err)
		}
		return &csipb.CreateVolumeResponse{Volume: &csipb.Volume{
			CapacityBytes: int64(resp.GetSizeBytes()),
			VolumeId:      resp.GetVolumeId(),
			VolumeContext: volumeContext(volumeContextProvisioningSnapshot, name),
			ContentSource: source,
		}}, nil
	}

	_, err = s.backend.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		Cluster:                  s.cluster,
		Meta:                     s.meta("csi-create-volume"),
		VolumeId:                 volumeID,
		SizeBytes:                sizeBytes,
		BlockSize:                params.BlockSizeBytes,
		ReplicationFactor:        params.ReplicationFactor,
		RedundancyBackend:        params.RedundancyBackend,
		EcProfileId:              params.ECProfileID,
		TopologyMode:             params.TopologyMode,
		AllocationChunkSizeBytes: params.AllocationChunkBytes,
		AllocationPageSizeBytes:  params.AllocationPageBytes,
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return nil, mapBackendError(err)
		}
	}
	volume, err := s.backend.GetVolume(ctx, &adminv1.GetVolumeRequest{Cluster: s.cluster, VolumeId: volumeID})
	if err != nil {
		return nil, mapBackendError(err)
	}
	if existingSize := uint64(volume.GetVolume().GetSizeBytes()); existingSize != 0 && existingSize != sizeBytes {
		return nil, status.Errorf(codes.AlreadyExists, "volume %s exists with size %d, requested %d", volumeID, existingSize, sizeBytes)
	}
	return &csipb.CreateVolumeResponse{Volume: volumeFromSummary(volume.GetVolume(), volumeContext("dynamic", name), nil)}, nil
}

func (s *Server) DeleteVolume(ctx context.Context, req *csipb.DeleteVolumeRequest) (*csipb.DeleteVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if _, err := s.backend.DeleteVolume(ctx, &adminv1.DeleteVolumeRequest{
		Cluster:  s.cluster,
		Meta:     s.meta("csi-delete-volume"),
		VolumeId: volumeID,
	}); err != nil && status.Code(err) != codes.NotFound {
		return nil, mapBackendError(err)
	}
	return &csipb.DeleteVolumeResponse{}, nil
}

func (s *Server) ValidateVolumeCapabilities(ctx context.Context, req *csipb.ValidateVolumeCapabilitiesRequest) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	if _, err := s.backend.GetVolume(ctx, &adminv1.GetVolumeRequest{Cluster: s.cluster, VolumeId: volumeID}); err != nil {
		return nil, mapBackendError(err)
	}
	return &csipb.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csipb.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
			VolumeContext:      req.GetVolumeContext(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (s *Server) CreateSnapshot(ctx context.Context, req *csipb.CreateSnapshotRequest) (*csipb.CreateSnapshotResponse, error) {
	name := strings.TrimSpace(req.GetName())
	sourceVolumeID := strings.TrimSpace(req.GetSourceVolumeId())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_volume_id is required")
	}
	if err := rejectUnknownSnapshotParameters(req.GetParameters()); err != nil {
		return nil, err
	}
	resp, err := s.backend.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        s.cluster,
		Meta:           s.meta("csi-create-snapshot"),
		SourceVolumeId: sourceVolumeID,
		IdempotencyKey: snapshotIdempotencyKey(name),
	})
	if err != nil {
		return nil, mapBackendError(err)
	}
	summary, err := s.backend.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{Cluster: s.cluster, SnapshotId: resp.GetSnapshotId()})
	if err != nil {
		return nil, mapBackendError(err)
	}
	return &csipb.CreateSnapshotResponse{Snapshot: snapshotFromSummary(summary.GetSnapshot())}, nil
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *csipb.DeleteSnapshotRequest) (*csipb.DeleteSnapshotResponse, error) {
	snapshotID := strings.TrimSpace(req.GetSnapshotId())
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	if _, err := s.backend.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		Cluster:    s.cluster,
		Meta:       s.meta("csi-delete-snapshot"),
		SnapshotId: snapshotID,
	}); err != nil && status.Code(err) != codes.NotFound {
		return nil, mapBackendError(err)
	}
	return &csipb.DeleteSnapshotResponse{}, nil
}

func (s *Server) ListSnapshots(ctx context.Context, req *csipb.ListSnapshotsRequest) (*csipb.ListSnapshotsResponse, error) {
	var snapshots []*adminv1.SnapshotSummary
	if strings.TrimSpace(req.GetSnapshotId()) != "" {
		resp, err := s.backend.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{Cluster: s.cluster, SnapshotId: strings.TrimSpace(req.GetSnapshotId())})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return &csipb.ListSnapshotsResponse{}, nil
			}
			return nil, mapBackendError(err)
		}
		snapshots = []*adminv1.SnapshotSummary{resp.GetSnapshot()}
	} else {
		resp, err := s.backend.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
			Cluster:        s.cluster,
			SourceVolumeId: strings.TrimSpace(req.GetSourceVolumeId()),
		})
		if err != nil {
			return nil, mapBackendError(err)
		}
		snapshots = resp.GetSnapshots()
	}
	start := 0
	if token := strings.TrimSpace(req.GetStartingToken()); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil || parsed < 0 || parsed > len(snapshots) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid starting_token %q", token)
		}
		start = parsed
	}
	maxEntries := int(req.GetMaxEntries())
	end := len(snapshots)
	if maxEntries > 0 && start+maxEntries < end {
		end = start + maxEntries
	}
	out := &csipb.ListSnapshotsResponse{Entries: make([]*csipb.ListSnapshotsResponse_Entry, 0, end-start)}
	for _, snapshot := range snapshots[start:end] {
		out.Entries = append(out.Entries, &csipb.ListSnapshotsResponse_Entry{Snapshot: snapshotFromSummary(snapshot)})
	}
	if end < len(snapshots) {
		out.NextToken = strconv.Itoa(end)
	}
	return out, nil
}

func (s *Server) GetSnapshot(ctx context.Context, req *csipb.GetSnapshotRequest) (*csipb.GetSnapshotResponse, error) {
	snapshotID := strings.TrimSpace(req.GetSnapshotId())
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	resp, err := s.backend.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{Cluster: s.cluster, SnapshotId: snapshotID})
	if err != nil {
		return nil, mapBackendError(err)
	}
	return &csipb.GetSnapshotResponse{Snapshot: snapshotFromSummary(resp.GetSnapshot())}, nil
}

func (s *Server) ControllerExpandVolume(ctx context.Context, req *csipb.ControllerExpandVolumeRequest) (*csipb.ControllerExpandVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	targetBytes, err := requestedCapacityBytes(req.GetCapacityRange())
	if err != nil {
		return nil, err
	}
	resp, err := s.backend.ExpandVolume(ctx, &adminv1.ExpandVolumeRequest{
		Cluster:         s.cluster,
		Meta:            s.meta("csi-expand-volume"),
		VolumeId:        volumeID,
		TargetSizeBytes: targetBytes,
		IdempotencyKey:  expandIdempotencyKey(volumeID, targetBytes),
	})
	if err != nil {
		return nil, mapBackendError(err)
	}
	return &csipb.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(resp.GetSizeBytes()),
		NodeExpansionRequired: req.GetVolumeCapability().GetMount() != nil,
	}, nil
}

func controllerCapability(kind csipb.ControllerServiceCapability_RPC_Type) *csipb.ControllerServiceCapability {
	return &csipb.ControllerServiceCapability{
		Type: &csipb.ControllerServiceCapability_Rpc{
			Rpc: &csipb.ControllerServiceCapability_RPC{Type: kind},
		},
	}
}

func (s *Server) meta(reason string) *adminv1.RequestMeta {
	return &adminv1.RequestMeta{Actor: s.actor, Reason: reason}
}

func volumeContext(kind, csiName string) map[string]string {
	return map[string]string{
		volumeContextProvisioningKindKey: kind,
		"namrbd.io/csi-name":             csiName,
	}
}

func volumeFromSummary(summary *adminv1.VolumeSummary, ctx map[string]string, source *csipb.VolumeContentSource) *csipb.Volume {
	if summary == nil {
		return &csipb.Volume{VolumeContext: ctx, ContentSource: source}
	}
	return &csipb.Volume{
		CapacityBytes: int64(summary.GetSizeBytes()),
		VolumeId:      summary.GetVolumeId(),
		VolumeContext: ctx,
		ContentSource: source,
	}
}

func snapshotFromSummary(summary *adminv1.SnapshotSummary) *csipb.Snapshot {
	if summary == nil {
		return &csipb.Snapshot{}
	}
	return &csipb.Snapshot{
		SizeBytes:      int64(summary.GetSourceSizeBytes()),
		SnapshotId:     summary.GetSnapshotId(),
		SourceVolumeId: summary.GetSourceVolumeId(),
		CreationTime:   summary.GetCreatedAt(),
		ReadyToUse:     summary.GetState() == adminv1.SnapshotState_SNAPSHOT_STATE_AVAILABLE,
	}
}

func volumeIDFromName(name string) string {
	sum := crc32.ChecksumIEEE([]byte("csi-volume:" + strings.TrimSpace(name)))
	if sum == 0 {
		sum = 1
	}
	return fmt.Sprintf("%08x", sum)
}

func snapshotIdempotencyKey(name string) string {
	return "csi-create-snapshot:" + strings.TrimSpace(name)
}

func restoreIdempotencyKey(name, snapshotID string) string {
	return "csi-restore:" + strings.TrimSpace(name) + ":" + strings.TrimSpace(snapshotID)
}

func expandIdempotencyKey(volumeID string, sizeBytes uint64) string {
	return fmt.Sprintf("csi-expand:%s:%d", strings.TrimSpace(volumeID), sizeBytes)
}

func mapBackendError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
