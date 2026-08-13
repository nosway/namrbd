package driver

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	nodeAttachmentGenerationKey       = "namrbd.io/attachment-generation"
	volumeContextProvisioningKindKey  = "namrbd.io/provisioning-kind"
	volumeContextProvisioningSnapshot = "snapshot"
)

type NodeHelper interface {
	Attach(context.Context, NodeAttachRequest) (NodeAttachment, error)
	LookupAttachment(context.Context, NodeLookupAttachmentRequest) (NodeAttachment, error)
	Detach(context.Context, NodeDetachRequest) error
	FormatIfNeeded(context.Context, NodeFormatRequest) error
	Mount(context.Context, NodeMountRequest) error
	BindBlock(context.Context, NodeBindBlockRequest) error
	Unmount(context.Context, string) error
	ReloadSize(context.Context, NodeReloadSizeRequest) error
	GrowFilesystem(context.Context, NodeGrowFilesystemRequest) error
}

type NodeAttachRequest struct {
	VolumeID string
	NodeID   string
	Readonly bool
}

type NodeAttachment struct {
	DevicePath   string
	DeviceID     uint32
	AttachmentID string
	Generation   uint64
}

type NodeLookupAttachmentRequest struct {
	VolumeID string
}

type NodeDetachRequest struct {
	VolumeID     string
	DeviceID     uint32
	AttachmentID string
	Generation   uint64
}

type NodeFormatRequest struct {
	DevicePath string
	FSType     string
}

type NodeMountRequest struct {
	Source   string
	Target   string
	FSType   string
	Flags    []string
	Readonly bool
	Bind     bool
}

type NodeBindBlockRequest struct {
	DevicePath string
	TargetPath string
	Readonly   bool
}

type NodeReloadSizeRequest struct {
	VolumeID   string
	DevicePath string
	DeviceID   uint32
}

type NodeGrowFilesystemRequest struct {
	DevicePath string
	VolumePath string
	FSType     string
}

type nodeStageRecord struct {
	VolumeID          string
	StagingTargetPath string
	Mode              string
	FSType            string
	DevicePath        string
	DeviceID          uint32
	AttachmentID      string
	Generation        uint64
}

type nodePublishRecord struct {
	VolumeID          string
	StagingTargetPath string
	TargetPath        string
	Mode              string
	Readonly          bool
	Generation        uint64
}

var (
	errNodeAttachmentNotFound  = errors.New("node attachment not found")
	errNodeAttachmentAmbiguous = errors.New("node attachment ambiguous")
)

func (s *Server) NodeGetInfo(context.Context, *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	return &csipb.NodeGetInfoResponse{NodeId: s.nodeID}, nil
}

func (s *Server) NodeGetCapabilities(context.Context, *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{Capabilities: []*csipb.NodeServiceCapability{
		nodeCapability(csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		nodeCapability(csipb.NodeServiceCapability_RPC_EXPAND_VOLUME),
	}}, nil
}

func (s *Server) NodeStageVolume(ctx context.Context, req *csipb.NodeStageVolumeRequest) (*csipb.NodeStageVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	stagingTarget := strings.TrimSpace(req.GetStagingTargetPath())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := validateAbsolutePath(stagingTarget, "staging_target_path"); err != nil {
		return nil, err
	}
	cap, err := validateSingleVolumeCapability(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	mode := volumeMode(cap)
	fsType, err := filesystemType(cap)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if staged, ok := s.staged[volumeID]; ok {
		s.mu.Unlock()
		if staged.StagingTargetPath == stagingTarget && staged.Mode == mode && staged.FSType == fsType {
			return &csipb.NodeStageVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "volume %s is already staged at %s", volumeID, staged.StagingTargetPath)
	}
	for _, staged := range s.staged {
		if staged.StagingTargetPath == stagingTarget {
			s.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "staging_target_path %s is already used by volume %s", stagingTarget, staged.VolumeID)
		}
	}
	s.mu.Unlock()

	attachment, err := s.nodeStageAttachment(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(attachment.DevicePath) == "" {
		return nil, status.Error(codes.Internal, "node helper attach returned empty device path")
	}
	if strings.TrimSpace(attachment.AttachmentID) == "" {
		return nil, status.Error(codes.Internal, "node helper attach returned empty attachment id")
	}
	if attachment.Generation == 0 {
		return nil, status.Error(codes.Internal, "node helper attach returned zero generation")
	}
	if mode == "filesystem" {
		if err := s.nodeHelper.FormatIfNeeded(ctx, NodeFormatRequest{DevicePath: attachment.DevicePath, FSType: fsType}); err != nil {
			_ = s.nodeHelper.Detach(ctx, NodeDetachRequest{
				VolumeID:     volumeID,
				DeviceID:     attachment.DeviceID,
				AttachmentID: attachment.AttachmentID,
				Generation:   attachment.Generation,
			})
			return nil, mapBackendError(err)
		}
		if err := s.nodeHelper.Mount(ctx, NodeMountRequest{
			Source: attachment.DevicePath,
			Target: stagingTarget,
			FSType: fsType,
			Flags:  cap.GetMount().GetMountFlags(),
		}); err != nil {
			_ = s.nodeHelper.Detach(ctx, NodeDetachRequest{
				VolumeID:     volumeID,
				DeviceID:     attachment.DeviceID,
				AttachmentID: attachment.AttachmentID,
				Generation:   attachment.Generation,
			})
			return nil, mapBackendError(err)
		}
		if shouldGrowFilesystemOnStage(req.GetVolumeContext()) {
			if err := s.nodeHelper.ReloadSize(ctx, NodeReloadSizeRequest{
				VolumeID:   volumeID,
				DevicePath: attachment.DevicePath,
				DeviceID:   attachment.DeviceID,
			}); err != nil {
				_ = s.nodeHelper.Unmount(ctx, stagingTarget)
				_ = s.nodeHelper.Detach(ctx, NodeDetachRequest{
					VolumeID:     volumeID,
					DeviceID:     attachment.DeviceID,
					AttachmentID: attachment.AttachmentID,
					Generation:   attachment.Generation,
				})
				return nil, mapBackendError(err)
			}
			if err := s.nodeHelper.GrowFilesystem(ctx, NodeGrowFilesystemRequest{
				DevicePath: attachment.DevicePath,
				VolumePath: stagingTarget,
				FSType:     fsType,
			}); err != nil {
				_ = s.nodeHelper.Unmount(ctx, stagingTarget)
				_ = s.nodeHelper.Detach(ctx, NodeDetachRequest{
					VolumeID:     volumeID,
					DeviceID:     attachment.DeviceID,
					AttachmentID: attachment.AttachmentID,
					Generation:   attachment.Generation,
				})
				return nil, mapBackendError(err)
			}
		}
	}

	s.mu.Lock()
	s.staged[volumeID] = nodeStageRecord{
		VolumeID:          volumeID,
		StagingTargetPath: stagingTarget,
		Mode:              mode,
		FSType:            fsType,
		DevicePath:        attachment.DevicePath,
		DeviceID:          attachment.DeviceID,
		AttachmentID:      attachment.AttachmentID,
		Generation:        attachment.Generation,
	}
	s.mu.Unlock()
	return &csipb.NodeStageVolumeResponse{}, nil
}

func (s *Server) nodeStageAttachment(ctx context.Context, volumeID string) (NodeAttachment, error) {
	attachment, err := s.nodeHelper.LookupAttachment(ctx, NodeLookupAttachmentRequest{VolumeID: volumeID})
	if err == nil {
		return normalizeRecoveredAttachment(volumeID, attachment), nil
	}
	switch {
	case isNodeAttachmentNotFound(err):
		attachment, err = s.nodeHelper.Attach(ctx, NodeAttachRequest{
			VolumeID: volumeID,
			NodeID:   s.nodeID,
			Readonly: false,
		})
		if err != nil {
			return NodeAttachment{}, mapBackendError(err)
		}
		return attachment, nil
	case errors.Is(err, errNodeAttachmentAmbiguous):
		return NodeAttachment{}, status.Errorf(codes.FailedPrecondition, "volume %s has ambiguous local attachments", volumeID)
	default:
		return NodeAttachment{}, mapBackendError(err)
	}
}

func normalizeRecoveredAttachment(volumeID string, attachment NodeAttachment) NodeAttachment {
	if strings.TrimSpace(attachment.AttachmentID) == "" {
		attachment.AttachmentID = fmt.Sprintf("recovered-local:%s:%d", strings.TrimSpace(volumeID), attachment.DeviceID)
	}
	return attachment
}

func (s *Server) NodePublishVolume(ctx context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	stagingTarget := strings.TrimSpace(req.GetStagingTargetPath())
	targetPath := strings.TrimSpace(req.GetTargetPath())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := validateAbsolutePath(stagingTarget, "staging_target_path"); err != nil {
		return nil, err
	}
	if err := validateAbsolutePath(targetPath, "target_path"); err != nil {
		return nil, err
	}
	cap, err := validateSingleVolumeCapability(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	mode := volumeMode(cap)

	s.mu.Lock()
	staged, ok := s.staged[volumeID]
	if !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not staged", volumeID)
	}
	if staged.StagingTargetPath != stagingTarget {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "staging_target_path mismatch for volume %s", volumeID)
	}
	if staged.Mode != mode {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "volume mode mismatch for volume %s", volumeID)
	}
	if generation, ok := requestedAttachmentGeneration(req.GetPublishContext(), req.GetVolumeContext()); ok && generation != staged.Generation {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "stale attachment generation requested=%d staged=%d", generation, staged.Generation)
	}
	if published, ok := s.published[targetPath]; ok {
		s.mu.Unlock()
		if published.VolumeID == volumeID &&
			published.StagingTargetPath == stagingTarget &&
			published.Mode == mode &&
			published.Readonly == req.GetReadonly() &&
			published.Generation == staged.Generation {
			return &csipb.NodePublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.AlreadyExists, "target_path %s is already published", targetPath)
	}
	s.mu.Unlock()

	if mode == "block" {
		if err := s.nodeHelper.BindBlock(ctx, NodeBindBlockRequest{
			DevicePath: staged.DevicePath,
			TargetPath: targetPath,
			Readonly:   req.GetReadonly(),
		}); err != nil {
			return nil, mapBackendError(err)
		}
	} else {
		if err := s.nodeHelper.Mount(ctx, NodeMountRequest{
			Source:   staged.StagingTargetPath,
			Target:   targetPath,
			Readonly: req.GetReadonly(),
			Bind:     true,
		}); err != nil {
			return nil, mapBackendError(err)
		}
	}

	s.mu.Lock()
	s.published[targetPath] = nodePublishRecord{
		VolumeID:          volumeID,
		StagingTargetPath: stagingTarget,
		TargetPath:        targetPath,
		Mode:              mode,
		Readonly:          req.GetReadonly(),
		Generation:        staged.Generation,
	}
	s.mu.Unlock()
	return &csipb.NodePublishVolumeResponse{}, nil
}

func (s *Server) NodeUnpublishVolume(ctx context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	targetPath := strings.TrimSpace(req.GetTargetPath())
	if strings.TrimSpace(req.GetVolumeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := validateAbsolutePath(targetPath, "target_path"); err != nil {
		return nil, err
	}

	s.mu.Lock()
	published, ok := s.published[targetPath]
	if !ok {
		s.mu.Unlock()
		return &csipb.NodeUnpublishVolumeResponse{}, nil
	}
	if published.VolumeID != strings.TrimSpace(req.GetVolumeId()) {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "target_path %s belongs to a different volume", targetPath)
	}
	s.mu.Unlock()

	if err := s.nodeHelper.Unmount(ctx, targetPath); err != nil {
		return nil, mapBackendError(err)
	}
	s.mu.Lock()
	delete(s.published, targetPath)
	s.mu.Unlock()
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (s *Server) NodeUnstageVolume(ctx context.Context, req *csipb.NodeUnstageVolumeRequest) (*csipb.NodeUnstageVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	stagingTarget := strings.TrimSpace(req.GetStagingTargetPath())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if err := validateAbsolutePath(stagingTarget, "staging_target_path"); err != nil {
		return nil, err
	}

	s.mu.Lock()
	staged, ok := s.staged[volumeID]
	if !ok {
		s.mu.Unlock()
		return &csipb.NodeUnstageVolumeResponse{}, nil
	}
	if staged.StagingTargetPath != stagingTarget {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "staging_target_path mismatch for volume %s", volumeID)
	}
	for _, published := range s.published {
		if published.VolumeID == volumeID {
			s.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s is still published", volumeID)
		}
	}
	s.mu.Unlock()

	if staged.Mode == "filesystem" {
		if err := s.nodeHelper.Unmount(ctx, stagingTarget); err != nil {
			return nil, mapBackendError(err)
		}
	}
	if err := s.nodeHelper.Detach(ctx, NodeDetachRequest{
		VolumeID:     volumeID,
		DeviceID:     staged.DeviceID,
		AttachmentID: staged.AttachmentID,
		Generation:   staged.Generation,
	}); err != nil {
		return nil, mapBackendError(err)
	}

	s.mu.Lock()
	delete(s.staged, volumeID)
	s.mu.Unlock()
	return &csipb.NodeUnstageVolumeResponse{}, nil
}

func (s *Server) NodeExpandVolume(ctx context.Context, req *csipb.NodeExpandVolumeRequest) (*csipb.NodeExpandVolumeResponse, error) {
	volumeID := strings.TrimSpace(req.GetVolumeId())
	volumePath := strings.TrimSpace(req.GetVolumePath())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_path is required")
	}
	var targetBytes uint64
	if req.GetCapacityRange() != nil {
		var err error
		targetBytes, err = requestedCapacityBytes(req.GetCapacityRange())
		if err != nil {
			return nil, err
		}
	}

	staged, err := s.nodeExpansionStage(ctx, req, volumeID)
	if err != nil {
		return nil, err
	}
	if err := validateAbsolutePath(volumePath, "volume_path"); err != nil {
		return nil, err
	}
	if err := s.nodeHelper.ReloadSize(ctx, NodeReloadSizeRequest{
		VolumeID:   volumeID,
		DevicePath: staged.DevicePath,
		DeviceID:   staged.DeviceID,
	}); err != nil {
		return nil, mapBackendError(err)
	}
	if req.GetVolumeCapability().GetMount() != nil || staged.Mode == "filesystem" {
		if err := s.nodeHelper.GrowFilesystem(ctx, NodeGrowFilesystemRequest{
			DevicePath: staged.DevicePath,
			VolumePath: volumePath,
			FSType:     staged.FSType,
		}); err != nil {
			return nil, mapBackendError(err)
		}
	}
	return &csipb.NodeExpandVolumeResponse{CapacityBytes: int64(targetBytes)}, nil
}

func (s *Server) nodeExpansionStage(ctx context.Context, req *csipb.NodeExpandVolumeRequest, volumeID string) (nodeStageRecord, error) {
	s.mu.Lock()
	staged, ok := s.staged[volumeID]
	s.mu.Unlock()
	if ok {
		return staged, nil
	}

	stagingTarget := strings.TrimSpace(req.GetStagingTargetPath())
	if stagingTarget != "" {
		if err := validateAbsolutePath(stagingTarget, "staging_target_path"); err != nil {
			return nodeStageRecord{}, err
		}
	}
	mode := "filesystem"
	fsType := ""
	if cap := req.GetVolumeCapability(); cap != nil {
		checked, err := validateSingleVolumeCapability(cap)
		if err != nil {
			return nodeStageRecord{}, err
		}
		mode = volumeMode(checked)
		var fsErr error
		fsType, fsErr = filesystemType(checked)
		if fsErr != nil {
			return nodeStageRecord{}, fsErr
		}
	}

	attachment, err := s.nodeHelper.LookupAttachment(ctx, NodeLookupAttachmentRequest{VolumeID: volumeID})
	if err != nil {
		switch {
		case isNodeAttachmentNotFound(err):
			return nodeStageRecord{}, status.Errorf(codes.NotFound, "volume %s is not staged and no local attachment was found", volumeID)
		case errors.Is(err, errNodeAttachmentAmbiguous):
			return nodeStageRecord{}, status.Errorf(codes.FailedPrecondition, "volume %s has ambiguous local attachments", volumeID)
		default:
			return nodeStageRecord{}, mapBackendError(err)
		}
	}
	if strings.TrimSpace(attachment.DevicePath) == "" {
		return nodeStageRecord{}, status.Error(codes.Internal, "node helper lookup returned empty device path")
	}
	return nodeStageRecord{
		VolumeID:          volumeID,
		StagingTargetPath: stagingTarget,
		Mode:              mode,
		FSType:            fsType,
		DevicePath:        attachment.DevicePath,
		DeviceID:          attachment.DeviceID,
		AttachmentID:      attachment.AttachmentID,
		Generation:        attachment.Generation,
	}, nil
}

func isNodeAttachmentNotFound(err error) bool {
	return errors.Is(err, errNodeAttachmentNotFound) || status.Code(err) == codes.NotFound
}

func nodeCapability(kind csipb.NodeServiceCapability_RPC_Type) *csipb.NodeServiceCapability {
	return &csipb.NodeServiceCapability{
		Type: &csipb.NodeServiceCapability_Rpc{
			Rpc: &csipb.NodeServiceCapability_RPC{Type: kind},
		},
	}
}

func validateSingleVolumeCapability(cap *csipb.VolumeCapability) (*csipb.VolumeCapability, error) {
	if cap == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_capability is required")
	}
	if err := validateVolumeCapabilities([]*csipb.VolumeCapability{cap}); err != nil {
		return nil, err
	}
	return cap, nil
}

func validateAbsolutePath(pathValue, label string) error {
	if pathValue == "" {
		return status.Errorf(codes.InvalidArgument, "%s is required", label)
	}
	if !filepath.IsAbs(pathValue) {
		return status.Errorf(codes.InvalidArgument, "%s must be absolute", label)
	}
	return nil
}

func volumeMode(cap *csipb.VolumeCapability) string {
	if cap.GetBlock() != nil {
		return "block"
	}
	return "filesystem"
}

func filesystemType(cap *csipb.VolumeCapability) (string, error) {
	if cap.GetMount() == nil {
		return "", nil
	}
	fsType := strings.TrimSpace(cap.GetMount().GetFsType())
	if fsType == "" {
		fsType = "ext4"
	}
	switch fsType {
	case "ext4", "xfs":
		return fsType, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "unsupported fs_type %q", fsType)
	}
}

func shouldGrowFilesystemOnStage(volumeContext map[string]string) bool {
	return strings.TrimSpace(volumeContext[volumeContextProvisioningKindKey]) == volumeContextProvisioningSnapshot
}

func requestedAttachmentGeneration(values ...map[string]string) (uint64, bool) {
	for _, value := range values {
		raw := strings.TrimSpace(value[nodeAttachmentGenerationKey])
		if raw == "" {
			continue
		}
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			return 0, true
		}
		return parsed, true
	}
	return 0, false
}

func nodeStageMetadata(staged nodeStageRecord) map[string]string {
	return map[string]string{
		"volume_id":          staged.VolumeID,
		"staging_target":     staged.StagingTargetPath,
		"mode":               staged.Mode,
		"fs_type":            staged.FSType,
		"device_path":        staged.DevicePath,
		"device_id":          fmt.Sprintf("%d", staged.DeviceID),
		"attachment_id":      staged.AttachmentID,
		"generation":         fmt.Sprintf("%d", staged.Generation),
		"attachment_gen_key": nodeAttachmentGenerationKey,
	}
}
