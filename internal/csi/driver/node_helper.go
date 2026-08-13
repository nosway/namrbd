package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type CommandNodeHelperConfig struct {
	NodeID        string
	GatewayURL    string
	NamrbdctlPath string
}

type CommandNodeHelper struct {
	nodeID        string
	gatewayURL    string
	namrbdctlPath string
}

func NewCommandNodeHelper(cfg CommandNodeHelperConfig) *CommandNodeHelper {
	path := strings.TrimSpace(cfg.NamrbdctlPath)
	if path == "" {
		path = "namrbdctl"
	}
	return &CommandNodeHelper{
		nodeID:        strings.TrimSpace(cfg.NodeID),
		gatewayURL:    strings.TrimSpace(cfg.GatewayURL),
		namrbdctlPath: path,
	}
}

func (h *CommandNodeHelper) LookupAttachment(ctx context.Context, req NodeLookupAttachmentRequest) (NodeAttachment, error) {
	volumeID := strings.TrimSpace(req.VolumeID)
	if volumeID == "" {
		return NodeAttachment{}, fmt.Errorf("%w: volume id is required", errNodeAttachmentNotFound)
	}
	devices, err := h.runJSONArray(ctx, h.namrbdctlPath, "list-devices")
	if err != nil {
		return NodeAttachment{}, err
	}
	var found *NodeAttachment
	for _, device := range devices {
		if jsonString(device["volume_id"]) != volumeID || !jsonBool(device["attached"]) {
			continue
		}
		deviceID64, ok := jsonUint(device["device_id"])
		diskName := jsonString(device["disk_name"])
		if !ok || deviceID64 > uint64(^uint32(0)) || diskName == "" {
			return NodeAttachment{}, fmt.Errorf("list-devices response missing device_id or disk_name for volume %s", volumeID)
		}
		attachment := NodeAttachment{
			DevicePath: filepath.Join("/dev", diskName),
			DeviceID:   uint32(deviceID64),
		}
		if generation, ok := jsonUint(device["generation"]); ok {
			attachment.Generation = generation
		}
		if found != nil {
			return NodeAttachment{}, fmt.Errorf("%w: volume %s", errNodeAttachmentAmbiguous, volumeID)
		}
		found = &attachment
	}
	if found == nil {
		return NodeAttachment{}, fmt.Errorf("%w: volume %s", errNodeAttachmentNotFound, volumeID)
	}
	return *found, nil
}

func (h *CommandNodeHelper) Attach(ctx context.Context, req NodeAttachRequest) (NodeAttachment, error) {
	if h.gatewayURL == "" {
		return NodeAttachment{}, fmt.Errorf("gateway url is required for node attach")
	}
	nodeID := strings.TrimSpace(req.NodeID)
	if nodeID == "" {
		nodeID = h.nodeID
	}
	if nodeID == "" {
		return NodeAttachment{}, fmt.Errorf("node id is required for node attach")
	}
	createOut, err := h.runJSON(ctx, h.namrbdctlPath, "create-device")
	if err != nil {
		return NodeAttachment{}, err
	}
	deviceID64, ok := jsonUint(createOut["device_id"])
	diskName := jsonString(createOut["disk_name"])
	if !ok || deviceID64 > uint64(^uint32(0)) || diskName == "" {
		return NodeAttachment{}, fmt.Errorf("create-device response missing device_id or disk_name")
	}
	deviceID := uint32(deviceID64)

	attachOut, err := h.runJSON(ctx, h.namrbdctlPath,
		"attach",
		"--device", strconv.FormatUint(uint64(deviceID), 10),
		"--host", nodeID,
		"--volume", req.VolumeID,
		"--gateway", h.gatewayURL)
	if err != nil {
		_ = h.run(ctx, h.namrbdctlPath, "destroy-device", "--device", strconv.FormatUint(uint64(deviceID), 10))
		return NodeAttachment{}, err
	}
	attachmentID := jsonString(attachOut["attachment_id"])
	generation, ok := jsonUint(attachOut["generation"])
	if attachmentID == "" || !ok || generation == 0 {
		_ = h.run(ctx, h.namrbdctlPath,
			"detach",
			"--device", strconv.FormatUint(uint64(deviceID), 10),
			"--host", nodeID,
			"--volume", req.VolumeID,
			"--gateway", h.gatewayURL)
		_ = h.run(ctx, h.namrbdctlPath, "destroy-device", "--device", strconv.FormatUint(uint64(deviceID), 10))
		return NodeAttachment{}, fmt.Errorf("attach response missing attachment_id or generation")
	}
	return NodeAttachment{
		DevicePath:   filepath.Join("/dev", diskName),
		DeviceID:     deviceID,
		AttachmentID: attachmentID,
		Generation:   generation,
	}, nil
}

func (h *CommandNodeHelper) Detach(ctx context.Context, req NodeDetachRequest) error {
	var detachErr error
	if h.gatewayURL != "" {
		detachErr = h.run(ctx, h.namrbdctlPath,
			"detach",
			"--device", strconv.FormatUint(uint64(req.DeviceID), 10),
			"--host", h.nodeID,
			"--volume", req.VolumeID,
			"--gateway", h.gatewayURL)
	} else {
		detachErr = h.run(ctx, h.namrbdctlPath,
			"detach",
			"--local-only",
			"--device", strconv.FormatUint(uint64(req.DeviceID), 10),
			"--volume", req.VolumeID)
	}
	destroyErr := h.run(ctx, h.namrbdctlPath, "destroy-device", "--device", strconv.FormatUint(uint64(req.DeviceID), 10))
	if detachErr != nil {
		return detachErr
	}
	return destroyErr
}

func (h *CommandNodeHelper) FormatIfNeeded(ctx context.Context, req NodeFormatRequest) error {
	start := time.Now()
	if err := exec.CommandContext(ctx, "blkid", req.DevicePath).Run(); err == nil {
		log.Printf("namrbd_csi_node_format_probe_completed device=%s fstype=%s formatted=true duration_ms=%d", req.DevicePath, req.FSType, time.Since(start).Milliseconds())
		return nil
	} else {
		log.Printf("namrbd_csi_node_format_probe_completed device=%s fstype=%s formatted=false duration_ms=%d", req.DevicePath, req.FSType, time.Since(start).Milliseconds())
	}
	switch req.FSType {
	case "ext4":
		return h.run(ctx, "mkfs.ext4", "-F", "-E", "nodiscard", req.DevicePath)
	case "xfs":
		return h.run(ctx, "mkfs.xfs", "-f", "-K", req.DevicePath)
	default:
		return fmt.Errorf("unsupported filesystem type %q", req.FSType)
	}
}

func (h *CommandNodeHelper) Mount(ctx context.Context, req NodeMountRequest) error {
	if req.Bind {
		if err := os.MkdirAll(req.Target, 0o755); err != nil {
			return err
		}
		if err := h.run(ctx, "mount", "--bind", req.Source, req.Target); err != nil {
			return err
		}
		if req.Readonly {
			return h.run(ctx, "mount", "-o", "remount,bind,ro", req.Target)
		}
		return nil
	}
	if err := os.MkdirAll(req.Target, 0o755); err != nil {
		return err
	}
	options := mountOptions(req.Flags, req.Readonly)
	args := []string{}
	if req.FSType != "" {
		args = append(args, "-t", req.FSType)
	}
	if options != "" {
		args = append(args, "-o", options)
	}
	args = append(args, req.Source, req.Target)
	return h.run(ctx, "mount", args...)
}

func (h *CommandNodeHelper) BindBlock(ctx context.Context, req NodeBindBlockRequest) error {
	if err := os.MkdirAll(filepath.Dir(req.TargetPath), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(req.TargetPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	_ = file.Close()
	if err := h.run(ctx, "mount", "--bind", req.DevicePath, req.TargetPath); err != nil {
		return err
	}
	if req.Readonly {
		return h.run(ctx, "mount", "-o", "remount,bind,ro", req.TargetPath)
	}
	return nil
}

func (h *CommandNodeHelper) Unmount(ctx context.Context, targetPath string) error {
	if err := h.run(ctx, "umount", targetPath); err != nil {
		return err
	}
	_ = os.Remove(targetPath)
	return nil
}

func (h *CommandNodeHelper) ReloadSize(ctx context.Context, req NodeReloadSizeRequest) error {
	if h.gatewayURL == "" {
		return fmt.Errorf("gateway url is required for reload-size")
	}
	_, err := h.runJSON(ctx, h.namrbdctlPath,
		"volume-reload-size",
		"--device", strconv.FormatUint(uint64(req.DeviceID), 10),
		"--host", h.nodeID,
		"--volume", req.VolumeID,
		"--gateway", h.gatewayURL)
	return err
}

func (h *CommandNodeHelper) GrowFilesystem(ctx context.Context, req NodeGrowFilesystemRequest) error {
	switch req.FSType {
	case "", "ext4":
		return h.run(ctx, "resize2fs", req.DevicePath)
	case "xfs":
		return h.run(ctx, "xfs_growfs", req.VolumePath)
	default:
		return fmt.Errorf("unsupported filesystem type %q", req.FSType)
	}
}

func (h *CommandNodeHelper) runJSON(ctx context.Context, name string, args ...string) (map[string]any, error) {
	out, err := h.runCombinedOutput(ctx, name, args...)
	if err != nil {
		return nil, commandError(name, args, out, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		return nil, fmt.Errorf("%s output is not JSON: %w", name, err)
	}
	return decoded, nil
}

func (h *CommandNodeHelper) runJSONArray(ctx context.Context, name string, args ...string) ([]map[string]any, error) {
	out, err := h.runCombinedOutput(ctx, name, args...)
	if err != nil {
		return nil, commandError(name, args, out, err)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		return nil, fmt.Errorf("%s output is not JSON array: %w", name, err)
	}
	return decoded, nil
}

func (h *CommandNodeHelper) run(ctx context.Context, name string, args ...string) error {
	out, err := h.runCombinedOutput(ctx, name, args...)
	if err != nil {
		return commandError(name, args, out, err)
	}
	return nil
}

func (h *CommandNodeHelper) runCombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	start := time.Now()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	result := "ok"
	if err != nil {
		result = "error"
	}
	log.Printf("namrbd_csi_node_command_completed command=%s args=%q result=%s duration_ms=%d", name, args, result, time.Since(start).Milliseconds())
	return out, err
}

func commandError(name string, args []string, out []byte, err error) error {
	detail := strings.TrimSpace(string(out))
	if detail == "" {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, detail)
}

func mountOptions(flags []string, readonly bool) string {
	out := make([]string, 0, len(flags)+1)
	if readonly {
		out = append(out, "ro")
	}
	for _, flag := range flags {
		if trimmed := strings.TrimSpace(flag); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, ",")
}

func jsonString(value any) string {
	if v, ok := value.(string); ok {
		return v
	}
	return ""
}

func jsonBool(value any) bool {
	v, ok := value.(bool)
	return ok && v
}

func jsonUint(value any) (uint64, bool) {
	switch v := value.(type) {
	case float64:
		if v >= 0 && v == float64(uint64(v)) {
			return uint64(v), true
		}
	case int:
		if v >= 0 {
			return uint64(v), true
		}
	case uint64:
		return v, true
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		return parsed, err == nil
	}
	return 0, false
}
