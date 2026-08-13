package driver

import (
	"strconv"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VolumeParameters struct {
	VolumeID             string
	RedundancyBackend    string
	ReplicationFactor    uint32
	ECProfileID          string
	TopologyMode         string
	BlockSizeBytes       uint32
	AllocationChunkBytes uint32
	AllocationPageBytes  uint32
}

const (
	redundancyBackendReplicated = "replicated"
	redundancyBackendEC         = "ec"

	topologyModeLegacy      = "legacy"
	topologyModePrefer      = "prefer"
	topologyModeStrict      = "strict"
	topologyModeSpreadAlias = "spread"
)

var allowedVolumeParameters = map[string]struct{}{
	"volume_id":             {},
	"redundancy_backend":    {},
	"replication_factor":    {},
	"ec_profile":            {},
	"topology_mode":         {},
	"block_size":            {},
	"allocation_chunk_size": {},
	"allocation_page_size":  {},
}

func parseVolumeParameters(params map[string]string) (VolumeParameters, error) {
	out := VolumeParameters{
		RedundancyBackend: redundancyBackendReplicated,
		ReplicationFactor: 3,
		TopologyMode:      topologyModePrefer,
		BlockSizeBytes:    4096,
	}
	topologyModeSet := false
	for _, key := range sortedKeys(params) {
		if _, ok := allowedVolumeParameters[key]; !ok {
			return VolumeParameters{}, status.Errorf(codes.InvalidArgument, "unsupported parameter %q", key)
		}
		value := strings.TrimSpace(params[key])
		switch key {
		case "volume_id":
			out.VolumeID = value
		case "redundancy_backend":
			switch value {
			case "", redundancyBackendReplicated, redundancyBackendEC:
				if value != "" {
					out.RedundancyBackend = value
				}
			default:
				return VolumeParameters{}, status.Errorf(codes.InvalidArgument, "unsupported redundancy_backend %q", value)
			}
		case "replication_factor":
			parsed, err := parseUint32Param(value, key)
			if err != nil {
				return VolumeParameters{}, err
			}
			out.ReplicationFactor = parsed
		case "ec_profile":
			out.ECProfileID = value
		case "topology_mode":
			if value == "" {
				continue
			}
			normalized, err := normalizeTopologyModeParam(value)
			if err != nil {
				return VolumeParameters{}, err
			}
			out.TopologyMode = normalized
			topologyModeSet = true
		case "block_size":
			parsed, err := parseSizeParam(value, key)
			if err != nil {
				return VolumeParameters{}, err
			}
			out.BlockSizeBytes = uint32(parsed)
		case "allocation_chunk_size":
			parsed, err := parseSizeParam(value, key)
			if err != nil {
				return VolumeParameters{}, err
			}
			out.AllocationChunkBytes = uint32(parsed)
		case "allocation_page_size":
			parsed, err := parseSizeParam(value, key)
			if err != nil {
				return VolumeParameters{}, err
			}
			out.AllocationPageBytes = uint32(parsed)
		}
	}
	if out.RedundancyBackend == redundancyBackendEC {
		out.ReplicationFactor = 1
		if !topologyModeSet {
			out.TopologyMode = topologyModeStrict
		}
		if out.TopologyMode != topologyModeStrict {
			return VolumeParameters{}, status.Errorf(codes.InvalidArgument, "topology_mode %q is not supported when redundancy_backend=ec", out.TopologyMode)
		}
		if out.ECProfileID == "" {
			return VolumeParameters{}, status.Error(codes.InvalidArgument, "ec_profile is required when redundancy_backend=ec")
		}
	}
	return out, nil
}

func normalizeTopologyModeParam(value string) (string, error) {
	switch value {
	case topologyModeLegacy, topologyModePrefer, topologyModeStrict:
		return value, nil
	case topologyModeSpreadAlias:
		return topologyModePrefer, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "unsupported topology_mode %q", value)
	}
}

func rejectUnknownSnapshotParameters(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	return status.Errorf(codes.InvalidArgument, "unsupported snapshot parameter %q", sortedKeys(params)[0])
}

func requestedCapacityBytes(capacity *csipb.CapacityRange) (uint64, error) {
	if capacity == nil || (capacity.GetRequiredBytes() == 0 && capacity.GetLimitBytes() == 0) {
		return 0, status.Error(codes.InvalidArgument, "capacity_range.required_bytes is required")
	}
	required := capacity.GetRequiredBytes()
	limit := capacity.GetLimitBytes()
	if required < 0 || limit < 0 {
		return 0, status.Error(codes.InvalidArgument, "capacity_range values must be non-negative")
	}
	if required == 0 {
		required = limit
	}
	if limit != 0 && required > limit {
		return 0, status.Error(codes.OutOfRange, "capacity_range.required_bytes exceeds limit_bytes")
	}
	if required == 0 {
		return 0, status.Error(codes.InvalidArgument, "capacity_range.required_bytes is required")
	}
	return uint64(required), nil
}

func parseUint32Param(raw, label string) (uint32, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, status.Errorf(codes.InvalidArgument, "%s is required", label)
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
	if err != nil || parsed == 0 {
		return 0, status.Errorf(codes.InvalidArgument, "invalid %s %q", label, raw)
	}
	return uint32(parsed), nil
}

func parseSizeParam(raw, label string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, status.Errorf(codes.InvalidArgument, "%s is required", label)
	}
	multiplier := uint64(1)
	number := raw
	switch suffix := raw[len(raw)-1]; suffix {
	case 'K', 'k':
		multiplier = 1 << 10
		number = raw[:len(raw)-1]
	case 'M', 'm':
		multiplier = 1 << 20
		number = raw[:len(raw)-1]
	case 'G', 'g':
		multiplier = 1 << 30
		number = raw[:len(raw)-1]
	case 'T', 't':
		multiplier = 1 << 40
		number = raw[:len(raw)-1]
	}
	value, err := strconv.ParseUint(strings.TrimSpace(number), 10, 64)
	if err != nil || value == 0 {
		return 0, status.Errorf(codes.InvalidArgument, "invalid %s %q", label, raw)
	}
	if value > ^uint64(0)/multiplier {
		return 0, status.Errorf(codes.InvalidArgument, "%s overflows uint64", label)
	}
	parsed := value * multiplier
	if parsed > uint64(^uint32(0)) && (label == "block_size" || strings.HasPrefix(label, "allocation_")) {
		return 0, status.Errorf(codes.InvalidArgument, "%s overflows uint32", label)
	}
	return parsed, nil
}

func validateVolumeCapabilities(caps []*csipb.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume_capabilities is required")
	}
	for i, cap := range caps {
		if cap == nil {
			return status.Errorf(codes.InvalidArgument, "volume_capabilities[%d] is nil", i)
		}
		if cap.GetBlock() == nil && cap.GetMount() == nil {
			return status.Errorf(codes.InvalidArgument, "volume_capabilities[%d] must specify block or mount", i)
		}
		mode := cap.GetAccessMode().GetMode()
		switch mode {
		case csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csipb.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER:
		default:
			return status.Errorf(codes.InvalidArgument, "access mode %s is not supported", mode.String())
		}
	}
	return nil
}
