package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/nosway/namrbd/control/netlinktlv"
)

type AttachRequest struct {
	HostID   string
	VolumeID uint64
}

// RESTServer is delivered from userland tool to kernel via netlink.
// Kernel module uses this list to call REST API directly.
type RESTServer struct {
	ID          uint32 `json:"id"`
	Address     string `json:"address"`
	Port        uint16 `json:"port"`
	UseTLS      bool   `json:"use_tls"`
	APIPrefix   string `json:"api_prefix"`
	BearerToken string `json:"bearer_token,omitempty"`
}

type GatewayEndpoint struct {
	Address string
	Port    uint16
}

type AttachManifest struct {
	VolumeID         uint64
	Generation       uint64
	DurabilityMode   string
	MaxIOSize        uint32
	MaxInflightReqs  uint32
	MaxInflightBytes uint64
	AuthToken        string
	GatewayEndpoints []GatewayEndpoint
}

type ControlEndpointSpec struct {
	Address     string `json:"address"`
	Port        uint16 `json:"port"`
	UseTLS      bool   `json:"use_tls"`
	ServerName  string `json:"server_name,omitempty"`
	AuthMode    string `json:"auth_mode,omitempty"`
	APIPrefix   string `json:"api_prefix,omitempty"`
	BearerToken string `json:"bearer_token,omitempty"`
}

type DataplaneEndpointSpec struct {
	PathID     uint32 `json:"path_id"`
	GatewayID  string `json:"gateway_id,omitempty"`
	Address    string `json:"address"`
	Port       uint16 `json:"port"`
	UseTLS     bool   `json:"use_tls"`
	ServerName string `json:"server_name,omitempty"`
	AuthMode   string `json:"auth_mode,omitempty"`
	Priority   uint32 `json:"priority,omitempty"`
}

type AttachManifestDocument struct {
	VolumeID                           string                  `json:"volume_id"`
	Generation                         uint64                  `json:"generation,omitempty"`
	AttachmentGeneration               uint64                  `json:"attachment_generation,omitempty"`
	SizeBytes                          uint64                  `json:"size_bytes,omitempty"`
	BlockSize                          uint32                  `json:"block_size,omitempty"`
	ChunkSizeBytes                     uint32                  `json:"chunk_size_bytes,omitempty"`
	ExtentPageBytes                    uint32                  `json:"extent_page_bytes,omitempty"`
	AttachmentID                       string                  `json:"attachment_id,omitempty"`
	AttachedHostID                     string                  `json:"attached_host_id,omitempty"`
	AttachedDeviceID                   uint32                  `json:"attached_device_id"`
	WriterFencingEpoch                 uint64                  `json:"writer_fencing_epoch,omitempty"`
	RuntimePathExpansionEligibleAtUnix int64                   `json:"runtime_path_expansion_eligible_at_unix,omitempty"`
	HandoffRequired                    bool                    `json:"handoff_required,omitempty"`
	HandoffReason                      string                  `json:"handoff_reason,omitempty"`
	HandoffTargetGatewaySet            []string                `json:"handoff_target_gateway_set,omitempty"`
	ControllerPriorityClass            string                  `json:"controller_priority_class,omitempty"`
	ControllerRecommendedActions       []string                `json:"controller_recommended_actions,omitempty"`
	ClusterPriorityMismatchActions     []string                `json:"cluster_priority_mismatch_actions,omitempty"`
	DurabilityMode                     string                  `json:"durability_mode,omitempty"`
	MaxIOSize                          uint32                  `json:"max_io_size,omitempty"`
	MaxZeroLikeIOSize                  uint32                  `json:"max_zero_like_io_size,omitempty"`
	MaxInflightReqs                    uint32                  `json:"max_inflight_requests,omitempty"`
	MaxInflightBytes                   uint64                  `json:"max_inflight_bytes,omitempty"`
	InitialZeroMapTrusted              bool                    `json:"initial_zero_map_trusted,omitempty"`
	InitialZeroMapAllZero              bool                    `json:"initial_zero_map_all_zero,omitempty"`
	InitialZeroMapGranuleBytes         uint32                  `json:"initial_zero_map_granule_bytes,omitempty"`
	InitialZeroMapCheckedPages         int                     `json:"initial_zero_map_checked_pages,omitempty"`
	InitialZeroMapCheckedChunks        uint64                  `json:"initial_zero_map_checked_chunks,omitempty"`
	AuthToken                          string                  `json:"auth_token,omitempty"`
	DataplaneAuth                      map[string]any          `json:"dataplane_auth,omitempty"`
	ControlEndpoints                   []ControlEndpointSpec   `json:"control_endpoints"`
	DataplaneEndpoints                 []DataplaneEndpointSpec `json:"dataplane_endpoints"`
}

type PathHealthState string

const (
	PathHealthHealthy PathHealthState = "healthy"
	PathHealthSuspect PathHealthState = "suspect"
	PathHealthDown    PathHealthState = "down"
)

type PathSelectionPlan struct {
	Active     []DataplaneEndpointSpec
	Standby    []DataplaneEndpointSpec
	Suppressed []DataplaneEndpointSpec
}

// KernelControl models commands sent from userland tool to kernel module.
// ConfigureRESTServers is expected to be transported over netlink.
// Attach/Detach are expected to trigger kernel-side direct REST calls.
type KernelControl interface {
	ConfigureRESTServers(ctx context.Context, servers []RESTServer) error
	AttachVolumeViaREST(ctx context.Context, req AttachRequest) (AttachManifest, error)
	DetachVolumeViaREST(ctx context.Context, hostID string, volumeID uint64) error
}

type Agent struct {
	Kernel KernelControl
}

func BuildNetlinkPayload(servers []RESTServer) ([]byte, error) {
	nlServers := make([]netlinktlv.RESTServer, 0, len(servers))
	for _, s := range servers {
		nlServers = append(nlServers, netlinktlv.RESTServer{
			ID:          s.ID,
			Address:     s.Address,
			Port:        s.Port,
			UseTLS:      s.UseTLS,
			APIPrefix:   s.APIPrefix,
			BearerToken: s.BearerToken,
		})
	}
	data, err := netlinktlv.EncodeConfigREST(netlinktlv.ConfigRESTRequest{
		DeviceID: 0,
		Servers:  nlServers,
	})
	if err != nil {
		return nil, err
	}
	// Caller sends `CmdConfigREST` through generic netlink command field.
	return data, nil
}

func (a Agent) ConfigureKernelREST(ctx context.Context, servers []RESTServer) error {
	if len(servers) == 0 {
		return fmt.Errorf("no REST servers provided")
	}
	return a.Kernel.ConfigureRESTServers(ctx, servers)
}

func (a Agent) AttachVolume(ctx context.Context, req AttachRequest) (AttachManifest, error) {
	manifest, err := a.Kernel.AttachVolumeViaREST(ctx, req)
	if err != nil {
		return AttachManifest{}, fmt.Errorf("kernel REST attach failed: %w", err)
	}
	return manifest, nil
}

func (a Agent) DetachVolume(ctx context.Context, hostID string, volumeID uint64) error {
	if err := a.Kernel.DetachVolumeViaREST(ctx, hostID, volumeID); err != nil {
		return fmt.Errorf("kernel REST detach failed: %w", err)
	}
	return nil
}

func PrepareAttachManifest(raw string) (string, []RESTServer, error) {
	return prepareAttachManifest(raw, true)
}

func prepareAttachManifest(raw string, requireKernelAttachFields bool) (string, []RESTServer, error) {
	if raw == "" {
		return "", nil, fmt.Errorf("attach manifest is required")
	}

	var doc AttachManifestDocument
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return "", nil, fmt.Errorf("parse attach manifest: %w", err)
	}
	if err := validateAttachManifestDocument(doc); err != nil {
		return "", nil, err
	}
	if requireKernelAttachFields {
		if err := validateKernelAttachManifestRaw(raw); err != nil {
			return "", nil, err
		}
		if err := validateKernelAttachManifestDocument(doc); err != nil {
			return "", nil, err
		}
	}

	doc.ControlEndpoints = normalizeControlEndpoints(doc.ControlEndpoints)
	doc.DataplaneEndpoints = normalizeDataplaneEndpoints(doc.DataplaneEndpoints)

	servers := make([]RESTServer, 0, len(doc.ControlEndpoints))
	for idx, endpoint := range doc.ControlEndpoints {
		apiPrefix := endpoint.APIPrefix
		if apiPrefix == "" {
			apiPrefix = "/api/v1"
		}
		servers = append(servers, RESTServer{
			ID:          uint32(idx + 1),
			Address:     endpoint.Address,
			Port:        endpoint.Port,
			UseTLS:      endpoint.UseTLS,
			APIPrefix:   apiPrefix,
			BearerToken: endpoint.BearerToken,
		})
	}

	normalized, err := json.Marshal(doc)
	if err != nil {
		return "", nil, fmt.Errorf("marshal attach manifest: %w", err)
	}
	return string(normalized), servers, nil
}

func validateAttachManifestDocument(doc AttachManifestDocument) error {
	if doc.VolumeID == "" {
		return fmt.Errorf("attach manifest volume_id is required")
	}
	if len(doc.ControlEndpoints) == 0 {
		return fmt.Errorf("attach manifest control_endpoints is required")
	}
	if len(doc.DataplaneEndpoints) == 0 {
		return fmt.Errorf("attach manifest dataplane_endpoints is required")
	}

	for i, endpoint := range doc.ControlEndpoints {
		if endpoint.Address == "" {
			return fmt.Errorf("control_endpoints[%d].address is required", i)
		}
		if endpoint.Port == 0 {
			return fmt.Errorf("control_endpoints[%d].port is required", i)
		}
	}

	seenPathIDs := map[uint32]struct{}{}
	for i, endpoint := range doc.DataplaneEndpoints {
		if endpoint.Address == "" {
			return fmt.Errorf("dataplane_endpoints[%d].address is required", i)
		}
		if endpoint.Port == 0 {
			return fmt.Errorf("dataplane_endpoints[%d].port is required", i)
		}
		if _, ok := seenPathIDs[endpoint.PathID]; ok {
			return fmt.Errorf("duplicate dataplane_endpoints path_id=%d", endpoint.PathID)
		}
		seenPathIDs[endpoint.PathID] = struct{}{}
	}
	return nil
}

func validateKernelAttachManifestRaw(raw string) error {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return fmt.Errorf("parse attach manifest: %w", err)
	}
	for _, key := range []string{
		"generation",
		"size_bytes",
		"block_size",
		"attachment_id",
		"attached_host_id",
		"attached_device_id",
	} {
		value, ok := doc[key]
		if !ok || len(value) == 0 || string(value) == "null" {
			return fmt.Errorf("attach manifest %s is required", key)
		}
	}
	return nil
}

func validateKernelAttachManifestDocument(doc AttachManifestDocument) error {
	if doc.Generation == 0 {
		return fmt.Errorf("attach manifest generation is required")
	}
	if doc.SizeBytes == 0 {
		return fmt.Errorf("attach manifest size_bytes is required")
	}
	if doc.BlockSize == 0 {
		return fmt.Errorf("attach manifest block_size is required")
	}
	if doc.AttachmentID == "" {
		return fmt.Errorf("attach manifest attachment_id is required")
	}
	if doc.AttachedHostID == "" {
		return fmt.Errorf("attach manifest attached_host_id is required")
	}
	return nil
}

func normalizeControlEndpoints(endpoints []ControlEndpointSpec) []ControlEndpointSpec {
	normalized := make([]ControlEndpointSpec, 0, len(endpoints))
	seen := map[string]struct{}{}
	for _, endpoint := range endpoints {
		key := fmt.Sprintf("%s|%d|%t|%s|%s", endpoint.Address, endpoint.Port, endpoint.UseTLS, endpoint.APIPrefix, endpoint.BearerToken)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, endpoint)
	}
	return normalized
}

func normalizeDataplaneEndpoints(endpoints []DataplaneEndpointSpec) []DataplaneEndpointSpec {
	normalized := append([]DataplaneEndpointSpec(nil), endpoints...)
	sort.SliceStable(normalized, func(i, j int) bool {
		if normalized[i].Priority != normalized[j].Priority {
			return normalized[i].Priority > normalized[j].Priority
		}
		if normalized[i].PathID != normalized[j].PathID {
			return normalized[i].PathID < normalized[j].PathID
		}
		if normalized[i].Address != normalized[j].Address {
			return normalized[i].Address < normalized[j].Address
		}
		return normalized[i].Port < normalized[j].Port
	})
	for i := range normalized {
		normalized[i].PathID = uint32(i)
	}
	return normalized
}

func BuildPathSelectionPlan(raw string, health map[uint32]PathHealthState, maxActive int) (PathSelectionPlan, error) {
	normalized, _, err := prepareAttachManifest(raw, false)
	if err != nil {
		return PathSelectionPlan{}, err
	}

	var doc AttachManifestDocument
	if err := json.Unmarshal([]byte(normalized), &doc); err != nil {
		return PathSelectionPlan{}, fmt.Errorf("parse normalized attach manifest: %w", err)
	}

	healthy := make([]DataplaneEndpointSpec, 0, len(doc.DataplaneEndpoints))
	suspect := make([]DataplaneEndpointSpec, 0, len(doc.DataplaneEndpoints))
	suppressed := make([]DataplaneEndpointSpec, 0)

	for _, endpoint := range doc.DataplaneEndpoints {
		switch health[endpoint.PathID] {
		case PathHealthDown:
			suppressed = append(suppressed, endpoint)
		case PathHealthSuspect:
			suspect = append(suspect, endpoint)
		default:
			healthy = append(healthy, endpoint)
		}
	}

	candidates := append([]DataplaneEndpointSpec(nil), healthy...)
	candidates = append(candidates, suspect...)

	if maxActive <= 0 || maxActive > len(candidates) {
		maxActive = len(candidates)
	}

	active, standby := selectActiveAndStandbyPaths(candidates, maxActive)

	return PathSelectionPlan{
		Active:     active,
		Standby:    standby,
		Suppressed: suppressed,
	}, nil
}

func AllowedPathIDs(plan PathSelectionPlan) []uint32 {
	out := make([]uint32, 0, len(plan.Active))
	for _, endpoint := range plan.Active {
		out = append(out, endpoint.PathID)
	}
	return out
}

func selectActiveAndStandbyPaths(candidates []DataplaneEndpointSpec, maxActive int) ([]DataplaneEndpointSpec, []DataplaneEndpointSpec) {
	if maxActive <= 0 || len(candidates) == 0 {
		return []DataplaneEndpointSpec{}, append([]DataplaneEndpointSpec(nil), candidates...)
	}

	selected := make([]bool, len(candidates))
	active := make([]DataplaneEndpointSpec, 0, minInt(maxActive, len(candidates)))
	seenGateways := map[string]struct{}{}

	for i, endpoint := range candidates {
		if len(active) >= maxActive {
			break
		}
		key := dataplaneGatewayKey(endpoint)
		if key == "" {
			continue
		}
		if _, ok := seenGateways[key]; ok {
			continue
		}
		seenGateways[key] = struct{}{}
		selected[i] = true
		active = append(active, endpoint)
	}

	for i, endpoint := range candidates {
		if len(active) >= maxActive {
			break
		}
		if selected[i] {
			continue
		}
		selected[i] = true
		active = append(active, endpoint)
	}

	standby := make([]DataplaneEndpointSpec, 0, len(candidates)-len(active))
	for i, endpoint := range candidates {
		if selected[i] {
			continue
		}
		standby = append(standby, endpoint)
	}
	return active, standby
}

func dataplaneGatewayKey(endpoint DataplaneEndpointSpec) string {
	if endpoint.GatewayID != "" {
		return endpoint.GatewayID
	}
	if endpoint.ServerName != "" {
		return endpoint.ServerName
	}
	return endpoint.Address
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
