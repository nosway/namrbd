// Package serviceconfig defines the reviewed configuration schema for the six
// long-running NAMRBD processes.
//
// AA-IMPL-001A owns this schema. It carries the stable settings that today are
// supplied as command-line flags: 73 on namrbd-gateway, 37 on
// namrbd-iscsi-gateway, 27 on sbs-service, 9 on sbs-data, 10 on
// namrbd-csi-driver, and 5 on namrbd-mcp. At t2_large those command lines would
// be copied across 100 SBS nodes and 64 gateways, where they drift.
//
// Two things deliberately do not live here:
//
//   - Secret values. Fields carry a SecretRef naming a file, environment
//     variable, or KMS key. See secret.go.
//   - Membership and serving maps. SBS node membership comes from the
//     TiKV-backed sbs-service registry and iSCSI target/LUN/export mappings come
//     from the sbs-service iSCSI registry. A config file that carried either
//     would become a second authority.
//
// The loader, precedence rules, and redacted summary are AA-IMPL-001B. Nothing
// imports this package yet; adopting it per process is AA-IMPL-001D through
// AA-IMPL-001H.
package serviceconfig

import "github.com/nosway/namrbd/internal/depavail"

// SchemaVersion is the config schema contract version. A process refuses a
// config whose major version it does not understand rather than guessing.
const SchemaVersion = 1

// Profile selects how strictly a process treats its own configuration.
//
// The large_scale profile is what makes the AA-IMPL-001 gate testable: it
// rejects the legacy static flags that cannot be operated at t2_large.
const (
	// ProfileDev permits legacy static flags and lab toggles.
	ProfileDev = "dev"
	// ProfileLargeScale rejects legacy static flags, requires config-file
	// authority for stable settings, and requires secret references.
	ProfileLargeScale = "large_scale"
)

// File is the top level of every service config file.
type File struct {
	SchemaVersion int `yaml:"schema_version"`
	// Revision is declared by the operator and bumped on every reviewed change.
	// It exists so a human can say "node 41 is still on revision 6" without
	// diffing files. A content digest is recorded alongside it at load time to
	// catch the case where the file changed but the revision was not bumped.
	Revision int    `yaml:"revision"`
	Profile  string `yaml:"profile"`
	// Process names which service this file configures. It is checked against
	// the binary reading it, so a gateway cannot silently start from an
	// sbs-service config.
	Process string `yaml:"process"`

	Gateway     *GatewayConfig      `yaml:"gateway,omitempty"`
	ISCSIGetway *ISCSIGatewayConfig `yaml:"iscsi_gateway,omitempty"`
	SBSService  *SBSServiceConfig   `yaml:"sbs_service,omitempty"`
	SBSData     *SBSDataConfig      `yaml:"sbs_data,omitempty"`
	CSIDriver   *CSIDriverConfig    `yaml:"csi_driver,omitempty"`
	MCP         *MCPConfig          `yaml:"mcp,omitempty"`
}

// Process names, matching the binary names.
const (
	ProcessGateway      = "namrbd-gateway"
	ProcessISCSIGateway = "namrbd-iscsi-gateway"
	ProcessSBSService   = "sbs-service"
	ProcessSBSData      = "sbs-data"
	ProcessCSIDriver    = "namrbd-csi-driver"
	ProcessMCP          = "namrbd-mcp"
)

// ---------------------------------------------------------------------------
// Shared blocks
// ---------------------------------------------------------------------------

// TLSConfig replaces the --tls-cert-file/--tls-key-file pair. The key is a
// reference; the certificate stays a path because it is not secret.
type TLSConfig struct {
	Enable     bool      `yaml:"enable"`
	CertFile   string    `yaml:"cert_file,omitempty"`
	Key        SecretRef `yaml:"key,omitempty"`
	ServerName string    `yaml:"server_name,omitempty"`
}

// EtcdConfig is the gateway fleet membership channel. It is never SBS
// node/store membership authority; see AA-IMPL-005.
type EtcdConfig struct {
	Endpoints []string   `yaml:"endpoints"`
	Root      string     `yaml:"root"`
	TLS       *TLSConfig `yaml:"tls,omitempty"`
}

// ObservabilityConfig covers the listener and trace policy that every process
// exposes in some form.
type ObservabilityConfig struct {
	Listen string `yaml:"listen,omitempty"`
	// Trace is opt-in. Production-like profiles keep it false, because verbose
	// TiKV and dataplane tracing is itself a scale hazard (AA-IMPL-003).
	Trace bool `yaml:"trace"`
	// DebugEndpoints exposes lab-only debug routes.
	DebugEndpoints bool `yaml:"debug_endpoints"`
}

// ---------------------------------------------------------------------------
// Per-process blocks
// ---------------------------------------------------------------------------

// GatewayConfig covers namrbd-gateway. Static --volumes and
// --sbs-cluster-replicas have no representation here on purpose: volume and
// replica membership come from sbs-service, and carrying them in config would
// recreate the drift this slice exists to remove.
type GatewayConfig struct {
	GatewayID string `yaml:"gateway_id"`

	Listen               string `yaml:"listen"`
	DataListen           string `yaml:"data_listen,omitempty"`
	AdvertiseControlAddr string `yaml:"advertise_control_address,omitempty"`
	AdvertiseDataAddr    string `yaml:"advertise_data_address,omitempty"`
	DataDisable          bool   `yaml:"data_disable"`

	TLS  *TLSConfig  `yaml:"tls,omitempty"`
	Etcd *EtcdConfig `yaml:"etcd,omitempty"`

	// SBSAdminEndpoint is the sbs-service admin API this gateway consumes its
	// published views from.
	SBSAdminEndpoint string `yaml:"sbs_admin_endpoint"`
	MetadataBackend  string `yaml:"metadata_backend,omitempty"`
	DataBackendMode  string `yaml:"data_backend_mode,omitempty"`

	Cache     GatewayCacheConfig     `yaml:"cache"`
	Reconcile GatewayReconcileConfig `yaml:"reconcile"`
	Dataplane GatewayDataplaneConfig `yaml:"dataplane"`

	Observability ObservabilityConfig `yaml:"observability"`

	// Dependency availability thresholds. Omitted means the shipped
	// defaults; see docs/phase-aa-entry-plan.md Section 4.
	Dependency *depavail.Thresholds `yaml:"dependency,omitempty"`
}

// GatewayCacheConfig holds the TTLs that must not drift between gateways,
// because a stale cache on one node produces reads the others do not.
type GatewayCacheConfig struct {
	VolumeTTLSeconds           int `yaml:"volume_ttl_seconds"`
	ZeroEvidenceTTLSeconds     int `yaml:"zero_evidence_ttl_seconds"`
	OpenReuseTTLSeconds        int `yaml:"open_reuse_ttl_seconds"`
	ChunkIDAllocationCacheSize int `yaml:"chunk_id_allocation_cache_size"`
	WritePlanTTLSeconds        int `yaml:"write_plan_ttl_seconds"`
	BeginWriteVolumeTTLSeconds int `yaml:"begin_write_volume_state_ttl_seconds"`
}

// GatewayReconcileConfig bounds the path-plan loop. AA-IMPL-003 requires this
// to process bounded changed sets rather than scanning every gateway and volume.
type GatewayReconcileConfig struct {
	PathPlanIntervalSeconds      int `yaml:"path_plan_interval_seconds"`
	LeaseTTLSeconds              int `yaml:"lease_ttl_seconds"`
	StatusRefreshIntervalSeconds int `yaml:"status_refresh_interval_seconds"`
	ChunkGCIntervalSeconds       int `yaml:"chunk_gc_interval_seconds"`
	ChunkGCBatchSize             int `yaml:"chunk_gc_batch_size"`
}

// GatewayDataplaneConfig holds admission limits and the token material, which
// moves from literal command-line values to references.
type GatewayDataplaneConfig struct {
	MaxInflightRequests int       `yaml:"max_inflight_requests"`
	MaxInflightBytes    int64     `yaml:"max_inflight_bytes"`
	MaxIOSize           int       `yaml:"max_io_size"`
	TokenKey            SecretRef `yaml:"token_key,omitempty"`
	SessionKey          SecretRef `yaml:"session_key,omitempty"`
	TokenTTLSeconds     int       `yaml:"token_ttl_seconds"`
	WireVersion         int       `yaml:"wire_version,omitempty"`
}

// ISCSIGatewayConfig covers namrbd-iscsi-gateway instance identity only.
//
// Target IQN, LUN ID, export ID, volume ID, export lease, and export epoch are
// absent by design. They are serving-map authority and come from the
// TiKV-backed sbs-service iSCSI registry (AA-IMPL-009). Today they are startup
// flags, which is why one process serves one export.
type ISCSIGatewayConfig struct {
	GatewayID string `yaml:"gateway_id"`

	// AdvertisePortals are the portal endpoints this instance publishes into
	// the etcd fleet registry (AA-IMPL-008).
	AdvertisePortals []string `yaml:"advertise_portals"`

	Etcd *EtcdConfig `yaml:"etcd,omitempty"`

	SBSEndpoint      string     `yaml:"sbs_endpoint"`
	SBSAdminEndpoint string     `yaml:"sbs_admin_endpoint"`
	SBSEndpointTLS   *TLSConfig `yaml:"sbs_endpoint_tls,omitempty"`

	Auth   ISCSIAuthConfig   `yaml:"auth"`
	Reload ISCSIReloadConfig `yaml:"reload"`

	Observability ObservabilityConfig `yaml:"observability"`

	// Dependency availability thresholds. Omitted means the shipped
	// defaults; see docs/phase-aa-entry-plan.md Section 4.
	Dependency *depavail.Thresholds `yaml:"dependency,omitempty"`
}

// ISCSIAuthConfig replaces --auth-mode, --chap-secret-ref, and
// --allowed-initiator-iqns.
type ISCSIAuthConfig struct {
	Mode                 string    `yaml:"mode"`
	CHAPSecret           SecretRef `yaml:"chap_secret,omitempty"`
	AllowedInitiatorIQNs []string  `yaml:"allowed_initiator_iqns,omitempty"`
}

// ISCSIReloadConfig is the policy for picking up serving-registry changes
// without a restart. AA-IMPL-011 implements the behavior; this is where an
// operator tunes it.
type ISCSIReloadConfig struct {
	Mode                 string `yaml:"mode"`
	PollIntervalSeconds  int    `yaml:"poll_interval_seconds,omitempty"`
	MaxExportsPerProcess int    `yaml:"max_exports_per_process"`
}

// Reload modes.
const (
	ReloadModeWatch = "watch"
	ReloadModePoll  = "poll"
	ReloadModeNone  = "none"
)

// SBSServiceConfig covers sbs-service.
type SBSServiceConfig struct {
	ClusterID    string `yaml:"cluster_id"`
	SBSClusterID string `yaml:"sbs_cluster_id"`
	NodeID       string `yaml:"node_id"`
	// MetadataBackend is the cluster-wide metadata authority. Production-like
	// profiles require TiKV; Pebble remains available only to development
	// fixtures that deliberately select the dev profile.
	MetadataBackend string `yaml:"metadata_backend,omitempty"`

	GRPCListen  string `yaml:"grpc_listen"`
	HTTPListen  string `yaml:"http_listen,omitempty"`
	PayloadRoot string `yaml:"payload_root,omitempty"`

	TiKV         TiKVConfig      `yaml:"tikv"`
	Leader       SBSLeaderConfig `yaml:"leader"`
	Health       SBSHealthConfig `yaml:"health"`
	WriteEffects SBSWriteEffects `yaml:"write_effects"`

	Observability ObservabilityConfig `yaml:"observability"`

	// Dependency availability thresholds. Omitted means the shipped
	// defaults; see docs/phase-aa-entry-plan.md Section 4.
	Dependency *depavail.Thresholds `yaml:"dependency,omitempty"`
}

// TiKVConfig is the SBS metadata authority connection and its scan budget.
type TiKVConfig struct {
	PDEndpoints    []string   `yaml:"pd_endpoints"`
	Keyspace       string     `yaml:"keyspace,omitempty"`
	APIVersion     string     `yaml:"api_version,omitempty"`
	TimeoutSeconds int        `yaml:"timeout_seconds"`
	TLS            *TLSConfig `yaml:"tls,omitempty"`
	// ScanPageSize and BatchGetSize are the AA-IMPL-003 budget knobs. They are
	// config, not constants, so an operator can lower them under pressure.
	ScanPageSize int `yaml:"scan_page_size"`
	BatchGetSize int `yaml:"batch_get_size"`
	// OperationTrace is verbose and off in production-like profiles.
	OperationTrace bool `yaml:"operation_trace"`
}

// SBSLeaderConfig holds the leader lease timings.
type SBSLeaderConfig struct {
	LeaseDurationSeconds int `yaml:"lease_duration_seconds"`
	RenewIntervalSeconds int `yaml:"renew_interval_seconds"`
}

// SBSHealthConfig is the bounded health reconciler from AA-IMPL-007. Sharding
// is what keeps health cost independent of cluster size.
type SBSHealthConfig struct {
	ShardCount              int `yaml:"shard_count"`
	ConcurrencyPerShard     int `yaml:"concurrency_per_shard"`
	IntervalSeconds         int `yaml:"interval_seconds"`
	TimeoutSeconds          int `yaml:"timeout_seconds"`
	SuspectThreshold        int `yaml:"suspect_threshold"`
	DownThreshold           int `yaml:"down_threshold"`
	RecoveryCooldownSeconds int `yaml:"recovery_cooldown_seconds"`
}

// SBSWriteEffects is the accepted Phase X product-default write-effect profile.
type SBSWriteEffects struct {
	ServiceOwned             bool `yaml:"service_owned"`
	NativeAllocationFastPath bool `yaml:"native_allocation_fast_path"`
	BatchMax                 int  `yaml:"batch_max"`
	LaneBucketCount          int  `yaml:"lane_bucket_count"`
	AsyncMutationFinalize    bool `yaml:"async_mutation_finalize"`
}

// SBSDataConfig covers sbs-data. StoreConfigPath keeps the existing
// --store-config file as an included document rather than absorbing it, so the
// current reload path stays intact.
type SBSDataConfig struct {
	ClusterID       string `yaml:"cluster_id,omitempty"`
	SBSClusterID    string `yaml:"sbs_cluster_id,omitempty"`
	NodeID          string `yaml:"node_id,omitempty"`
	DataPath        string `yaml:"data_path"`
	StoreConfigPath string `yaml:"store_config_path,omitempty"`
	GRPCListen      string `yaml:"grpc_listen"`
	HTTPListen      string `yaml:"http_listen,omitempty"`

	Observability ObservabilityConfig `yaml:"observability"`
}

// CSIDriverConfig covers namrbd-csi-driver. It is deployed per Kubernetes node,
// which is exactly the case where copied command lines drift.
type CSIDriverConfig struct {
	DriverName     string   `yaml:"driver_name"`
	NodeID         string   `yaml:"node_id,omitempty"`
	Endpoint       string   `yaml:"endpoint"`
	AdminEndpoints []string `yaml:"admin_endpoints"`
	ClusterID      string   `yaml:"cluster_id"`
	SBSClusterID   string   `yaml:"sbs_cluster_id"`
	GatewayURL     string   `yaml:"gateway_url,omitempty"`

	Observability ObservabilityConfig `yaml:"observability"`
}

// MCPConfig covers namrbd-mcp. Mode and approval policy are recorded here so an
// operator can see the posture without reading the unit file.
type MCPConfig struct {
	OperationsEndpoint string `yaml:"operations_endpoint"`
	Mode               string `yaml:"mode"`
	ApprovalPolicy     string `yaml:"approval_policy"`
	OperationOutputDir string `yaml:"operation_output_dir,omitempty"`
	HTTPTimeoutSeconds int    `yaml:"http_timeout_seconds"`

	Observability ObservabilityConfig `yaml:"observability"`
}

// MCP postures.
const (
	MCPModeObserve = "observe"
	MCPModeOperate = "operate"
)

// DependencyThresholds returns the configured thresholds for a process, or the
// shipped defaults when the section is absent.
//
// Returning defaults rather than a zero Thresholds matters: a zero grace would
// declare every momentary reconnect an outage, so an omitted section must mean
// "the shipped values" and never "no grace at all".
func DependencyThresholds(t *depavail.Thresholds) depavail.Thresholds {
	if t == nil {
		return depavail.DefaultThresholds()
	}
	return *t
}
