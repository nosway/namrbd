package serviceconfig

import (
	"fmt"
	"github.com/nosway/namrbd/internal/depavail"
	"sort"
	"strings"
)

// Result carries every problem found in one config, so an operator fixes a file
// once rather than one error per restart.
type Result struct {
	Errors   []string
	Warnings []string
}

// OK reports whether the config is admissible.
func (r *Result) OK() bool { return len(r.Errors) == 0 }

func (r *Result) errf(format string, args ...any) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

func (r *Result) warnf(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// Validate checks a parsed config file against the schema contract.
//
// It enforces three things the flag-based world cannot: that the file declares
// which process it configures, that exactly one process block is present, and
// that no field carries a secret literal.
func Validate(f *File) *Result {
	r := &Result{}

	if f == nil {
		r.errf("config is empty")
		return r
	}
	if f.SchemaVersion != SchemaVersion {
		r.errf("schema_version is %d, this build understands %d", f.SchemaVersion, SchemaVersion)
	}
	if f.Revision <= 0 {
		r.errf("revision must be a positive integer; it is how an operator identifies which config a node is running")
	}
	switch f.Profile {
	case ProfileDev, ProfileLargeScale:
	case "":
		r.errf("profile is required; use %q or %q", ProfileDev, ProfileLargeScale)
	default:
		r.errf("profile %q is not one of %q or %q", f.Profile, ProfileDev, ProfileLargeScale)
	}

	blocks := map[string]bool{
		ProcessGateway:      f.Gateway != nil,
		ProcessISCSIGateway: f.ISCSIGetway != nil,
		ProcessSBSService:   f.SBSService != nil,
		ProcessSBSData:      f.SBSData != nil,
		ProcessCSIDriver:    f.CSIDriver != nil,
		ProcessMCP:          f.MCP != nil,
	}
	present := []string{}
	for name, ok := range blocks {
		if ok {
			present = append(present, name)
		}
	}
	sort.Strings(present)

	switch {
	case f.Process == "":
		r.errf("process is required; one of %s", strings.Join(sortedProcesses(), ", "))
	case !isKnownProcess(f.Process):
		r.errf("process %q is not a known long-running service", f.Process)
	}
	switch len(present) {
	case 0:
		r.errf("no process block is present")
	case 1:
		if f.Process != "" && present[0] != f.Process {
			r.errf("process is %q but the file carries a %q block", f.Process, present[0])
		}
	default:
		r.errf("config carries %d process blocks (%s); a config file configures exactly one process",
			len(present), strings.Join(present, ", "))
	}

	large := f.Profile == ProfileLargeScale

	if g := f.Gateway; g != nil {
		validateGateway(r, g, large)
	}
	if g := f.ISCSIGetway; g != nil {
		validateISCSIGateway(r, g, large)
	}
	if s := f.SBSService; s != nil {
		validateSBSService(r, s, large)
	}
	if d := f.SBSData; d != nil {
		validateSBSData(r, d, large)
	}
	if c := f.CSIDriver; c != nil {
		validateCSIDriver(r, c, large)
	}
	if m := f.MCP; m != nil {
		validateMCP(r, m, large)
	}
	return r
}

func sortedProcesses() []string {
	p := []string{ProcessGateway, ProcessISCSIGateway, ProcessSBSService,
		ProcessSBSData, ProcessCSIDriver, ProcessMCP}
	sort.Strings(p)
	return p
}

func isKnownProcess(name string) bool {
	for _, p := range sortedProcesses() {
		if p == name {
			return true
		}
	}
	return false
}

// requireLiteralFree rejects a plain string field that looks like it holds a
// secret value.
func requireLiteralFree(r *Result, field, value string) {
	if LooksLikeSecretLiteral(value) {
		r.errf("%s appears to hold a secret value; config files carry references, not secrets", field)
	}
}

func validateSecret(r *Result, field string, s SecretRef, requiredWhen bool) {
	if err := s.Validate(field); err != nil {
		r.errf("%s", err.Error())
	}
	if requiredWhen && s.Empty() {
		r.errf("%s is required here but names no source", field)
	}
}

func validateTLS(r *Result, field string, t *TLSConfig, large bool) {
	if t == nil || !t.Enable {
		return
	}
	if strings.TrimSpace(t.CertFile) == "" {
		r.errf("%s.cert_file is required when TLS is enabled", field)
	}
	requireLiteralFree(r, field+".cert_file", t.CertFile)
	validateSecret(r, field+".key", t.Key, true)
	_ = large
}

func validateObservability(r *Result, field string, o ObservabilityConfig, large bool) {
	if !large {
		return
	}
	// Verbose tracing and debug routes are a scale hazard and an exposure, so
	// the large_scale profile refuses them rather than trusting a runbook step.
	if o.Trace {
		r.errf("%s.trace must be false in the %s profile", field, ProfileLargeScale)
	}
	if o.DebugEndpoints {
		r.errf("%s.debug_endpoints must be false in the %s profile", field, ProfileLargeScale)
	}
}

// validateDependency checks the availability thresholds a process was given.
//
// An invalid set is an error rather than a silent fallback to defaults: an
// operator who wrote a grace of 0 meant something by it, and quietly serving
// them 300 instead would make the running behavior disagree with the reviewed
// file that produced it.
func validateDependency(r *Result, field string, t *depavail.Thresholds) {
	if t == nil {
		return
	}
	if err := t.Validate(); err != nil {
		r.errf("%s: %v", field, err)
	}
}

func validateGateway(r *Result, g *GatewayConfig, large bool) {
	if strings.TrimSpace(g.GatewayID) == "" {
		r.errf("gateway.gateway_id is required")
	}
	if strings.TrimSpace(g.Listen) == "" {
		r.errf("gateway.listen is required")
	}
	if strings.TrimSpace(g.SBSAdminEndpoint) == "" {
		r.errf("gateway.sbs_admin_endpoint is required; gateways consume published views, not raw SBS metadata")
	}
	validateTLS(r, "gateway.tls", g.TLS, large)
	validateSecret(r, "gateway.dataplane.token_key", g.Dataplane.TokenKey, false)
	validateSecret(r, "gateway.dataplane.session_key", g.Dataplane.SessionKey, false)
	validateObservability(r, "gateway.observability", g.Observability, large)
	validateDependency(r, "gateway.dependency", g.Dependency)

	if g.Cache.ChunkIDAllocationCacheSize < 0 {
		r.errf("gateway.cache.chunk_id_allocation_cache_size must not be negative")
	}
	if g.Reconcile.PathPlanIntervalSeconds <= 0 {
		r.errf("gateway.reconcile.path_plan_interval_seconds must be positive")
	}
	leaseTTLSeconds := g.Reconcile.LeaseTTLSeconds
	if leaseTTLSeconds == 0 {
		leaseTTLSeconds = 15
	}
	statusRefreshSeconds := g.Reconcile.StatusRefreshIntervalSeconds
	if statusRefreshSeconds == 0 {
		statusRefreshSeconds = 5
	}
	if leaseTTLSeconds < 0 {
		r.errf("gateway.reconcile.lease_ttl_seconds must not be negative")
	}
	if statusRefreshSeconds < 0 || statusRefreshSeconds >= leaseTTLSeconds {
		r.errf("gateway.reconcile.status_refresh_interval_seconds must be non-negative and shorter than lease_ttl_seconds")
	}
	if g.Dataplane.MaxInflightRequests <= 0 {
		r.errf("gateway.dataplane.max_inflight_requests must be positive")
	}

	if large {
		if g.Etcd == nil || len(g.Etcd.Endpoints) == 0 {
			r.errf("gateway.etcd.endpoints is required in the %s profile; fleet membership is etcd-backed", ProfileLargeScale)
		}
		if strings.TrimSpace(g.Etcd.GetRoot()) == "" {
			r.errf("gateway.etcd.root is required in the %s profile so products and environments stay partitioned", ProfileLargeScale)
		}
	}
}

// GetRoot tolerates a nil receiver so validation can report a missing root
// without a preceding nil check at every call site.
func (e *EtcdConfig) GetRoot() string {
	if e == nil {
		return ""
	}
	return e.Root
}

func validateISCSIGateway(r *Result, g *ISCSIGatewayConfig, large bool) {
	if strings.TrimSpace(g.GatewayID) == "" {
		r.errf("iscsi_gateway.gateway_id is required")
	}
	if strings.TrimSpace(g.SBSAdminEndpoint) == "" {
		r.errf("iscsi_gateway.sbs_admin_endpoint is required; the serving registry is the mapping authority")
	}
	if len(g.AdvertisePortals) == 0 {
		r.errf("iscsi_gateway.advertise_portals is required so the fleet registry can publish reachable portals")
	}
	validateTLS(r, "iscsi_gateway.sbs_endpoint_tls", g.SBSEndpointTLS, large)
	validateSecret(r, "iscsi_gateway.auth.chap_secret", g.Auth.CHAPSecret, g.Auth.Mode == "chap")
	validateObservability(r, "iscsi_gateway.observability", g.Observability, large)
	validateDependency(r, "iscsi_gateway.dependency", g.Dependency)

	switch g.Reload.Mode {
	case ReloadModeWatch, ReloadModePoll, ReloadModeNone:
	case "":
		r.errf("iscsi_gateway.reload.mode is required; use watch, poll, or none")
	default:
		r.errf("iscsi_gateway.reload.mode %q is not watch, poll, or none", g.Reload.Mode)
	}
	if g.Reload.Mode == ReloadModePoll && g.Reload.PollIntervalSeconds <= 0 {
		r.errf("iscsi_gateway.reload.poll_interval_seconds must be positive when mode is poll")
	}
	if g.Reload.MaxExportsPerProcess <= 0 {
		r.errf("iscsi_gateway.reload.max_exports_per_process must be positive")
	}

	if large {
		if g.Etcd == nil || len(g.Etcd.Endpoints) == 0 {
			r.errf("iscsi_gateway.etcd.endpoints is required in the %s profile; fleet health is etcd-backed", ProfileLargeScale)
		}
		if strings.TrimSpace(g.Etcd.GetRoot()) == "" {
			r.errf("iscsi_gateway.etcd.root is required in the %s profile so the iSCSI fleet stays separate from block gateways", ProfileLargeScale)
		}
		if g.Reload.Mode == ReloadModeNone {
			r.errf("iscsi_gateway.reload.mode must not be none in the %s profile; every mapping change would need a restart", ProfileLargeScale)
		}
		// t2_large places 1000 exports across 32 iSCSI gateways.
		if g.Reload.MaxExportsPerProcess < 32 {
			r.errf("iscsi_gateway.reload.max_exports_per_process is %d; the %s profile needs at least 32 to place 1000 exports across 32 gateways",
				g.Reload.MaxExportsPerProcess, ProfileLargeScale)
		}
	}
}

func validateSBSService(r *Result, s *SBSServiceConfig, large bool) {
	for field, v := range map[string]string{
		"sbs_service.cluster_id":     s.ClusterID,
		"sbs_service.sbs_cluster_id": s.SBSClusterID,
		"sbs_service.node_id":        s.NodeID,
		"sbs_service.grpc_listen":    s.GRPCListen,
	} {
		if strings.TrimSpace(v) == "" {
			r.errf("%s is required", field)
		}
	}
	validateTLS(r, "sbs_service.tikv.tls", s.TiKV.TLS, large)
	validateObservability(r, "sbs_service.observability", s.Observability, large)
	validateDependency(r, "sbs_service.dependency", s.Dependency)

	if len(s.TiKV.PDEndpoints) == 0 {
		r.errf("sbs_service.tikv.pd_endpoints is required")
	}
	if s.TiKV.TimeoutSeconds <= 0 {
		r.errf("sbs_service.tikv.timeout_seconds must be positive")
	}
	if s.Leader.LeaseDurationSeconds <= 0 || s.Leader.RenewIntervalSeconds <= 0 {
		r.errf("sbs_service.leader lease and renew intervals must be positive")
	}
	if s.Leader.RenewIntervalSeconds >= s.Leader.LeaseDurationSeconds {
		r.errf("sbs_service.leader.renew_interval_seconds (%d) must be shorter than lease_duration_seconds (%d), or the lease expires before it renews",
			s.Leader.RenewIntervalSeconds, s.Leader.LeaseDurationSeconds)
	}
	if s.Health.SuspectThreshold > 0 && s.Health.DownThreshold > 0 &&
		s.Health.DownThreshold <= s.Health.SuspectThreshold {
		r.errf("sbs_service.health.down_threshold (%d) must exceed suspect_threshold (%d)",
			s.Health.DownThreshold, s.Health.SuspectThreshold)
	}

	if large {
		if strings.TrimSpace(s.MetadataBackend) != "tikv" {
			r.errf("sbs_service.metadata_backend must be tikv in the %s profile", ProfileLargeScale)
		}
		// AA-IMPL-003 budgets.
		if s.TiKV.ScanPageSize <= 0 || s.TiKV.ScanPageSize > 512 {
			r.errf("sbs_service.tikv.scan_page_size is %d; the %s profile bounds it to 1..512", s.TiKV.ScanPageSize, ProfileLargeScale)
		}
		if s.TiKV.BatchGetSize <= 0 || s.TiKV.BatchGetSize > 128 {
			r.errf("sbs_service.tikv.batch_get_size is %d; the %s profile bounds it to 1..128", s.TiKV.BatchGetSize, ProfileLargeScale)
		}
		if s.TiKV.OperationTrace {
			r.errf("sbs_service.tikv.operation_trace must be false in the %s profile", ProfileLargeScale)
		}
		// The write-effects batch decides how many keys a commit read asks for:
		// two per item, a volume state key and an idempotency key. Setting a
		// batch larger than the BatchGet bound allows means every commit is
		// split into chunks, which is safe but means the two budgets were set
		// against each other rather than together.
		if s.WriteEffects.BatchMax > 0 && s.TiKV.BatchGetSize > 0 &&
			s.WriteEffects.BatchMax*2 > s.TiKV.BatchGetSize {
			r.errf("sbs_service.write_effects.batch_max is %d, which asks for %d keys per commit read, "+
				"above the tikv.batch_get_size bound of %d; lower the batch or raise the bound so the two agree",
				s.WriteEffects.BatchMax, s.WriteEffects.BatchMax*2, s.TiKV.BatchGetSize)
		}
		// AA-IMPL-007 requires sharded health at >= ceil(nodes/25); 100 nodes
		// means at least 4 shards.
		if s.Health.ShardCount < 4 {
			r.errf("sbs_service.health.shard_count is %d; the %s profile needs at least 4 shards at 100 nodes",
				s.Health.ShardCount, ProfileLargeScale)
		}
		if s.Health.ConcurrencyPerShard <= 0 || s.Health.ConcurrencyPerShard > 16 {
			r.errf("sbs_service.health.concurrency_per_shard is %d; the %s profile bounds it to 1..16",
				s.Health.ConcurrencyPerShard, ProfileLargeScale)
		}
	}
}

func validateSBSData(r *Result, d *SBSDataConfig, large bool) {
	if large && strings.TrimSpace(d.ClusterID) == "" {
		r.errf("sbs_data.cluster_id is required in the %s profile", ProfileLargeScale)
	}
	if large && strings.TrimSpace(d.SBSClusterID) == "" {
		r.errf("sbs_data.sbs_cluster_id is required in the %s profile", ProfileLargeScale)
	}
	if large && strings.TrimSpace(d.NodeID) == "" {
		r.errf("sbs_data.node_id is required in the %s profile", ProfileLargeScale)
	}
	if strings.TrimSpace(d.DataPath) == "" {
		r.errf("sbs_data.data_path is required")
	}
	if strings.TrimSpace(d.GRPCListen) == "" {
		r.errf("sbs_data.grpc_listen is required")
	}
	validateObservability(r, "sbs_data.observability", d.Observability, large)
}

func validateCSIDriver(r *Result, c *CSIDriverConfig, large bool) {
	if strings.TrimSpace(c.DriverName) == "" {
		r.errf("csi_driver.driver_name is required")
	}
	if strings.TrimSpace(c.Endpoint) == "" {
		r.errf("csi_driver.endpoint is required")
	}
	if len(c.AdminEndpoints) == 0 {
		r.errf("csi_driver.admin_endpoints is required")
	}
	validateObservability(r, "csi_driver.observability", c.Observability, large)
	if large && len(c.AdminEndpoints) < 2 {
		r.warnf("csi_driver.admin_endpoints lists one endpoint; a single admin endpoint is a availability single point at scale")
	}
}

func validateMCP(r *Result, m *MCPConfig, large bool) {
	if strings.TrimSpace(m.OperationsEndpoint) == "" {
		r.errf("mcp.operations_endpoint is required")
	}
	switch m.Mode {
	case MCPModeObserve, MCPModeOperate:
	case "":
		r.errf("mcp.mode is required; use observe or operate")
	default:
		r.errf("mcp.mode %q is not observe or operate", m.Mode)
	}
	if strings.TrimSpace(m.ApprovalPolicy) == "" {
		r.errf("mcp.approval_policy is required")
	}
	validateObservability(r, "mcp.observability", m.Observability, large)
	if large && m.Mode == MCPModeOperate {
		// The current supported MCP surface is read-only. Operate posture has no support
		// evidence, so the strict profile refuses it rather than leaving it to
		// a review step.
		r.errf("mcp.mode %q is not admissible in the %s profile; MCP support evidence is read-only", MCPModeOperate, ProfileLargeScale)
	}
}
