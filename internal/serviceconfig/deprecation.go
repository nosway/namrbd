package serviceconfig

import "sort"

// Deprecation records one flag that a config key replaces.
//
// The record exists so a flag is never removed silently. An operator upgrading
// needs to know what a flag became, when it stops working, and whether it still
// works in the meantime; a release note written after the fact cannot answer
// the middle question.
type Deprecation struct {
	Process string `json:"process"`
	Flag    string `json:"flag"`
	// ConfigKey is the dotted config path that replaces it. Empty means the
	// setting has no replacement, which is itself information: it is going away.
	ConfigKey string `json:"config_key"`
	// DeprecatedIn is the release where the flag started warning.
	DeprecatedIn string `json:"deprecated_in"`
	// RemovedIn is the release where the flag stops working. Empty means no
	// removal is scheduled yet.
	RemovedIn string `json:"removed_in"`
	// DevProfileOnly reports whether the flag still works in the dev profile
	// after deprecation. Lab fixtures depend on several of these.
	DevProfileOnly bool   `json:"dev_profile_only"`
	Note           string `json:"note,omitempty"`
}

// The release these deprecations were announced in. Config adoption lands after
// NAMRBD 1.0, so nothing here is removed in 1.0 itself.
const configAdoptionRelease = "post-1.0"

var deprecations = []Deprecation{
	// namrbd-gateway
	{ProcessGateway, "listen", "gateway.listen", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "data-listen", "gateway.data_listen", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "gateway-id", "gateway.gateway_id", configAdoptionRelease, "", false, "remains an override for per-node identity"},
	{ProcessGateway, "etcd-endpoints", "gateway.etcd.endpoints", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "etcd-root", "gateway.etcd.root", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "sbs-admin-endpoint", "gateway.sbs_admin_endpoint", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "tls-cert-file", "gateway.tls.cert_file", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "tls-key-file", "gateway.tls.key.file", configAdoptionRelease, "", false, ""},
	{ProcessGateway, "dataplane-token-key", "gateway.dataplane.token_key", configAdoptionRelease, "", false,
		"the flag takes literal key material; the config takes a reference, so the value must be moved into a file or variable"},
	{ProcessGateway, "dataplane-session-key", "gateway.dataplane.session_key", configAdoptionRelease, "", false,
		"the flag takes literal key material; the config takes a reference, so the value must be moved into a file or variable"},
	{ProcessGateway, "volumes", "", configAdoptionRelease, "", true,
		"volume membership comes from sbs-service; there is no config replacement"},
	{ProcessGateway, "sbs-cluster-replicas", "", configAdoptionRelease, "", true,
		"replica membership comes from the sbs-service registry; there is no config replacement"},

	// namrbd-iscsi-gateway
	{ProcessISCSIGateway, "iscsi-gateway-id", "iscsi_gateway.gateway_id", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "portal", "iscsi_gateway.advertise_portals", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "sbs-admin-endpoint", "iscsi_gateway.sbs_admin_endpoint", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "auth-mode", "iscsi_gateway.auth.mode", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "chap-secret-ref", "iscsi_gateway.auth.chap_secret.file", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "allowed-initiator-iqns", "iscsi_gateway.auth.allowed_initiator_iqns", configAdoptionRelease, "", false, ""},
	{ProcessISCSIGateway, "target-iqn", "", configAdoptionRelease, "", true,
		"serving maps come from the sbs-service iSCSI registry; there is no config replacement"},
	{ProcessISCSIGateway, "lun-id", "", configAdoptionRelease, "", true,
		"serving maps come from the sbs-service iSCSI registry; there is no config replacement"},
	{ProcessISCSIGateway, "export-id", "", configAdoptionRelease, "", true,
		"serving maps come from the sbs-service iSCSI registry; there is no config replacement"},
	{ProcessISCSIGateway, "volume-id", "", configAdoptionRelease, "", true,
		"the export-to-volume mapping comes from the registry; there is no config replacement"},

	// sbs-service
	{ProcessSBSService, "cluster-id", "sbs_service.cluster_id", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "sbs-cluster-id", "sbs_service.sbs_cluster_id", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "node-id", "sbs_service.node_id", configAdoptionRelease, "", false, "remains an override for per-node identity"},
	{ProcessSBSService, "grpc-listen", "sbs_service.grpc_listen", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "http-listen", "sbs_service.http_listen", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "tikv-pd-endpoints", "sbs_service.tikv.pd_endpoints", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "tikv-keyspace", "sbs_service.tikv.keyspace", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "tikv-key-file", "sbs_service.tikv.tls.key.file", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "leader-lease-duration", "sbs_service.leader.lease_duration_seconds", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "leader-renew-interval", "sbs_service.leader.renew_interval_seconds", configAdoptionRelease, "", false, ""},
	{ProcessSBSService, "write-effects-batch-max", "sbs_service.write_effects.batch_max", configAdoptionRelease, "", true, ""},
	{ProcessSBSService, "metadata-path", "", configAdoptionRelease, "", true,
		"the local metadata backend is a bootstrap development path; there is no config replacement"},

	// sbs-data
	{ProcessSBSData, "cluster-id", "sbs_data.cluster_id", configAdoptionRelease, "", false, "stable cluster identity belongs in the reviewed service config"},
	{ProcessSBSData, "sbs-cluster-id", "sbs_data.sbs_cluster_id", configAdoptionRelease, "", false, "stable SBS cluster identity belongs in the reviewed service config"},
	{ProcessSBSData, "node-id", "sbs_data.node_id", configAdoptionRelease, "", false, "remains an override for per-node identity"},
	{ProcessSBSData, "path", "sbs_data.data_path", configAdoptionRelease, "", false, ""},
	{ProcessSBSData, "store-config", "sbs_data.store_config_path", configAdoptionRelease, "", false,
		"the store layout document itself is unchanged; only the path moves"},
	{ProcessSBSData, "grpc-listen", "sbs_data.grpc_listen", configAdoptionRelease, "", false, ""},
	{ProcessSBSData, "http-listen", "sbs_data.http_listen", configAdoptionRelease, "", false, ""},
	{ProcessSBSData, "enable-lab-store-debug", "sbs_data.observability.debug_endpoints", configAdoptionRelease, "", true, ""},
	{ProcessSBSData, "data-operation-trace", "sbs_data.observability.trace", configAdoptionRelease, "", true, ""},

	// namrbd-csi-driver
	{ProcessCSIDriver, "driver-name", "csi_driver.driver_name", configAdoptionRelease, "", false, ""},
	{ProcessCSIDriver, "node-id", "csi_driver.node_id", configAdoptionRelease, "", false, "remains an override for per-node identity"},
	{ProcessCSIDriver, "endpoint", "csi_driver.endpoint", configAdoptionRelease, "", false, ""},
	{ProcessCSIDriver, "admin-endpoint", "csi_driver.admin_endpoints", configAdoptionRelease, "", false,
		"the singular flag becomes the first entry of the list"},
	{ProcessCSIDriver, "admin-endpoints", "csi_driver.admin_endpoints", configAdoptionRelease, "", false, ""},
	{ProcessCSIDriver, "cluster-id", "csi_driver.cluster_id", configAdoptionRelease, "", false, ""},
	{ProcessCSIDriver, "sbs-cluster-id", "csi_driver.sbs_cluster_id", configAdoptionRelease, "", false, ""},
	{ProcessCSIDriver, "gateway-url", "csi_driver.gateway_url", configAdoptionRelease, "", false, ""},

	// namrbd-mcp
	{ProcessMCP, "operations-endpoint", "mcp.operations_endpoint", configAdoptionRelease, "", false, ""},
	{ProcessMCP, "mode", "mcp.mode", configAdoptionRelease, "", false, ""},
	{ProcessMCP, "approval-policy", "mcp.approval_policy", configAdoptionRelease, "", false, ""},
	{ProcessMCP, "operation-output-dir", "mcp.operation_output_dir", configAdoptionRelease, "", false, ""},
	{ProcessMCP, "http-timeout", "mcp.http_timeout_seconds", configAdoptionRelease, "", false, ""},
}

// DeprecationsFor returns the records for one process, flag-sorted.
func DeprecationsFor(process string) []Deprecation {
	var out []Deprecation
	for _, d := range deprecations {
		if d.Process == process {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Flag < out[j].Flag })
	return out
}

// AllDeprecations returns every record, for the release-note package.
func AllDeprecations() []Deprecation {
	out := append([]Deprecation(nil), deprecations...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Process != out[j].Process {
			return out[i].Process < out[j].Process
		}
		return out[i].Flag < out[j].Flag
	})
	return out
}

// DeprecationFor finds one record.
func DeprecationFor(process, flag string) (Deprecation, bool) {
	for _, d := range deprecations {
		if d.Process == process && d.Flag == flag {
			return d, true
		}
	}
	return Deprecation{}, false
}
