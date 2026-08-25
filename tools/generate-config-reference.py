#!/usr/bin/env python3
"""Generate the canonical daemon configuration reference.

The reference is deliberately source-derived rather than a transcription of
the example YAML files.  It keeps three values separate:

* the value produced by decoding an omitted YAML key (normally a Go zero value),
* the daemon's no-config built-in/flag default, and
* the value chosen by the checked-in large_scale example.

It also records whether an accepted schema key is consumed by the current
startup path.  The serviceconfig reload classifier is a design contract only:
no shipped daemon currently calls serviceconfig.Reload in production.

Only the Python standard library is used.  Go struct tags, flag registrations,
override registries, examples, reload policies, and the explicit process/file
inventory are checked before Markdown is written or compared.
"""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
import difflib
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "reference" / "config"
PUBLIC_OUTPUT_DIR = ROOT / "docs-src" / "reference" / "config"

SCHEMA_SOURCES = (
    ROOT / "internal" / "serviceconfig" / "schema.go",
    ROOT / "internal" / "serviceconfig" / "secret.go",
    ROOT / "internal" / "depavail" / "thresholds.go",
)
REGISTRY_SOURCE = ROOT / "internal" / "serviceconfig" / "registry.go"
VALIDATION_SOURCE = ROOT / "internal" / "serviceconfig" / "validate.go"
LOADER_SOURCE = ROOT / "internal" / "serviceconfig" / "loader.go"
RELOAD_SOURCE = ROOT / "internal" / "serviceconfig" / "reload.go"


@dataclass(frozen=True)
class ProcessSpec:
    binary: str
    root_key: str
    struct_name: str
    config_file: str
    adoption_file: str


PROCESSES = (
    ProcessSpec("namrbd-gateway", "gateway", "GatewayConfig", "namrbd-gateway.yaml", "cmd/namrbd-gateway/serviceconfig_adoption.go"),
    ProcessSpec("namrbd-iscsi-gateway", "iscsi_gateway", "ISCSIGatewayConfig", "namrbd-iscsi-gateway.yaml", "cmd/namrbd-iscsi-gateway/serviceconfig_adoption.go"),
    ProcessSpec("sbs-service", "sbs_service", "SBSServiceConfig", "sbs-service.yaml", "cmd/sbs-service/serviceconfig_adoption.go"),
    ProcessSpec("sbs-data", "sbs_data", "SBSDataConfig", "sbs-data.yaml", "cmd/sbs-data/serviceconfig_adoption.go"),
    ProcessSpec("namrbd-csi-driver", "csi_driver", "CSIDriverConfig", "namrbd-csi-driver.yaml", "cmd/namrbd-csi-driver/serviceconfig_adoption.go"),
    ProcessSpec("namrbd-mcp", "mcp", "MCPConfig", "namrbd-mcp.yaml", "cmd/namrbd-mcp/serviceconfig_adoption.go"),
)
PROCESS_BY_BINARY = {item.binary: item for item in PROCESSES}
PROCESS_CONSTANTS = {
    "ProcessGateway": "namrbd-gateway",
    "ProcessISCSIGateway": "namrbd-iscsi-gateway",
    "ProcessSBSService": "sbs-service",
    "ProcessSBSData": "sbs-data",
    "ProcessCSIDriver": "namrbd-csi-driver",
    "ProcessMCP": "namrbd-mcp",
}


@dataclass(frozen=True)
class StructField:
    go_name: str
    go_type: str
    yaml_name: str
    omitempty: bool
    source: Path
    line: int


@dataclass(frozen=True)
class LeafField:
    path: str
    go_type: str
    source: Path
    line: int
    optional_parent: str


@dataclass(frozen=True)
class Binding:
    flags: tuple[str, ...] = ()
    status: str = "applied at startup"
    note: str = ""


# Runtime bindings are explicit on purpose.  A new schema leaf must either be
# covered here or by ACCEPTED_ONLY below; otherwise generation fails instead of
# quietly calling the new key supported.
BINDINGS: dict[str, Binding] = {}


def add_bindings(prefix: str, pairs: dict[str, str | tuple[str, ...]], *, status: str = "applied at startup") -> None:
    for suffix, flags in pairs.items():
        if isinstance(flags, str):
            flag_tuple = (flags,) if flags else ()
        else:
            flag_tuple = flags
        BINDINGS[f"{prefix}.{suffix}"] = Binding(flag_tuple, status)


add_bindings("gateway", {
    "gateway_id": "gateway-id", "listen": "control-http-listen", "data_listen": "data-listen",
    "advertise_control_address": "advertise-control-address", "advertise_data_address": "advertise-data-address",
    "data_disable": "data-disable", "tls.enable": "tls-enable", "tls.cert_file": "tls-cert-file",
    "tls.key": "tls-key-file", "tls.server_name": "tls-server-name", "etcd.endpoints": "etcd-endpoints",
    "etcd.root": "etcd-root", "sbs_admin_endpoint": "sbs-service-endpoint", "metadata_backend": "metadata-backend",
    "data_backend_mode": "data-backend-mode", "cache.volume_ttl_seconds": "volume-cache-ttl",
    "cache.zero_evidence_ttl_seconds": "sbs-zero-evidence-cache-ttl", "cache.open_reuse_ttl_seconds": "sbs-open-reuse-ttl",
    "cache.chunk_id_allocation_cache_size": "sbs-chunk-id-allocation-cache-size",
    "cache.write_plan_ttl_seconds": "sbs-write-plan-cache-ttl",
    "cache.begin_write_volume_state_ttl_seconds": "sbs-begin-write-volume-state-cache-ttl",
    "reconcile.path_plan_interval_seconds": "path-plan-reconcile-interval", "reconcile.lease_ttl_seconds": "gateway-lease-ttl",
    "reconcile.status_refresh_interval_seconds": "gateway-status-refresh-interval",
    "reconcile.chunk_gc_interval_seconds": "chunk-gc-interval", "reconcile.chunk_gc_batch_size": "chunk-gc-batch-size",
    "dataplane.max_inflight_requests": "max-inflight-requests", "dataplane.max_inflight_bytes": "max-inflight-bytes",
    "dataplane.max_io_size": "max-io-size", "dataplane.token_key": "dataplane-token-key",
    "dataplane.session_key": "dataplane-session-key", "dataplane.token_ttl_seconds": "dataplane-token-ttl",
    "dataplane.wire_version": "dataplane-wire-version", "observability.trace": "dataplane-request-trace",
    "dependency": "",
})

add_bindings("iscsi_gateway", {
    "gateway_id": "iscsi-gateway-id", "advertise_portals": "portal", "etcd.endpoints": "", "etcd.root": "",
    "sbs_endpoint": "sbs-data-endpoint", "sbs_admin_endpoint": "sbs-service-endpoint",
    "sbs_endpoint_tls.enable": "sbs-endpoint-tls", "sbs_endpoint_tls.server_name": "sbs-endpoint-server-name",
    "auth.mode": "auth-mode", "auth.chap_secret.file": "chap-secret-ref",
    "auth.allowed_initiator_iqns": "allowed-initiator-iqns", "reload.poll_interval_seconds": "",
    "reload.max_exports_per_process": "", "observability.listen": "observability-listen", "dependency": "",
})

add_bindings("sbs_service", {
    "cluster_id": "cluster-id", "sbs_cluster_id": "sbs-cluster-id", "node_id": "node-id",
    "metadata_backend": "metadata-backend", "grpc_listen": "sbs-service-listen", "http_listen": "sbs-service-http-listen",
    "payload_root": "payload-root", "tikv.pd_endpoints": "tikv-pd-endpoints", "tikv.keyspace": "tikv-keyspace",
    "tikv.api_version": "tikv-api-version", "tikv.timeout_seconds": "tikv-timeout",
    "tikv.tls.enable": "tikv-tls-enabled", "tikv.tls.cert_file": "tikv-cert-file", "tikv.tls.key.file": "tikv-key-file",
    "tikv.operation_trace": "tikv-operation-trace", "leader.lease_duration_seconds": "leader-lease-duration",
    "leader.renew_interval_seconds": "leader-renew-interval", "health.shard_count": "",
    "health.concurrency_per_shard": "", "health.interval_seconds": "", "health.timeout_seconds": "",
    "health.suspect_threshold": "", "health.down_threshold": "", "health.recovery_cooldown_seconds": "",
    "write_effects.service_owned": "service-owned-write-effects",
    "write_effects.native_allocation_fast_path": "native-allocation-fast-path",
    "write_effects.batch_max": "write-effects-batch-max", "write_effects.lane_bucket_count": "write-effects-lane-bucket-count",
    "write_effects.async_mutation_finalize": "async-write-mutation-finalize", "dependency": "",
})

add_bindings("sbs_data", {
    "cluster_id": "cluster-id", "sbs_cluster_id": "sbs-cluster-id", "node_id": "node-id", "data_path": "path",
    "store_config_path": "store-config", "grpc_listen": "sbs-data-listen", "http_listen": "sbs-data-http-listen",
    "observability.trace": "data-operation-trace", "observability.debug_endpoints": "enable-lab-store-debug",
})

add_bindings("csi_driver", {
    "driver_name": "driver-name", "node_id": "node-id", "endpoint": "endpoint",
    "admin_endpoints": ("admin-endpoint", "admin-endpoints"), "cluster_id": "cluster-id",
    "sbs_cluster_id": "sbs-cluster-id", "gateway_url": "gateway-url",
})

add_bindings("mcp", {
    "operations_endpoint": "operations-endpoint", "mode": "mode", "approval_policy": "approval-policy",
    "operation_output_dir": "operation-output-dir", "http_timeout_seconds": "http-timeout",
})

# Known precedence exceptions in the compatibility adoption layer.  These are
# not allowlisted post-file overrides even though the binary still has flags.
for _path in ("sbs_data.cluster_id", "sbs_data.sbs_cluster_id"):
    _binding = BINDINGS[_path]
    BINDINGS[_path] = Binding(
        _binding.flags,
        "applied at startup; a non-empty YAML value replaces environment-backed and explicitly typed flag values",
    )
_binding = BINDINGS["csi_driver.driver_name"]
BINDINGS["csi_driver.driver_name"] = Binding(
    _binding.flags,
    "applied at startup; a non-empty YAML value replaces an explicitly typed --driver-name",
)
_binding = BINDINGS["iscsi_gateway.sbs_endpoint_tls.enable"]
BINDINGS["iscsi_gateway.sbs_endpoint_tls.enable"] = Binding(
    _binding.flags,
    "applied at startup; when the TLS block is present, the YAML boolean replaces even an explicitly typed --sbs-endpoint-tls",
)
_binding = BINDINGS["iscsi_gateway.auth.chap_secret.file"]
BINDINGS["iscsi_gateway.auth.chap_secret.file"] = Binding(
    _binding.flags,
    "passed through as a file path without the shared secret Resolver; Community target startup then rejects CHAP",
)
for _path, _status in {
    "gateway.observability.trace": (
        "applied from YAML unless explicitly typed --dataplane-request-trace is preserved after file validation; "
        "true can currently bypass the large_scale false rule"
    ),
    "gateway.reconcile.path_plan_interval_seconds": (
        "applied from YAML unless explicitly typed --path-plan-reconcile-interval is preserved after file validation; "
        "0s can currently bypass the positive YAML rule and disable the worker"
    ),
    "gateway.reconcile.lease_ttl_seconds": (
        "applied to the control config at startup; a YAML value of 0 is normalized by runtime behavior to 15 seconds"
    ),
    "gateway.reconcile.status_refresh_interval_seconds": (
        "applied to the control config at startup; a YAML value of 0 is normalized by runtime behavior to 5 seconds"
    ),
    "gateway.dataplane.max_io_size": (
        "negative YAML is ignored; 0 reaches the control config but dataplane construction restores dataplane.DefaultMaxIOSize (4128768); positive values apply"
    ),
    "gateway.dataplane.token_ttl_seconds": (
        "negative YAML is ignored; 0 reaches the control config but token issuance normalizes it to 5 minutes; positive values apply"
    ),
}.items():
    _binding = BINDINGS[_path]
    BINDINGS[_path] = Binding(_binding.flags, _status)
for _path in ("sbs_data.observability.trace", "sbs_data.observability.debug_endpoints"):
    _binding = BINDINGS[_path]
    BINDINGS[_path] = Binding(
        _binding.flags,
        "applied at startup; an environment-backed flag default is preserved after YAML validation and can currently bypass the large_scale false rule",
    )

_sbs_service_post_validation_env = {
    "sbs_service.cluster_id", "sbs_service.sbs_cluster_id", "sbs_service.metadata_backend",
    "sbs_service.payload_root", "sbs_service.tikv.pd_endpoints", "sbs_service.tikv.keyspace",
    "sbs_service.tikv.api_version", "sbs_service.tikv.timeout_seconds", "sbs_service.tikv.tls.enable",
    "sbs_service.tikv.tls.cert_file", "sbs_service.tikv.tls.key.file", "sbs_service.tikv.operation_trace",
    "sbs_service.leader.lease_duration_seconds", "sbs_service.leader.renew_interval_seconds",
    "sbs_service.health.shard_count", "sbs_service.health.concurrency_per_shard",
    "sbs_service.health.interval_seconds", "sbs_service.health.timeout_seconds",
    "sbs_service.health.suspect_threshold", "sbs_service.health.down_threshold",
    "sbs_service.health.recovery_cooldown_seconds", "sbs_service.write_effects.service_owned",
    "sbs_service.write_effects.native_allocation_fast_path", "sbs_service.write_effects.async_mutation_finalize",
}
for _path in _sbs_service_post_validation_env:
    _binding = BINDINGS[_path]
    BINDINGS[_path] = Binding(
        _binding.flags,
        "applied at startup; direct environment/legacy CLI state can be preserved after YAML validation (known precedence drift)",
    )
for _path in ("sbs_service.write_effects.batch_max", "sbs_service.write_effects.lane_bucket_count"):
    _binding = BINDINGS[_path]
    BINDINGS[_path] = Binding(
        _binding.flags,
        "applied at startup; positive YAML replaces the environment-created initial default; explicit CLI wins in dev and is rejected in large_scale",
    )


# Keys accepted by yaml.v3/serviceconfig.Validate but not fully consumed by the
# current process startup path.  Prefix matching is intentional for object
# fields such as etcd.tls and observability.
ACCEPTED_ONLY: dict[str, tuple[str, str]] = {
    "gateway.etcd.tls": ("accepted but not applied", "the gateway has no separate etcd TLS runtime binding"),
    "gateway.tls.key.env": ("rejected when set", "the gateway TLS startup binding currently accepts only a file reference"),
    "gateway.tls.key.kms": ("rejected when set", "the gateway TLS startup binding currently accepts only a file reference"),
    "gateway.dataplane.token_key.kms": ("rejected when set", "KMS secret resolution is not implemented in this build"),
    "gateway.dataplane.session_key.kms": ("rejected when set", "KMS secret resolution is not implemented in this build"),
    "gateway.observability.listen": ("rejected when set", "observability is served on the control listener"),
    "gateway.observability.debug_endpoints": ("accepted but not applied", "the gateway exposes no separately gated debug listener"),
    "iscsi_gateway.etcd.tls": ("accepted but not applied", "the iSCSI fleet client has no etcd TLS runtime binding"),
    "iscsi_gateway.sbs_endpoint_tls.cert_file": ("accepted but not applied", "the process has no client-certificate flag"),
    "iscsi_gateway.sbs_endpoint_tls.key": ("accepted but not applied", "the process has no client-key binding"),
    "iscsi_gateway.auth.chap_secret.env": ("rejected when set", "the current process accepts only a file reference"),
    "iscsi_gateway.auth.chap_secret.kms": ("rejected when set", "the current process accepts only a file reference"),
    "iscsi_gateway.reload.mode": ("accepted but not applied", "the value is copied but the current runtime does not select behavior from it"),
    "iscsi_gateway.observability.trace": ("accepted but not applied", "the process has no trace toggle"),
    "iscsi_gateway.observability.debug_endpoints": ("accepted but not applied", "the process has no separately gated debug listener"),
    "sbs_service.tikv.scan_page_size": ("accepted/validated but not applied", "no current daemon flag or runtime consumer is wired"),
    "sbs_service.tikv.batch_get_size": ("accepted/validated but not applied", "no current daemon flag or runtime consumer is wired"),
    "sbs_service.tikv.tls.key.env": ("accepted but not applied", "the TiKV client currently consumes only a key-file path"),
    "sbs_service.tikv.tls.key.kms": ("accepted but not applied", "the TiKV client currently consumes only a key-file path"),
    "sbs_service.tikv.tls.server_name": ("accepted but not applied", "the TiKV client has no server-name binding"),
    "sbs_service.observability": ("accepted but not applied", "observability uses the HTTP listener and has no config bindings"),
    "sbs_data.observability.listen": ("accepted but not applied", "observability is served on sbs_data.http_listen"),
    "csi_driver.observability": ("accepted but not applied", "the current CSI driver exposes no matching config bindings"),
    "mcp.observability": ("accepted but not applied", "the current MCP server exposes no matching config bindings"),
}


# Environment variables used as pre-config flag defaults.  They are effective
# overrides only where the adoption path explicitly preserves them.  Registry
# overrides are parsed separately and take precedence over these entries.
ENV_FIELDS: dict[str, tuple[str, ...]] = {
    "sbs_service.cluster_id": ("NAMRBD_CLUSTER_ID",),
    "sbs_service.sbs_cluster_id": ("NAMRBD_SBS_CLUSTER_ID",),
    "sbs_service.node_id": ("NAMRBD_SBS_SERVICE_NODE_ID", "NAMRBD_NODE_ID (legacy)"),
    "sbs_service.metadata_backend": ("NAMRBD_SBS_METADATA_BACKEND",),
    "sbs_service.grpc_listen": ("NAMRBD_SBS_SERVICE_GRPC_LISTEN", "NAMRBD_SBS_ADMIN_ADDR (legacy)"),
    "sbs_service.http_listen": ("NAMRBD_SBS_SERVICE_HTTP_LISTEN", "NAMRBD_BIND_ADDR (legacy)"),
    "sbs_service.payload_root": ("NAMRBD_SBS_PAYLOAD_ROOT",),
    "sbs_service.tikv.pd_endpoints": ("NAMRBD_TIKV_PD_ENDPOINTS",),
    "sbs_service.tikv.keyspace": ("NAMRBD_TIKV_KEYSPACE",),
    "sbs_service.tikv.api_version": ("NAMRBD_TIKV_API_VERSION",),
    "sbs_service.tikv.timeout_seconds": ("NAMRBD_TIKV_TIMEOUT",),
    "sbs_service.tikv.tls.enable": ("NAMRBD_TIKV_TLS_ENABLED",),
    "sbs_service.tikv.tls.cert_file": ("NAMRBD_CERT_FILE",),
    "sbs_service.tikv.tls.key.file": ("NAMRBD_KEY_FILE",),
    "sbs_service.tikv.operation_trace": ("NAMRBD_TIKV_OPERATION_TRACE",),
    "sbs_service.leader.lease_duration_seconds": ("NAMRBD_SBS_LEADER_LEASE_DURATION",),
    "sbs_service.leader.renew_interval_seconds": ("NAMRBD_SBS_LEADER_RENEW_INTERVAL",),
    "sbs_service.health.shard_count": ("NAMRBD_SBS_DATA_HEALTH_SHARD_COUNT",),
    "sbs_service.health.concurrency_per_shard": ("NAMRBD_SBS_DATA_HEALTH_CONCURRENCY",),
    "sbs_service.health.interval_seconds": ("NAMRBD_SBS_DATA_HEALTH_CHECK_INTERVAL",),
    "sbs_service.health.timeout_seconds": ("NAMRBD_SBS_DATA_HEALTH_TIMEOUT",),
    "sbs_service.health.suspect_threshold": ("NAMRBD_SBS_DATA_SUSPECT_AFTER",),
    "sbs_service.health.down_threshold": ("NAMRBD_SBS_DATA_DOWN_AFTER",),
    "sbs_service.health.recovery_cooldown_seconds": ("NAMRBD_SBS_DATA_RECOVER_COOLDOWN",),
    "sbs_service.write_effects.service_owned": ("NAMRBD_SBS_SERVICE_OWNED_WRITE_EFFECTS",),
    "sbs_service.write_effects.native_allocation_fast_path": ("NAMRBD_SBS_NATIVE_ALLOCATION_FAST_PATH",),
    "sbs_service.write_effects.batch_max": ("NAMRBD_SBS_WRITE_EFFECTS_BATCH_MAX (initial default; nonzero YAML replaces it)",),
    "sbs_service.write_effects.lane_bucket_count": ("NAMRBD_SBS_WRITE_EFFECTS_LANE_BUCKET_COUNT (initial default; nonzero YAML replaces it)",),
    "sbs_service.write_effects.async_mutation_finalize": ("NAMRBD_SBS_ASYNC_WRITE_MUTATION_FINALIZE",),
    "sbs_data.cluster_id": ("NAMRBD_CLUSTER_ID (initial default; nonempty YAML replaces it)",),
    "sbs_data.sbs_cluster_id": ("NAMRBD_SBS_CLUSTER_ID (initial default; nonempty YAML replaces it)",),
    "sbs_data.data_path": ("NAMRBD_SBS_DATA_PATH", "NAMRBD_SBS_DATA_DIR (legacy)"),
    "sbs_data.store_config_path": ("NAMRBD_SBS_STORE_CONFIG",),
    "sbs_data.grpc_listen": ("NAMRBD_SBS_DATA_GRPC_LISTEN", "NAMRBD_SBS_GRPC_ADDR (legacy)"),
    "sbs_data.http_listen": ("NAMRBD_SBS_DATA_HTTP_LISTEN", "NAMRBD_BIND_ADDR (legacy)"),
    "sbs_data.observability.trace": ("NAMRBD_SBS_DATA_OPERATION_TRACE",),
    "sbs_data.observability.debug_endpoints": ("NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG",),
    "csi_driver.cluster_id": ("NAMRBD_CLUSTER_ID",),
    "csi_driver.sbs_cluster_id": ("NAMRBD_SBS_CLUSTER_ID", "SBS_CLUSTER_ID (legacy)"),
    "csi_driver.node_id": ("NAMRBD_CSI_NODE_ID",),
    "csi_driver.gateway_url": ("NAMRBD_GATEWAY_URL",),
}


# Fields whose built-in value is not a directly registered flag default.
BUILTIN_DEFAULTS: dict[str, str] = {
    "schema_version": "not applicable (YAML contract requires 1)",
    "revision": "not applicable (operator revision)",
    "profile": "not applicable (must be selected)",
    "process": "not applicable (must match the binary)",
    "gateway.dependency": "300 s / 300 s / 5000 ms / 15000 ms",
    "gateway.advertise_control_address": "empty; derived from the control listener",
    "gateway.advertise_data_address": "empty; derived from the data listener (wildcard host becomes loopback)",
    "gateway.cache.volume_ttl_seconds": "30 s",
    "gateway.cache.chunk_id_allocation_cache_size": "256",
    "gateway.dataplane.max_io_size": "4128768",
    "iscsi_gateway.advertise_portals": "empty (the CLI --portal default is empty)",
    "iscsi_gateway.etcd.endpoints": "empty (config-only fleet setting)",
    "iscsi_gateway.etcd.root": "empty (config-only fleet setting)",
    "iscsi_gateway.reload.mode": "empty (config-only; current runtime does not consume it)",
    "iscsi_gateway.reload.poll_interval_seconds": "0; runtime registry-refresh fallback 5 s",
    "iscsi_gateway.reload.max_exports_per_process": "64",
    "iscsi_gateway.dependency": "300 s / 300 s / 5000 ms / 15000 ms",
    "sbs_service.health.shard_count": "1",
    "sbs_service.sbs_cluster_id": "empty; then falls back to cluster_id without a config file",
    "sbs_service.health.concurrency_per_shard": "nodeHealthShardConcurrency",
    "sbs_service.health.interval_seconds": "10 s",
    "sbs_service.health.timeout_seconds": "2 s",
    "sbs_service.health.suspect_threshold": "3",
    "sbs_service.health.down_threshold": "6",
    "sbs_service.health.recovery_cooldown_seconds": "30 s",
    "sbs_service.write_effects.service_owned": "true",
    "sbs_service.write_effects.native_allocation_fast_path": "true",
    "sbs_service.write_effects.batch_max": "16",
    "sbs_service.write_effects.lane_bucket_count": "0",
    "sbs_service.tikv.scan_page_size": "no current runtime binding",
    "sbs_service.tikv.batch_get_size": "no current runtime binding",
    "sbs_service.dependency": "300 s / 300 s / 5000 ms / 15000 ms",
    "sbs_data.sbs_cluster_id": "empty; then falls back to cluster_id",
    "csi_driver.admin_endpoints": "primary 127.0.0.1:9897; additional list empty",
    "csi_driver.driver_name": "block.namrbd.io",
    "csi_driver.sbs_cluster_id": "sbs-lab",
    "mcp.operations_endpoint": "http://127.0.0.1:9081",
    "mcp.mode": "observe",
    "mcp.approval_policy": "dry-run",
    "mcp.operation_output_dir": ".cache/namrbd-mcp-operations",
    "mcp.http_timeout_seconds": "3 s",
}


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"read {path.relative_to(ROOT)}: {error}") from error


def parse_structs() -> dict[str, list[StructField]]:
    structs: dict[str, list[StructField]] = {}
    field_re = re.compile(r'^\s*([A-Za-z_]\w*)\s+([^\s`]+)\s+`[^`]*yaml:"([^" ,]+)([^" ]*)"')
    for source in SCHEMA_SOURCES:
        lines = read_text(source).splitlines()
        current = ""
        fields: list[StructField] = []
        for line_no, line in enumerate(lines, 1):
            start = re.match(r"^type\s+(\w+)\s+struct\s*\{", line)
            if start:
                current = start.group(1)
                fields = []
                continue
            if current and line.startswith("}"):
                structs[current] = fields
                current = ""
                continue
            if not current:
                continue
            match = field_re.match(line)
            if not match:
                continue
            yaml_name = match.group(3)
            fields.append(StructField(
                match.group(1), match.group(2), yaml_name,
                "omitempty" in match.group(4), source, line_no,
            ))
    required = {"File", "SecretRef", "Thresholds", *(item.struct_name for item in PROCESSES)}
    missing = sorted(required - structs.keys())
    if missing:
        raise ValueError(f"schema struct parser missed: {', '.join(missing)}")
    return structs


def clean_type(go_type: str) -> tuple[str, bool]:
    optional = go_type.startswith("*")
    value = go_type.lstrip("*")
    if "." in value and not value.startswith("[]"):
        value = value.rsplit(".", 1)[1]
    return value, optional


def walk_leaves(
    structs: dict[str, list[StructField]], struct_name: str, prefix: str,
    optional_parent: str = "",
) -> list[LeafField]:
    out: list[LeafField] = []
    for field in structs[struct_name]:
        path = f"{prefix}.{field.yaml_name}" if prefix else field.yaml_name
        child_type, pointer = clean_type(field.go_type)
        child_optional = optional_parent
        if pointer or field.omitempty:
            child_optional = child_optional or path
        if child_type in structs:
            out.extend(walk_leaves(structs, child_type, path, child_optional))
        else:
            out.append(LeafField(path, field.go_type, field.source, field.line, child_optional))
    return out


def process_leaves(structs: dict[str, list[StructField]], spec: ProcessSpec) -> list[LeafField]:
    common: list[LeafField] = []
    roots: dict[str, str] = {}
    for field in structs["File"]:
        child_type, _ = clean_type(field.go_type)
        if child_type in {item.struct_name for item in PROCESSES}:
            roots[field.yaml_name] = child_type
            continue
        if child_type in structs:
            raise ValueError(f"unexpected top-level object {field.yaml_name}: {child_type}")
        common.append(LeafField(field.yaml_name, field.go_type, field.source, field.line, ""))
    expected_roots = {item.root_key: item.struct_name for item in PROCESSES}
    if roots != expected_roots:
        raise ValueError(f"process block inventory drift: got {roots!r}, expected {expected_roots!r}")
    return common + walk_leaves(structs, spec.struct_name, spec.root_key)


def parse_scalar(value: str):
    value = value.strip()
    if value in {"true", "false"}:
        return value == "true"
    if value in {"null", "~"}:
        return None
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def flatten_example(path: Path) -> dict[str, object]:
    """Parse the intentionally small YAML subset used by configs/*.yaml."""
    out: dict[str, object] = {}
    stack: list[tuple[int, str]] = []
    for line_no, raw in enumerate(read_text(path).splitlines(), 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        text = raw.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if text.startswith("- "):
            if not stack:
                raise ValueError(f"{path.relative_to(ROOT)}:{line_no}: list item without a key")
            key = stack[-1][1]
            values = out.setdefault(key, [])
            if not isinstance(values, list):
                raise ValueError(f"{path.relative_to(ROOT)}:{line_no}: mixed scalar/list for {key}")
            values.append(parse_scalar(text[2:]))
            continue
        if ":" not in text:
            raise ValueError(f"{path.relative_to(ROOT)}:{line_no}: unsupported YAML line")
        key, raw_value = text.split(":", 1)
        prefix = stack[-1][1] if stack else ""
        dotted = f"{prefix}.{key.strip()}" if prefix else key.strip()
        if raw_value.strip() == "":
            stack.append((indent, dotted))
            continue
        out[dotted] = parse_scalar(raw_value)
    return out


def split_go_args(text: str) -> list[str]:
    args: list[str] = []
    start = 0
    depth = 0
    quote = ""
    escaped = False
    for index, char in enumerate(text):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\" and quote == '"':
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(text[start:index].strip())
            start = index + 1
    args.append(text[start:].strip())
    return args


def balanced_call(source: str, open_paren: int) -> tuple[str, int]:
    depth = 0
    quote = ""
    escaped = False
    for index in range(open_paren, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\" and quote == '"':
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return source[open_paren + 1:index], index + 1
    raise ValueError("unterminated Go call while extracting flag defaults")


def go_string(expr: str) -> str | None:
    expr = expr.strip()
    if not (expr.startswith('"') and expr.endswith('"')):
        return None
    try:
        return json.loads(expr)
    except json.JSONDecodeError:
        return None


def flag_defaults(spec: ProcessSpec) -> dict[str, str]:
    source = read_text(ROOT / "cmd" / spec.binary / "main.go")
    call_re = re.compile(r"\b(?:flag|fs)\.(String|Bool|Duration|Int|Int64|Uint|Uint64|Float64)(Var)?\s*\(")
    out: dict[str, str] = {}
    cursor = 0
    while True:
        match = call_re.search(source, cursor)
        if not match:
            break
        body, cursor = balanced_call(source, match.end() - 1)
        args = split_go_args(body)
        name_index = 1 if match.group(2) else 0
        default_index = 2 if match.group(2) else 1
        if len(args) <= default_index:
            continue
        name = go_string(args[name_index])
        if name:
            out[name] = args[default_index]
    return out


def literal_default(expr: str) -> str:
    expr = expr.strip()
    # Strip the common environment readers while retaining their fallback.
    for name in ("getenvOrDefault", "getenvCompatOrDefault", "getenvDuration", "getenvBool", "getenvBoolOrDefault", "getenvInt", "getenv"):
        marker = name + "("
        if expr.startswith(marker) and expr.endswith(")"):
            args = split_go_args(expr[len(marker):-1])
            if len(args) >= 2:
                if name == "getenv" and "HOSTNAME" in args[0]:
                    return "host environment or " + literal_default(args[1])
                return literal_default(args[1])
    if expr.startswith("maxInt(") and expr.endswith(")"):
        args = split_go_args(expr[7:-1])
        if args:
            return literal_default(args[0])
    for cast in ("uint", "uint64", "int", "int64"):
        marker = cast + "("
        if expr.startswith(marker) and expr.endswith(")"):
            return literal_default(expr[len(marker):-1])
    text = go_string(expr)
    if text is not None:
        return json.dumps(text)
    duration = re.fullmatch(r"(\d+)\s*\*\s*time\.(Second|Minute|Millisecond)", expr)
    if duration:
        value = int(duration.group(1))
        unit = duration.group(2)
        if unit == "Minute":
            return f"{value * 60} s"
        if unit == "Millisecond":
            return f"{value} ms"
        return f"{value} s"
    if re.fullmatch(r"-?\d+(?:\s*\*\s*\d+)+", expr):
        node = ast.parse(expr, mode="eval")
        if all(isinstance(item, (ast.Expression, ast.BinOp, ast.Mult, ast.Constant)) for item in ast.walk(node)):
            return str(eval(compile(node, "<default>", "eval"), {"__builtins__": {}}, {}))
    if expr in {"true", "false"} or re.fullmatch(r"-?\d+", expr):
        return expr
    if expr == "defaultGatewayID()":
        return "host-derived gateway ID"
    return f"source expression: {expr}"


def lookup_binding(path: str) -> Binding | None:
    candidates = [key for key in BINDINGS if path == key or path.startswith(key + ".")]
    if not candidates:
        return None
    return BINDINGS[max(candidates, key=len)]


def lookup_accepted_only(path: str) -> tuple[str, str] | None:
    candidates = [key for key in ACCEPTED_ONLY if path == key or path.startswith(key + ".")]
    if not candidates:
        return None
    return ACCEPTED_ONLY[max(candidates, key=len)]


def lookup_env_fields(path: str) -> tuple[str, ...]:
    candidates = [key for key in ENV_FIELDS if path == key or path.startswith(key + ".")]
    if not candidates:
        return ()
    return ENV_FIELDS[max(candidates, key=len)]


def decoded_default(field: LeafField) -> str:
    if field.path in {"schema_version", "revision", "profile", "process"}:
        return {"schema_version": "0", "revision": "0", "profile": '""', "process": '""'}[field.path]
    if ".dependency." in field.path:
        defaults = {
            "etcd_unavailable_grace_seconds": "300",
            "tikv_unavailable_grace_seconds": "300",
            "projection_stale_degraded_ms": "5000",
            "projection_stale_blocked_ms": "15000",
        }
        return f"parent omitted -> {defaults[field.path.rsplit('.', 1)[1]]} effective"
    raw, pointer = clean_type(field.go_type)
    if field.optional_parent:
        zero = "false" if raw == "bool" else "[]" if raw.startswith("[]") else "0" if raw in {"int", "int64"} else '""'
        return f"{field.optional_parent} omitted; otherwise {zero}"
    if raw == "bool":
        return "false"
    if raw.startswith("[]"):
        return "[]"
    if raw in {"int", "int64", "uint", "uint64"}:
        return "0"
    if pointer:
        return "null"
    return '""'


def schema_type(go_type: str) -> str:
    raw, _ = clean_type(go_type)
    return {
        "string": "string", "bool": "boolean", "int": "integer", "int64": "integer (64-bit)",
        "[]string": "array<string>",
    }.get(raw, raw)


def validation_for(path: str) -> str:
    exact = {
        "schema_version": "must equal 1",
        "revision": "must be a positive integer",
        "profile": "required; dev or large_scale",
        "process": "required; must match the binary and the one process block",
        "gateway.gateway_id": "required, non-blank",
        "gateway.listen": "required, non-blank",
        "gateway.sbs_admin_endpoint": "required, non-blank",
        "gateway.cache.chunk_id_allocation_cache_size": "must be >= 0",
        "gateway.cache.volume_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero or positive values apply",
        "gateway.cache.zero_evidence_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero or positive values apply",
        "gateway.cache.open_reuse_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero or positive values apply",
        "gateway.cache.write_plan_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero or positive values apply",
        "gateway.cache.begin_write_volume_state_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero or positive values apply",
        "gateway.reconcile.path_plan_interval_seconds": "must be > 0",
        "gateway.reconcile.lease_ttl_seconds": "must be >= 0; zero validates as effective 15 s",
        "gateway.reconcile.status_refresh_interval_seconds": "must be >= 0 and shorter than lease TTL; zero validates as effective 5 s",
        "gateway.reconcile.chunk_gc_interval_seconds": "zero disables; negative YAML is accepted but ignored by adoption, retaining the current/built-in value",
        "gateway.reconcile.chunk_gc_batch_size": "only positive YAML applies; zero or negative is accepted but retains the current/built-in value",
        "gateway.dataplane.max_inflight_requests": "must be > 0",
        "gateway.dataplane.max_inflight_bytes": "only positive YAML applies; zero or negative is accepted but retains the current/built-in value",
        "gateway.dataplane.max_io_size": "negative YAML is accepted but ignored by adoption; zero is later normalized to DefaultMaxIOSize (4128768); positive values apply",
        "gateway.dataplane.token_ttl_seconds": "negative YAML is accepted but ignored by adoption; zero is later normalized to a 5-minute issuance TTL; positive values apply",
        "gateway.dataplane.wire_version": "only positive YAML applies; zero or negative retains the current/built-in value",
        "gateway.etcd.endpoints": "required in large_scale",
        "gateway.etcd.root": "required, non-blank in large_scale",
        "iscsi_gateway.gateway_id": "required, non-blank",
        "iscsi_gateway.advertise_portals": "must contain at least one portal",
        "iscsi_gateway.sbs_admin_endpoint": "required, non-blank",
        "iscsi_gateway.sbs_endpoint": "required by the large_scale runtime path",
        "iscsi_gateway.etcd.endpoints": "required in large_scale",
        "iscsi_gateway.etcd.root": "required, non-blank in large_scale",
        "iscsi_gateway.reload.mode": "required; watch, poll, or none; none forbidden in large_scale",
        "iscsi_gateway.reload.poll_interval_seconds": "must be > 0 when reload.mode=poll",
        "iscsi_gateway.reload.max_exports_per_process": "must be > 0; >= 32 in large_scale; runtime safety cap 64",
        "sbs_service.cluster_id": "required, non-blank",
        "sbs_service.sbs_cluster_id": "required, non-blank",
        "sbs_service.node_id": "required, non-blank",
        "sbs_service.grpc_listen": "required, non-blank",
        "sbs_service.metadata_backend": "must be tikv in large_scale",
        "sbs_service.tikv.pd_endpoints": "must contain at least one endpoint",
        "sbs_service.tikv.timeout_seconds": "must be > 0",
        "sbs_service.tikv.scan_page_size": "large_scale range 1..512",
        "sbs_service.tikv.batch_get_size": "large_scale range 1..128; must be >= 2 * write_effects.batch_max",
        "sbs_service.tikv.operation_trace": "must be false in large_scale",
        "sbs_service.leader.lease_duration_seconds": "must be > 0 and longer than renew interval",
        "sbs_service.leader.renew_interval_seconds": "must be > 0 and shorter than lease duration",
        "sbs_service.health.shard_count": "must be >= 4 in large_scale; nonpositive dev values retain the current/built-in value",
        "sbs_service.health.concurrency_per_shard": "large_scale range 1..16; nonpositive dev values retain the current/built-in value",
        "sbs_service.health.interval_seconds": "only positive YAML applies; nonpositive values are accepted in dev but retain the current/built-in value",
        "sbs_service.health.timeout_seconds": "only positive YAML applies; nonpositive values are accepted in dev but retain the current/built-in value",
        "sbs_service.health.suspect_threshold": "when positive with down threshold, must be lower; nonpositive values retain the current/built-in value",
        "sbs_service.health.down_threshold": "when positive with suspect threshold, must be higher; nonpositive values retain the current/built-in value",
        "sbs_service.health.recovery_cooldown_seconds": "only positive YAML applies; nonpositive values retain the current/built-in value",
        "sbs_service.write_effects.batch_max": "only positive YAML applies; in large_scale, 2 * value must not exceed tikv.batch_get_size",
        "sbs_service.write_effects.lane_bucket_count": "only positive YAML applies; zero or negative retains the current/built-in value",
        "sbs_data.cluster_id": "required, non-blank in large_scale",
        "sbs_data.sbs_cluster_id": "required, non-blank in large_scale",
        "sbs_data.node_id": "required, non-blank in large_scale",
        "sbs_data.data_path": "required, non-blank",
        "sbs_data.store_config_path": "startup path requires a readable file in large_scale",
        "sbs_data.grpc_listen": "required, non-blank",
        "sbs_data.observability.trace": "YAML must be false in large_scale; an environment-backed value is preserved after validation (known precedence drift)",
        "sbs_data.observability.debug_endpoints": "YAML must be false in large_scale; an environment-backed value is preserved after validation (known precedence drift)",
        "csi_driver.driver_name": "required, non-blank",
        "csi_driver.node_id": "large_scale startup requires YAML or NAMRBD_CSI_NODE_ID",
        "csi_driver.endpoint": "required, non-blank",
        "csi_driver.admin_endpoints": "required; large_scale startup requires at least two",
        "mcp.operations_endpoint": "required, non-blank",
        "mcp.mode": "required; observe or operate; operate forbidden in large_scale",
        "mcp.approval_policy": "required, non-blank",
        "mcp.http_timeout_seconds": "positive values apply; zero or negative keeps the no-config timeout",
    }
    if path in exact:
        return exact[path]
    if path.startswith("gateway.etcd.tls.") or path.startswith("iscsi_gateway.etcd.tls."):
        return "type/unknown-key checks only; this nested TLS block is not validated or consumed"
    if path.endswith(".observability.trace") or path.endswith(".observability.debug_endpoints"):
        return "must be false in large_scale"
    if ".dependency." in path:
        if path.endswith("projection_stale_blocked_ms"):
            return "must be > 0 and exceed projection_stale_degraded_ms"
        return "must be > 0"
    if ".tls." in path or path.startswith("gateway.tls.") or "sbs_endpoint_tls." in path:
        if path.endswith(".cert_file"):
            return "required when that TLS block is enabled; secret-like literals rejected"
        if any(path.endswith("." + suffix) for suffix in ("key.file", "key.env", "key.kms")):
            return "at most one secret source; a key source is required when TLS is enabled"
    if ".chap_secret." in path:
        return "at most one secret source; required when auth.mode=chap"
    if ".token_key." in path or ".session_key." in path:
        return "at most one secret source; the reference may be omitted"
    return "type/unknown-key checks only; no additional serviceconfig.Validate rule"


def parse_registry() -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    text = read_text(REGISTRY_SOURCE)
    catalog = read_text(ROOT / "internal" / "envcompat" / "catalog.go")
    specs: dict[str, tuple[str, ...]] = {}
    for match in re.finditer(r"(\w+)\s*=\s*New\(([^\n]+)\)", catalog):
        values = tuple(re.findall(r'"([A-Z][A-Z0-9_]*)"', match.group(2)))
        if values:
            specs[match.group(1)] = values
    out: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    for match in re.finditer(r'(?:str|compatStr)\("([a-z0-9_.]+)",\s*(?:"([A-Z0-9_]+)"|envcompat\.(\w+)),\s*"([a-z0-9-]+)"', text):
        field, literal_env, spec_name, flag_name = match.groups()
        if literal_env:
            envs = (literal_env,)
        else:
            raw_envs = specs.get(spec_name or "", ())
            envs = tuple(
                name if index == 0 else name + " (legacy)"
                for index, name in enumerate(raw_envs)
            )
        if not envs:
            raise ValueError(f"cannot resolve envcompat spec for registry field {field}")
        out[field] = (envs, (flag_name,))
    for match in re.finditer(r'\{Field:\s*"([a-z0-9_.]+)",\s*Env:\s*"([A-Z0-9_]+)",\s*Flag:\s*"([a-z0-9-]+)"', text):
        field, env_name, flag_name = match.groups()
        out[field] = ((env_name,), (flag_name,))
    expected_count = 22
    if len(out) != expected_count:
        raise ValueError(f"override registry inventory drift: got {len(out)} entries, expected {expected_count}")
    return out


def parse_reload_policies() -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str]]]:
    text = read_text(RELOAD_SOURCE)
    process: dict[str, tuple[str, str]] = {}
    top: dict[str, tuple[str, str]] = {}
    current = ""
    in_top = False
    for line in text.splitlines():
        start = re.match(r"\s*(Process\w+):\s*\{", line)
        if start:
            current = start.group(1)
            in_top = False
            continue
        if line.startswith("var topLevelPolicy"):
            current = ""
            in_top = True
            continue
        match = re.match(r'\s*"([a-z0-9_.]+)":\s*\{(ReloadLive|ReloadRestart),\s*"([^"]*)"\}', line)
        if match:
            path, klass, why = match.groups()
            value = ("live" if klass == "ReloadLive" else "restart", why)
            if in_top:
                top[path] = value
            elif current:
                process[f"{PROCESS_CONSTANTS[current]}\0{path}"] = value
        if (current or in_top) and line.strip() == "}":
            current = ""
            in_top = False
    if set(top) != {"schema_version", "revision", "profile", "process"}:
        raise ValueError(f"top-level reload policy inventory drift: {sorted(top)}")
    return process, top


def reload_for(binary: str, path: str, process: dict[str, tuple[str, str]], top: dict[str, tuple[str, str]]) -> str:
    if path in top:
        klass, _ = top[path]
        return klass
    prefix = binary + "\0"
    candidates: list[tuple[str, tuple[str, str]]] = []
    for key, value in process.items():
        if not key.startswith(prefix):
            continue
        policy_path = key[len(prefix):]
        if path == policy_path or path.startswith(policy_path + "."):
            candidates.append((policy_path, value))
    if not candidates:
        raise ValueError(f"no reload classification for {binary} {path}")
    return max(candidates, key=lambda item: len(item[0]))[1][0]


def fmt(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return "null"
    return str(value)


def md_code(value: str) -> str:
    return "`" + value.replace("|", "\\|").replace("`", "\\`") + "`"


def contract_fingerprint(spec: ProcessSpec, fields: Iterable[LeafField], example: dict[str, object]) -> str:
    digest = hashlib.sha256()
    payload = {
        "process": spec.binary,
        "fields": [(f.path, f.go_type, f.optional_parent) for f in fields],
        "example": sorted(example.items()),
        "bindings": sorted((key, value.flags, value.status, value.note) for key, value in BINDINGS.items() if key.startswith(spec.root_key + ".")),
        "accepted_only": sorted((key, value) for key, value in ACCEPTED_ONLY.items() if key.startswith(spec.root_key + ".")),
        "env": sorted((key, value) for key, value in ENV_FIELDS.items() if key.startswith(spec.root_key + ".")),
    }
    digest.update(json.dumps(payload, sort_keys=True, ensure_ascii=True).encode())
    # These files are configuration-specific contracts.  Including them makes
    # a validation/application semantic change visible even when no YAML tag
    # changed and therefore forces an explicit reference regeneration.
    for path in (VALIDATION_SOURCE, LOADER_SOURCE, RELOAD_SOURCE, ROOT / spec.adoption_file):
        digest.update(read_text(path).encode())
    return digest.hexdigest()[:16]


def render_index(counts: dict[str, int]) -> str:
    lines = [
        "<!-- generated by tools/generate-config-reference.py; DO NOT EDIT. -->",
        "# Daemon configuration reference",
        "",
        "Canonical reference for the six long-running NAMRBD daemon YAML schemas.",
        "The schema source is `internal/serviceconfig`; checked-in values under",
        "`configs/` are deployment examples, not default injection.",
        "",
        "## Process inventory",
        "",
        "| Process | Example | Leaf keys | Reference |",
        "| --- | --- | ---: | --- |",
    ]
    for spec in PROCESSES:
        lines.append(f"| `{spec.binary}` | `configs/{spec.config_file}` | {counts[spec.binary]} | [{spec.binary}]({spec.binary}.md) |")
    lines += [
        "",
        "## Precedence and defaults",
        "",
        "The intended loader order, from lowest to highest, is:",
        "",
        "```text",
        "built-in daemon/flag default < YAML file < allowlisted environment override < explicitly typed allowlisted CLI override",
        "```",
        "",
        "Only an explicitly typed flag is a CLI override. Canonical environment",
        "names win over legacy names; conflicting canonical/legacy values are a",
        "startup error in `large_scale`. Some adoption paths preserve older",
        "environment-backed flag defaults or legacy flags beyond the short registry",
        "allowlist; each process page records the observed field-level behavior.",
        "",
        "YAML decoding itself installs no defaults. Omitted scalar keys decode to",
        "Go zero values, optional blocks decode as nil, and validation may then",
        "reject them. The only shared semantic fallback is an omitted `dependency`",
        "block, which selects 300 s etcd grace, 300 s TiKV grace, 5000 ms degraded",
        "projection lag, and 15000 ms blocked projection lag.",
        "",
        "## Runtime and reload status",
        "",
        "`schema accepted` and `runtime applied` are separate columns by design.",
        "Several version-1 keys parse and validate but have no current daemon binding;",
        "those gaps are named rather than silently described as supported.",
        "",
        "`internal/serviceconfig/reload.go` classifies fields as logically live or",
        "restart-required, but no shipped daemon currently calls `serviceconfig.Reload`",
        "from a production path. All six YAML files are therefore startup-only today;",
        "the per-field reload class is a future contract, not a hot-reload promise.",
        "",
        "## File-wide validation",
        "",
        "Unknown YAML keys are rejected. Exactly one process block must be present",
        "and must match `process`. Raw files are scanned for likely secret literals",
        "before decoding. In `large_scale`, the config file itself must be owner-only.",
        "A modeled secret reference names at most one of `file`, `env`, or `kms`, but",
        "unconsumed blocks do not all execute that validation. The shared Resolver's",
        "secret-file mode/owner check and fail-closed KMS behavior apply only where a",
        "daemon calls it; today that is the gateway dataplane token/session path.",
        "Other references have the narrower runtime status recorded per process.",
        "",
        "## Regeneration",
        "",
        "```bash",
        "python3 tools/generate-config-reference.py --write",
        "python3 tools/generate-config-reference.py --check",
        "```",
        "",
    ]
    return "\n".join(lines)


def render_process(
    spec: ProcessSpec,
    fields: list[LeafField],
    example: dict[str, object],
    registry: dict[str, tuple[tuple[str, ...], tuple[str, ...]]],
    defaults: dict[str, str],
    reload_process: dict[str, tuple[str, str]],
    reload_top: dict[str, tuple[str, str]],
) -> str:
    fingerprint = contract_fingerprint(spec, fields, example)
    lines = [
        "<!-- generated by tools/generate-config-reference.py; DO NOT EDIT. -->",
        f"<!-- config-contract-sha256: {fingerprint} -->",
        f"# `{spec.binary}` configuration",
        "",
        f"Example: `configs/{spec.config_file}`. Schema block: `{spec.root_key}`.",
        "The example column is not a built-in default. `No-config default` records",
        "the current daemon/flag fallback where one exists; source expressions are",
        "left named when resolving them would duplicate product constants.",
        "",
        "All `live` reload labels below are classification-only. The daemon has no",
        "production service-config reload call site, so changing the file currently",
        "requires a process restart.",
        "",
        "## Field reference",
        "",
        "| YAML key | Type | YAML omission | No-config default | Shipped example | Environment / CLI input | Runtime status | Validation | Reload class |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for field in fields:
        path = field.path
        binding = lookup_binding(path)
        accepted = lookup_accepted_only(path)
        if path not in {"schema_version", "revision", "profile", "process"} and binding is None and accepted is None:
            raise ValueError(f"runtime inventory missing for {spec.binary} key {path}")

        registry_item = registry.get(path)
        # csi admin_endpoints.0 is a loader pseudo-path representing the first
        # list entry, not a YAML key. Fold it into the list field.
        if path == "csi_driver.admin_endpoints" and "csi_driver.admin_endpoints.0" in registry:
            first = registry["csi_driver.admin_endpoints.0"]
            envs = tuple(dict.fromkeys((*(registry_item or ((), ()))[0], *first[0])))
            flags = tuple(dict.fromkeys((*(registry_item or ((), ()))[1], *first[1])))
            registry_item = (envs, flags)
        envs = list(lookup_env_fields(path))
        cli_flags = list(binding.flags if binding else ())
        if registry_item:
            envs = [*registry_item[0], *[item for item in envs if item not in registry_item[0]]]
            cli_flags = [*registry_item[1], *[item for item in cli_flags if item not in registry_item[1]]]

        inputs: list[str] = []
        if envs:
            inputs.append("env " + ", ".join(md_code(item) for item in envs))
        if cli_flags:
            inputs.append("CLI " + ", ".join(md_code("--" + item) for item in cli_flags))
        if not inputs:
            inputs.append("file/config-only")
        if registry_item:
            inputs.append("allowlisted override")
        elif envs or cli_flags:
            inputs.append("legacy adoption path; see precedence note")

        no_config = BUILTIN_DEFAULTS.get(path)
        if no_config is None and ".dependency." in path:
            no_config = decoded_default(field).removeprefix("parent omitted -> ").removesuffix(" effective")
        if no_config is None and cli_flags:
            expressions = [defaults[name] for name in cli_flags if name in defaults]
            no_config = " / ".join(dict.fromkeys(literal_default(item) for item in expressions)) if expressions else "no registered flag default"
        if no_config is None:
            no_config = "not applicable or not currently bound"

        sample = fmt(example[path]) if path in example else "—"
        if accepted:
            runtime = accepted[0] + ": " + accepted[1]
        elif path in {"schema_version", "revision", "profile", "process"}:
            runtime = "loader contract"
        else:
            runtime = binding.status if binding else "unknown"

        lines.append("| " + " | ".join([
            md_code(path), md_code(schema_type(field.go_type)), md_code(decoded_default(field)),
            md_code(no_config), md_code(sample), "<br>".join(inputs), runtime.replace("|", "\\|"),
            validation_for(path).replace("|", "\\|"), md_code(reload_for(spec.binary, path, reload_process, reload_top)),
        ]) + " |")

    lines += [
        "",
        "## Process-specific caveats",
        "",
    ]
    caveats = {
        "namrbd-gateway": [
            "The shipped example sets `gateway.dataplane.max_io_size` to 4194304 while the current no-config product constant resolves to 4128768; the example intentionally replaces the built-in.",
            "`gateway.etcd.tls` is schema-accepted but is not passed to the current etcd client.",
            "When config and CLI leave dataplane keys empty, runtime fallback variables are `NAMRBD_DP_TOKEN_SIGNING_KEY` and `NAMRBD_DP_SESSION_KEY`; they do not outrank a resolved YAML secret reference.",
            "Negative duration values handled by `setDuration` are accepted when no separate validation rule exists but ignored, retaining the current/built-in value. For `chunk_gc_interval_seconds`, only zero disables the worker.",
            "Runtime normalization is a second step after config adoption: lease/status zero become 15 s/5 s, max-I/O zero is restored to `dataplane.DefaultMaxIOSize` (4128768), and token-TTL zero becomes the 5-minute issuance default. Negative values without a validation rule are generally accepted by the schema but ignored by the adoption helper.",
            "Non-allowlisted explicitly typed legacy flags are preserved after YAML validation; such flags can bypass file-only validation gates. Concretely, typed `--dataplane-request-trace=true` can bypass the large_scale trace=false rule and typed `--path-plan-reconcile-interval=0s` can bypass the positive YAML rule and disable reconciliation. The CLI reference and large_scale rejection lists remain part of the effective contract.",
        ],
        "namrbd-iscsi-gateway": [
            "The override registry names `--advertise-portals`, but the binary exposes `--portal`; with `--config`, the YAML portal can replace an explicitly typed `--portal`.",
            "The shipped CHAP plus initiator-allowlist example passes serviceconfig validation, but the current gotgt runtime rejects both settings and cannot start from that example unchanged.",
            "Reload mode is schema-accepted, but the current runtime does not select reload behavior from it.",
            "When `sbs_endpoint_tls` is present, its `enable` boolean is assigned unconditionally and can replace an explicitly typed `--sbs-endpoint-tls`, including replacing true with false.",
            "A CHAP `file` reference is passed through as a path without the shared serviceconfig Resolver's mode/owner checks. The Community target then fails closed because CHAP itself is unsupported; no secret material is resolved first.",
        ],
        "sbs-service": [
            "The shipped example chooses write-effects batch 64 and lane buckets 8; current no-config defaults are 16 and 0 respectively.",
            "The shipped TiKV TLS/v2 example has no YAML CA-file key. The current TiKV startup still needs `NAMRBD_CA_FILE`; the example is not standalone-startable without that external value.",
            "TiKV scan-page and batch-get budgets validate but are not wired into the current daemon runtime.",
            "Direct environment-backed values and some legacy CLI values are preserved after the YAML file is validated. Environment values can currently bypass large_scale metadata-backend, TiKV trace, leader-timing, and health bounds; allowed legacy CLI values can bypass their corresponding file-only checks. Explicit metadata-backend and TiKV-trace flags are separately rejected by the large_scale CLI gate. This is recorded behavior, not a supported override pattern.",
            "The seven `health.*` inputs are environment-derived local variables. The names used by adoption bookkeeping are not registered CLI flags.",
        ],
        "sbs-data": [
            "`cluster_id` and `sbs_cluster_id` environment/CLI values are initial flag defaults, not registered post-file overrides; non-empty YAML values replace them.",
            "`observability.listen` is accepted but ignored because the daemon serves observability on `http_listen`.",
            "`NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG` and `NAMRBD_SBS_DATA_OPERATION_TRACE` are preserved after file validation and can currently bypass the large_scale false rule; this is a known precedence drift, not supported production configuration.",
            "Three non-schema durability LAB defaults—`NAMRBD_SBS_LAB_DISABLE_IDEMPOTENCY_SYNC`, `NAMRBD_SBS_LAB_CACHE_OPEN_VOLUME_SPEC`, and `NAMRBD_SBS_LAB_DISABLE_PHYSICAL_WRITE_IDEMPOTENCY`—are read before config adoption. The large_scale gate rejects their CLI flags but does not currently reject these environment variables, so they can bypass that CLI-only gate.",
            "The service YAML is startup-only. The document named by `store_config_path` has a separate HTTP reload path only when the development debug endpoint is enabled; that does not reload this service YAML.",
        ],
        "namrbd-csi-driver": [
            "The shipped `driver_name` is an example value and differs from the code constant used without a config file.",
            "A non-empty YAML `driver_name` currently replaces even an explicitly typed `--driver-name`; this is a recorded precedence exception.",
            "large_scale rejects explicit `--driver-name`, `--vendor-version`, `--cluster-id`, `--sbs-cluster-id`, and `--namrbdctl`; `--vendor-version` and `--namrbdctl` are non-schema runtime flags but remain part of the startup gate.",
            "`NAMRBDCTL` supplies the helper-path default before adoption and is not rejected by the large_scale CLI gate, so it can bypass the explicit `--namrbdctl` rejection. Vendor version is CLI-only and has no corresponding environment-default bypass.",
            "The entire observability block is schema-accepted but has no current CSI runtime binding.",
        ],
        "namrbd-mcp": [
            "The entire observability block is schema-accepted but has no current MCP runtime binding.",
            "In large_scale, observe is the only admissible posture; an explicit CLI override is revalidated after loading.",
        ],
    }[spec.binary]
    lines.extend(f"- {item}" for item in caveats)
    lines += [
        "",
        "## Source of truth",
        "",
        "- Schema: `internal/serviceconfig/schema.go`, `internal/serviceconfig/secret.go`",
        "- Validation: `internal/serviceconfig/validate.go`, `internal/depavail/thresholds.go`",
        "- Override precedence: `internal/serviceconfig/loader.go`, `internal/serviceconfig/registry.go`",
        f"- Runtime binding: `{spec.adoption_file}`",
        "- Reload classification: `internal/serviceconfig/reload.go` (not runtime-wired)",
        "",
    ]
    return "\n".join(lines)


def generated_pages() -> dict[Path, str]:
    actual_configs = {path.name for path in (ROOT / "configs").glob("*.yaml")}
    expected_configs = {item.config_file for item in PROCESSES}
    if actual_configs != expected_configs:
        raise ValueError(f"daemon config file inventory drift: got {sorted(actual_configs)}, expected {sorted(expected_configs)}")

    structs = parse_structs()
    registry = parse_registry()
    reload_process, reload_top = parse_reload_policies()
    pages: dict[Path, str] = {}
    counts: dict[str, int] = {}
    all_schema_paths: set[str] = set()
    for spec in PROCESSES:
        fields = process_leaves(structs, spec)
        counts[spec.binary] = len(fields)
        example = flatten_example(ROOT / "configs" / spec.config_file)
        field_paths = {field.path for field in fields}
        unknown_example = sorted(set(example) - field_paths)
        if unknown_example:
            raise ValueError(f"{spec.config_file} keys absent from schema: {', '.join(unknown_example)}")
        if example.get("process") != spec.binary:
            raise ValueError(f"{spec.config_file} process is {example.get('process')!r}, expected {spec.binary!r}")
        if not any(path.startswith(spec.root_key + ".") for path in example):
            raise ValueError(f"{spec.config_file} has no {spec.root_key} fields")
        all_schema_paths.update(field_paths)
        pages[Path(f"{spec.binary}.md")] = render_process(
            spec, fields, example, registry, flag_defaults(spec), reload_process, reload_top,
        )

    # Registry pseudo-path .0 is the one deliberate exception; everything else
    # must name an actual YAML leaf.
    pseudo = {"csi_driver.admin_endpoints.0"}
    unknown_registry = sorted(set(registry) - all_schema_paths - pseudo)
    if unknown_registry:
        raise ValueError(f"override registry paths absent from schema: {', '.join(unknown_registry)}")
    unknown_metadata = sorted((set(BINDINGS) | set(ACCEPTED_ONLY) | set(ENV_FIELDS)) - all_schema_paths)
    # Prefix entries cover nested leaves and therefore need not themselves be leaves.
    unknown_metadata = [path for path in unknown_metadata if not any(leaf.startswith(path + ".") for leaf in all_schema_paths)]
    if unknown_metadata:
        raise ValueError(f"config reference metadata paths absent from schema: {', '.join(unknown_metadata)}")
    pages[Path("index.md")] = render_index(counts)
    return pages


def check_pages(pages: dict[Path, str]) -> int:
    errors = 0
    expected = {OUTPUT_DIR / relative for relative in pages}
    actual = set(OUTPUT_DIR.glob("*.md")) if OUTPUT_DIR.exists() else set()
    for extra in sorted(actual - expected):
        print(f"unexpected generated config reference page: {extra.relative_to(ROOT)}", file=sys.stderr)
        errors += 1
    for relative, wanted in sorted(pages.items(), key=lambda item: str(item[0])):
        path = OUTPUT_DIR / relative
        current = read_text(path) if path.exists() else ""
        if current == wanted:
            continue
        errors += 1
        print(f"config reference drift: {path.relative_to(ROOT)}", file=sys.stderr)
        diff = difflib.unified_diff(
            current.splitlines(), wanted.splitlines(),
            fromfile=str(path.relative_to(ROOT)), tofile=f"generated/{relative}", lineterm="",
        )
        for line in list(diff)[:200]:
            print(line, file=sys.stderr)
    return errors


def write_pages(pages: dict[Path, str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    expected = {OUTPUT_DIR / relative for relative in pages}
    for old in OUTPUT_DIR.glob("*.md"):
        if old not in expected:
            old.unlink()
    for relative, content in pages.items():
        (OUTPUT_DIR / relative).write_text(content, encoding="utf-8")


PUBLIC_REQUIRED_TOKENS: dict[str, tuple[str, ...]] = {
    "index.md": (
        "built-in", "YAML file", "environment", "CLI", "startup",
        "schema accepts", "runtime",
    ),
    "namrbd-gateway.md": (
        "gateway.gateway_id", "gateway.dataplane.max_io_size", "gateway.etcd.tls",
        "accepted", "runtime", "4194304", "4128768",
    ),
    "namrbd-iscsi-gateway.md": (
        "iscsi_gateway.gateway_id", "iscsi_gateway.sbs_endpoint_tls.cert_file",
        "iscsi_gateway.reload.mode", "accepted", "runtime", "chap",
    ),
    "sbs-service.md": (
        "sbs_service.cluster_id", "sbs_service.tikv.scan_page_size",
        "sbs_service.tikv.batch_get_size", "sbs_service.observability",
        "accepted", "runtime", "NAMRBD_CA_FILE",
    ),
    "sbs-data.md": (
        "sbs_data.data_path", "sbs_data.observability.listen", "accepted", "runtime",
    ),
    "namrbd-csi-driver.md": (
        "csi_driver.driver_name", "csi_driver.observability", "accepted", "runtime",
    ),
    "namrbd-mcp.md": (
        "mcp.operations_endpoint", "mcp.observability", "accepted", "runtime",
    ),
}


def check_public_pages(structs: dict[str, list[StructField]]) -> int:
    """Validate hand-authored public pages against the exported source schema."""
    errors = 0
    expected = {PUBLIC_OUTPUT_DIR / name for name in PUBLIC_REQUIRED_TOKENS}
    actual = set(PUBLIC_OUTPUT_DIR.glob("*.md")) if PUBLIC_OUTPUT_DIR.exists() else set()
    for missing in sorted(expected - actual):
        print(f"missing public config reference page: {missing.relative_to(ROOT)}", file=sys.stderr)
        errors += 1
    for extra in sorted(actual - expected):
        print(f"unexpected public config reference page: {extra.relative_to(ROOT)}", file=sys.stderr)
        errors += 1
    for name, tokens in PUBLIC_REQUIRED_TOKENS.items():
        path = PUBLIC_OUTPUT_DIR / name
        if not path.exists():
            continue
        content = read_text(path)
        for token in tokens:
            if token not in content:
                print(f"public config reference {path.relative_to(ROOT)} lacks required token {token!r}", file=sys.stderr)
                errors += 1

    index_path = PUBLIC_OUTPUT_DIR / "index.md"
    if index_path.exists():
        index_content = read_text(index_path)
        common_paths = [
            field.path for field in process_leaves(structs, PROCESSES[0])
            if not field.path.startswith(PROCESSES[0].root_key + ".")
        ]
        for dotted in common_paths:
            if dotted not in index_content:
                print(f"public config reference {index_path.relative_to(ROOT)} lacks schema leaf {dotted}", file=sys.stderr)
                errors += 1

    for spec in PROCESSES:
        path = PUBLIC_OUTPUT_DIR / f"{spec.binary}.md"
        if not path.exists():
            continue
        content = read_text(path)
        leaves = [
            field.path for field in process_leaves(structs, spec)
            if field.path.startswith(spec.root_key + ".")
        ]
        for dotted in leaves:
            if dotted not in content:
                print(f"public config reference {path.relative_to(ROOT)} lacks schema leaf {dotted}", file=sys.stderr)
                errors += 1
    return errors


def schema_leaf_count(structs: dict[str, list[StructField]]) -> int:
    return len({field.path for spec in PROCESSES for field in process_leaves(structs, spec)})


def print_summary(leaf_count: int, public_page_count: int, error_count: int) -> None:
    print(
        "config reference summary: "
        f"process_count={len(PROCESSES)} schema_leaf_count={leaf_count} "
        f"public_page_count={public_page_count} error_count={error_count}",
        file=sys.stderr,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="regenerate checked-in Markdown")
    mode.add_argument("--check", action="store_true", help="fail if checked-in Markdown differs")
    parser.add_argument(
        "--public-only", action="store_true",
        help="validate only hand-authored docs-src pages (works in the exported Community tree)",
    )
    args = parser.parse_args()
    try:
        structs = parse_structs()
        leaf_count = schema_leaf_count(structs)
        public_count = len(list(PUBLIC_OUTPUT_DIR.glob("*.md"))) if PUBLIC_OUTPUT_DIR.exists() else 0
        if args.public_only:
            errors = check_public_pages(structs)
            print_summary(leaf_count, public_count, errors)
            return 0 if errors == 0 else 1
        pages = generated_pages()
        if args.check:
            errors = check_pages(pages) + check_public_pages(structs)
            print_summary(leaf_count, public_count, errors)
            return 0 if errors == 0 else 1
        write_pages(pages)
        errors = check_public_pages(structs)
        print_summary(leaf_count, public_count, errors)
        return 0 if errors == 0 else 1
    except (OSError, ValueError) as error:
        print(f"generate config reference: {error}", file=sys.stderr)
        print_summary(0, 0, 1)
        return 1


if __name__ == "__main__":
    sys.exit(main())
