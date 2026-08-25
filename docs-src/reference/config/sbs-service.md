# `sbs-service` configuration

Community example: `configs/sbs-service.yaml`. See the [configuration
index](index.md) for the common file, secret-reference, and precedence rules.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `sbs_service.cluster_id` | string | `namrbd-dev` | `namrbd-prod` | Required. |
| `sbs_service.sbs_cluster_id` | string | empty, then falls back to cluster ID in CLI mode | `sbs-prod` | Required by config validation, so a YAML file cannot rely on the CLI-mode fallback. |
| `sbs_service.node_id` | string | `sbs-svc-1` | `sbs-svc-01` | Required. |
| `sbs_service.metadata_backend` | string | `pebble` | `tikv` | Must be `tikv` in `large_scale`. Runtime accepts the backend-specific contract. |
| `sbs_service.grpc_listen` | string address | `0.0.0.0:9443` | `0.0.0.0:9090` | Required. |
| `sbs_service.http_listen` | string address | `0.0.0.0:9081` | `127.0.0.1:9092` | Health and observability listener. |
| `sbs_service.payload_root` | string path | empty | `/var/lib/namrbd/sbs` | Optional local payload root. |
| `sbs_service.tikv.pd_endpoints` | list of strings | empty | three HTTPS endpoints | Required by schema validation even in a `dev` file using another metadata backend. |
| `sbs_service.tikv.keyspace` | string | empty | `namrbd-prod` | Passed to the TiKV client. |
| `sbs_service.tikv.api_version` | string | `v1` | `v2` | Runtime TiKV versions are `v1`, `v1ttl`, and `v2`. |
| `sbs_service.tikv.timeout_seconds` | integer seconds | `3` | `5` | Must be greater than `0`. |
| `sbs_service.tikv.tls.enable` | boolean | `false` | `true` | When true, schema validation requires cert and key. |
| `sbs_service.tikv.tls.cert_file` | string path | empty | `/etc/namrbd/tls/tikv-client.crt` | Applied as the client certificate. |
| `sbs_service.tikv.tls.key.file` | string path | empty | `/etc/namrbd/tls/tikv-client.key` | The only key-reference form applied by this daemon. |
| `sbs_service.tikv.tls.key.env` | string environment name | no runtime binding | omitted | Schema-accepted but not applied. |
| `sbs_service.tikv.tls.key.kms` | string key name | no runtime binding | omitted | Schema-accepted but not applied; KMS resolution is unavailable. |
| `sbs_service.tikv.tls.server_name` | string | no runtime binding | omitted | Schema-accepted but the TiKV client has no corresponding adoption flag. |
| `sbs_service.tikv.scan_page_size` | integer | no daemon runtime value | `512` | Required in `large_scale`, range `1..512`; validation-only in the current daemon. |
| `sbs_service.tikv.batch_get_size` | integer | no daemon runtime value | `128` | Required in `large_scale`, range `1..128`; validation-only in the current daemon. |
| `sbs_service.tikv.operation_trace` | boolean | `false` | `false` | Must be false in `large_scale`; applied to the TiKV trace flag. |
| `sbs_service.leader.lease_duration_seconds` | integer seconds | `10` | `15` | Must be greater than `0`. |
| `sbs_service.leader.renew_interval_seconds` | integer seconds | `3` | `5` | Must be greater than `0` and shorter than lease duration. |
| `sbs_service.health.shard_count` | integer | `1` | `4` | At least `4` in `large_scale`; runtime may use more shards as the eligible node set grows. |
| `sbs_service.health.concurrency_per_shard` | integer | `16` | `16` | Range `1..16` in `large_scale`. |
| `sbs_service.health.interval_seconds` | integer seconds | `10` | `10` | A positive YAML value is applied; zero retains the built-in. |
| `sbs_service.health.timeout_seconds` | integer seconds | `2` | `2` | A positive YAML value is applied; zero retains the built-in. |
| `sbs_service.health.suspect_threshold` | integer | `3` | `3` | A positive YAML value is applied. |
| `sbs_service.health.down_threshold` | integer | `6` | `6` | When both thresholds are positive, must exceed `suspect_threshold`. |
| `sbs_service.health.recovery_cooldown_seconds` | integer seconds | `30` | `30` | A positive YAML value is applied; zero retains the built-in. |
| `sbs_service.write_effects.service_owned` | boolean | `true` | `true` | Applied directly. |
| `sbs_service.write_effects.native_allocation_fast_path` | boolean | `true` | `true` | Applied directly. |
| `sbs_service.write_effects.batch_max` | integer | `16` | `64` | A positive value is applied. In `large_scale`, `batch_max * 2` must not exceed `tikv.batch_get_size`. |
| `sbs_service.write_effects.lane_bucket_count` | integer | `0` | `8` | A positive value is applied; schema validation sets no independent bound. |
| `sbs_service.write_effects.async_mutation_finalize` | boolean | `false` | `false` | Applied directly. |
| `sbs_service.dependency.etcd_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`; see the [shared block](index.md#dependency-threshold-block). |
| `sbs_service.dependency.tikv_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`. |
| `sbs_service.dependency.projection_stale_degraded_ms` | integer milliseconds | `5000` when the whole block is omitted | `5000` | Must be greater than `0`. |
| `sbs_service.dependency.projection_stale_blocked_ms` | integer milliseconds | `15000` when the whole block is omitted | `15000` | Must exceed the degraded threshold. |
| `sbs_service.observability.listen` | string address | no separate listener | `127.0.0.1:9102` | Schema-accepted but not consumed; observability stays on `http_listen`. |
| `sbs_service.observability.trace` | boolean | `false` | `false` | Must be false in `large_scale`; no runtime binding. |
| `sbs_service.observability.debug_endpoints` | boolean | `false` | `false` | Must be false in `large_scale`; no runtime binding. |

The scan and batch budget fields currently constrain file admission but do not
configure a runtime consumer. Likewise, the example observability port is not
opened by this daemon.

When TiKV TLS is enabled, the runtime client also needs a CA path, but the YAML
schema has no CA field. Supply `NAMRBD_CA_FILE` or explicitly type
`--tikv-ca-file`; those are direct flag inputs rather than YAML fields.

## Environment overrides

| YAML field | Environment input | Legacy alias |
| --- | --- | --- |
| `cluster_id` | `NAMRBD_CLUSTER_ID` | none |
| `sbs_cluster_id` | `NAMRBD_SBS_CLUSTER_ID` | none |
| `node_id` | `NAMRBD_SBS_SERVICE_NODE_ID` | `NAMRBD_NODE_ID` |
| `metadata_backend` | `NAMRBD_SBS_METADATA_BACKEND` | none |
| `grpc_listen` | `NAMRBD_SBS_SERVICE_GRPC_LISTEN` | `NAMRBD_SBS_ADMIN_ADDR` |
| `http_listen` | `NAMRBD_SBS_SERVICE_HTTP_LISTEN` | `NAMRBD_BIND_ADDR` |
| `payload_root` | `NAMRBD_SBS_PAYLOAD_ROOT` | none |
| `tikv.pd_endpoints` | `NAMRBD_TIKV_PD_ENDPOINTS` | none |
| `tikv.keyspace` | `NAMRBD_TIKV_KEYSPACE` | none |
| `tikv.api_version` | `NAMRBD_TIKV_API_VERSION` | none |
| `tikv.timeout_seconds` | `NAMRBD_TIKV_TIMEOUT` | none |
| `tikv.tls.enable` | `NAMRBD_TIKV_TLS_ENABLED` | none |
| `tikv.tls.cert_file` | `NAMRBD_CERT_FILE` | none |
| `tikv.tls.key.file` | `NAMRBD_KEY_FILE` | none |
| `tikv.operation_trace` | `NAMRBD_TIKV_OPERATION_TRACE` | none |
| `leader.lease_duration_seconds` | `NAMRBD_SBS_LEADER_LEASE_DURATION` | none |
| `leader.renew_interval_seconds` | `NAMRBD_SBS_LEADER_RENEW_INTERVAL` | none |
| `health.shard_count` | `NAMRBD_SBS_DATA_HEALTH_SHARD_COUNT` | none |
| `health.concurrency_per_shard` | `NAMRBD_SBS_DATA_HEALTH_CONCURRENCY` | none |
| `health.interval_seconds` | `NAMRBD_SBS_DATA_HEALTH_CHECK_INTERVAL` | none |
| `health.timeout_seconds` | `NAMRBD_SBS_DATA_HEALTH_TIMEOUT` | none |
| `health.suspect_threshold` | `NAMRBD_SBS_DATA_SUSPECT_AFTER` | none |
| `health.down_threshold` | `NAMRBD_SBS_DATA_DOWN_AFTER` | none |
| `health.recovery_cooldown_seconds` | `NAMRBD_SBS_DATA_RECOVER_COOLDOWN` | none |
| `write_effects.service_owned` | `NAMRBD_SBS_SERVICE_OWNED_WRITE_EFFECTS` | none |
| `write_effects.native_allocation_fast_path` | `NAMRBD_SBS_NATIVE_ALLOCATION_FAST_PATH` | none |
| `write_effects.batch_max` | `NAMRBD_SBS_WRITE_EFFECTS_BATCH_MAX` | none |
| `write_effects.lane_bucket_count` | `NAMRBD_SBS_WRITE_EFFECTS_LANE_BUCKET_COUNT` | none |
| `write_effects.async_mutation_finalize` | `NAMRBD_SBS_ASYNC_WRITE_MUTATION_FINALIZE` | none |

These values are read while flag defaults are constructed; the adoption code
then preserves them over YAML. Positive YAML `write_effects.batch_max` and
`lane_bucket_count` are exceptions: although the daemon reads
`NAMRBD_SBS_WRITE_EFFECTS_BATCH_MAX` and
`NAMRBD_SBS_WRITE_EFFECTS_LANE_BUCKET_COUNT` as defaults, those variables are
missing from the adoption precedence map, so YAML replaces them when
`--config` is used.

The dependency, scan/batch budget, and observability fields have no environment
override.

Validation runs against the parsed file before most of these direct
environment-derived runtime values are preserved. The winning direct value is
not revalidated. In the current implementation, an environment value can
therefore bypass `large_scale` checks such as `metadata_backend: tikv`,
`tikv.operation_trace: false`, the leader timing relationship, and the health
shard/concurrency bounds. Treat those environment names as trusted deployment
inputs and keep them consistent with the reviewed file. This is an enforcement
gap, not a supported way to relax the profile.

## CLI overrides

Explicit flags exist for identity, metadata backend, service listeners, TiKV
connection/TLS/trace, leader timings, and the write-effect settings. When
allowed, an explicitly typed flag outranks YAML. The seven `health.*` values
are environment-derived locals; there are no `--health-*` flags for them.

Under `large_scale`, explicit flags for metadata backend/path, operation trace,
the two TiKV commit-mode switches, write-effect batch/lane, and asynchronous
finalization are rejected even though several corresponding YAML values are
valid. The CA flag remains a separate runtime input because no YAML key exists.

As with direct environment inputs, most winning CLI values are not revalidated
against the file after adoption. The `large_scale` rejection set closes the
listed metadata/trace/write-effect cases, but other explicitly typed values can
still violate file-only checks, for example the leader lease/renew relation.
This is also an enforcement gap rather than a supported override contract.

## Restart policy

Actual behavior: every service-YAML edit requires restart.

The unconnected logical policy marks TiKV timeout/scan/batch/trace, dependency,
health, and observability as live. Identity, backend, listeners, payload root,
TiKV endpoints/keyspace/API/TLS, leader timings, and the write-effect block are
restart fields.

## Sources checked

- `configs/sbs-service.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/sbs-service/{main,serviceconfig_adoption}.go`
