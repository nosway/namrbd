# `namrbd-gateway` configuration

Community example: `configs/namrbd-gateway.yaml`. The common top-level contract,
security rules, and precedence terminology are defined in the [configuration
index](index.md).

`--config` is optional. The table's “Built-in” column is the value before a
config file is adopted; runtime-derived values are deliberately not presented
as fixed strings.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `gateway.gateway_id` | string | host name, falling back to `gw-local` | `gw-01` | Required; published identity. |
| `gateway.listen` | string address | `0.0.0.0:9701` | `0.0.0.0:8080` | Required; control and observability listener. |
| `gateway.data_listen` | string address | `:9700` | `0.0.0.0:8081` | Bound at startup. |
| `gateway.advertise_control_address` | string address | empty, then derived from the control listener | `gw-01.namrbd.internal:8080` | Empty is a dynamic default, not a fixed address. |
| `gateway.advertise_data_address` | string address | empty, then derived from the data listener; wildcard hosts resolve to loopback | `gw-01.namrbd.internal:8081` | Empty is dynamically derived. |
| `gateway.data_disable` | boolean | `false` | `false` | Controls whether the data listener is started. |
| `gateway.tls.enable` | boolean | `false` | `true` | When true, certificate and key are required. |
| `gateway.tls.cert_file` | string path | empty | `/etc/namrbd/tls/gateway.crt` | Required when TLS is enabled. |
| `gateway.tls.key.file` | string path | unset | `/etc/namrbd/tls/gateway.key` | Supported key source; mutually exclusive with the other key sources. |
| `gateway.tls.key.env` | string environment name | unset | omitted | Schema-accepted, but this daemon rejects this key source. |
| `gateway.tls.key.kms` | string key name | unset | omitted | Schema-accepted, but this daemon rejects this key source and KMS resolution is unavailable. |
| `gateway.tls.server_name` | string | empty | `gw-01.namrbd.internal` | Advertised TLS server name. |
| `gateway.etcd.endpoints` | list of strings | `127.0.0.1:2379` | three HTTPS endpoints | Non-empty in `large_scale`; joined and passed to the etcd client. |
| `gateway.etcd.root` | string | `/namrbd` | `/namrbd/prod` | Required in `large_scale`. |
| `gateway.etcd.tls.enable` | boolean | no runtime binding | `true` | Schema-accepted but not validated or consumed by this daemon. |
| `gateway.etcd.tls.cert_file` | string path | no runtime binding | `/etc/namrbd/tls/etcd-client.crt` | Schema-accepted but not consumed. |
| `gateway.etcd.tls.key.file` | string path | no runtime binding | `/etc/namrbd/tls/etcd-client.key` | Schema-accepted but not consumed. |
| `gateway.etcd.tls.key.env` | string environment name | no runtime binding | omitted | Schema-accepted but not consumed. |
| `gateway.etcd.tls.key.kms` | string key name | no runtime binding | omitted | Schema-accepted but not consumed. |
| `gateway.etcd.tls.server_name` | string | no runtime binding | omitted | Schema-accepted but not consumed. |
| `gateway.sbs_admin_endpoint` | string endpoint | `NAMRBD_SBS_SERVICE_ENDPOINT`, otherwise empty | `sbs-service.namrbd.internal:9090` | Required. |
| `gateway.metadata_backend` | string | `memory` | `etcd` | Passed to repository initialization; invalid values fail there. |
| `gateway.data_backend_mode` | string | `c6` | `sbs` | Passed to backend selection. |
| `gateway.cache.volume_ttl_seconds` | integer seconds | `30` | `30` | Zero is accepted and applied. |
| `gateway.cache.zero_evidence_ttl_seconds` | integer seconds | `0` | `30` | Zero disables the cache. |
| `gateway.cache.open_reuse_ttl_seconds` | integer seconds | `0` | `60` | Zero disables reuse. |
| `gateway.cache.chunk_id_allocation_cache_size` | integer | `256` | `256` | Must not be negative; zero disables preallocation. |
| `gateway.cache.write_plan_ttl_seconds` | integer seconds | `0` | `0` | Zero disables the cache. |
| `gateway.cache.begin_write_volume_state_ttl_seconds` | integer seconds | `0` | `0` | Zero disables the cache. |
| `gateway.reconcile.path_plan_interval_seconds` | integer seconds | `5` | `15` | Must be greater than `0`. |
| `gateway.reconcile.lease_ttl_seconds` | integer seconds | `15` | `15` | Must not be negative. Validation compares zero as `15`; adoption passes literal zero, then etcd lease startup normalizes it back to `15`. |
| `gateway.reconcile.status_refresh_interval_seconds` | integer seconds | `5` | `5` | Must be non-negative and shorter than lease TTL. Validation compares zero as `5`; adoption passes literal zero, then lease startup normalizes it back to `5`. |
| `gateway.reconcile.chunk_gc_interval_seconds` | integer seconds | `30` | `300` | Zero disables the worker. A negative YAML value is ignored by adoption, preserving the current or built-in value. |
| `gateway.reconcile.chunk_gc_batch_size` | integer | `256` | `256` | A value greater than zero is applied; zero retains the built-in flag value. |
| `gateway.dataplane.max_inflight_requests` | integer | `128` | `512` | Must be greater than `0`. |
| `gateway.dataplane.max_inflight_bytes` | 64-bit integer bytes | `8388608` | `268435456` | Only a value greater than zero replaces the built-in. |
| `gateway.dataplane.max_io_size` | integer bytes | `4128768` | `4194304` | A negative YAML value is ignored. Zero remains in the flag and HTTP config, while binary data-plane construction restores `4128768`; positive values are converted to unsigned and have no schema upper bound. |
| `gateway.dataplane.token_key.file` | string path | unset | `/etc/namrbd/secrets/dataplane-token.key` | Supported secret source. If the whole reference is empty, runtime falls back to raw `NAMRBD_DP_TOKEN_SIGNING_KEY`. |
| `gateway.dataplane.token_key.env` | string environment name | unset | omitted | Supported secret source; the named variable supplies the secret. |
| `gateway.dataplane.token_key.kms` | string key name | unset | omitted | Accepted, but resolution is unavailable and fails closed. |
| `gateway.dataplane.session_key.file` | string path | unset | omitted | Supported secret source. If the whole reference is empty, runtime falls back to raw `NAMRBD_DP_SESSION_KEY`. |
| `gateway.dataplane.session_key.env` | string environment name | unset | `NAMRBD_DATAPLANE_SESSION_KEY` | Supported secret source; the named variable supplies the secret. |
| `gateway.dataplane.session_key.kms` | string key name | unset | omitted | Accepted, but resolution is unavailable and fails closed. |
| `gateway.dataplane.token_ttl_seconds` | integer seconds | `300` | `300` | Zero is accepted and adopted, but token issuance normalizes a non-positive TTL back to `300`; a negative YAML value is ignored during adoption. |
| `gateway.dataplane.wire_version` | integer | `1` | omitted | Only a positive YAML value is applied; runtime enables version 2 only when this is `2` and both key prerequisites exist. |
| `gateway.dependency.etcd_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`; see the [shared block](index.md#dependency-threshold-block). |
| `gateway.dependency.tikv_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`. |
| `gateway.dependency.projection_stale_degraded_ms` | integer milliseconds | `5000` when the whole block is omitted | `5000` | Must be greater than `0`. |
| `gateway.dependency.projection_stale_blocked_ms` | integer milliseconds | `15000` when the whole block is omitted | `15000` | Must exceed the degraded threshold. |
| `gateway.observability.listen` | string address | control listener | omitted | Must be absent or empty. A non-empty value is a startup error. |
| `gateway.observability.trace` | boolean | `false` | `false` | Maps to the data-request trace flag; must be false in `large_scale`. |
| `gateway.observability.debug_endpoints` | boolean | `false` | `false` | Must be false in `large_scale`; accepted `true` in `dev` but has no runtime toggle. |

The example's etcd TLS block is therefore not an effective etcd TLS
configuration in the current binary. Its presence in the schema must not be
read as runtime support.

Several cache and data-plane numeric fields have no schema lower-bound. Where
an adoption helper requires a non-negative or positive value, an invalidly
negative YAML value is silently ignored and the current/built-in flag value is
kept instead of producing a validation error. The table calls out fields whose
zero behavior differs across consumers.

## Environment overrides

| YAML field | Canonical environment variable | Legacy alias |
| --- | --- | --- |
| `gateway.gateway_id` | `NAMRBD_GATEWAY_ID` | none |
| `gateway.listen` | `NAMRBD_GATEWAY_CONTROL_LISTEN` | `NAMRBD_GATEWAY_LISTEN` |
| `gateway.data_listen` | `NAMRBD_GATEWAY_DATA_LISTEN` | none |
| `gateway.advertise_control_address` | `NAMRBD_GATEWAY_ADVERTISE_CONTROL_ADDRESS` | none |
| `gateway.advertise_data_address` | `NAMRBD_GATEWAY_ADVERTISE_DATA_ADDRESS` | none |
| `gateway.sbs_admin_endpoint` | `NAMRBD_SBS_SERVICE_ENDPOINT` | `NAMRBD_GATEWAY_SBS_ADMIN_ENDPOINT` |

The two `NAMRBD_DP_*` variables are direct runtime fallbacks, not shared-loader
overrides. A non-empty secret reference in YAML is resolved before that fallback
and therefore supplies the runtime value.

## CLI overrides

The six environment-override fields map to `--gateway-id`,
`--control-http-listen`, `--data-listen`, `--advertise-control-address`,
`--advertise-data-address`, and `--sbs-service-endpoint`.

This daemon additionally preserves an explicitly typed corresponding flag for
every consumed YAML field: `--data-disable`, `--tls-*`, `--etcd-endpoints`,
`--etcd-root`, `--metadata-backend`, `--data-backend-mode`, the cache and
reconcile flags, `--max-inflight-*`, `--max-io-size`, the data-plane key/TTL/wire
flags, and `--dataplane-request-trace`. The dependency block and unconsumed
fields have no CLI override.

File validation runs before these explicit flags are preserved, and the
winning values are not generally revalidated. The current `large_scale`
rejection set does not include the data-plane trace or reconcile flags. For
example, a valid file with `observability.trace: false` can still end up traced
through `--dataplane-request-trace=true`, and
`--path-plan-reconcile-interval=0s` can disable the worker after a positive YAML
interval passed validation. These are enforcement gaps, not supported ways to
relax the profile or file contract.

## Restart policy

Actual behavior: every service-YAML edit requires restart.

The unconnected logical policy classifies cache, reconcile, dependency,
data-plane limits/TTL, and observability as `live`. Identity, listeners,
advertised addresses, TLS, etcd, service endpoint, backend selection,
data-plane secret references, and wire version are `restart` fields. Revision
alone is logically live; schema version, process, and profile are restart
fields.

## Sources checked

- `configs/namrbd-gateway.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/namrbd-gateway/{main,serviceconfig_adoption}.go`
