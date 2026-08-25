# `namrbd-iscsi-gateway` configuration

Community example: `configs/namrbd-iscsi-gateway.yaml`. The Community build
limits the process to three distinct exported volumes; the larger process
safety value in `max_exports_per_process` does not change that edition limit.
See the [configuration index](index.md) for common file and security rules.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `iscsi_gateway.gateway_id` | string | empty | `iscsi-gw-01` | Required. |
| `iscsi_gateway.advertise_portals` | list of address strings | empty; a typed `--portal` is copied later when no list exists | `10.20.0.11:3260` | At least one entry is required. The first portal is bound; the full list is advertised. |
| `iscsi_gateway.etcd.endpoints` | list of strings | empty | three HTTPS endpoints | Non-empty in `large_scale`; consumed by fleet registration. |
| `iscsi_gateway.etcd.root` | string | empty | `/namrbd/prod/iscsi-gateways` | Required in `large_scale`. |
| `iscsi_gateway.etcd.tls.enable` | boolean | no runtime binding | omitted | Schema-accepted but not validated or consumed. |
| `iscsi_gateway.etcd.tls.cert_file` | string path | no runtime binding | omitted | Schema-accepted but not consumed. |
| `iscsi_gateway.etcd.tls.key.file` | string path | no runtime binding | omitted | Schema-accepted but not consumed. |
| `iscsi_gateway.etcd.tls.key.env` | string environment name | no runtime binding | omitted | Schema-accepted but not consumed. |
| `iscsi_gateway.etcd.tls.key.kms` | string key name | no runtime binding | omitted | Schema-accepted but not consumed. |
| `iscsi_gateway.etcd.tls.server_name` | string | no runtime binding | omitted | Schema-accepted but not consumed. |
| `iscsi_gateway.sbs_endpoint` | string endpoint | empty | `sbs-service.namrbd.internal:9091` | Shared validation does not require it, but the `large_scale` runtime requires both SBS endpoints. |
| `iscsi_gateway.sbs_admin_endpoint` | string endpoint | empty | `sbs-service.namrbd.internal:9090` | Required by validation and serving-registry startup. |
| `iscsi_gateway.sbs_endpoint_tls.enable` | boolean | `false` | `true` | Applied to the SBS data client. |
| `iscsi_gateway.sbs_endpoint_tls.cert_file` | string path | empty | `/etc/namrbd/tls/sbs-client.crt` | Required by schema validation when enabled, but not consumed by the daemon. |
| `iscsi_gateway.sbs_endpoint_tls.key.file` | string path | empty | `/etc/namrbd/tls/sbs-client.key` | Required as one possible reference when TLS is enabled, but not consumed by the daemon. |
| `iscsi_gateway.sbs_endpoint_tls.key.env` | string environment name | empty | omitted | Schema-accepted as an alternative key source, but not consumed. |
| `iscsi_gateway.sbs_endpoint_tls.key.kms` | string key name | empty | omitted | Schema-accepted as an alternative key source, but not consumed. |
| `iscsi_gateway.sbs_endpoint_tls.server_name` | string | empty | omitted | Applied as the TLS server name. |
| `iscsi_gateway.auth.mode` | string enum | `none` | `chap` | Runtime accepts `none` or `chap`; see the Community runtime boundary below. |
| `iscsi_gateway.auth.chap_secret.file` | string path | unset | `/etc/namrbd/secrets/chap.secret` | Required for `chap` on the schema path; the only secret source adoption accepts and passes through. Community runtime fails closed before reading it. |
| `iscsi_gateway.auth.chap_secret.env` | string environment name | unset | omitted | Schema-accepted, but this daemon rejects the source. |
| `iscsi_gateway.auth.chap_secret.kms` | string key name | unset | omitted | Schema-accepted, but this daemon rejects the source and KMS resolution is unavailable. |
| `iscsi_gateway.auth.allowed_initiator_iqns` | list of strings | empty | two IQNs | Normalized and de-duplicated before target startup. |
| `iscsi_gateway.reload.mode` | string enum | empty | `watch` | Validation accepts `watch`, `poll`, or `none`; `none` is rejected in `large_scale`. The runtime currently does not branch on this value. |
| `iscsi_gateway.reload.poll_interval_seconds` | integer seconds | `0`, then runtime fallback `5` | omitted | Must be positive when mode is `poll`; the runtime ticker uses `5` when the value is zero or less. |
| `iscsi_gateway.reload.max_exports_per_process` | integer | `64` | `64` | Must be positive, at least `32` in `large_scale`, and at most the runtime safety cap `64`. It does not override the Community limit. |
| `iscsi_gateway.dependency.etcd_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`; see the [shared block](index.md#dependency-threshold-block). |
| `iscsi_gateway.dependency.tikv_unavailable_grace_seconds` | integer seconds | `300` when the whole block is omitted | `300` | Must be greater than `0`. |
| `iscsi_gateway.dependency.projection_stale_degraded_ms` | integer milliseconds | `5000` when the whole block is omitted | `5000` | Must be greater than `0`. |
| `iscsi_gateway.dependency.projection_stale_blocked_ms` | integer milliseconds | `15000` when the whole block is omitted | `15000` | Must exceed the degraded threshold. |
| `iscsi_gateway.observability.listen` | string address | empty, listener disabled | `127.0.0.1:9101` | Applied to the health, readiness, and metrics HTTP server. |
| `iscsi_gateway.observability.trace` | boolean | `false` | `false` | Must be false in `large_scale`; no runtime trace toggle exists. |
| `iscsi_gateway.observability.debug_endpoints` | boolean | `false` | `false` | Must be false in `large_scale`; no runtime toggle exists. |

### Community authentication boundary

The shipped target stack currently supports unauthenticated mode only. A CHAP
(`chap`) mode or a non-empty initiator allowlist fails closed during target
startup. Consequently the checked-in example passes YAML/schema validation but
cannot start a current Community target with its example authentication block.
For the currently executable Community path, use `mode: none`, omit
`chap_secret`, and leave `allowed_initiator_iqns` empty.

The example client certificate and key are also not wired into the SBS client.
Enabling TLS uses the server-name setting and the client's current trust path;
the YAML certificate/key fields must not be treated as active client identity.

## Environment and CLI overrides

| YAML field | Canonical environment variable | Legacy alias | Registered CLI flag |
| --- | --- | --- | --- |
| `iscsi_gateway.gateway_id` | `NAMRBD_ISCSI_GATEWAY_ID` | none | `--iscsi-gateway-id` |
| `iscsi_gateway.sbs_endpoint` | `NAMRBD_ISCSI_SBS_DATA_ENDPOINT` | `NAMRBD_ISCSI_SBS_ENDPOINT` | `--sbs-data-endpoint` |
| `iscsi_gateway.sbs_admin_endpoint` | `NAMRBD_ISCSI_SBS_SERVICE_ENDPOINT` | `NAMRBD_ISCSI_SBS_ADMIN_ENDPOINT` | `--sbs-service-endpoint` |
| `iscsi_gateway.advertise_portals` | `NAMRBD_ISCSI_ADVERTISE_PORTALS` | none | `--advertise-portals` is registered internally but does not exist in the binary |

The first three real CLI flags and their environment variables outrank YAML.
Portal precedence is a current implementation exception: the binary exposes
`--portal`, not the registered `--advertise-portals`, so a non-empty YAML portal
list replaces an explicitly typed `--portal`.

Other corresponding CLI flags, including authentication, endpoint TLS, and
observability flags, are not protected as shared-loader overrides. Adoption
replaces strings when the YAML value is non-empty and replaces list/integer
settings when their block supplies them. In particular, the presence of
`sbs_endpoint_tls` replaces an explicitly typed TLS-enable flag even when the
YAML value is `false`.

## Registry refresh versus YAML reload

The `reload` block describes refresh of registry-owned serving data. It does
not reload this YAML. In the `large_scale` path, the current implementation
always runs a polling ticker; `poll_interval_seconds` controls its period, but
`mode` is not consulted after startup.

Actual service-YAML behavior remains startup-only, so every YAML edit requires
a process restart. The unconnected logical policy marks the allowlist,
dependency, registry-refresh, and observability fields live, and identity,
portals, etcd, SBS endpoints/TLS, and authentication mode/secret as restart
fields.

## Sources checked

- `configs/namrbd-iscsi-gateway.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/namrbd-iscsi-gateway/{main,serviceconfig_adoption,main_large_scale}.go`
- `iscsi/{auth,live_reload,supervisor,edition_community}.go`
