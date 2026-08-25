# `sbs-data` configuration

Community example: `configs/sbs-data.yaml`. See the [configuration
index](index.md) for the common file and precedence rules.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `sbs_data.cluster_id` | string | `NAMRBD_CLUSTER_ID`, otherwise `namrbd-dev` | `namrbd-prod` | Required in `large_scale`. |
| `sbs_data.sbs_cluster_id` | string | `NAMRBD_SBS_CLUSTER_ID`, otherwise empty and then the cluster ID | `sbs-prod` | Required in `large_scale`; the post-load fallback is used only when the effective value remains empty. |
| `sbs_data.node_id` | string | `NAMRBD_SBS_DATA_NODE_ID`, otherwise `sbs-data-1` | `sbs-data-01` | Required in `large_scale`. |
| `sbs_data.data_path` | string path | `NAMRBD_SBS_DATA_PATH`, its legacy alias, or `./var/sbs-data` | `/var/lib/namrbd/sbs-data` | Required. Local payload and metadata path. |
| `sbs_data.store_config_path` | string path | `NAMRBD_SBS_STORE_CONFIG`, otherwise empty | `/etc/namrbd/store-config.yaml` | A non-empty YAML value is required and that YAML path is checked for readability during `large_scale` adoption. Names a separate store-layout document. |
| `sbs_data.grpc_listen` | string address | `NAMRBD_SBS_DATA_GRPC_LISTEN`, its legacy alias, or `0.0.0.0:9444` | `0.0.0.0:9091` | Required. SBS data-plane gRPC listener. |
| `sbs_data.http_listen` | string address | `NAMRBD_SBS_DATA_HTTP_LISTEN`, its legacy alias, or `0.0.0.0:9082` | `127.0.0.1:9093` | Optional in schema; serves health, metrics, and the Community operational endpoints. |
| `sbs_data.observability.listen` | string address | no separate listener | `127.0.0.1:9103` | Schema-accepted but not consumed. Observability remains on `http_listen`. |
| `sbs_data.observability.trace` | boolean | `NAMRBD_SBS_DATA_OPERATION_TRACE`, otherwise `false` | `false` | Applied to per-operation data tracing; must be false in `large_scale`. |
| `sbs_data.observability.debug_endpoints` | boolean | `NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG`, otherwise `false` | `false` | In `dev`, maps to lab-only store mutation routes. It must be false in `large_scale` and is not a supported Community production setting. |

The example's observability port is therefore not opened. Use `http_listen` for
the read-only operational HTTP surface.

`store_config_path` does not inline or replace the store-layout schema. The
daemon loads that second document after adopting this file, and `--store-config`
cannot be combined with repeated `--store` arguments.

The `large_scale` admission check currently examines the path written in YAML,
not the effective CLI/environment replacement. Therefore the file must still
contain a readable `store_config_path`; an override alone cannot satisfy this
check. After admission, the environment or explicit CLI path is the one used
at runtime.

## Environment overrides

| YAML field | Environment input | Legacy alias | Actual precedence with `--config` |
| --- | --- | --- | --- |
| `sbs_data.cluster_id` | `NAMRBD_CLUSTER_ID` | none | Current drift: a non-empty YAML value replaces the environment value. |
| `sbs_data.sbs_cluster_id` | `NAMRBD_SBS_CLUSTER_ID` | none | Current drift: a non-empty YAML value replaces the environment value. |
| `sbs_data.node_id` | `NAMRBD_SBS_DATA_NODE_ID` | none | Environment replaces YAML through the shared registry. |
| `sbs_data.data_path` | `NAMRBD_SBS_DATA_PATH` | `NAMRBD_SBS_DATA_DIR` | Environment replaces YAML. |
| `sbs_data.store_config_path` | `NAMRBD_SBS_STORE_CONFIG` | none | Environment replaces YAML. |
| `sbs_data.grpc_listen` | `NAMRBD_SBS_DATA_GRPC_LISTEN` | `NAMRBD_SBS_GRPC_ADDR` | Environment replaces YAML. |
| `sbs_data.http_listen` | `NAMRBD_SBS_DATA_HTTP_LISTEN` | `NAMRBD_BIND_ADDR` | Environment replaces YAML. |
| `sbs_data.observability.trace` | `NAMRBD_SBS_DATA_OPERATION_TRACE` | none | Environment replaces YAML. |

The trace and debug variables are direct post-validation inputs. They are not
supported production overrides: `large_scale` rejects `true` in YAML, while
the current `NAMRBD_SBS_DATA_OPERATION_TRACE=true` and
`NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG=true` environment paths can bypass that
check. Leave both unset in Community deployments.

## CLI overrides

| YAML field | CLI flag | Actual precedence with `--config` |
| --- | --- | --- |
| `sbs_data.cluster_id` | `--cluster-id` | Current drift: a non-empty YAML value replaces even an explicitly typed flag. |
| `sbs_data.sbs_cluster_id` | `--sbs-cluster-id` | Current drift: a non-empty YAML value replaces even an explicitly typed flag. |
| `sbs_data.node_id` | `--node-id` | Explicit CLI replaces YAML. |
| `sbs_data.data_path` | `--path` | Explicit CLI replaces YAML. |
| `sbs_data.store_config_path` | `--store-config` | Explicit CLI replaces YAML. |
| `sbs_data.grpc_listen` | `--sbs-data-listen` | Explicit CLI replaces YAML. |
| `sbs_data.http_listen` | `--sbs-data-http-listen` | Explicit CLI replaces YAML. |
| `sbs_data.observability.trace` | `--data-operation-trace` | Explicit CLI replaces YAML, but the flag is rejected with `large_scale`. |

The debug mutation flag is likewise rejected when explicitly typed under
`large_scale`. Three other durability controls are not YAML schema fields but
do have direct environment defaults:

- `NAMRBD_SBS_LAB_DISABLE_IDEMPOTENCY_SYNC`
- `NAMRBD_SBS_LAB_CACHE_OPEN_VOLUME_SPEC`
- `NAMRBD_SBS_LAB_DISABLE_PHYSICAL_WRITE_IDEMPOTENCY`

Their explicit CLI forms are rejected with `large_scale`, but current `true`
environment values bypass that CLI-only gate. They are unsupported deployment
inputs; leave them unset in Community deployments.

## Restart and store-layout reload

Actual service-YAML behavior: every edit requires an `sbs-data` restart.

The unconnected logical policy marks `store_config_path` and observability as
live; identity, data path, and both listeners are restart fields. This policy
does not expose a service-YAML hot-reload operation today.

The `/debug/store-config-reload` operation exists only on the development-only
HTTP surface when debug endpoints are enabled. It rereads only the separate
document named by `store_config_path` and rejects removal of an existing store;
it does not reread `sbs-data.yaml`. It is unavailable in a supported
`large_scale` Community deployment.

## Sources checked

- `configs/sbs-data.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/sbs-data/{main,serviceconfig_adoption}.go`
