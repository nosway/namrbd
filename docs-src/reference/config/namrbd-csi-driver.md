# `namrbd-csi-driver` configuration

Community example: `configs/namrbd-csi-driver.yaml`. See the [configuration
index](index.md) for the common file and precedence rules.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `csi_driver.driver_name` | string | `block.namrbd.io` | `namrbd.csi.nosway.io` | Required. Must match the Kubernetes `CSIDriver` object. The example is not the built-in name. |
| `csi_driver.node_id` | string | `NAMRBD_CSI_NODE_ID`, then `HOSTNAME`, then the runtime host name, finally `namrbd-csi-node` | `worker-01` | Dynamically derived when omitted. `large_scale` requires an effective YAML, environment, or explicitly typed CLI node ID during adoption; the generic hostname fallback alone does not satisfy that admission check. |
| `csi_driver.endpoint` | string URI | `unix:///tmp/namrbd-csi.sock` | `unix:///csi/csi.sock` | Required. Runtime accepts a non-empty `unix://` path or `tcp://` address. |
| `csi_driver.admin_endpoints` | list of strings | environment-provided primary/list when set; otherwise primary `127.0.0.1:9897` and list empty | two service endpoints | Required. Entries may be plain endpoints or `node_id=endpoint`. `large_scale` adoption requires at least two entries. |
| `csi_driver.cluster_id` | string | `NAMRBD_CLUSTER_ID`, otherwise `namrbd-lab` | `namrbd-prod` | Passed to the SBS admin requests; schema validation does not require a non-empty value. |
| `csi_driver.sbs_cluster_id` | string | `NAMRBD_SBS_CLUSTER_ID`, legacy alias, or `sbs-lab` | `sbs-prod` | Passed to SBS and attach helpers; schema validation does not require a non-empty value. |
| `csi_driver.gateway_url` | string URL | `NAMRBD_GATEWAY_URL`, otherwise empty | `https://gateway.namrbd.internal:8080` | Optional attach gateway URL. |
| `csi_driver.observability.listen` | string address | no runtime binding | `127.0.0.1:9104` | Accepted but ignored; the driver opens no observability listener from this block. |
| `csi_driver.observability.trace` | boolean | no runtime binding | `false` | Must be false in `large_scale`; otherwise accepted but ignored. |
| `csi_driver.observability.debug_endpoints` | boolean | no runtime binding | `false` | Must be false in `large_scale`; otherwise accepted but ignored. |

The node ID is dynamic by design, so no single hostname is documented as its
default. The schema validator emits only a warning for one admin endpoint in
`large_scale`, but daemon adoption strengthens that condition to a startup
error.

## Environment overrides

| YAML field | Environment input | Legacy alias | Notes |
| --- | --- | --- | --- |
| `csi_driver.node_id` | `NAMRBD_CSI_NODE_ID` | none | Replaces YAML. |
| `csi_driver.endpoint` | `NAMRBD_CSI_ENDPOINT` | none | Shared-loader override; without `--config`, the binary uses its built-in endpoint. |
| `csi_driver.admin_endpoints` | `NAMRBD_SBS_SERVICE_ENDPOINTS` | `NAMRBD_ADMIN_ENDPOINTS` | Comma or whitespace-separated list; replaces the YAML list. |
| `csi_driver.admin_endpoints[0]` | `NAMRBD_SBS_SERVICE_ENDPOINT` | `NAMRBD_ADMIN_ENDPOINT` | Replaces the primary entry. |
| `csi_driver.cluster_id` | `NAMRBD_CLUSTER_ID` | none | Replaces YAML. |
| `csi_driver.sbs_cluster_id` | `NAMRBD_SBS_CLUSTER_ID` | `SBS_CLUSTER_ID` | Replaces YAML. |
| `csi_driver.gateway_url` | `NAMRBD_GATEWAY_URL` | none | Replaces YAML. |

There are no environment overrides for `driver_name` or the observability
block.

`NAMRBDCTL` is a direct default for the CLI-only helper path; it is not a YAML
override. The `large_scale` gate rejects an explicitly typed `--namrbdctl` but
does not reject this environment path, which is a current enforcement gap.
Leave it unset unless the packaged helper location intentionally differs.

## CLI overrides

Explicit `--node-id`, `--endpoint`, `--admin-endpoints`, `--admin-endpoint`,
`--cluster-id`, `--sbs-cluster-id`, and `--gateway-url` replace the matching
YAML value. The primary and plural admin flags jointly form the leader-aware
endpoint set.

`driver_name` is a current exception: in `dev`, a non-empty YAML value replaces
an explicitly typed `--driver-name`. Under `large_scale`, the daemon rejects
explicit `--driver-name`, `--cluster-id`, and `--sbs-cluster-id` flags so these
cluster-wide values cannot differ per node. It also rejects the CLI-only
`--vendor-version` flag and the non-YAML `--namrbdctl` flag.

## Restart policy

Actual behavior: every service-YAML edit requires a driver restart.

The unconnected logical policy marks `gateway_url` and observability as live.
Driver identity, node identity, CSI endpoint, SBS endpoints, and cluster IDs are
restart fields.

## Sources checked

- `configs/namrbd-csi-driver.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/namrbd-csi-driver/{main,serviceconfig_adoption}.go`
- `internal/csi/driver/server.go`
