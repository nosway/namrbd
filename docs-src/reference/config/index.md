# Service configuration reference

This reference covers the six versioned YAML files accepted by the Community
build. It describes the code that is shipped today, including fields that the
schema accepts but a daemon does not yet consume at runtime.

| Process | Example | Reference |
| --- | --- | --- |
| `namrbd-gateway` | `configs/namrbd-gateway.yaml` | [Gateway](namrbd-gateway.md) |
| `namrbd-iscsi-gateway` | `configs/namrbd-iscsi-gateway.yaml` | [iSCSI gateway](namrbd-iscsi-gateway.md) |
| `sbs-service` | `configs/sbs-service.yaml` | [SBS service](sbs-service.md) |
| `sbs-data` | `configs/sbs-data.yaml` | [SBS data](sbs-data.md) |
| `namrbd-csi-driver` | `configs/namrbd-csi-driver.yaml` | [CSI driver](namrbd-csi-driver.md) |
| `namrbd-mcp` | `configs/namrbd-mcp.yaml` | [MCP server](namrbd-mcp.md) |

The example values are deployment examples, not defaults. Each daemon page
shows them separately from the built-in value used when `--config` is absent.
The YAML decoder itself injects no defaults: omitted scalar fields initially
have the Go zero value, and validation or the daemon's adoption code then
decides whether that is admissible.

## Common file contract

| Key | Type | Required value | Example value | Meaning |
| --- | --- | --- | --- | --- |
| `schema_version` | integer | exactly `1` | `1` | Schema contract version. |
| `revision` | integer | greater than `0` | `1` | Operator-assigned revision. It is independent of the loader's content digest. |
| `profile` | string enum | `dev` or `large_scale` | `large_scale` | Operational strictness profile. It is not an edition or license selector. |
| `process` | string enum | one of the six process names above | process-specific | Must match both the binary and the sole process block in the file. |
| process block | object | exactly one of `gateway`, `iscsi_gateway`, `sbs_service`, `sbs_data`, `csi_driver`, or `mcp` | process-specific | A file cannot configure multiple daemons. |

Unknown keys are startup errors. The loader also rejects text that looks like
an inline password, token, key, or certificate before decoding YAML. With the
`large_scale` profile, the config file must have no group or other permission
bits; install it as mode `0600`.

## Precedence

The shared loader's intended order, from lowest to highest, is:

```text
built-in daemon value < YAML file < allowed environment override < explicitly typed CLI override
```

Environment and CLI overrides are allowlisted per daemon. A CLI default is not
an explicit override; only a flag actually present in the command line wins.
Fields absent from a page's override table are file-authoritative unless that
page records a daemon-specific adoption exception.

Canonical environment names win over deprecated aliases. The aliases warn in
v1.0.x and are rejected from v1.1.0. A `large_scale` startup fails when a
canonical name and its legacy alias are both present with different values.

Several daemons also read environment variables while constructing their
built-in flag values. The daemon pages distinguish these direct inputs from
the shared override allowlist and call out current precedence drift.

## Secret references

A secret-valued field is an object with at most one source:

```yaml
key:
  file: /etc/namrbd/secret.key
# or: env: NAMRBD_SECRET_KEY
# or: kms: provider/key/name
```

`file`, `env`, and `kms` are mutually exclusive strings. KMS references parse,
but resolution is not implemented in this build; a daemon path that tries to
resolve one therefore fails closed. Today the shared resolver is used by the
gateway data-plane token and session references. For those fields under
`large_scale`, a file source must have no group/other permission bits and must
be owned by the running user. Gateway listener TLS and SBS TiKV keys are passed
on as paths, while the current iSCSI authentication path fails closed before it
can provide CHAP; they do not receive that shared resolver's file-mode/owner
check. Accepted-but-unconsumed references do not reach a resolver either.

## Dependency threshold block

`gateway.dependency`, `iscsi_gateway.dependency`, and
`sbs_service.dependency` share this schema:

| Key | Type | Shipped default | Rule |
| --- | --- | --- | --- |
| `etcd_unavailable_grace_seconds` | integer seconds | `300` | Must be greater than `0`. |
| `tikv_unavailable_grace_seconds` | integer seconds | `300` | Must be greater than `0`. |
| `projection_stale_degraded_ms` | integer milliseconds | `5000` | Must be greater than `0`. |
| `projection_stale_blocked_ms` | integer milliseconds | `15000` | Must be greater than `projection_stale_degraded_ms`. |

The shipped defaults apply only when the whole `dependency` block is omitted.
If the block is present, every leaf must be supplied; an omitted leaf becomes
zero and fails validation.

## Restart and reload

All six daemons currently load their service YAML only during startup; the
effective service-YAML contract is **startup-only**. Although
the shared package classifies fields as logically `live` or `restart`, no
daemon calls that generic reload implementation. Consequently every edit to
one of these six service YAML files requires a daemon restart today. The pages
show the logical policy separately so it is not mistaken for an available hot
reload interface.

The iSCSI `reload` block controls refresh of registry-owned serving data, not
reload of this YAML. The `sbs-data` store-layout document named by
`store_config_path` also has a separate lifecycle; changing that included file
is not the same as reloading `configs/sbs-data.yaml`.

## Source boundary

This reference was checked against `internal/serviceconfig/schema.go`,
`loader.go`, `registry.go`, `validate.go`, `reload.go`, the six daemon adoption
files under `cmd/`, and the six examples under `configs/`.
