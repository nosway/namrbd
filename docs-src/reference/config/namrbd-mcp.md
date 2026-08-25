# `namrbd-mcp` configuration

Community example: `configs/namrbd-mcp.yaml`. See the [configuration
index](index.md) for the common file and precedence rules.

The supported Community MCP surface is read-only. Use the `observe` posture;
parser acceptance of another posture in `dev` is not evidence of a supported
write surface.

## Schema

| YAML key | Type | Built-in without `--config` | Example | Validation and runtime consumption |
| --- | --- | --- | --- | --- |
| `mcp.operations_endpoint` | string URL | `http://127.0.0.1:9081` | `https://sbs-service.namrbd.internal:9092` | Required. Leading/trailing whitespace and trailing `/` are removed by normalization. |
| `mcp.mode` | string enum | `observe` | `observe` | Schema accepts `observe` or `operate`; `large_scale` refuses `operate`. Community deployments should use `observe`. |
| `mcp.approval_policy` | string | `dry-run` | `dry-run` | Must be non-empty. CLI help names `dry-run`, `external-token`, and `local-confirmation`, but current schema/runtime validation does not enforce that list. |
| `mcp.operation_output_dir` | string path | `.cache/namrbd-mcp-operations` | `/var/lib/namrbd/mcp-operations` | Empty YAML preserves the built-in or explicit CLI value. Reserved output directory for operation records. |
| `mcp.http_timeout_seconds` | integer seconds | `3` | `10` | A positive YAML value is applied. Zero or a negative value is ignored by adoption, after which normalization preserves or restores `3s`. |
| `mcp.observability.listen` | string address | no runtime binding | `127.0.0.1:9105` | Accepted but ignored. MCP serves over stdio and does not open this listener. |
| `mcp.observability.trace` | boolean | no runtime binding | `false` | Must be false in `large_scale`; otherwise accepted but ignored. |
| `mcp.observability.debug_endpoints` | boolean | no runtime binding | `false` | Must be false in `large_scale`; otherwise accepted but ignored. |

## Environment override

`NAMRBD_MCP_OPERATIONS_ENDPOINT` replaces
`mcp.operations_endpoint` through the shared loader when `--config` is used.
There are no YAML environment overrides for the remaining fields.

## CLI overrides

An explicitly typed `--operations-endpoint`, `--mode`, `--approval-policy`,
`--operation-output-dir`, or `--http-timeout` replaces the corresponding YAML
value. The timeout CLI flag uses Go duration syntax such as `3s`, while the YAML
field is an integer number of seconds.

After all overrides, `large_scale` checks the effective posture again and
rejects `operate`.

## Restart policy

Actual behavior: every service-YAML edit requires an MCP process restart.

The unconnected logical policy marks the operations endpoint, output directory,
HTTP timeout, and observability block as live. Mode and approval policy are
restart fields.

## Sources checked

- `configs/namrbd-mcp.yaml`
- `internal/serviceconfig/{schema,loader,registry,validate,reload}.go`
- `cmd/namrbd-mcp/{main,serviceconfig_adoption}.go`
- `internal/mcpops/config.go`
