# CLI Command Reference

This reference is generated from freshly built, untagged Community binaries at
the current source revision and filtered through the reviewed public Community
edition policy. It covers every shipped Community binary. Internal, fixture,
hidden, and Enterprise-adjacent parser surfaces are retained only in the
canonical internal reference and are not published here as supported syntax. The
[Feature Status](../../feature-status.md) page remains authoritative for release
support and edition availability.

`namrbd-iscsictl` is deprecated and not shipped in v1.0; use `sbsctl iscsi`.
Internal debug and benchmark binaries are not part of this public reference.
Historical `namrbd-meta` source is archived and is not an active command surface.

## Binary map

| Binary | Purpose | Distribution | Reference shape |
| --- | --- | --- | --- |
| [`namrbdctl`](namrbdctl.md) | Linux host/device control and gateway-facing volume I/O | Shipped (Community and Enterprise) | 30 command paths |
| [`sbsctl`](sbsctl.md) | SBS cluster, volume, snapshot, maintenance, and basic iSCSI administration | Shipped; this page is generated from the Community build | 76 command paths |
| [`namrbd-gateway`](namrbd-gateway.md) | Gateway control and data-plane daemon | Shipped daemon | daemon flags |
| [`sbs-service`](sbs-service.md) | SBS metadata and administrative authority | Shipped daemon | daemon flags |
| [`sbs-data`](sbs-data.md) | SBS payload service | Shipped daemon | daemon flags |
| [`namrbd-iscsi-gateway`](namrbd-iscsi-gateway.md) | Basic iSCSI target gateway | Shipped daemon; Community supports at most three distinct exported volumes | daemon flags |
| [`namrbd-csi-driver`](namrbd-csi-driver.md) | Kubernetes CSI Identity, Controller, and Node service | Shipped daemon | daemon flags |
| [`namrbd-mcp`](namrbd-mcp.md) | Read-only MCP operations server | Shipped daemon; observe posture only in large_scale | daemon flags |

## Common invocation rules

- `namrbdctl help COMMAND`, `sbsctl help COMMAND [SUBCOMMAND ...]`, and a
  trailing `help` on their leaf commands request command help.
- `--json` is a root convenience. Where a leaf has `--output`, `sbsctl`
  rewrites it to `--output=json`; `namrbdctl` emits its JSON result/error form.
- Flags are parsed before positional leftovers by Go's `flag` package. Commands
  in this reference use the command path itself as the positional argument;
  leaf inputs are flags unless a synopsis explicitly says otherwise. Most leaf
  handlers currently do not reject surplus positional operands, so an ignored
  extra token must never be treated as a successful input.
- Configuration precedence for context-aware CLIs is built-in default, context
  file, environment, then explicit CLI flag. Daemon `--config` precedence and
  its narrower override allowlist are documented on each daemon page and in the
  configuration reference.
- Deprecated flag aliases emit a warning on stderr and are rewritten to the
  canonical spelling. Deprecated environment variables use canonical-over-
  legacy precedence, warn in v1.0.x, and are removed in v1.1.0.

## Exit status contract

| Status | General meaning | Important exceptions |
| --- | --- | --- |
| `0` | Success, graceful daemon stop, or supported help/version request | With JSON output selected, `namrbdctl validate-volume` and `namrbdctl validate-all` currently return `0` for an invalid report; their text paths return `1`. JSON consumers must inspect the result fields. |
| `1` | Runtime/operation failure or `fatalf` validation failure | Many `sbsctl` missing-required-flag checks use this status. |
| `2` | Unknown command, malformed command line, or flag parse/config admission failure | `namrbd-mcp --help` also returns `2`; `namrbd-iscsi-gateway` uses `2` for startup/config/admission validation and `1` for a running self-test or serving failure. |

Scripts must not treat every nonzero status as the same error class: parse and
admission failures (`2`) are different from an attempted operation that failed
(`1`). JSON-producing commands keep diagnostics on stderr.

## Edition and hidden surfaces

The checked-in pages are generated without `-tags=enterprise` and then filtered
through an explicit public-surface policy. Enterprise command groups, advanced
iSCSI HA/ALUA controls, Enterprise-only common-command options, and flags hidden
from normal help are absent. The generator fails if a future denied command,
flag, or environment family survives into the public pages.

## Reproducing this reference

```bash
make cli-reference
make cli-reference-check
```

The check rebuilds the Community command packages, executes every discovered
leaf help path without contacting a service, normalizes host-derived defaults,
and fails on a generated Markdown diff in either the public or canonical
internal output tree.
