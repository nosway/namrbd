# `namrbd-mcp` reference

**Purpose:** Read-only MCP operations server

**Scope:** Shipped daemon; observe posture only in large_scale

**Top-level help status:** `2`

**Version selector:** first argument `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage of namrbd-mcp:
  -approval-policy string
        approval policy: dry-run, external-token, or local-confirmation (default "dry-run")
  -config string
        service config file path
  -http-timeout duration
        HTTP collector timeout (default 3s)
  -mode string
        MCP posture: observe or operate (default "observe")
  -operation-output-dir string
        directory for future MCP operation records (default ".cache/namrbd-mcp-operations")
  -operations-endpoint string
        sbs-service read-only operations endpoint (default "http://127.0.0.1:9081")
```

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_MCP_OPERATIONS_ENDPOINT` | config override (canonical) |

## Source of truth

- Entry point: `cmd/namrbd-mcp`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
