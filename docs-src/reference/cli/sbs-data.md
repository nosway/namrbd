# `sbs-data` reference

**Purpose:** SBS payload service

**Scope:** Shipped daemon

**Top-level help status:** `0`

**Version selector:** any argument equal to `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage: sbs-data [flags]

Flags:
  --cluster-id string
      NAMRBD cluster id (default namrbd-dev)
  --config string
      service config file path (AA-IMPL-001G); store layout stays in --store-config (default )
  --node-id string
      sbs-data node id (default sbs-data-1)
  --path string
      local sbs-data path (default ./var/sbs-data)
  --sbs-cluster-id string
      SBS cluster id; defaults to --cluster-id when omitted (default )
  --sbs-data-http-listen string
      listen address for sbs-data HTTP health and observability (default 0.0.0.0:9082)
  --sbs-data-listen string
      listen address for sbs-data gRPC (default 0.0.0.0:9444)
  --store value
      payload store spec path:shards=N,weight=W[,id=ID] (repeatable) (default )
  --store-config string
      YAML store config file path (default )
```

## Deprecated flag aliases

| Accepted legacy spelling | Canonical spelling |
| --- | --- |
| `--grpc-listen` | `--sbs-data-listen` |
| `--http-listen` | `--sbs-data-http-listen` |

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_BIND_ADDR` | deprecated compatibility alias |
| `NAMRBD_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_SBS_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_SBS_DATA_DIR` | deprecated compatibility alias |
| `NAMRBD_SBS_DATA_GRPC_LISTEN` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_DATA_HTTP_LISTEN` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_DATA_NODE_ID` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_DATA_PATH` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_GRPC_ADDR` | deprecated compatibility alias |
| `NAMRBD_SBS_STORE_CONFIG` | direct runtime/default input |

## Behavior notes

- When `--config` is present, cluster-ID environment/CLI values are initial defaults rather than registered post-file overrides and can be replaced by YAML. The configuration reference records the field-specific precedence contract.

## Source of truth

- Entry point: `cmd/sbs-data`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
