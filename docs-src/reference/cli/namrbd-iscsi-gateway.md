# `namrbd-iscsi-gateway` reference

**Purpose:** Basic iSCSI target gateway

**Scope:** Shipped daemon; Community supports at most three distinct exported volumes

**Top-level help status:** `0`

**Version selector:** first argument `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage: namrbd-iscsi-gateway [flags]

Flags:
  --allowed-initiator-iqns string
      comma-separated initiator IQN allowlist for summary/admission evidence (default )
  --attachment-id string
      SBS attachment id for writer context (default )
  --auth-mode string
      iSCSI auth mode: none or chap (default none)
  --chap-secret-ref string
      CHAP secret reference for --auth-mode=chap; raw secrets are not accepted (default )
  --config string
      service config file path (AA-IMPL-001E); when set, it supplies instance settings while target/LUN/export mappings stay registry-owned (default )
  --export-id string
      export id (default memory)
  --generation uint
      SBS attachment generation for writer context (default 0)
  --iscsi-gateway-id string
      local iSCSI gateway id for SBS request context (default )
  --json
      emit final JSON summary to stdout (default false)
  --lun-id uint
      iSCSI LUN id for registry-backed SBS export (default 0)
  --observability-listen string
      optional HTTP listen address for /healthz, /readyz, and /metrics (default )
  --operation-jsonl string
      optional operation JSONL artifact path (default )
  --portal string
      explicit portal address (default )
  --registry-required
      fail startup unless the iSCSI registry is loaded (default false)
  --sbs-data-endpoint string
      sbs-data VolumeService gRPC endpoint host:port for --backend=sbs (default )
  --sbs-device-id uint
      optional SBS/SCSI device id for the iSCSI LUN; defaults from LUN WWN (default 0)
  --sbs-endpoint-server-name string
      TLS server name for --sbs-data-endpoint (default )
  --sbs-endpoint-tls
      use TLS for --sbs-data-endpoint (default false)
  --sbs-host-id string
      SBS host id override for the iSCSI export (default )
  --sbs-service-endpoint string
      sbs-service AdminService gRPC endpoint host:port for registry-backed iSCSI config (default )
  --serve
      start gotgt iSCSI target (default false)
  --session-id string
      SBS session id override for request context (default )
  --summary-json string
      optional summary JSON artifact path (default )
  --target-iqn string
      optional target IQN override (default )
  --volume-id string
      SBS volume id for --backend=sbs (default )
```

## Deprecated flag aliases

| Accepted legacy spelling | Canonical spelling |
| --- | --- |
| `--sbs-endpoint` | `--sbs-data-endpoint` |
| `--sbs-admin-endpoint` | `--sbs-service-endpoint` |

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_ISCSI_ADVERTISE_PORTALS` | config override (canonical) |
| `NAMRBD_ISCSI_GATEWAY_ID` | config override (canonical) |
| `NAMRBD_ISCSI_SBS_ADMIN_ENDPOINT` | deprecated compatibility alias |
| `NAMRBD_ISCSI_SBS_DATA_ENDPOINT` | config override (canonical) |
| `NAMRBD_ISCSI_SBS_ENDPOINT` | deprecated compatibility alias |
| `NAMRBD_ISCSI_SBS_SERVICE_ENDPOINT` | config override (canonical) |

## Behavior notes

- The config override registry names `--advertise-portals`, but the actual CLI exposes `--portal`; with `--config`, a YAML portal can therefore replace an explicit `--portal` value. This is a documented implementation limitation, not recommended precedence.

## Source of truth

- Entry point: `cmd/namrbd-iscsi-gateway`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
