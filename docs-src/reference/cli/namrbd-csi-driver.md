# `namrbd-csi-driver` reference

**Purpose:** Kubernetes CSI Identity, Controller, and Node service

**Scope:** Shipped daemon

**Top-level help status:** `0`

**Version selector:** first argument `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage of namrbd-csi-driver:
  -admin-endpoint string
        primary sbs-service gRPC endpoint (default "127.0.0.1:9897")
  -admin-endpoints string
        optional comma/space-separated sbs-service gRPC endpoints; entries may be node_id=endpoint
  -cluster-id string
        NAMRBD cluster id (default "namrbd-lab")
  -config string
        service config file path (AA-IMPL-001H)
  -driver-name string
        CSI driver name (default "block.namrbd.io")
  -endpoint string
        CSI listening endpoint, unix://path or tcp://host:port (default "unix:///tmp/namrbd-csi.sock")
  -gateway-url string
        NAMRBD gateway URL used by CSI Node attach
  -namrbdctl string
        namrbdctl path used by CSI Node helper (default "namrbdctl")
  -node-id string
        CSI node id
  -sbs-cluster-id string
        SBS cluster id (default "sbs-lab")
  -vendor-version string
        CSI vendor version (default "v1.0.0")
```

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `HOSTNAME` | direct runtime/default input |
| `NAMRBDCTL` | direct runtime/default input |
| `NAMRBD_ADMIN_ENDPOINT` | deprecated compatibility alias |
| `NAMRBD_ADMIN_ENDPOINTS` | deprecated compatibility alias |
| `NAMRBD_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_CSI_ENDPOINT` | config override (canonical) |
| `NAMRBD_CSI_NODE_ID` | config override (canonical), direct runtime/default input |
| `NAMRBD_GATEWAY_URL` | direct runtime/default input |
| `NAMRBD_SBS_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_SBS_SERVICE_ENDPOINT` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_SERVICE_ENDPOINTS` | config override (canonical), direct runtime/default input |
| `SBS_CLUSTER_ID` | deprecated compatibility alias |

## Source of truth

- Entry point: `cmd/namrbd-csi-driver`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
