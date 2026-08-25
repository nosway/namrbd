# `namrbd-gateway` reference

**Purpose:** Gateway control and data-plane daemon

**Scope:** Shipped daemon

**Top-level help status:** `0`

**Version selector:** any argument equal to `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage: namrbd-gateway [flags]

Flags:
  --advertise-control-address string
      control-plane address advertised in metadata/discovery (defaults to host from --control-http-listen in dev) (default )
  --advertise-data-address string
      dataplane address advertised in metadata/discovery (defaults to host from --data-listen, or 127.0.0.1 for wildcard listen) (default )
  --chunk-gc-batch-size int
      maximum allocation chunk (AC) garbage candidates to process per volume in one sweep (default 256)
  --chunk-gc-interval duration
      background allocation chunk (AC) garbage collection interval; <=0 disables the worker (default 30s)
  --config string
      service config file path (AA-IMPL-001D); when set, it supplies stable settings and explicitly typed flags still win (default )
  --control-http-listen string
      HTTP control-plane listen address (default 0.0.0.0:9701)
  --data-backend-mode string
      data backend mode: c6|sbs|sbs-local (sbs-cluster is a deprecated alias for sbs) (default c6)
  --data-disable
      disable dataplane listener while still advertising dataplane endpoint metadata (default false)
  --data-listen string
      binary dataplane listen address (default :9700)
  --dataplane-session-key string
      dataplane session derivation key (or NAMRBD_DP_SESSION_KEY) (default )
  --dataplane-token-key string
      dataplane token signing key (or NAMRBD_DP_TOKEN_SIGNING_KEY) (default )
  --dataplane-token-ttl duration
      dataplane token TTL (default 5m0s)
  --dataplane-wire-version int
      dataplane wire version: 1 or 2 (default 1)
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root path (default /namrbd)
  --gateway-id string
      gateway id advertised in metadata (default <hostname>)
  --gateway-lease-ttl duration
      TTL for gateway liveness lease in etcd (default 15s)
  --gateway-status-refresh-interval duration
      gateway status refresh interval in etcd; jittered by +/-20% (default 5s)
  --max-inflight-bytes uint
      dataplane inflight byte limit (default 8388608)
  --max-inflight-requests uint
      dataplane inflight request limit (default 128)
  --max-io-size uint
      dataplane max io size in bytes (default 4128768)
  --max-zero-like-io-size uint
      dataplane max DISCARD/WRITE_ZEROES logical range size in bytes (default 268435456)
  --metadata-backend string
      metadata backend: memory|etcd (default memory)
  --path-plan-reconcile-interval duration
      background desired/observed gateway path-plan reconcile interval; <=0 disables the worker (default 5s)
  --print-config
      emit an equivalent service config for this invocation on stdout and exit (AA-IMPL-002) (default false)
  --redis-addr string
      redis address (requires -tags legacy_redis) (default 127.0.0.1:6379)
  --sbs-append-only-service-write-effects
      commit SBS write idempotency append-only and wait for service-owned metadata effects (default true)
  --sbs-chunk-id-allocation-cache-size uint
      preallocate this many SBS chunk IDs per gateway volume cache refill; 0 disables (default 256)
  --sbs-cluster-metadata-backend string
      legacy/dev raw SBS cluster metadata backend: pebble; primary admin mode does not open raw SBS cluster metadata (default )
  --sbs-cluster-metadata-path string
      legacy/dev pebble SBS cluster metadata path; primary admin mode must not set this (default )
  --sbs-cluster-metadata-root string
      legacy/dev raw SBS cluster metadata root prefix (default sbs/cluster)
  --sbs-initial-zero-map-evidence
      advertise trusted all-zero allocation evidence for kernel local zero-map initialization (default true)
  --sbs-local-path string
      filesystem path for single-node local SBS when --data-backend-mode=sbs (default )
  --sbs-page-scoped-write-metadata
      prefer page-scoped SBS write metadata commits when the metadata authority supports them (default false)
  --sbs-placement-apply-timeout duration
      timeout for SBS cluster placement apply calls; 0 uses the default and <0 disables the timeout wrapper (default 5s)
  --sbs-quorum-early-replica-writes
      return from non-strict SBS replica writes after any write quorum while remaining replica writes, including a slow primary, complete in the background (default false)
  --sbs-service-endpoint string
      sbs-service admin/internal gRPC endpoint for SBS target, volume, placement, and write authority (default )
  --store-backend string
      storage backend: memory (redis requires -tags legacy_redis) (default memory)
  --tls-cert-file string
      TLS certificate file for control-plane HTTP listener (default )
  --tls-enable
      enable TLS for control-plane HTTP listener (default false)
  --tls-key-file string
      TLS private key file for control-plane HTTP listener (default )
  --tls-server-name string
      advertised TLS server name for control-plane endpoint (default )
  --volume-cache-ttl duration
      TTL for local volume spec cache populated from admin volume lookup or legacy/dev raw metadata (default 30s)
```

## Deprecated flag aliases

| Accepted legacy spelling | Canonical spelling |
| --- | --- |
| `--listen` | `--control-http-listen` |
| `--sbs-admin-endpoint` | `--sbs-service-endpoint` |

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_DP_SESSION_KEY` | direct runtime/default input |
| `NAMRBD_DP_TOKEN_SIGNING_KEY` | direct runtime/default input |
| `NAMRBD_GATEWAY_ADVERTISE_CONTROL_ADDRESS` | config override (canonical) |
| `NAMRBD_GATEWAY_ADVERTISE_DATA_ADDRESS` | config override (canonical) |
| `NAMRBD_GATEWAY_CONTROL_LISTEN` | config override (canonical), direct runtime/default input |
| `NAMRBD_GATEWAY_DATA_LISTEN` | config override (canonical) |
| `NAMRBD_GATEWAY_ID` | config override (canonical) |
| `NAMRBD_GATEWAY_LISTEN` | deprecated compatibility alias |
| `NAMRBD_GATEWAY_SBS_ADMIN_ENDPOINT` | deprecated compatibility alias |
| `NAMRBD_SBS_SERVICE_ENDPOINT` | config override (canonical), direct runtime/default input |

## Source of truth

- Entry point: `cmd/namrbd-gateway`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
