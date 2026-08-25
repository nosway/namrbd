# `sbs-service` reference

**Purpose:** SBS metadata and administrative authority

**Scope:** Shipped daemon

**Top-level help status:** `0`

**Version selector:** any argument equal to `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
Usage: sbs-service [flags]

Flags:
  --cluster-id string
      cluster id (default namrbd-dev)
  --config string
      service config file path (AA-IMPL-001F); when set it supplies stable settings, while environment variables and explicitly typed flags still win (default )
  --leader-lease-duration duration
      leader lease duration (default 10s)
  --leader-renew-interval duration
      leader lease renew interval (default 3s)
  --metadata-backend string
      metadata backend: pebble or tikv (default pebble)
  --metadata-path string
      local metadata path for bootstrap development (default ./var/sbs-metadata)
  --native-allocation-fast-path
      enable native-allocation fast path for already-normalized allocation-backed write effects (default true)
  --node-id string
      local sbs-service node id (default sbs-svc-1)
  --payload-root string
      local replica payload root for automatic payload GC (default )
  --sbs-cluster-id string
      SBS cluster id; defaults to --cluster-id when omitted (default )
  --sbs-service-http-listen string
      listen address for sbs-service HTTP health and observability (default 0.0.0.0:9081)
  --sbs-service-listen string
      listen address for sbs-service gRPC (default 0.0.0.0:9443)
  --service-owned-write-effects
      own the ordered service-side write-effects queue for append-only write metadata mode (default true)
  --tikv-api-version string
      TiKV API version for metadata backend (default v1)
  --tikv-async-commit
      enable TiKV async commit for metadata transactions (default false)
  --tikv-ca-file string
      CA file for TiKV metadata backend (default )
  --tikv-cert-file string
      client cert file for TiKV metadata backend (default )
  --tikv-key-file string
      client key file for TiKV metadata backend (default )
  --tikv-keyspace string
      TiKV keyspace for metadata backend (default )
  --tikv-one-phase-commit
      enable TiKV one-phase commit for eligible metadata transactions (default false)
  --tikv-operation-trace
      emit structured TiKV metadata operation latency trace events (default false)
  --tikv-pd-endpoints string
      comma-separated TiKV PD endpoints (default )
  --tikv-timeout duration
      TiKV metadata request timeout (default 3s)
  --tikv-tls-enabled
      enable TLS for TiKV metadata backend (default false)
  --write-effects-batch-max int
      maximum service-owned write-effects items to commit in one metadata batch (default 16)
```

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_BIND_ADDR` | deprecated compatibility alias |
| `NAMRBD_CA_FILE` | direct runtime/default input |
| `NAMRBD_CERT_FILE` | direct runtime/default input |
| `NAMRBD_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_CLUSTER_STATUS_DETAIL_TIMEOUT` | direct runtime/default input |
| `NAMRBD_KEY_FILE` | direct runtime/default input |
| `NAMRBD_NODE_ID` | deprecated compatibility alias |
| `NAMRBD_OBSERVABILITY_SNAPSHOT_TIMEOUT` | direct runtime/default input |
| `NAMRBD_SBS_ADMIN_ADDR` | deprecated compatibility alias |
| `NAMRBD_SBS_ALLOCATION_CHUNK_SIZE` | direct runtime/default input |
| `NAMRBD_SBS_ALLOCATION_PAGE_SIZE` | direct runtime/default input |
| `NAMRBD_SBS_AUTO_REBALANCE_FOREGROUND_WRITE_SETTLE_AGE` | direct runtime/default input |
| `NAMRBD_SBS_AUTO_REBALANCE_MIN_VOLUME_AGE` | direct runtime/default input |
| `NAMRBD_SBS_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_SBS_DATA_DOWN_AFTER` | direct runtime/default input |
| `NAMRBD_SBS_DATA_HEALTH_CHECK_INTERVAL` | direct runtime/default input |
| `NAMRBD_SBS_DATA_HEALTH_CONCURRENCY` | direct runtime/default input |
| `NAMRBD_SBS_DATA_HEALTH_SHARD_COUNT` | direct runtime/default input |
| `NAMRBD_SBS_DATA_HEALTH_TIMEOUT` | direct runtime/default input |
| `NAMRBD_SBS_DATA_RECOVER_AFTER` | direct runtime/default input |
| `NAMRBD_SBS_DATA_RECOVER_COOLDOWN` | direct runtime/default input |
| `NAMRBD_SBS_DATA_SUSPECT_AFTER` | direct runtime/default input |
| `NAMRBD_SBS_EXTENT_SIZE` | direct runtime/default input |
| `NAMRBD_SBS_LEADER_LEASE_DURATION` | direct runtime/default input |
| `NAMRBD_SBS_LEADER_RENEW_INTERVAL` | direct runtime/default input |
| `NAMRBD_SBS_MAX_CONCURRENT_PAYLOAD_GCS` | direct runtime/default input |
| `NAMRBD_SBS_METADATA_BACKEND` | direct runtime/default input |
| `NAMRBD_SBS_NATIVE_ALLOCATION_FAST_PATH` | direct runtime/default input |
| `NAMRBD_SBS_PAUSE_PAYLOAD_GCS` | direct runtime/default input |
| `NAMRBD_SBS_PAYLOAD_ROOT` | direct runtime/default input |
| `NAMRBD_SBS_PLACEMENT_APPLY_TIMEOUT` | direct runtime/default input |
| `NAMRBD_SBS_PLACEMENT_RESOLVER_CACHE_TTL` | direct runtime/default input |
| `NAMRBD_SBS_PUBLISHED_VIEW_CACHE_TTL` | direct runtime/default input |
| `NAMRBD_SBS_SERVICE_GRPC_LISTEN` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_SERVICE_HTTP_LISTEN` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_SERVICE_NODE_ID` | config override (canonical), direct runtime/default input |
| `NAMRBD_SBS_SERVICE_OWNED_WRITE_EFFECTS` | direct runtime/default input |
| `NAMRBD_SBS_STATE_DIR` | direct runtime/default input |
| `NAMRBD_SBS_TRANSITION_COPY_CHUNK_SIZE` | direct runtime/default input |
| `NAMRBD_SBS_WRITE_EFFECTS_BATCH_COALESCE_WAIT` | direct runtime/default input |
| `NAMRBD_SBS_WRITE_EFFECTS_BATCH_MAX` | direct runtime/default input |
| `NAMRBD_TIKV_API_VERSION` | direct runtime/default input |
| `NAMRBD_TIKV_ASYNC_COMMIT` | direct runtime/default input |
| `NAMRBD_TIKV_KEYSPACE` | direct runtime/default input |
| `NAMRBD_TIKV_ONE_PHASE_COMMIT` | direct runtime/default input |
| `NAMRBD_TIKV_OPERATION_TRACE` | direct runtime/default input |
| `NAMRBD_TIKV_PD_ENDPOINTS` | direct runtime/default input |
| `NAMRBD_TIKV_TIMEOUT` | direct runtime/default input |
| `NAMRBD_TIKV_TLS_ENABLED` | direct runtime/default input |

## Source of truth

- Entry point: `cmd/sbs-service`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
