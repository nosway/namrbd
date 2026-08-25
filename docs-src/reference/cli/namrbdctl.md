# `namrbdctl` reference

**Purpose:** Linux host/device control and gateway-facing volume I/O

**Scope:** Shipped (Community and Enterprise)

**Top-level help status:** `0`

**Version selector:** first argument `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
usage:
  namrbdctl [--json] help COMMAND
  namrbdctl create-device
  namrbdctl destroy-device --device DEVICE_ID
  namrbdctl config-rest --device DEVICE_ID --server id,address,port,tls,api_prefix[,token]
  namrbdctl attach --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway URL | --etcd-endpoints HOST:PORT[,HOST:PORT...]] [--discovery-max-paths N --discovery-prefer-gateway GW]
  namrbdctl reconfigure-data-paths --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway https://HOST:PORT --gateway-ca-file CA.pem --discovery-max-paths N --discovery-prefer-gateway GW]
  namrbdctl volume-reload-size --device DEVICE_ID --host HOST --volume VOLUME_ID --gateway https://HOST:PORT [--gateway-ca-file CA.pem]
  namrbdctl detach --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway https://HOST:PORT --gateway-ca-file CA.pem]
  namrbdctl status --device DEVICE_ID [--gateway URL --volume VOLUME_ID --report-runtime-feedback]
  namrbdctl list-devices
direct-etcd metadata read commands:
  namrbdctl gateway-list [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl gateway-get --gateway GATEWAY_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl attachment-get --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-list [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-get --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-status --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl validate-volume --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl validate-all [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
direct-etcd metadata mutation commands:
  namrbdctl gateway-put --from-file PATH [--gateway GATEWAY_ID] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-create [--name NAME] --size <n>M|<n>G|<n>T [--block-size <n>K] [--allocation-chunk-size <n>K|<n>M] [--allocation-page-size <n>M|<n>G] [--access-mode exclusive|shared] [--state available|disabled] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-update --volume VOLUME_ID [--name NAME] [--size <n>M|<n>G|<n>T] [--access-mode exclusive|shared] [--state available|in_use|disabled] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-set-state --volume VOLUME_ID --state available|in_use|disabled [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
  namrbdctl volume-delete --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]
gateway-direct commands:
  namrbdctl info --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--gateway-ca-file CA.pem]
  namrbdctl discover-gateways --gateway http://127.0.0.1:9701 [--gateway-ca-file CA.pem]
  namrbdctl discover-volume --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--gateway-ca-file CA.pem]
  namrbdctl plan-volume-paths --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--path-health PATH_ID=healthy|suspect|down --max-active N --gateway-ca-file CA.pem]
  namrbdctl cluster-metrics --gateway http://127.0.0.1:9701 [--gateway-ca-file CA.pem]
  namrbdctl apply-volume-path-plan --device DEVICE_ID --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--path-health PATH_ID=healthy|suspect|down --max-active N --gateway-ca-file CA.pem]
  namrbdctl read --gateway http://127.0.0.1:9701 --volume VOLUME_ID --offset BYTES --length BYTES [--out FILE --gateway-ca-file CA.pem]
  namrbdctl write --gateway http://127.0.0.1:9701 --volume VOLUME_ID --offset BYTES --data-file FILE [--gateway-ca-file CA.pem]
  namrbdctl version
```

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_CA_FILE` | direct runtime/default input |
| `NAMRBD_CONTEXT` | direct runtime/default input |
| `NAMRBD_ETCD_ENDPOINTS` | direct runtime/default input |
| `NAMRBD_ETCD_ROOT` | direct runtime/default input |
| `NAMRBD_GATEWAY_ENDPOINTS` | direct runtime/default input |
| `NAMRBD_HOST_ID` | direct runtime/default input |
| `NAMRBD_TIMEOUT` | direct runtime/default input |

## Behavior notes

- The direct-etcd metadata commands construct independent FlagSets; they do not load `namrbdctl` context-file or environment defaults. Pass their etcd endpoint/root flags explicitly when the built-ins are not correct.

## Command index

| Command path | Help invocation |
| --- | --- |
| `namrbdctl apply-volume-path-plan` | `namrbdctl help apply-volume-path-plan` |
| `namrbdctl attach` | `namrbdctl help attach` |
| `namrbdctl attachment-get` | `namrbdctl help attachment-get` |
| `namrbdctl cluster-metrics` | `namrbdctl help cluster-metrics` |
| `namrbdctl config-rest` | `namrbdctl help config-rest` |
| `namrbdctl create-device` | `namrbdctl help create-device` |
| `namrbdctl destroy-device` | `namrbdctl help destroy-device` |
| `namrbdctl detach` | `namrbdctl help detach` |
| `namrbdctl discover-gateways` | `namrbdctl help discover-gateways` |
| `namrbdctl discover-volume` | `namrbdctl help discover-volume` |
| `namrbdctl gateway-get` | `namrbdctl help gateway-get` |
| `namrbdctl gateway-list` | `namrbdctl help gateway-list` |
| `namrbdctl gateway-put` | `namrbdctl help gateway-put` |
| `namrbdctl info` | `namrbdctl help info` |
| `namrbdctl list-devices` | `namrbdctl help list-devices` |
| `namrbdctl plan-volume-paths` | `namrbdctl help plan-volume-paths` |
| `namrbdctl read` | `namrbdctl help read` |
| `namrbdctl reconfigure-data-paths` | `namrbdctl help reconfigure-data-paths` |
| `namrbdctl status` | `namrbdctl help status` |
| `namrbdctl validate-all` | `namrbdctl help validate-all` |
| `namrbdctl validate-volume` | `namrbdctl help validate-volume` |
| `namrbdctl volume-create` | `namrbdctl help volume-create` |
| `namrbdctl volume-delete` | `namrbdctl help volume-delete` |
| `namrbdctl volume-get` | `namrbdctl help volume-get` |
| `namrbdctl volume-list` | `namrbdctl help volume-list` |
| `namrbdctl volume-reload-size` | `namrbdctl help volume-reload-size` |
| `namrbdctl volume-set-state` | `namrbdctl help volume-set-state` |
| `namrbdctl volume-status` | `namrbdctl help volume-status` |
| `namrbdctl volume-update` | `namrbdctl help volume-update` |
| `namrbdctl write` | `namrbdctl help write` |

## Command flags and defaults

### `namrbdctl apply-volume-path-plan`

```text
Usage: namrbdctl apply-volume-path-plan [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --max-active int
      maximum active dataplane paths (0 = all non-down) (default 0)
  --path-health value
      path health override: PATH_ID=healthy|suspect|down (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl attach`

```text
Usage: namrbdctl attach [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --discovery-max-paths int
      limit active dataplane paths from gateway discovery (0 = all) (default 0)
  --discovery-owner-only
      use only owner-gateway dataplane paths from gateway discovery (default false)
  --discovery-prefer-gateway string
      prefer this gateway id when selecting discovery dataplane paths (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL for userspace-mediated attach (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for mediated gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --host string
      host id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl attachment-get`

```text
Usage: namrbdctl attachment-get [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl cluster-metrics`

```text
Usage: namrbdctl cluster-metrics [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
```

### `namrbdctl config-rest`

```text
Usage: namrbdctl config-rest [flags]

Flags:
  --device uint
      device id (default 0)
  --server value
      server spec: id,address,port,tls,api_prefix[,bearer_token] (default )
```

### `namrbdctl create-device`

```text
Usage: namrbdctl create-device [flags]

Flags:
```

### `namrbdctl destroy-device`

```text
Usage: namrbdctl destroy-device [flags]

Flags:
  --device uint
      device id (default 0)
```

### `namrbdctl detach`

```text
Usage: namrbdctl detach [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL for userspace-mediated detach (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for mediated gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --host string
      host id (default )
  --local-only
      detach only local kernel state without contacting the gateway (default false)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl discover-gateways`

```text
Usage: namrbdctl discover-gateways [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
```

### `namrbdctl discover-volume`

```text
Usage: namrbdctl discover-volume [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --max-paths int
      limit active dataplane paths in output (0 = all) (default 0)
  --owner-only
      show only owner-gateway dataplane paths (default false)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl gateway-get`

```text
Usage: namrbdctl gateway-get [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --gateway string
      gateway id (default )
  --json
      emit JSON output (default false)
```

### `namrbdctl gateway-list`

```text
Usage: namrbdctl gateway-list [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
```

### `namrbdctl gateway-put`

```text
Usage: namrbdctl gateway-put [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --from-file string
      JSON file containing gateway record (default )
  --gateway string
      gateway id override (default )
  --json
      emit JSON output (default false)
```

### `namrbdctl info`

```text
Usage: namrbdctl info [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl list-devices`

```text
Usage: namrbdctl list-devices [flags]

Flags:
```

### `namrbdctl plan-volume-paths`

```text
Usage: namrbdctl plan-volume-paths [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --max-active int
      maximum active dataplane paths (0 = all non-down) (default 0)
  --path-health value
      path health override: PATH_ID=healthy|suspect|down (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl read`

```text
Usage: namrbdctl read [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --length uint
      length bytes (default 0)
  --offset uint
      offset bytes (default 0)
  --out string
      output file path (optional) (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl reconfigure-data-paths`

```text
Usage: namrbdctl reconfigure-data-paths [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --discovery-max-paths int
      limit active dataplane paths from gateway discovery (0 = all) (default 0)
  --discovery-owner-only
      use only owner-gateway dataplane paths from gateway discovery (default false)
  --discovery-prefer-gateway string
      prefer this gateway id when selecting discovery dataplane paths (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL for discovery-expanded manifest (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --host string
      host id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl status`

```text
Usage: namrbdctl status [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --expected-path-plan-revision uint
      expected path plan revision for convergence check (default 0)
  --feedback-source-host string
      source host id for runtime feedback (default )
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --report-runtime-feedback
      report runtime lane attention/recommended actions back to gateway control-plane (default false)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits); defaults to attached runtime volume (default )
```

### `namrbdctl validate-all`

```text
Usage: namrbdctl validate-all [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
```

### `namrbdctl validate-volume`

```text
Usage: namrbdctl validate-volume [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-create`

```text
Usage: namrbdctl volume-create [flags]

Flags:
  --access-mode string
      access mode: exclusive|shared (default exclusive)
  --allocation-chunk-size string
      allocation chunk size as <n>K|<n>M (binary KiB/MiB) (default 64K)
  --allocation-page-size string
      allocation page size as <n>M|<n>G (binary MiB/GiB) (default 4M)
  --block-size string
      block size as <n>K only (binary KiB, e.g. 4K) (default 4K)
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --name string
      globally unique volume name (default )
  --size string
      volume size with unit: <n>M|<n>G|<n>T (default )
  --state string
      initial state: available|disabled (default available)
```

### `namrbdctl volume-delete`

```text
Usage: namrbdctl volume-delete [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-get`

```text
Usage: namrbdctl volume-get [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-list`

```text
Usage: namrbdctl volume-list [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
```

### `namrbdctl volume-reload-size`

```text
Usage: namrbdctl volume-reload-size [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --device uint
      device id (default 0)
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --host string
      host id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-set-state`

```text
Usage: namrbdctl volume-set-state [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --state string
      target state: available|in_use|disabled (default )
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-status`

```text
Usage: namrbdctl volume-status [flags]

Flags:
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl volume-update`

```text
Usage: namrbdctl volume-update [flags]

Flags:
  --access-mode string
      access mode: exclusive|shared (default )
  --etcd-endpoints string
      comma-separated etcd endpoints (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root prefix (default /namrbd)
  --json
      emit JSON output (default false)
  --name string
      globally unique volume name (default )
  --size string
      target volume size with unit: <n>M|<n>G|<n>T (default )
  --state string
      target state: available|in_use|disabled (default )
  --volume string
      volume id (8 lowercase hex digits) (default )
```

### `namrbdctl write`

```text
Usage: namrbdctl write [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-file string
      input data file path (default )
  --etcd-endpoints string
      comma-separated etcd endpoints used for gateway discovery (default 127.0.0.1:2379)
  --etcd-root string
      etcd metadata root used for gateway discovery (default /namrbd)
  --gateway string
      gateway base URL (default http://127.0.0.1:9701)
  --gateway-ca-file string
      PEM CA bundle for gateway TLS (default )
  --gateway-discovery-limit int
      maximum gateway fleet records examined during endpoint discovery (1-512) (default 128)
  --offset uint
      offset bytes (default 0)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      gateway request timeout (default 10s)
  --volume string
      volume id (8 lowercase hex digits) (default )
```

## Source of truth

- Entry point: `cmd/namrbdctl`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
