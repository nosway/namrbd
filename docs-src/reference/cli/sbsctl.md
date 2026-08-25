# `sbsctl` reference

**Purpose:** SBS cluster, volume, snapshot, maintenance, and basic iSCSI administration

**Scope:** Shipped; this page is generated from the Community build

**Top-level help status:** `0`

**Version selector:** first argument `version` or `--version`; success status `0`

This page is generated from the untagged Community build and filtered
through the reviewed public Community edition policy. Defaults shown
inside help are executable defaults after environment lookup;
`<hostname>` marks a runtime-derived host value.

## Top-level help

```text
usage: sbsctl [--json] <command> [args]
       sbsctl help <command> [subcommand]
commands:
  cluster init|status
  node join|update-topology|status|drain|drain status|remove
  topology zone create|list|get|update|delete
  store status|tuning
  volume create|restore-from-snapshot|expand|delete|purge|status|health|placement|transitions|list
  snapshot create|get|list|delete
  iscsi status|lun
  repair list|show
  rebalance list
  maintenance throttle|pause|resume
  operations list|show
  testio open|read|write|flush
  version
```

## Deprecated flag aliases

| Accepted legacy spelling | Canonical spelling |
| --- | --- |
| `--admin-endpoint` | `--sbs-service-endpoint` |
| `--admin-http-endpoint` | `--sbs-service-http-endpoint` |

## Environment variables

The inventory below is source-derived. `config override` means the
variable is on the shared service-config allowlist; `direct runtime`
means the command package reads it while constructing defaults or a
runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.

| Variable | Classification |
| --- | --- |
| `NAMRBD_ATTACHMENT_ID` | direct runtime/default input |
| `NAMRBD_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_CONTEXT` | direct runtime/default input |
| `NAMRBD_GATEWAY_ID` | direct runtime/default input |
| `NAMRBD_NODE_ID` | deprecated compatibility alias |
| `NAMRBD_OUTPUT` | deprecated compatibility alias |
| `NAMRBD_SBSCTL_OUTPUT` | direct runtime/default input |
| `NAMRBD_SBSCTL_TIMEOUT` | direct runtime/default input |
| `NAMRBD_SBS_ADMIN_ADDR` | deprecated compatibility alias |
| `NAMRBD_SBS_ADMIN_ENDPOINTS` | deprecated compatibility alias |
| `NAMRBD_SBS_CLUSTER_ID` | direct runtime/default input |
| `NAMRBD_SBS_DATA_ENDPOINTS` | direct runtime/default input |
| `NAMRBD_SBS_GRPC_ADDR` | deprecated compatibility alias |
| `NAMRBD_SBS_METADATA_PATH` | direct runtime/default input |
| `NAMRBD_SBS_METADATA_ROOT` | direct runtime/default input |
| `NAMRBD_SBS_NODE_ID` | direct runtime/default input |
| `NAMRBD_SBS_PAYLOAD_ROOT` | direct runtime/default input |
| `NAMRBD_SBS_SERVICE_ENDPOINTS` | direct runtime/default input |
| `NAMRBD_SBS_SERVICE_HTTP_ENDPOINT` | direct runtime/default input |
| `NAMRBD_SBS_ZONE` | direct runtime/default input |
| `NAMRBD_TIMEOUT` | deprecated compatibility alias |
| `NAMRBD_ZONE` | deprecated compatibility alias |
| `SBS_ADMIN_ENDPOINTS` | deprecated compatibility alias |
| `SBS_CLUSTER_ID` | deprecated compatibility alias |
| `SBS_DATA_ENDPOINTS` | deprecated compatibility alias |
| `SBS_GRPC_ADDR` | deprecated compatibility alias |
| `SBS_NODE_ADMIN_HTTP` | deprecated compatibility alias |
| `SBS_NODE_ID` | deprecated compatibility alias |
| `SBS_OUTPUT` | deprecated compatibility alias |
| `SBS_TIMEOUT` | deprecated compatibility alias |
| `SBS_ZONE` | deprecated compatibility alias |
| `USER` | direct runtime/default input |

## Behavior notes

- The compiled top-level/group help is not an exhaustive command inventory. The command index below applies the reviewed public Community edition filter to the compiled leaf FlagSets.
- Root `--json` also selects the structured fatal-error path. A leaf `--output=json` flag controls successful result formatting but does not by itself change fatal-error output.

## Command index

| Command path | Help invocation |
| --- | --- |
| `sbsctl cluster init` | `sbsctl help cluster init` |
| `sbsctl cluster status` | `sbsctl help cluster status` |
| `sbsctl iscsi initiator allow` | `sbsctl help iscsi initiator allow` |
| `sbsctl iscsi initiator deny` | `sbsctl help iscsi initiator deny` |
| `sbsctl iscsi initiator get` | `sbsctl help iscsi initiator get` |
| `sbsctl iscsi initiator list` | `sbsctl help iscsi initiator list` |
| `sbsctl iscsi initiator set-auth` | `sbsctl help iscsi initiator set-auth` |
| `sbsctl iscsi lun export` | `sbsctl help iscsi lun export` |
| `sbsctl iscsi lun get` | `sbsctl help iscsi lun get` |
| `sbsctl iscsi lun list` | `sbsctl help iscsi lun list` |
| `sbsctl iscsi lun set-mode` | `sbsctl help iscsi lun set-mode` |
| `sbsctl iscsi lun unexport` | `sbsctl help iscsi lun unexport` |
| `sbsctl iscsi portal create` | `sbsctl help iscsi portal create` |
| `sbsctl iscsi portal delete` | `sbsctl help iscsi portal delete` |
| `sbsctl iscsi portal disable` | `sbsctl help iscsi portal disable` |
| `sbsctl iscsi portal enable` | `sbsctl help iscsi portal enable` |
| `sbsctl iscsi portal get` | `sbsctl help iscsi portal get` |
| `sbsctl iscsi portal list` | `sbsctl help iscsi portal list` |
| `sbsctl iscsi session disconnect` | `sbsctl help iscsi session disconnect` |
| `sbsctl iscsi session get` | `sbsctl help iscsi session get` |
| `sbsctl iscsi session list` | `sbsctl help iscsi session list` |
| `sbsctl iscsi status gateway` | `sbsctl help iscsi status gateway` |
| `sbsctl iscsi status target` | `sbsctl help iscsi status target` |
| `sbsctl iscsi target create` | `sbsctl help iscsi target create` |
| `sbsctl iscsi target delete` | `sbsctl help iscsi target delete` |
| `sbsctl iscsi target disable` | `sbsctl help iscsi target disable` |
| `sbsctl iscsi target enable` | `sbsctl help iscsi target enable` |
| `sbsctl iscsi target get` | `sbsctl help iscsi target get` |
| `sbsctl iscsi target list` | `sbsctl help iscsi target list` |
| `sbsctl maintenance pause` | `sbsctl help maintenance pause` |
| `sbsctl maintenance payload-gc` | `sbsctl help maintenance payload-gc` |
| `sbsctl maintenance resume` | `sbsctl help maintenance resume` |
| `sbsctl maintenance throttle` | `sbsctl help maintenance throttle` |
| `sbsctl node drain` | `sbsctl help node drain` |
| `sbsctl node drain status` | `sbsctl help node drain status` |
| `sbsctl node join` | `sbsctl help node join` |
| `sbsctl node leave` | `sbsctl help node leave` |
| `sbsctl node list` | `sbsctl help node list` |
| `sbsctl node projection rebuild` | `sbsctl help node projection rebuild` |
| `sbsctl node projection status` | `sbsctl help node projection status` |
| `sbsctl node remove` | `sbsctl help node remove` |
| `sbsctl node status` | `sbsctl help node status` |
| `sbsctl node update-registration` | `sbsctl help node update-registration` |
| `sbsctl node update-topology` | `sbsctl help node update-topology` |
| `sbsctl operations list` | `sbsctl help operations list` |
| `sbsctl operations show` | `sbsctl help operations show` |
| `sbsctl rebalance list` | `sbsctl help rebalance list` |
| `sbsctl repair list` | `sbsctl help repair list` |
| `sbsctl repair show` | `sbsctl help repair show` |
| `sbsctl snapshot create` | `sbsctl help snapshot create` |
| `sbsctl snapshot delete` | `sbsctl help snapshot delete` |
| `sbsctl snapshot get` | `sbsctl help snapshot get` |
| `sbsctl snapshot list` | `sbsctl help snapshot list` |
| `sbsctl store status` | `sbsctl help store status` |
| `sbsctl store tuning` | `sbsctl help store tuning` |
| `sbsctl testio flush` | `sbsctl help testio flush` |
| `sbsctl testio open` | `sbsctl help testio open` |
| `sbsctl testio read` | `sbsctl help testio read` |
| `sbsctl testio write` | `sbsctl help testio write` |
| `sbsctl topology zone create` | `sbsctl help topology zone create` |
| `sbsctl topology zone delete` | `sbsctl help topology zone delete` |
| `sbsctl topology zone get` | `sbsctl help topology zone get` |
| `sbsctl topology zone list` | `sbsctl help topology zone list` |
| `sbsctl topology zone update` | `sbsctl help topology zone update` |
| `sbsctl volume allocation-page` | `sbsctl help volume allocation-page` |
| `sbsctl volume create` | `sbsctl help volume create` |
| `sbsctl volume delete` | `sbsctl help volume delete` |
| `sbsctl volume expand` | `sbsctl help volume expand` |
| `sbsctl volume health` | `sbsctl help volume health` |
| `sbsctl volume list` | `sbsctl help volume list` |
| `sbsctl volume placement` | `sbsctl help volume placement` |
| `sbsctl volume purge` | `sbsctl help volume purge` |
| `sbsctl volume replica-targets` | `sbsctl help volume replica-targets` |
| `sbsctl volume restore-from-snapshot` | `sbsctl help volume restore-from-snapshot` |
| `sbsctl volume status` | `sbsctl help volume status` |
| `sbsctl volume transitions` | `sbsctl help volume transitions` |

## Command flags and defaults

### `sbsctl cluster init`

```text
Usage: sbsctl cluster init [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --reason string
      reason (default bootstrap)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl cluster status`

```text
Usage: sbsctl cluster status [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi initiator allow`

```text
Usage: sbsctl iscsi initiator allow [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --auth-mode string
      auth mode: none or chap (default none)
  --chap-secret-ref string
      CHAP secret reference (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --enabled
      create ACL enabled (default true)
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --initiator-iqn string
      initiator IQN (default )
  --json
      emit JSON (default false)
  --lun-id uint
      single allowed LUN id (default 0)
  --lun-ids string
      comma-separated allowed LUN ids (default )
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi initiator deny`

```text
Usage: sbsctl iscsi initiator deny [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --initiator-iqn string
      initiator IQN (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm destructive registry mutation (default false)
```

### `sbsctl iscsi initiator get`

```text
Usage: sbsctl iscsi initiator get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --initiator-iqn string
      initiator IQN (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi initiator list`

```text
Usage: sbsctl iscsi initiator list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      optional target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi initiator set-auth`

```text
Usage: sbsctl iscsi initiator set-auth [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --auth-mode string
      auth mode: none or chap (default )
  --chap-secret-ref string
      CHAP secret reference (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --initiator-iqn string
      initiator IQN (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi lun export`

```text
Usage: sbsctl iscsi lun export [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --enabled
      export LUN enabled (default true)
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --export-id string
      export id (default )
  --export-mode string
      export mode: read_write or read_only (default read_write)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --logical-block-size-bytes uint
      logical block size bytes (default 4096)
  --lun-id uint
      LUN id (default 0)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      SBS volume id (default )
```

### `sbsctl iscsi lun get`

```text
Usage: sbsctl iscsi lun get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --lun-id uint
      LUN id (default 0)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      optional expected SBS volume id (default )
```

### `sbsctl iscsi lun list`

```text
Usage: sbsctl iscsi lun list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      optional target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi lun set-mode`

```text
Usage: sbsctl iscsi lun set-mode [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --export-mode string
      export mode: read_write or read_only (default )
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --lun-id uint
      LUN id (default 0)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi lun unexport`

```text
Usage: sbsctl iscsi lun unexport [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --force
      remove connected session records from registry (default false)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --lun-id uint
      LUN id (default 0)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm destructive registry mutation (default false)
```

### `sbsctl iscsi portal create`

```text
Usage: sbsctl iscsi portal create [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --address string
      portal listen address, host:port (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --enabled
      create portal enabled (default true)
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --gateway-id string
      owning iSCSI gateway id (default )
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi portal delete`

```text
Usage: sbsctl iscsi portal delete [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --force
      remove target portal references when possible (default false)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm destructive registry mutation (default false)
```

### `sbsctl iscsi portal disable`

```text
Usage: sbsctl iscsi portal disable [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi portal enable`

```text
Usage: sbsctl iscsi portal enable [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi portal get`

```text
Usage: sbsctl iscsi portal get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi portal list`

```text
Usage: sbsctl iscsi portal list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi session disconnect`

```text
Usage: sbsctl iscsi session disconnect [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --force
      force registry disconnect request (default false)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --session-id string
      session id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm disruptive session mutation (default false)
```

### `sbsctl iscsi session get`

```text
Usage: sbsctl iscsi session get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --session-id string
      session id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi session list`

```text
Usage: sbsctl iscsi session list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --connected-only
      show connected sessions only (default false)
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --initiator-iqn string
      optional initiator IQN (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      optional target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi status gateway`

```text
Usage: sbsctl iscsi status gateway [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi status target`

```text
Usage: sbsctl iscsi status target [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi target create`

```text
Usage: sbsctl iscsi target create [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --enabled
      create target enabled (default true)
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --export-id string
      export id (default )
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --portal-id string
      portal id (default )
  --portal-ids string
      comma-separated portal ids (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi target delete`

```text
Usage: sbsctl iscsi target delete [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --force
      remove child LUN/ACL/session registry entries (default false)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm destructive registry mutation (default false)
```

### `sbsctl iscsi target disable`

```text
Usage: sbsctl iscsi target disable [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi target enable`

```text
Usage: sbsctl iscsi target enable [flags]

Flags:
  --actor string
      mutation actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --expected-registry-revision uint
      expected registry revision; 0 disables the precondition (default 0)
  --idempotency-key string
      mutation idempotency key (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --reason string
      mutation reason (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi target get`

```text
Usage: sbsctl iscsi target get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-iqn string
      target IQN (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl iscsi target list`

```text
Usage: sbsctl iscsi target list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint for output authority context (default )
  --json
      emit JSON (default false)
  --output string
      output format: json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl maintenance pause`

```text
Usage: sbsctl maintenance pause [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --drains
      pause drains (default false)
  --reason string
      reason (default maintenance-pause)
  --rebalances
      pause rebalances (default false)
  --repairs
      pause repairs (default false)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl maintenance payload-gc`

```text
Usage: sbsctl maintenance payload-gc [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --metadata-path string
      path to SBS cluster metadata pebble directory (default )
  --metadata-root string
      metadata root prefix (default sbs/cluster)
  --output string
      output format: table|json (default )
  --payload-root string
      path to local replica payload root directory (default )
  --sbs-service-http-endpoint string
      node-local admin/debug HTTP endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --volume-id string
      optional canonical volume id to sweep; when empty sweeps all volumes (default )
```

### `sbsctl maintenance resume`

```text
Usage: sbsctl maintenance resume [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --drains
      resume drains (default false)
  --reason string
      reason (default maintenance-resume)
  --rebalances
      resume rebalances (default false)
  --repairs
      resume repairs (default false)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl maintenance throttle`

```text
Usage: sbsctl maintenance throttle [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --drains uint
      max concurrent drains (default 0)
  --reason string
      reason (default maintenance-throttle)
  --rebalances uint
      max concurrent rebalances (default 0)
  --repairs uint
      max concurrent repairs (default 0)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node drain`

```text
Usage: sbsctl node drain [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --reason string
      reason (default drain)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm drain (default false)
```

### `sbsctl node drain status`

```text
Usage: sbsctl node drain status [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node join`

```text
Usage: sbsctl node join [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --auto-create-zone
      create the zone if missing (default false)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --grpc-endpoint string
      sbs-data gRPC endpoint (default )
  --node-id string
      node id (default )
  --reason string
      reason (default join)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --sbs-service-http-endpoint string
      node-local admin/debug HTTP endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --zone string
      zone (default )
```

### `sbsctl node leave`

```text
Usage: sbsctl node leave [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --reason string
      reason (default leave)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm leave and drain (default false)
```

### `sbsctl node list`

```text
Usage: sbsctl node list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --include-tombstones
      include removed membership tombstones (default false)
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node projection rebuild`

```text
Usage: sbsctl node projection rebuild [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --reason string
      reason (default operator projection rebuild)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node projection status`

```text
Usage: sbsctl node projection status [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node remove`

```text
Usage: sbsctl node remove [flags]

Flags:
  --acknowledge-data-loss-risk
      acknowledge force-remove data loss risk (default false)
  --actor string
      actor (default namrbd-docs)
  --approval-id string
      break-glass approval id for force remove (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --force
      force remove (default false)
  --node-id string
      node id (default )
  --reason string
      reason (default remove)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm remove (default false)
```

### `sbsctl node status`

```text
Usage: sbsctl node status [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node update-registration`

```text
Usage: sbsctl node update-registration [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --grpc-endpoint string
      replacement sbs-data gRPC endpoint (default )
  --node-id string
      node id (default )
  --reason string
      reason (default update-node-registration)
  --roles string
      comma-separated node roles (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --sbs-service-http-endpoint string
      replacement sbs-data admin HTTP endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --store-ids string
      comma-separated store IDs (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl node update-topology`

```text
Usage: sbsctl node update-topology [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --auto-create-zone
      create the zone if missing (default false)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --reason string
      reason (default node-update-topology)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --zone string
      zone (default )
```

### `sbsctl operations list`

```text
Usage: sbsctl operations list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --kind string
      optional operation kind filter (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --state string
      optional state filter: queued|running|completed|failed|canceled (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl operations show`

```text
Usage: sbsctl operations show [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --operation-id string
      operation id (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl rebalance list`

```text
Usage: sbsctl rebalance list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl repair list`

```text
Usage: sbsctl repair list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl repair show`

```text
Usage: sbsctl repair show [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --placement-ref string
      optional placement ref filter (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl snapshot create`

```text
Usage: sbsctl snapshot create [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --idempotency-key string
      idempotency key; empty lets the service derive one when implemented (default )
  --reason string
      reason (default create-snapshot)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --source-volume-id string
      source volume id (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl snapshot delete`

```text
Usage: sbsctl snapshot delete [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --reason string
      reason (default delete-snapshot)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --snapshot-id string
      snapshot id (default )
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm snapshot delete (default false)
```

### `sbsctl snapshot get`

```text
Usage: sbsctl snapshot get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --snapshot-id string
      snapshot id (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl snapshot list`

```text
Usage: sbsctl snapshot list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --include-deleted
      include deleted snapshots (default false)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --source-volume-id string
      source volume id filter (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl store status`

```text
Usage: sbsctl store status [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-service-http-endpoint string
      node-local admin/debug HTTP endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl store tuning`

```text
Usage: sbsctl store tuning [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --node-id string
      node id (default )
  --output string
      output format: table|json (default )
  --reason string
      reason (default store-tuning)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --store-tuning value
      store tuning spec store_id=<id>,allocation_weight=<n> (weight=<n> is accepted as compatibility alias; repeatable) (default )
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl testio flush`

```text
Usage: sbsctl testio flush [flags]

Flags:
  --attachment-generation uint
      attachment generation (default 1)
  --attachment-id string
      attachment id (default sbsctl-test-attachment)
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint (default )
  --gateway-id string
      gateway id (default )
  --handle string
      volume handle (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl testio open`

```text
Usage: sbsctl testio open [flags]

Flags:
  --attachment-generation uint
      attachment generation (default 1)
  --attachment-id string
      attachment id (default sbsctl-test-attachment)
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint (default )
  --gateway-id string
      gateway id (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl testio read`

```text
Usage: sbsctl testio read [flags]

Flags:
  --attachment-generation uint
      attachment generation (default 1)
  --attachment-id string
      attachment id (default sbsctl-test-attachment)
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data-endpoint string
      sbs-data gRPC endpoint (default )
  --gateway-id string
      gateway id (default )
  --handle string
      volume handle (default )
  --length uint
      length bytes (default 0)
  --offset uint
      offset bytes (default 0)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl testio write`

```text
Usage: sbsctl testio write [flags]

Flags:
  --attachment-generation uint
      attachment generation (default 1)
  --attachment-id string
      attachment id (default sbsctl-test-attachment)
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --data string
      plain text payload (default )
  --data-endpoint string
      sbs-data gRPC endpoint (default )
  --data-hex string
      hex encoded payload (default )
  --gateway-id string
      gateway id (default )
  --handle string
      volume handle (default )
  --offset uint
      offset bytes (default 0)
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl topology zone create`

```text
Usage: sbsctl topology zone create [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --display-name string
      display name (default )
  --label value
      label k=v (repeatable) (default )
  --reason string
      reason (default topology-zone-create)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --zone string
      zone id (default )
```

### `sbsctl topology zone delete`

```text
Usage: sbsctl topology zone delete [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --reason string
      reason (default topology-zone-delete)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --yes
      confirm delete (default false)
  --zone string
      zone id (default )
```

### `sbsctl topology zone get`

```text
Usage: sbsctl topology zone get [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --zone string
      zone id (default )
```

### `sbsctl topology zone list`

```text
Usage: sbsctl topology zone list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl topology zone update`

```text
Usage: sbsctl topology zone update [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --disable
      set lifecycle disabled (default false)
  --display-name string
      display name (default )
  --enable
      set lifecycle active (default false)
  --label value
      label k=v (repeatable) (default )
  --reason string
      reason (default topology-zone-update)
  --retire
      set lifecycle retiring (default false)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --zone string
      zone id (default )
```

### `sbsctl volume allocation-page`

```text
Usage: sbsctl volume allocation-page [flags]

Flags:
  --allocation-chunk-size string
      allocation chunk size, e.g. 64K (default )
  --allocation-page-size string
      allocation page size, e.g. 4M (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --page-no uint
      allocation page number (default 0)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume create`

```text
Usage: sbsctl volume create [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --allocation-chunk-size string
      allocation chunk size, e.g. 64K; empty uses server default (default )
  --allocation-page-size string
      allocation page size, e.g. 256K; empty uses server default (default )
  --block-size string
      block size, e.g. 4K (default 4K)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --extent-size string
      logical extent size, e.g. 4M; empty uses server default (default )
  --policy-name string
      placement policy (default )
  --reason string
      reason (default create-volume)
  --replication-factor uint
      replication factor (default 1)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --size string
      volume size, e.g. 10G or 100T (default )
  --timeout duration
      request timeout (default 10s)
  --topology-mode string
      topology mode; empty uses backend default (default )
  --volume-id string
      volume id (default )
```

### `sbsctl volume delete`

```text
Usage: sbsctl volume delete [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --reason string
      reason (default delete-volume)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
  --yes
      confirm delete (default false)
```

### `sbsctl volume expand`

```text
Usage: sbsctl volume expand [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --add-size string
      size to add, e.g. 10G (default )
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --idempotency-key string
      idempotency key (default )
  --reason string
      reason (default expand-volume)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --target-size string
      target volume size, e.g. 100G (default )
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume health`

```text
Usage: sbsctl volume health [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume list`

```text
Usage: sbsctl volume list [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
```

### `sbsctl volume placement`

```text
Usage: sbsctl volume placement [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --sbs-service-http-endpoint string
      optional node-local admin/debug HTTP endpoint for current placement rows (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume purge`

```text
Usage: sbsctl volume purge [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --i-confirmed-deletion
      explicitly acknowledge destructive purge semantics (default false)
  --reason string
      reason (default purge-volume)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
  --yes
      confirm purge (default false)
```

### `sbsctl volume replica-targets`

```text
Usage: sbsctl volume replica-targets [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume restore-from-snapshot`

```text
Usage: sbsctl volume restore-from-snapshot [flags]

Flags:
  --actor string
      actor (default namrbd-docs)
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --idempotency-key string
      idempotency key; empty derives from source snapshot and target volume id (default )
  --reason string
      reason (default restore-volume-from-snapshot)
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --size string
      restored target size, e.g. 10G; empty uses source snapshot size (default )
  --source-snapshot-id string
      source snapshot id (default )
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      restored target volume id (default )
```

### `sbsctl volume status`

```text
Usage: sbsctl volume status [flags]

Flags:
  --cluster-id string
      cluster id (default )
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-cluster-id string
      sbs cluster id (default )
  --sbs-service-endpoint string
      cluster-wide sbs-admin gRPC endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --summary-mode string
      summary mode: full|spec-only (default )
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

### `sbsctl volume transitions`

```text
Usage: sbsctl volume transitions [flags]

Flags:
  --context string
      context name inside context file (default )
  --context-file string
      path to context file (default )
  --output string
      output format: table|json (default )
  --sbs-service-http-endpoint string
      node-local admin/debug HTTP endpoint (default )
  --show-config-sources
      print resolved config values and their sources (default false)
  --timeout duration
      request timeout (default 10s)
  --volume-id string
      volume id (default )
```

## Source of truth

- Entry point: `cmd/sbsctl`
- Shared help and deprecated-flag behavior: `internal/cliux`
- Environment rename behavior: `internal/envcompat`
