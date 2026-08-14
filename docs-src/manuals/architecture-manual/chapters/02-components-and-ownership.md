Chapter 3

Edition boundary: Community edition component ownership and Enterprise edition only ownership rows are both present.

# Components And Ownership

## Ownership Rule

Every state transition needs an owner, an observer, and an explicit non-owner.

- Data references
- API calls
- HA boundaries
- Maintenance flows

<div class="summary" markdown="1">

NAMRBD is intentionally split into host-local runtime, gateway admission/routing, SBS cluster authority, node-local storage execution, metadata backends, and integration adapters. This split keeps each failure mode reviewable.

`sbs-service` owns cluster-wide storage metadata, topology, placement, health reconciliation, and maintenance. The kernel owns host-local device/path state. The gateway routes and reports. `sbs-data` executes local storage operations. CSI adapts external Kubernetes calls.

Component dependencies are therefore read in two directions: API calls show who asks for work, while metadata references show whose state is being trusted. A component may call or cache another component's view without becoming the owner of that state.

</div>

<figure class="architecture-figure" markdown="1">

![NAMRBD component ownership diagram showing dependencies and authority boundaries](../assets/diagrams/component-ownership.svg)

<figcaption>Shared English SVG for component ownership. Calls and cached views are shown separately from the stores and layers that own authoritative state.</figcaption>

</figure>

## Component Matrix

| Component | Owns | Observes / Consumes | Does Not Own |
|----|----|----|----|
| Linux kernel module | Host-local block device, path health, queue/no-path policy, applied manifest. | Gateway-provided dataplane endpoints, volume/generation/attachment data. | Volume lifecycle, SBS membership, placement, repair, GC. |
| `namrbdctl` | Host-side orchestration calls and kernel control requests. | Gateway attach/info responses and kernel status. | SBS cluster metadata semantics. |
| `namrbd-gateway` | Host admission, request conversion, SBS client calls, short-lived read-through caches. | etcd attachment/generation, SBS published placement view. | SBS raw metadata authority, repair/rebalance/drain, EC rebuild. |
| `sbs-service` | Cluster metadata, volume geometry, placement, topology, snapshots, clones, GC roots, maintenance, enterprise Backup/DR target/policy/run/artifact/hold/status state, and remote DR replication-link/recovery-point/shipping-manifest state. | `sbs-data` health/capacity reports and operation results. | Host-local blk-mq queueing and kernel path retry policy. |
| `sbs-service` operations views | Read-only JSON assembly for `namrbd.sbs.observability.v1`, membership status, capacity, reclaim, operation summaries, MCP descriptors, GUI descriptors, workflow hardening evidence, and the static operations console. | `sbs-service` metadata, `sbs-data` health detail, and gateway/control-plane membership/liveness summaries. | Storage mutation authority, live iSCSI HA authority, GUI/MCP action authority, or a replacement for AdminService and gateway control-plane state. |
| `sbs-data` | Local payload read/write/delete, local store metadata, local idempotency. | SBS service commands and local store state. | Cluster membership, global placement, global reachability truth. |
| `namrbd-csi-driver` | CSI Identity/Controller/Node translation. | NAMRBD admin, gateway, and host APIs. | Storage semantics, snapshot cut points, topology, fencing, read-view, GC. |

<div class="diagram" markdown="1">

<div class="diagram-title">Authority layers</div>

<div class="grid" markdown="1">

<div class="mini-card" markdown="1">

### Host local

Kernel module and `namrbdctl` own local device and path application.

</div>

<div class="mini-card" markdown="1">

### Gateway edge

Gateway adapts host requests into SBS calls and reports runtime state.

</div>

<div class="mini-card" markdown="1">

### SBS authority

`sbs-service` owns cluster metadata and product storage semantics.

</div>

<div class="mini-card" markdown="1">

### Node local

`sbs-data` owns local payload execution and store health.

</div>

</div>

</div>

## Dependency Views By Operation

The same components participate differently depending on the operation. The useful review question is not only "who is called?" but also "which metadata is trusted, and who is allowed to mutate it?"

Read each row as a concept flow: a caller invokes an API, the API reads or mutates an owned authority, and any cache or published view remains evidence rather than ownership.

| Operation View | Data References | API Calls / Transport | Boundary To Preserve |
|----|----|----|----|
| Block device registration / attach | Gateway control metadata in etcd: volume spec, current attachment, writer generation, gateway registry. SBS target information comes from the `sbs-service` published placement view. | `namrbdctl` calls the gateway attach/control API, then applies the returned manifest to the kernel through host control such as netlink. The gateway reads etcd and queries `sbs-service` for gateway-facing views. | The kernel owns only the local device and applied paths. The gateway can validate and assemble a manifest, but SBS placement and volume semantics remain in SBS authority. |
| Foreground read/write/flush/discard/zero I/O | The kernel uses the applied manifest and local path health. The gateway uses attachment, generation, idempotency context, and a published SBS target view. `sbs-data` relies on local Pebble payload/store state and request context. | Linux block I/O enters the kernel, then uses persistent TCP dataplane paths to the gateway. The gateway converts the request to SBS context and calls `sbs/v1.VolumeService` gRPC on selected `sbs-data` nodes. | Gateway retry and routing are availability concerns. Correctness is defended by SBS metadata visibility, stale attachment/generation checks, idempotency, and payload persistence in the selected backend. |
| Detach / path removal / reconfigure | Detach consults gateway/control-plane attachment state in etcd and the kernel's current local device/path state. It may observe gateway liveness and path health, but it does not need to reinterpret raw SBS metadata. | Host tooling calls gateway detach/info paths and kernel control paths. The kernel tears down or updates local device/path state; the gateway updates control-plane attachment state and reports runtime status. | Detach is not a placement, repair, or storage metadata mutation. Stale writers must be fenced by attachment/generation checks rather than by trusting that a host-local path disappeared cleanly. |
| Gateway high availability | Gateway registry, liveness, attachment, and generation live in etcd. Gateway-facing replica target views come from `sbs-service` and may be cached briefly. Committed SBS metadata/payload state remains authoritative after any gateway restart. | Multiple gateways can expose HTTP control and persistent TCP dataplane endpoints. Host manifests and kernel path health decide which gateway paths are usable; gateway instances call SBS through the same published-view and `VolumeService` surfaces. | A gateway is replaceable routing/adaptation state, not durable storage authority. Gateway-local cache cannot become placement truth, and multi-gateway correctness must still rely on SBS fencing/idempotency and committed metadata. |
| SBS cluster high availability and metadata upkeep | `sbs-service` stores cluster bootstrap, leader/admin operations, node membership, node health, volume metadata, placement, allocation pages, and transition state in TiKV. `sbs-data` stores node-local payload and local execution state in Pebble. | `sbsctl` and gateway-facing control paths call `sbs-service` admin/published-view APIs. `sbs-service` reconciles node/store health through HTTP/debug health surfaces plus gRPC reachability, then publishes gateway-facing target views. | If `sbs-service` is unavailable, admin and maintenance work should pause or degrade, but already routed foreground I/O should not require gateway-local metadata authority. TiKV owns cluster metadata; local Pebble owns local payload state. |
| Backup/DR control plane | Backup targets, policies, run records, restore-drilled artifact availability, retention holds, purge dry-run guardrails, Backup/DR status summaries, remote DR replication links, recovery points, and shipping manifests. | Enterprise `sbsctl backup`, `sbsctl dr link`, `sbsctl dr recovery-point`, and `sbsctl dr shipping-manifest` call `sbs-service` admin APIs. Fixture validation tools may emit JSON evidence, but product state is persisted by `sbs-service`. | Backup/DR automation is enterprise-only. U-CTRL-003A records DR link, recovery-point, manifest, and shipping-worker admission state; remote transfer completion, standby import, promote/demote, and failover support remain gated. |
| SBS maintenance: repair, rebalance, drain, rebuild, scrub | Maintenance consumes TiKV topology, placement, allocation, node/store health, operation records, read-view roots, and backend descriptors. It also consumes local `sbs-data` health/capacity and payload operation results. | `sbsctl` or controllers call `sbs-service` admin APIs. `sbs-service` records transition state in TiKV, selects eligible sources and targets, then issues read/write/copy/delete work to `sbs-data` through SBS execution APIs. | Gateway and kernel may observe changed target availability and reload paths, but they do not own maintenance planning. Maintenance must preserve read-view roots, generation/idempotency boundaries, and backend-specific payload lifetime rules. |

## Dependency Reading Rules

An API call is not ownership. `namrbdctl` can ask the gateway to attach, the gateway can ask `sbs-service` for a placement view, and `sbs-service` can ask `sbs-data` to execute payload work, but the authoritative state remains with the component and metadata store named in the ownership matrix.

A cache is also not ownership. Gateway and `sbs-service` caches may reduce read latency or protect hot paths, but cache misses must fall back to the owning authority and cache hits must not bypass fencing, generation, idempotency, or reachability rules.

## Review Pattern

When reviewing a change, ask whether it moves authority across these boundaries. The change should name the authoritative writer for any state transition it touches. A gateway cache can make routing efficient, but it cannot become placement truth. A CSI handler can call a snapshot API, but it cannot decide snapshot cut points. A local `sbs-data` node can report shard health, but it cannot decide global reachability.

[\<- Previous](01-platform-overview.md) [Next: Linux Host Control And Data Plane -\>](03-linux-host-control-and-data-plane.md)
