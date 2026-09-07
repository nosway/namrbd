Appendix C

Advanced feature note: Enterprise interface rows describe designs under
development and validation. They are not public v1.1 API or support claims.
See [Feature Status](../../../feature-status.md).

# Interface Specifications

## Appendix

Component interface contracts and source pointers.

<div class="summary" markdown="1">

This appendix collects the component-to-component interface surfaces that architecture reviewers most often need. It is not a generated API reference; it is the architectural contract map and points to the source files that define the exact fields and RPC messages.

The primary order is host netlink, gateway REST, optional iSCSI target gateway, sbs-service gRPC, sbs-data gRPC, and observability HTTP URLs. Related surfaces that are important but not in that direct runtime chain are listed at the end.

Listener values below name the current daemon defaults from flags where a default exists. Deployment guides and validation targets may override them; rows marked explicit or N/A do not have an implicit network listener.

</div>

## Interface Stack

<div class="diagram" markdown="1">

<div class="diagram-title">Specification order</div>

<div class="flow" markdown="1">

<div class="box-accent">

Linux kernel module\
generic netlink

</div>

<div class="arrow">then</div>

<div class="box">

namrbd-gateway\
REST

</div>

<div class="arrow">then</div>

<div class="box-soft">

namrbd-iscsi-gateway\
iSCSI target

</div>

<div class="arrow">then</div>

<div class="box">

sbs-service\
admin gRPC

</div>

<div class="arrow">then</div>

<div class="box">

sbs-data\
volume gRPC

</div>

<div class="arrow">plus</div>

<div class="box-soft">

observability\
HTTP URLs

</div>

</div>

</div>

## Quick Interface Locator

| Interface surface | Owning program/component | Type | Default endpoint or port | Primary callers / notes |
|----|----|----|----|----|
| Host control | `namrbd_ctrl.ko`; userspace client `namrbdctl` | generic netlink | N/A; family `NAMRBD_CTRL` | Host-local device create, attach, detach, status, resize, and path-plan updates. |
| Gateway control and userspace I/O | `namrbd-gateway` | HTTP/HTTPS JSON URL | `--control-http-listen :9701`; install guide examples often use `http://gw01:9899` | `namrbdctl`, kernel compatibility attach path, CSI node helper, and userspace debug/load tools call `/api/v1`. |
| Gateway block dataplane | `namrbd-gateway` and `namrbd_blk.ko` | persistent TCP binary frame | `--data-listen :9700`; install guide examples often use `:9898` | Foreground block I/O wire protocol; not REST or gRPC. |
| iSCSI target portal | `namrbd-iscsi-gateway`; product control through `sbsctl iscsi` | iSCSI over TCP | Explicit `--portal`; standard initiator port is TCP/3260 | Optional iSCSI standard block protocol frontend; portal listening is configured explicitly. |
| SBS admin/control | `sbs-service` | gRPC | `--sbs-service-listen 0.0.0.0:9443` | `sbsctl`, `namrbd-gateway --sbs-service-endpoint`, CSI controller, and internal control adapters. |
| SBS service observability | `sbs-service` | HTTP URL | `--sbs-service-http-listen 0.0.0.0:9081` | Health, readiness, metrics, debug summary, transition, maintenance, EC, GC inspection routes, read-only operations views, and the static `/console/` dashboard. |
| SBS data volume execution | `sbs-data` | gRPC | `--sbs-data-listen 0.0.0.0:9444`; install guide node examples use `:9444` | Gateway and maintenance flows execute node-local volume, physical chunk, and shard operations. |
| SBS data observability/admin | `sbs-data` | HTTP URL | `--sbs-data-http-listen 0.0.0.0:9082` | Health, readiness, metrics, store health, allocation/extent inspection, and node-local store admin routes. |
| CSI driver endpoint | `namrbd-csi-driver` | CSI gRPC over Unix socket or TCP | `--endpoint unix:///tmp/namrbd-csi.sock`; no TCP port by default | Kubernetes sidecars call CSI Identity, Controller, and Node services. The driver dials SBS admin through `--admin-endpoint`, which defaults to `127.0.0.1:9897` and should be set to the active `sbs-service` endpoint. |
| etcd control metadata backend | `etcd`; clients include `namrbd-gateway` | etcd client endpoint | Client TCP/2379; peer TCP/2380 for etcd clustering | Backend authority for gateway/control-plane metadata, not a public NAMRBD API. |
| TiKV/PD SBS metadata backend | `pd-server`, `tikv-server`; client `sbs-service` | TiKV/PD client protocols | PD client TCP/2379; TiKV store TCP/20160; TiKV status TCP/20180 | Backend authority for SBS metadata. Gateway and CSI must consume owning APIs instead of raw TiKV records. |
| Internal SBS metadata services | `sbs-service` | internal gRPC services | No separate public port; internal authority surface behind `sbs-service` | Adapters such as placement resolver/apply, write session, EC metadata, and chunk id allocation. |
| Operator/debug CLIs | `namrbdctl`, `sbsctl`, `namrbd-debug` | CLI | N/A | Human/operator surfaces that call the network or netlink interfaces above. |

## 1. Linux Kernel Module Netlink

The host-local control interface is the generic netlink family `NAMRBD_CTRL`, version `0x1`. The exact UAPI source is `kernel/uapi/namrbd_netlink.h`; userspace helpers live under `control/netlinktlv`, `control/netlinkclient`, and `cmd/namrbdctl`.

| Surface | Contract |
|----|----|
| Command set | `CREATE_DEVICE`, `DESTROY_DEVICE`, `CONFIG_REST`, `ATTACH`, `DETACH`, `GET_STATUS`, `LIST_DEVICES`, `ATTACH_MANIFEST`, `DETACH_LOCAL`, `UPDATE_PATH_PLAN`, `RECONFIGURE_DATA_PATHS`, and `RESIZE_DEVICE`. |
| Request attributes | `DEVICE_ID`, `DISK_NAME`, REST server list, attach/detach request nests, manifest JSON, volume id, generation, size, path-plan revision, path masks, status, and error message. |
| Status attributes | Attached state, path count, down/degraded/draining masks, active lane count, blk-mq queue topology, path entries, lane entries, and no-path retry counters. |
| Authority boundary | Netlink changes host-local device runtime only. Volume lifecycle, attachment ownership, generation, placement, and repair state remain external authority owned by gateway metadata and the SBS cluster. |

## 2. namrbd Gateway REST API

The gateway exposes host-facing JSON/HTTP control and I/O routes under `/api/v1`. The route registration is in `gateway/httpapi/server.go`; the public architecture boundary is summarized in [Linux Host Control And Data Plane](03-linux-host-control-and-data-plane.md), [Metadata Authority](04-metadata-authority.md), and [Components And Ownership](02-components-and-ownership.md).

| Route family | Methods and meaning |
|----|----|
| Volume control | `GET /api/v1/volumes/{id}/info`, `POST /api/v1/volumes/{id}/attach`, `POST /api/v1/volumes/{id}/reload-size`, and `POST /api/v1/volumes/{id}/detach`. |
| Volume I/O | `POST /api/v1/volumes/{id}/read`, `write`, `flush`, `discard`, and `zero`. Read/write style requests carry `offset_bytes`, `length_bytes`, and optionally `data_base64`, `host_id`, `attachment_id`, or `device_id`. When guarded Performance gateway-local admission is explicitly enabled, read/write responses may include throttle observability fields for cap scope, throttle mode, requested/granted tokens and bytes, wait milliseconds, rejected ops, and whether the mode is cluster-wide. |
| Performance gateway admission | Foreground I/O admission is a default-off gateway mode configured by cap scope, throttle mode, IOPS cap, bandwidth cap, and burst settings. It admits before dispatch and can wait or return HTTP 429 with a throttle rejection code. Local fixture and per-gateway modes use the guarded gateway-local limiter. Cluster-volume mode requires `--sbs-service-endpoint` and consumes `sbs-service` shared budget leases before dispatch, reporting shared budget authority, gateway lease consumption, and the lease id in throttle observability. |
| Security/Compliance gateway security admission | The enterprise encrypted replicated payload path can be enabled when an SBS admin endpoint is configured. Data-key identity and key-version startup settings select the data-key version used for new writes and attach admission. Reads unwrap the key version recorded in the encrypted payload header, allowing old key-version compatibility after rotation. |
| Discovery | `GET /api/v1/discovery/gateways` and `GET /api/v1/discovery/volumes/{id}` publish gateway/path-plan information for the host runtime. |
| Runtime feedback | `POST /api/v1/debug/discovery/volumes/{id}/path-plan` and `POST /api/v1/debug/discovery/volumes/{id}/runtime-feedback` are controller/debug surfaces for path-plan evaluation and host runtime feedback. |
| SBS debug views | `/api/v1/debug/sbs-cluster/nodes/{id}`, `/api/v1/debug/sbs-cluster/volumes/{id}`, clone debug read/write routes, and `/api/v1/debug/sbs-cluster/metrics` expose cluster-state inspection through the gateway when configured. |
| Authority boundary | The gateway admits host requests and converts them to SBS calls, but it must not own persistent placement, payload, repair, rebalance, drain, or cluster-wide performance policy authority. |

## 2A. iSCSI Target Gateway <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

iSCSI adds `namrbd-iscsi-gateway` as an optional standard block protocol frontend beside the Linux-only kernel module path. It exposes NAMRBD/SBS-backed volumes through an iSCSI target so a standard initiator can discover a target IQN, log in, see a LUN, and issue block reads, writes, flushes, and supported command probes. This section is the public iSCSI runtime, CLI, protocol, edition, and evidence boundary; the implementation lives under `cmd/namrbd-iscsi-gateway`, `cmd/sbsctl`, `iscsi`, and the NAMRBD-managed gotgt fork at `third_party/gotgt`.

| Surface | Contract |
|----|----|
| Runtime modes | `--backend=memory` provides a volatile fixture LUN; `--backend=sbs` exposes an SBS-backed LUN through `gateway/service.SBSClient`. `--portal`, `--target-iqn`, `--serve`, `--summary-json`, `--operation-jsonl`, and `--json` define the iSCSI runtime/evidence surface. The root module replaces `github.com/gostor/gotgt` with `./third_party/gotgt` so target-stack fixes are reviewed inside NAMRBD. |
| Standard protocol surface | The target listens on the configured portal, normally TCP/3260 for standard initiator compatibility, and publishes one target IQN with one LUN for the initial iSCSI claim. SCSI command handling is intentionally conservative: inquiry/capacity/read/write/synchronize-cache basics, 512-byte logical blocks, iSCSI ERL 0, single connection per session, persistent-reservation rejection, and no MPIO/ALUA advertisement. |
| Compatibility baseline | iSCSI support claims Linux open-iscsi compatibility only. Required evidence is an SBS-backed Linux initiator smoke with discovery, login, guarded LUN selection, raw write/readback, flush observation, cleanup, `initiator_vendor=open-iscsi`, `readback_matched=true`, and `error_count=0`. Windows native initiator compatibility has optional memory-backend and SBS-backed connection/log-cleanup evidence; full SBS-backed Windows I/O remains a future compatibility track. macOS validation is excluded until a licensed ATTO Xtend SAN or approved alternative initiator is available. |
| Command semantics | READ and WRITE map to the selected backend adapter. SYNCHRONIZE CACHE maps to backend flush without claiming stronger durability than NAMRBD/SBS proves. Memory backend UNMAP is rejected; SBS backend UNMAP is valid only through the discard/reclaim contract. The NAMRBD gotgt fork patches remote backing-store UNMAP forwarding so SCSI UNMAP descriptors can reach the backend instead of returning silent success. iSCSI closure records target-side UNMAP evidence after that patch; it remains separate from broad performance or non-Linux support claims. |
| Edition boundary | Community edition includes the basic iSCSI gateway, `sbsctl iscsi`, and basic LUN export, with community iSCSI exports limited to three distinct volume ids. More than 3 exported volumes, unlimited export scale, iSCSI HA, MPIO/ALUA, advanced security/audit operations, and scale-oriented observability are Enterprise-only. |
| Control CLI | `sbsctl iscsi` is the SBS-cluster-backed iSCSI product control surface. `sbsctl iscsi` remains a local evidence CLI for smoke, status, portal, target, LUN, initiator, session, and summary fixture surfaces. Fixture/local-state failover output must not be described as live cluster-wide iSCSI HA authority until a service-owned metadata path and validation evidence exist. |
| Validation evidence | The validation summary records `compatibility_claim_scope=linux_open_iscsi_required_windows_known_issue_macos_excluded`, deploy/restart/validation flags, Linux compatibility matrix row, known issues, skipped unsupported features, and Community edition-boundary status. Required Linux evidence must be an SBS-backed smoke rather than a memory-only target startup; Windows evidence is optional unless a later compatibility track explicitly requires it. |
| Deferred validation | iSCSI HA product support remains future work after release/access QA closure. The preferred shape is MPIO-linked multi-portal access where multiple iSCSI portals/sessions map to one stable LUN identity and standard initiators coalesce paths. Active/passive VIP handoff is retained only as a last-resort fallback for non-MPIO environments and still needs multiple gateway processes, export lease handoff, standby write rejection, stale active rejection, initiator reconnect/login behavior, and post-failover readback continuity before it can be advertised. macOS, hypervisor, ALUA, persistent reservation, and performance claims remain separate evidence targets. |
| Authority boundary | The iSCSI gateway is a protocol frontend, not a new storage authority. SBS placement, payload, metadata, repair, and long-term lifecycle authority remain with the existing gateway/SBS control and data paths. |

## 3. sbs-service gRPC API <span class="edition-boundary-inline">Contains Enterprise edition only RPC rows</span>

The cluster control API is `sbs.admin.v1.AdminService` plus `sbs.admin.v1.OperationsService`. The source of truth is `proto/sbs/admin/v1/admin.proto` and `proto/sbs/admin/v1/operations.proto`.

| RPC group | Representative methods |
|----|----|
| Authenticated transport | In the Enterprise authenticated-admin profile, `--admin-grpc-listen` creates a TLS 1.3 listener for `AdminService` and `OperationsService` and removes both from the plaintext product/internal listener. The listener requires a private-CA client certificate, exactly one supported SAN identity, and a mandatory SHA-256 revocation file reloaded for every RPC. `internal/adminclient` consumers select `NAMRBD_ADMIN_TRANSPORT_MODE=mtls` and CA/certificate/key/server-name file references; incomplete TLS configuration fails without plaintext fallback. Authentication alone is not RBAC authorization evidence. |
| Cluster and leader | `ClusterInit`, `GetClusterStatus`, and `GetLeader`. |
| Node and topology | `ListNodes`, bounded `ListNodesPage`, `GetNode`, `JoinNode`, `UpdateNodeTopology`, `DrainNode`, `RemoveNode`, `ForceRemoveNode`, and topology zone CRUD. Page requests carry a bounded size, server token, source revision, and optional tombstone policy. |
| Volume and placement views | `ListVolumes`, bounded/filterable `ListVolumesPage`, `GetVolume`, `GetVolumePlacementView`, `GetVolumeAllocationPageView`, `GetReplicaTargetsView`, `CreateVolume`, `CreateVolumeFromSnapshot`, `ExpandVolume`, and `DeleteVolume`. A page can filter by health, redundancy backend, and topology mode without automatic completion. |
| EC, snapshot, clone | EC profile CRUD, snapshot CRUD, clone CRUD, and `MaterializeClone`. |
| Mobility repack | Enterprise mobility/repack target-volume materialize control-plane RPCs: `PlanVolumeRepack`, `StartVolumeRepack`, `GetVolumeRepack`, `ListVolumeRepacks`, and `CancelVolumeRepack`. V-REP-002 persists planned metadata, range records, and live/snapshot/clone protected roots; V-REP-002A/B treats existing Performance diff-index records as planning acceleration only, records `diff_index_revision`, `diff_index_complete`, and `fallback_reason`, and rejects complete under-copy indexes. Same-volume mutation, metadata-only EC profile flips, unsupported modes, and unsupported backup/DR/governance roots are rejected. V-REP-004B/C wires replicated and EC target copy/verify/publish through the `sbs-service` mutation gate and records userspace readback evidence; V-REP-005A adds local EC degraded-read evidence, while deployed large-scale, kernel, support, and public claims remain closed. |
| Backup/DR | Enterprise Backup/DR target, policy, run, artifact copy/availability, retention hold, purge-plan, and status RPCs such as `CreateBackupTarget`, `CreateBackupPolicy`, `StartBackupRun`, `AdvanceBackupRun`, `RunBackupArtifactCopy`, `TickBackupScheduler`, `MarkBackupArtifactAvailable`, `CreateBackupRetentionHold`, `PlanBackupPurge`, and `GetBackupStatus`. Run admission and transition summaries persist revision, scheduled occurrence and missed-count policy, lease owner/expiry, attempt, idempotency and checkpoint, retry/backoff, cancellation, background-budget generation, queue depth, active workers, and throttling. `AdvanceBackupRun` enforces ordered state, revision, lease, terminal-state, and `backup_copy` concurrency admission. `RunBackupArtifactCopy` (CLI `sbsctl backup run copy`) requires an available snapshot whose volume, snapshot, and root identities match the run; it reads only that immutable read-view, writes generation-scoped local or S3-compatible objects, persists ordered range/checksum/byte checkpoints after each object, verifies resumed objects and the completed manifest from the target, and advances only to `integrity_checking`. It does not mark an artifact available or replace userspace/kernel restore evidence. `TickBackupScheduler` is the bounded explicit tick; the Enterprise service leader checks for due policies every five seconds by default and invokes that same transaction path only when work is due. Scheduler admission alone is not proof that a restore-qualified artifact is deployed. `CreateBackupTarget` accepts either the existing local-fixture root or the `s3_compatible_remote` endpoint, region, bucket, prefix, path-style, and mounted-file `credential_ref` fields; remote summaries return only that reference plus normalized transport-security and compatibility-cell metadata, never resolved credential values. Production remote targets require HTTPS and reject mixed local/remote fields. The remote DR control-plane track adds replication-link, recovery-point, shipping-manifest, shipping-worker, and standby-import RPCs. `CreateDRShippingManifest` binds expected byte/object counts plus geometry and compatibility digests. `HeartbeatDRShippingWorker` persists monotonic attempt/checkpoint/transfer/verification progress, retry classification, cancellation, and a target verification receipt; the deprecated caller completion boolean is rejected, and completion is derived only when the target-verified counts equal the immutable manifest scope and a receipt exists. Enterprise `RunDRShippingWorker` is the source-side product exporter: it binds an admitted worker to an existing backup target adapter, lease owner, idempotency key, transfer generation, and chunk size; reads only the available immutable source snapshot; persists every verified object checkpoint; resumes without rereading completed source ranges; verifies the completed target manifest, including the payload-root digest; and derives the receipt and completion state. `ImportDRStandbyVolume` admits the immutable control envelope and seals a product target volume before data movement when a product transfer is bound. Enterprise `RunDRStandbyImport` (CLI `sbsctl dr standby-volume run-import`) runs on the target authority, independently rereads and verifies the target manifest and every object, imports through the internal product I/O adapter, performs userspace readback, and requires actual gateway `reload-size` plus write attempts to return the stable `dr_standby_read_only` protected-state conflict both before and after import. Product completion, byte/object counts, target receipt, readback, gateway rejection, lease, idempotency, and first/last error remain durable. `CreateDRReplicationLink` can pin an independent Ed25519 fencing authority by id and public key while summaries expose only its fingerprint. `PromoteDRStandbyVolume` requires a bounded signed `namrbd.dr.fencing.v1` decision binding both clusters, volumes, link, standby, next promotion generation, and fencing epoch; source fencing, dependency health, target integrity, and key access must all be attested. Promotion uses standby-generation CAS and idempotency, records the decision digest, and atomically removes the target's `dr_standby_read_only` protection. Demotion also uses CAS/idempotency and atomically restores that protection. Direct `sbs-data`, kernel, CSI, other protocol-path enforcement, rejoin/reseed/reverse-shipping failback, live two-authority failover, and public DR support remain separate gates. |
| Security/Compliance | Enterprise Security/Compliance provider, policy, data-key, lease, rotation, audit, and crypto erase RPCs such as provider create/check, policy create/bind, data-key create/get/disable/enable/destroy, `IssueKeyAccessLease`, `CheckSecurityDataKeyAccess`, `UnwrapSecurityDataKey`, key rotation plan/run, audit list/verify, and crypto erase plan/run. |
| Store and maintenance | `UpdateNodeStoreWeights`, `UpdateNodeStoreTuning`, `SetMaintenanceThrottle`, `PauseMaintenance`, `ResumeMaintenance`, bounded `ListRepairsPage`/`ListRebalancesPage`, and compatibility `ListRepairs`/`ListRebalances` calls subject to expensive-read admission. |
| Operations | `GetOperation`, bounded/filterable `ListOperationsPage`, and compatibility `ListOperations` expose queued, running, completed, failed, or canceled operation state. |
| Authority boundary | `sbs-service` owns cluster-wide control and metadata authority. It may forward node-local tuning to `sbs-data`, but it does not own local payload persistence. |

For an independent DR target authority, `ExportDRTargetEnvelope` emits only a
completed, target-verified shipping worker and its immutable link, recovery
point, manifest, object-checkpoint, and receipt evidence as canonical
`namrbd.dr.target-envelope.v1` JSON with a SHA-256 digest. The operator moves
that document with `sbsctl dr target-envelope export|import`.
`ImportDRTargetEnvelope` requires a distinct configured target cluster, a
locally configured transfer target and standby volume, exact document/wrapper
identity, and one idempotency key before atomically publishing the imported
control records. Source-local fencing fields are cleared during import; this
shipping envelope cannot itself grant target write authority or replace the
later independently attested promotion gate.

`ExportDRSourceFenceEnvelope` rereads the source replication link and product
volume and emits `namrbd.dr.source-fence-envelope.v1` only when the volume is
sealed by the link's deterministic receipt. On the target,
`ImportDRSourceFenceEnvelope` requires the matching imported link and ready
standby, recomputes that receipt, and verifies a short-lived promotion decision
with the fencing authority key pinned during target-envelope import. The target
persists the envelope and decision digests atomically. A remote promotion must
match both digests; local-source promotion continues to require the product
volume seal in the same transaction. `sbsctl dr source-fence-envelope
export|import` carries this handoff without exposing a private key.

`PrepareDROldPrimaryReseed` is the fail-closed bridge from a fenced original
primary to reverse shipping. It requires the original link generation and
source-fencing receipt, derives the only valid reverse source from the original
promoted target, and atomically replaces `dr_old_primary_fenced` with
`dr_old_primary_reseed_required`; it never makes the old primary writable.
The protected state binds the original link and receipt to the planned reverse
link/source identity. A new `ImportDRTargetEnvelope` accepts that non-empty
target only when the reverse envelope matches the durable reseed record, and
`ImportDRStandbyVolume` consumes the record while replacing the protection with
`dr_standby_read_only`. The CLI surface is `sbsctl dr standby-volume
prepare-old-primary-reseed`. For a deliberately demoted reverse source,
`FenceDRSourceVolume` accepts `demoted_standby_volume_id` only when that standby
is read-only, demotion-verified, and the exact inverse cluster/volume identity
of the reverse link; the same transaction replaces the standby seal with the
reverse link's deterministic old-primary fence without any writable interval.
The two-authority software fixture executes reverse product shipping, import,
readback, witnessed source fencing, and promotion back to the original site.
Deployed two-authority failback and release support remain later gates.

## 4. sbs-data gRPC API

The node-local storage execution API is `sbs.v1.VolumeService`, defined in `proto/sbs/v1/volume.proto`. Gateway and maintenance flows use it to execute local payload operations while carrying attachment, generation, request id, and idempotency context.

| RPC group | Representative methods and fields |
|----|----|
| Session/profile | `OpenVolume`, `CloseVolume`, `GetVolumeProfile`, and `GetVolumeStatus`. |
| Logical I/O | `Read`, `Write`, `Flush`, `Discard`, and `Zero`. |
| Physical and EC I/O | `ReadPhysicalChunk`, `WritePhysicalChunk`, `WriteECShard`, `ReadECShard`, and `DeleteECShard`. |
| Request context | `RequestContext` carries `request_id`, `gateway_id`, `host_id`, `session_id`, `attachment_id`, `generation`, `idempotency_key`, deadline, and trace id. |
| Error contract | `ErrorDetail` distinguishes not found, bad request, stale generation, attachment mismatch, idempotency conflict, unavailable, timeout, and internal errors. |
| Authority boundary | `sbs-data` owns node-local payload and store/shard persistence. It validates local request context but does not own cluster placement, membership, or maintenance orchestration. |

## 5. Observability HTTP URLs

Observability URLs are HTTP surfaces, but they are not all equivalent. Health and metrics routes are operational surfaces. Many `/debug` routes are fixture, validation, or controller aids and should not be treated as storage semantics authority.

| Component | URLs |
|----|----|
| gateway | `GET /api/v1/debug/gateway/metrics`, `GET /api/v1/debug/sbs-cluster/metrics`, and configured SBS cluster debug views. |
| sbs-service | `GET /healthz`, `GET /readyz`, `GET /metrics`, `GET /debug/summary`, `GET /debug/volume`, `GET /debug/transitions`, maintenance debug routes, payload GC debug route, and EC inspect/scrub/repair/rebalance/drain debug routes. |
| sbs-service operations views | Read-only Community-safe query URLs: `GET /console/`, aggregate `GET /api/v1/sbs/cluster`, paged `/api/v1/sbs/nodes` and `/api/v1/sbs/volumes`, point `/api/v1/sbs/node?id=...` and `/api/v1/sbs/volume?id=...`, `/api/v1/sbs/maintenance`, `/api/v1/sbs/capacity`, `/api/v1/sbs/reclaim`, `/api/v1/membership/status`, `/api/v1/operations/summary`, `/api/v1/operations/warnings`, `/api/v1/query/views`, `/api/v1/mcp/tools`, `/api/v1/gui/summary`, and `/api/v1/workflow/hardening`. Responses carry `namrbd.sbs.observability.v1`, projection/source revision and freshness, bounded request/detail class, warnings/errors, RBAC/redaction, read-only enforcement, and unsupported-claim visibility. The console is a same-origin static dashboard and never a mutation endpoint or raw-metadata full-scan fallback. |
| sbs-data | `GET /healthz`, `GET /readyz`, `GET /metrics`, `GET /debug/summary`, `GET /debug/store-health`, `GET /debug/allocation-pages`, `GET /debug/extent-pages`, `GET /debug/store-shards`, `POST /admin/store-weights`, and `POST /admin/store-tuning`. Validation-only routes include materialize/write-pattern/chunk-GC/store-state/store-config-reload when enabled. |
| Data discipline | JSON responses must remain machine-readable. Human diagnostics belong in logs or stderr for scripts, not mixed into JSON-producing paths. |

## 6. Kernel-Gateway Dataplane Wire Protocol

The foreground block I/O dataplane between `namrbd_blk.ko` and `namrbd-gateway` is a persistent TCP binary frame protocol, not REST. Chapter 14 explains the runtime behavior; the exact reusable codec sources are under `protocol/wirev1` and `protocol/wirev2`.

| Surface | Contract |
|----|----|
| wire v1 | `NMBR` magic, version `1`, fixed request/response headers, opcodes for read, write, flush, discard, write-zeroes, heartbeat, path probe, volume info, and barrier, plus response status codes such as generation mismatch, invalid range, path draining, quorum failed, retryable, busy, checksum, and internal. |
| Request identity | Frames carry `request_id`, `volume_id`, `generation`, `offset_bytes`, `length_bytes`, flags, and CRC. The kernel receive worker validates response opcode, request id, volume id, and generation before completing a block request. |
| wire v2 | Version `2` keeps the same data opcodes and adds session id, sequence number, auth length, HMAC auth tag, and handshake opcodes `HELLO`, `HELLO_ACK`, and `AUTH_ERR`. |
| Handshake payload | `HelloPayload` carries token, client nonce, device id, host id, supported auth modes, and requested path id. `HelloAckPayload` returns session id, server nonce, selected auth, expiration time, path id, and inflight limits. |
| Authority boundary | The wire protocol authenticates and frames admitted I/O for a host path. It does not replace attachment/generation fencing, SBS metadata commit rules, or cross-gateway ordering decisions. |

## 7. CSI gRPC API

The Kubernetes-facing interface is the standard CSI gRPC service set implemented by `cmd/namrbd-csi-driver` and `internal/csi/driver`. The driver translates CSI requests to `sbs.admin.v1` admin calls and node-local `namrbdctl` attach/mount helpers; it must not become storage semantics authority.

| CSI service | NAMRBD mapping |
|----|----|
| Identity | `GetPluginInfo`, `GetPluginCapabilities`, and `Probe` expose driver name, vendor version, controller-service support, online expansion support, and readiness. |
| Controller volume lifecycle | `CreateVolume`, `DeleteVolume`, `ValidateVolumeCapabilities`, and `ControllerExpandVolume` map to `sbs.admin.v1` create/delete/get/expand volume calls. StorageClass parameters become NAMRBD redundancy, EC, topology, block size, allocation chunk, and allocation page settings. |
| Snapshot lifecycle | `CreateSnapshot`, `DeleteSnapshot`, and snapshot listing map to NAMRBD snapshot admin APIs. CSI snapshot handles map to NAMRBD `snapshot_id`, and restore size comes from snapshot source size. |
| Restore from snapshot | CSI `CreateVolume` with `VolumeContentSource.snapshot` maps to `CreateVolumeFromSnapshot`. The external CSI result is a normal provisioned volume even if the backend implementation may use a clone-like view or materialized independent volume by edition/backend. |
| Node service | `NodeStageVolume`, `NodePublishVolume`, `NodeUnpublishVolume`, `NodeUnstageVolume`, `NodeExpandVolume`, `NodeGetInfo`, and `NodeGetCapabilities` attach via `namrbdctl`, format or bind block when requested, mount/publish to kubelet paths, reload device size, and grow filesystems. |
| Authority boundary | CSI owns Kubernetes object translation and node staging/publishing workflow. Volume truth, snapshot truth, placement, read views, fencing, and reclaim semantics remain in NAMRBD/SBS APIs. |

## 8. etcd And TiKV Client Boundaries

etcd and TiKV are storage backends used by NAMRBD components. They are not user-facing component APIs, but the architecture must describe which component may read or mutate each backend-backed authority set.

| Backend boundary | Contract |
|----|----|
| etcd control-plane authority | Gateway/control-plane metadata includes volume spec/state, attachment ownership, attachment generation, gateway identity, and liveness. Kernel and CSI flows consume this authority through gateway or admin APIs, not by reading etcd directly. |
| TiKV SBS metadata authority | `sbs-service` owns TiKV-backed cluster membership, placement, allocation pages, physical object descriptors, EC stripes, repair/rebalance/drain state, mutation operation records, idempotency records, and published gateway-facing views. Transactionally derived 64-shard summary/outbox state, cursor-bounded shadow rebuilds, placement-by-node affected sets, and leased maintenance work-ready indexes remain rebuildable access paths beneath the same source authority. |
| Client implementation | TiKV metadata uses the TxnKV client through `sbs/cluster/metadata/tikv.go`, with PD endpoints, API version, optional keyspace prefix, and TLS security. Legacy or object-store RawKV code is separate and must not be confused with SBS metadata authority. |
| Key families | The public architecture names record families and ownership, not raw key encodings. Raw keys such as volume, node, allocation, snapshot, clone, EC metadata, Backup/DR control records, DR replication-link records, DR recovery-point records, DR shipping-manifest records, and DR shipping-worker records remain internal persistence detail. |
| Authority boundary | Gateway cache, CSI calls, and operational scripts may observe backend-derived state, but they must not bypass the owning API to mutate etcd or TiKV records. |

## 9. Internal SBS Metadata gRPC

The `proto/sbs/internalapi/v1` services are internal authority surfaces used to move metadata decisions behind `sbs-service` instead of letting gateway paths depend on raw stores. They are not public admin APIs.

| Internal service | Contract |
|----|----|
| `ChunkIDAllocatorService` | `AllocateChunkIDs` reserves monotonic physical chunk id ranges owned by `sbs-service`. |
| `PlacementResolverService` | Resolves extent placements and allocation pages for live volumes, snapshots, and clones without exposing raw TiKV schemas to callers. |
| `PlacementApplyService` | `ApplyPlacementChanges` applies committed allocation page and extent-normalization effects at a service-owned commit point. |
| `WriteSessionService` | Reads and commits volume state, idempotency records, mutation operations, page-scoped/range-local/append-only write metadata, and clone delta allocation pages. |
| `ECMetadataService` | Reads/writes physical object and EC stripe descriptors and commits EC full-stripe writes or EC discard metadata with expected epoch/revision and idempotency context. |
| Authority boundary | These APIs are narrow internal authority adapters. They can be used to remove raw metadata dependencies from gateway-side code, but they do not create a public mutation surface for arbitrary callers. |

## 10. Operator CLI Surfaces <span class="edition-boundary-inline">Contains Enterprise edition only CLI rows</span>

Operator CLIs are product interfaces even when they call fixture-only paths. Command registration must match the community/enterprise boundary and must not imply a stronger authority than the backing API provides.

| CLI surface | Contract |
|----|----|
| `sbsctl dr shipping-manifest create` and `sbsctl dr shipping-worker admit|heartbeat|run|get|list` | Enterprise DR state and source-shipping surface backed by `sbs-service`. Manifest creation requires positive expected byte/object counts and geometry/compatibility digests. Worker heartbeat accepts monotonic attempt, checkpoint, transferred/verified counts, target receipt, cancellation, retry class, and bounded error detail. `--remote-transfer-completed` is deprecated and rejected; operators cannot promote transfer state with a boolean. `run` requires a worker admitted with `--transfer-target-id`, then binds worker generation, transfer generation, chunk size, lease, and idempotency while moving the immutable recovery point through the shared target adapter. Target-authority payload import and protocol-path read-only enforcement remain separate gates, so this source exporter alone does not claim DR support. |
| `sbsctl dr link fence-source`, `sbsctl dr fencing-decision generate-keypair|issue`, and `sbsctl dr standby-volume promote|demote` | Link creation pins the independent Ed25519 public key. `fence-source` uses link-generation CAS and idempotency to seal the product old-primary volume and return a receipt bound to link/source/target identity, fence generation, decision, and epoch. The offline fencing commands create the private file at mode `0600` and issue a short-lived decision that includes that receipt without emitting private material. Promotion requires the recorded source fence, its exact receipt/decision/epoch, current standby generation, and idempotency; missing, unpinned, expired, incomplete, stale, or mismatched evidence fails before target unseal. Demotion requires its own generation and idempotency key and reseals the target. These commands alone do not prove a two-authority DR topology, reseed, reverse shipping, or failback. |
| `sbsctl mobility repack plan|start|get|list|cancel` | `sbs-service` admin RPCs back this Enterprise mobility/repack operator surface. The controlled repack path accepts only `mode=target_volume_materialize` with a distinct target volume, emits repack summary fields such as protected root/range counts, copy/verify counters, publication/readback fields, support/public claims, `diff_index_used`, `diff_index_revision`, `diff_index_complete`, `metadata_fallback_used`, and `fallback_reason`, and keeps `support_claimed=false`. `start` requires the explicit `sbs-service` mutation gate; replicated and EC target publication require local userspace readback evidence, local EC degraded-read evidence, and separate large-scale live evidence before kernel, support, or public claims open. |
| `sbsctl performance policy dry-run` | Enterprise Performance policy fixture surface. It emits the Performance summary schema for policy id, generation, tier, caps, cap scope, throttle mode, StorageClass source, ok/error counts, and restart/kernel-skip flags. It is dry-run only, uses an observe-only fixture cap scope, and does not persist policy or enforce I/O caps. |
| `sbsctl performance status --fixture` | Enterprise Performance observe-only accounting fixture. It evaluates synthetic I/O events against policy caps and reports requested/granted tokens, would-wait duration, would-reject counts, rejected ops, cap scope, throttle mode, and invalid-policy rejection. It does not dispatch I/O or change gateway/kernel behavior. |
| `sbsctl performance budget dry-run --fixture` | Enterprise Performance background budget fixture surface. It reports budget classes for repair, rebuild, scrub, backup copy, restore warmup, and diff-index work, records repair/rebuild starvation floors, foreground p95/p99 latency, background progress and wait, and explicitly reconciles the view with the existing `sbs-service` maintenance throttle authority. It does not create a second throttle store or mutate running maintenance state. |
| `sbsctl performance policy create|get|list|bind` | Enterprise command skeleton reserved for the future `sbs-service` Performance policy API. Until that API lands, these commands fail explicitly instead of mutating local or gateway-owned state. |
| `sbsctl performance budget get|list` | Enterprise Performance live budget facade. These commands call `sbs-service` `GetMaintenanceStatus`, translate the existing maintenance throttle authority plus `sbs-service-background-budget` class records into the Performance background budget summary, and do not create a competing budget store. The live view reports maintenance generation, repair/rebuild concurrency, pause flags, budget class authority, and budget generation, but it does not claim foreground load or background progress evidence. |
| `sbsctl performance budget set` | Enterprise Performance budget mutation facade. Repair, rebuild, and drain concurrency still write through `SetMaintenanceThrottle`. A single background class mutation using `--class scrub|backup_copy|restore_warmup|diff_index` writes metadata through `SetBackgroundBudget`, then reads `GetMaintenanceStatus` to emit the same live budget summary with the accepted operation handle. Repair/rebuild class writes remain maintenance-throttle owned. The `backup_copy` concurrency value now fences the durable admitted-to-snapshotting worker claim; bandwidth/IOPS caps and the other background classes are not yet enforced by live workers. |
| `sbsctl performance budget lease` | Enterprise Performance shared foreground budget lease surface backed by `sbs-service` admin RPC `AcquireBudgetLease`. It records cluster-volume cap scope, requested/granted/denied tokens and bytes, wait/reject observability, outstanding active leases for the same volume, and accepted operation handles. A cap value of `0` means that dimension is unbounded, so gateway admission requests operation tokens only when `iops_cap` is active and byte budget only when `bandwidth_cap_bytes_per_sec` is active. Gateway admission consumes this lease before dispatch when `cap_scope=cluster_volume` and `--sbs-service-endpoint` are configured; remote two-gateway aggregate validation must deploy/restart the updated `sbs-service` lease authority and gateways while leaving `sbs-data` and kernel modules untouched. |
| `sbsctl performance restore-warmup dry-run --fixture` | Enterprise Performance restore warmup fixture surface. It reports cold, warming, ready, skipped, and failed warmup states, warmup bytes, cold versus warmed first-read latency, skipped and failed reasons, and confirms that Backup/DR artifact availability is unchanged. It does not mutate source snapshots, backup artifacts, restored payloads, or live warmup workers. |
| `sbsctl performance restore-warmup start|run|get|list|cancel` | Enterprise Performance live restore warmup metadata and worker-scaffold surface backed by `sbs-service` admin RPCs `StartRestoreWarmup`, `RunRestoreWarmup`, `GetRestoreWarmup`, `ListRestoreWarmups`, and `CancelRestoreWarmup`. The API records warmup state labels for Backup/DR available artifacts, and `run` advances existing cold/warming records with worker run count, validated bytes, pre-read completion, and failed-validation isolation. It still does not move restored payload data, mutate source snapshots or backup artifacts, or change gateway/kernel behavior. |
| `sbsctl performance diff-index validate --fixture` | Enterprise Performance diff-index validation fixture surface. It compares synthetic diff-index records against the Backup/DR correctness-first changed-block listing baseline, requires read-view identity beyond volume revision, permits conservative over-copy only as a safe superset, rejects under-copy, and records stale, partial, and missing coverage fallback counts. It does not persist a live diff-index, mutate reachability roots, drive GC, or change backup/materialize behavior. |
| `sbsctl performance diff-index build|scan|get|list|drop` | Enterprise Performance live diff-index metadata and scanner-scaffold surface backed by `sbs-service` admin RPCs `BuildDiffIndex`, `ScanDiffIndex`, `GetDiffIndex`, `ListDiffIndexes`, and `DropDiffIndex`. The API persists metadata-only candidate index records with read-view identity, coverage, changed ranges, Backup/DR baseline validation, fallback-required state, scanner run observability, and accepted operation handles. Complete records reject under-copy before persistence; partial and stale records remain fallback-required. The scanner scaffold reconstructs ranges from a Backup/DR baseline and keeps `product_fast_path_enabled=false`; it does not mutate reachability roots, drive GC, or accelerate backup/materialize/read paths. |
| `sbsctl performance ec-journal guarded --fixture` | Enterprise Performance guarded EC journal fixture surface. It records `guarded_mode=ec_same_stripe_batching`, committed-metadata acknowledgement boundary, p50/p95/p99, conflict count, replay count, fallback count, batch count, and kernel skip reason while checking same-stripe partial-write burst, interrupted replay, idempotency retry, snapshot old-data read, clone delta isolation, backup changed-listing compatibility, degraded read compatibility, and multi-gateway read-after-write. It does not change the product EC RMW path, persist a service-owned journal, restart services, or expose a product tier. |
| `sbsctl performance ec-journal guarded --set|--get` | Enterprise Performance live guarded EC control-plane metadata surface backed by `sbs-service` admin RPCs `SetECJournalGuardedMode` and `GetECJournalGuardedMode`. It records operator intent, generation, acknowledgement boundary, validation gate, accepted operation handles, and `guarded_mode_active_in_product=false`. It does not enable EC same-stripe batching, persist or replay a write journal, mutate reachability roots, change backup/diff-index behavior, or expose a product tier. |
| `sbsctl security ...` | Enterprise Security/Compliance security/compliance surface backed by `sbs-service` admin RPCs. Provider, policy, key, lease, rotation, audit, and crypto erase commands persist service-owned control records, report redacted refs/evidence, carry data-key versions for lease/access/unwrap checks, and keep plaintext key material out of JSON summaries and metadata. |
| `sbsctl rbac role list`, `permission list`, `binding put|get|list|delete`, and `audit list|verify` | Enterprise RBAC surface backed by `sbs-service` admin RPCs. The five built-in roles and descriptor-derived permissions are read-only definitions. SAN identities (`spiffe://`, URI, DNS, or email SAN form) bind to reviewed roles under the single metadata authority with generation CAS, idempotency, actor/reason, and durable tombstones. With `--admin-rbac-enforce`, the common interceptor requires the exact current binding generation supplied by `NAMRBD_ADMIN_RBAC_GENERATION`, denies unknown methods and wrong roles before handlers, and appends allow/deny decisions to a verified, redacted hash chain. Initial/recovery binding uses only the exact-SAN restricted `--admin-rbac-bootstrap-identity` mode. Local enforcement evidence does not replace deployed restart and certificate-rotation qualification. |
| `sbsctl iscsi ...` | SBS-cluster-backed iSCSI product control surface. Gate A is read-only status/list/get through SBS admin projections until a durable iSCSI registry API exists. |
| `sbsctl iscsi ...` | Local iSCSI fixture/smoke validation. It is not a user-facing SBS cluster management CLI, and fixture/local-state failover output must not be described as live cluster-wide iSCSI HA authority unless a later service-owned metadata path and validation evidence back it. |
| Authority boundary | `sbsctl` is an operator interface. Product policy authority remains with `sbs-service`; gateway-local or fixture-only policy output must not be described as cluster-wide QoS enforcement. |

## Generation And Refresh Rule

| Rule | Reason |
|----|----|
| Generated reference remains future work. | This appendix is hand-authored. Exact frame structs, protobuf messages, and route handlers still come from source files. |
| Public and internal surfaces stay separate. | CSI, gateway REST, and admin gRPC are externally callable surfaces. Internal SBS metadata gRPC and backend-store keys are authority implementation details. |
| Source changes require appendix refresh. | Any change to wire opcodes, CSI capabilities, proto services, route families, listener flags, default ports, transport type, or backend ownership should update this appendix and rerun static HTML validation. |
| Roadmap changes carry the same obligation. | Future roadmap plan or authoritative-doc updates that add, remove, rename, or change the semantics of an interface listed in Appendix C must update this appendix in the same change set, or explicitly mark the appendix refresh as a blocking follow-up. |

[\<- Previous](appendix-reference-map.md) [Architecture index -\>](../index.md)
