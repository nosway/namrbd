Operations Manual

Edition boundary: Community edition operations and Enterprise edition only administration sections are both present.

# NAMRBD Administrator Guide (Enterprise Service Baseline)

This document establishes the standard operational paths for NAMRBD, including the boundaries of Enterprise Backup & Remote Disaster Recovery, Dynamic Performance Provisioning & Warmup Tiers, Enterprise Security & Vault KMS, Community basic iSCSI target access, and Enterprise-only iSCSI HA/scale surfaces. For initial deployment, consult the [Installation Guide](installation-guide.md), and for the user quick-start guide, refer to the [User Manual](user-manual.md).

The legacy metadata command surface is archived. Administrators should use `namrbdctl`, `sbsctl`, `namrbd-debug`, and standard administrative APIs for volume lifecycle management, storage authority, and system diagnostics.

## 1. Component Ownership

| Component | Owns | Does not own |
|----|----|----|
| `namrbd-gateway` | host I/O forwarding, gateway registry, attach/path-plan control-plane state in `etcd`, dataplane metrics | SBS placement authority, EC membership, repair/rebalance/drain decisions |
| `sbs-service` | SBS authoritative metadata, cluster membership, topology, placement, volume/snapshot/restore, EC profile, maintenance transitions, enterprise Backup/DR target/policy/run/artifact/hold/status state, enterprise security policy/key/audit/crypto-erase state, and performance policy authority | Host device lifecycle, kernel queueing |
| `sbs-data` | node-local stores, replica chunks, EC shards, local health/store status | Global placement authority |
| Kernel modules | block device lifecycle, request queueing, path failover, DISCARD/WRITE_ZEROES dispatch | Metadata authority and placement decisions |
| `namrbd-csi-driver` | CSI translation for Kubernetes Create/Delete/Publish/Stage/Snapshot/Restore/Expand | Snapshot/clone/read-view/GC semantics |
| `namrbd-iscsi-gateway` | iSCSI target sessions, SCSI/iSCSI command mapping, protocol-visible status, summary artifacts | SBS metadata, placement, encryption key authority, discard/reclaim authority, active/standby HA lease authority |
| `namrbdctl` | host/device attach, detach, status, volume-facing primary CLI | Storage-side maintenance |
| `sbsctl` | SBS cluster/node/topology/volume/snapshot/restore/maintenance/guardrail operations, enterprise `backup`, `performance`, and `security` operations | Kernel device lifecycle |
| `sbsctl iscsi` | Community iSCSI status, list, and get operations backed by SBS cluster state | Live cluster-wide SBS storage metadata mutation unless backed by service-owned APIs and evidence |
| `namrbd-debug` | low-level inspect/validate/break-glass diagnostics | Routine user workflow |

Operational rule: gateway reads SBS authority through `sbs-service` surfaces. Primary gateway runtime must not be described as directly interpreting raw SBS TiKV metadata.

## 2. Standard Topology

Enterprise Service production-like topology:

- HA `etcd` for gateway/control-plane metadata.
- HA TiKV/PD for `sbs-service` authoritative SBS metadata.
- Multiple `sbs-service` nodes for cluster admin authority.
- Multiple `sbs-data` nodes with zone/topology labels and local stores.
- One or more gateways with unique `--gateway-id`.
- Attach hosts with current `namrbd_blk.ko`, `namrbd_ctrl.ko`, and `namrbdctl`.
- Optional Kubernetes CSI deployment with controller and node DaemonSet.
- Optional `namrbd-iscsi-gateway` process for standard iSCSI initiator access.

Keep environment boundaries explicit:

- Same gateway group: same `--etcd-endpoints` and `--etcd-root`.
- Same SBS cluster: same TiKV/PD endpoint set and keyspace.
- Different dev/stage/prod/lab: different roots/keyspaces.

### 2.1 Membership Change Workflow

Gateway, iSCSI, and SBS membership changes use the same operator envelope: plan, preflight, apply, synchronize, verify, rollback, and audit. Gateway membership and liveness are gateway/control-plane state; SBS node, topology, store tuning, drain, remove, and force-remove state are `sbs-service` AdminService authority. `sbs-data` contributes node-local health and capacity evidence, but it is not cluster membership authority.

Use read-only status first. Confirm `source_authority`, `collector_freshness_seconds`, `warning_count`, `first_error`, `last_error`, `rbac_checked`, `redaction_applied`, and `unsupported_claim_visible` before interpreting a membership or capacity view. Mutation-style membership workflows remain blocked unless an existing CLI/API path, RBAC rule, audit record, rollback behavior, and human approval gate are present.

## 3. Observability

Community public operations assets live under `deploy/observability/`. They include a Prometheus scrape example, starter alert rules, a Grafana overview dashboard, and a metric catalog that matches the SBS service/data Prometheus endpoints.

``` bash
make observability-assets-check
```

`sbs-service`, `sbs-data`, and `namrbd-gateway` expose `/healthz`, `/readyz`, and Prometheus-format `/metrics`. `namrbd-gateway` also exposes JSON debug metrics through `/api/v1/debug/gateway/metrics` and `/api/v1/debug/sbs-cluster/metrics`.

### 3.1 Host And Kernel

Use:

``` bash
namrbdctl status --device 0
lsblk /dev/namrbd0
dmesg | tail -n 100
```

Kernel watchpoints:

- attachment id and generation.
- path plan revision and runtime path status.
- device size after restore/expansion.
- request op identity: READ, WRITE, FLUSH, DISCARD, WRITE_ZEROES.
- resource backpressure counters and requeue reasons.

Current kernel contract inherited through Enterprise Service:

- `REQ_OP_DISCARD` maps to discard semantics.
- `REQ_OP_WRITE_ZEROES` maps to zero semantics.
- Flush waits for inflight and resource-requeued datapath backlog before reporting completion.
- Resource-busy is not a path failure.

### 3.2 Gateway

Use gateway control-plane and metrics endpoints for:

- volume info.
- attach/path-plan status.
- dataplane health.
- `io_identity` counters for zero vs discard.
- path distribution and path failure state.

Discard observability must distinguish:

- `operation=discard` vs `operation=zero`.
- `policy=true_reclaim` vs `policy=zero_fallback`.
- alignment and reclaim geometry.
- fallback reason.
- discard bytes and logical zero bytes.

### 3.3 `sbs-service`

Use:

``` bash
sbsctl cluster status --output json
sbsctl topology zone list
sbsctl node status --node-id data-01 --output json
sbsctl volume status --volume-id <volume_id> --output json
sbsctl volume transitions --volume-id <volume_id> --admin-http-endpoint http://service-01.example.com:9081
```

`sbs-service` owns:

- leader/quorum state.
- volume placement and transitions.
- snapshot/restore authority.
- EC profile and topology placement.
- repair/rebuild/scrub/rebalance/drain state.
- Backup/DR, performance policy, security policy/key/audit, and crypto-erase state when enterprise features are enabled.

### 3.4 `sbs-data`

Use:

``` bash
curl -fsS http://data-01.example.com:9082/healthz
sbsctl store status --admin-http-endpoint http://data-01.example.com:9082
```

Watch:

- store health.
- shard/store capacity.
- local payload failures.
- node-local latency.

### 3.5 Enterprise Backup & DR <span class="edition-boundary-inline">Enterprise edition only</span>

Enterprise Backup & DR is enterprise-only except for the community manual snapshot and manual restore-from-snapshot safety boundary. Operators should treat `sbs-service` as the authority for Backup/DR product state:

``` bash
sbsctl backup target list --output json
sbsctl backup policy list --output json
sbsctl backup run list --policy-id <policy_id> --output json
sbsctl backup artifact list --policy-id <policy_id> --output json
sbsctl backup status --policy-id <policy_id> --output json
```

Status watchpoints:

- `evidence_mode=product_state` for product API state.
- `artifact_available=true` only after artifact integrity recheck plus userspace and kernel readback evidence.
- `restore_drill_result=kernel_readback_passed` for a successful recovery point.
- `delete_protection_status=guarded` before any purge execution is considered.
- `community_leakage_status=blocked` to preserve the edition boundary.

The fixture closure path remains available through `--fixture` commands and the Backup/DR fixture validation target; do not treat fixture JSON as product state.

### 3.6 Enterprise Security & Vault KMS <span class="edition-boundary-inline">Enterprise edition only</span>

Enterprise Security & Vault KMS is enterprise-only. `sbs-service` owns key provider metadata, security policies, data-key records, key access leases, audit records, rotation plans, and crypto erase state. Gateways and kernels consume admission decisions and short-lived key access results; they do not own KMS policy or persist plaintext keys.

``` bash
sbsctl security provider list --output json
sbsctl security policy list --output json
sbsctl security key list --output json
sbsctl security audit list --output json
sbsctl security crypto-erase list --output json
```

Security watchpoints:

- `plaintext_key_emitted=false` and redacted provider credential references.
- disabled, missing, or destroyed keys fail closed for reads, writes, attach, restore, and backup reads.
- key rotation preserves old-object read compatibility until re-encrypt or explicit crypto erase completes.
- crypto erase must respect retention holds, protected artifacts, active attachments, and pending operations.
- Current security validation is integrated mock provider based; live external KMS network/provider destroy evidence remains a conditional follow-up when enabled.

### 3.7 Basic iSCSI Target Access <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

Basic iSCSI target access is an optional standard block protocol frontend. Community edition includes `namrbd-iscsi-gateway`, `sbsctl iscsi`, and basic LUN export for up to 3 distinct iSCSI-exported volumes. More than 3 exported volumes, unlimited export scale, iSCSI HA, MPIO/ALUA, advanced security/audit operations, and scale-oriented observability are Enterprise-only.

The current required compatibility claim is Linux open-iscsi. Windows native initiator has post-closure memory-backend success evidence and SBS-backed connection/log-cleanup evidence, but not full SBS-backed Windows read/write/readback/flush/cleanup support. macOS is excluded until licensed initiator evidence exists.

``` bash
sbsctl iscsi status gateway --json
sbsctl iscsi status target --target-iqn <target_iqn> --json
```

iSCSI watchpoints:

- `target_iqn`, `portal_address`, `lun_id`, `volume_id`, `backend_mode`, and `backend_adapter`.
- `active_iscsi_gateway_id`, `export_lease_id`, `export_epoch`, `attachment_id`, and `generation` for writer/fencing evidence.
- `initiator_os=linux`, `initiator_vendor=open-iscsi`, `readback_matched=true`, and `error_count=0` for the required compatibility gate.
- `macos_support_claimed=false`; do not advertise macOS or broad non-Linux support from current iSCSI evidence.
- CHAP and initiator allowlist runtime hooks fail closed with the current gotgt stack; raw CHAP secrets must never appear in JSON or logs.

### 3.8 Read-Only Operations API

`sbs-service` exposes a Community-safe read-only operations surface for tools, reports, GUI views, and observe-first MCP descriptors. The shared SBS observability schema is `namrbd.sbs.observability.v1`, owned by NAMRBD so that NAMROS and other consumers do not redefine SBS health, capacity, reclaim, or membership semantics.

``` bash
curl -fsS http://service-01.example.com:9081/api/v1/sbs/cluster
curl -fsS http://service-01.example.com:9081/api/v1/sbs/nodes
curl -fsS http://service-01.example.com:9081/api/v1/sbs/capacity
curl -fsS http://service-01.example.com:9081/api/v1/sbs/reclaim
curl -fsS http://service-01.example.com:9081/api/v1/membership/status
curl -fsS http://service-01.example.com:9081/api/v1/operations/summary
curl -fsS http://service-01.example.com:9081/api/v1/operations/warnings
curl -fsS http://service-01.example.com:9081/api/v1/query/views
curl -fsS http://service-01.example.com:9081/api/v1/mcp/tools
curl -fsS http://service-01.example.com:9081/api/v1/gui/summary
curl -fsS http://service-01.example.com:9081/api/v1/workflow/hardening
```

These URLs are views, not mutation authority. Capacity separates logical bytes, physical used/free bytes, reclaimable bytes, protected bytes, and unknown bytes. Reclaim views do not claim completion until protected-reference checks and before/after `sbs-data` free-byte evidence exist. MCP and GUI descriptors remain read-only; mutating tools and controls are disabled until separately reviewed.

The read-only operations console is served from the same `sbs-service` administration endpoint at `/console/`. It consumes the same operations views, with `/api/v1/sbs/cluster` as the primary snapshot, and presents status, topology, capacity, maintenance backlog, warnings, membership source authority, and reclaim evidence without adding a new source of truth. The console must show stale, partial, and failed collection states rather than hiding them, and packaged chart assets should work without a public CDN dependency.

Operator evidence bundles should preserve product/build identity, source authority and freshness, relevant query snapshots, recent operation history, warnings/errors, redaction status, runbook suggestions, and unavailable-evidence reasons. They must exclude secrets, tokens, raw payload content, and private deployment paths. Future state-changing console or MCP workflows require a reviewed API path, plan/preflight output, human approval, apply/synchronize/verify behavior, rollback guidance, audit records, RBAC/redaction checks, and an emergency read-only lock before they can be enabled.

## 4. Enterprise Storage Management <span class="edition-boundary-inline">Contains Enterprise edition only sections</span>

### 4.1 Volume Lifecycle

Create:

``` bash
sbsctl volume create \
  --volume-id <volume_id> \
  --size 1G \
  --block-size 4K \
  --replication-factor 3 \
  --policy-name spread-3az \
  --topology-mode strict
```

Status:

``` bash
sbsctl volume status --volume-id <volume_id> --output json
```

Attach:

``` bash
namrbdctl attach --device 0 --host <host_id> --volume <volume_id> --gateway http://gw01:9899
namrbdctl status --device 0
```

Detach:

``` bash
namrbdctl detach --device 0 --host <host_id> --volume <volume_id>
```

### 4.2 Snapshot, Clone, Restore

Enterprise Service inherits the current snapshot/clone/read-view semantics:

- Snapshot identity is an immutable read-view.
- Clone delta writes do not mutate the source snapshot.
- Restore from snapshot creates an ordinary attachable volume-like target.
- Snapshot/clone-aware GC must preserve live, snapshot, clone, restored, and pending-operation roots.

Operator restore command:

``` bash
sbsctl volume restore-from-snapshot \
  --snapshot-id <snapshot_id> \
  --volume-id <new_volume_id> \
  --size <size>
```

Kubernetes restore uses CSI `CreateVolume` with `VolumeContentSource.snapshot`, backed by the same storage contract.

### 4.3 Expansion

Volume expansion is grow-only. Restored filesystem volumes may require node-side size reload and filesystem grow before workloads observe the larger size.

Check the volume/device pair together:

``` bash
sbsctl volume status --volume-id <volume_id> --output json
namrbdctl status --device 0
lsblk /dev/namrbd0
```

### 4.4 EC Operations <span class="edition-boundary-inline">Enterprise edition only</span>

EC is an enterprise capability. Operator rules:

- EC profile and geometry are create-time immutable.
- Topology mode and failure domain must be chosen before provisioning.
- Degraded read, scrub, repair, rebuild, rebalance, and drain are `sbs-service` responsibilities.
- Online EC profile conversion is not a current operation; treat it as future controlled migration/repack work.

Use:

``` bash
sbsctl cluster status --output json
sbsctl repair list --output json
sbsctl rebalance list --output json
```

### 4.5 Enterprise Backup/DR Control Plane <span class="edition-boundary-inline">Enterprise edition only</span>

Create a target and policy:

``` bash
sbsctl backup target create \
  --target-id target-a \
  --type local_filesystem \
  --root /var/lib/namrbd-backup/target-a \
  --capacity-status ok

sbsctl backup policy create \
  --policy-id policy-a \
  --source-volume-id <volume_id> \
  --target-id target-a \
  --schedule every:24h \
  --retention-count 2 \
  --retention-age-days 7
```

Record a manual run and mark an artifact available only after restore drill evidence:

``` bash
sbsctl backup run start \
  --policy-id policy-a \
  --run-id run-a \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id>

sbsctl backup artifact availability \
  --artifact-id artifact-a \
  --run-id run-a \
  --target-id target-a \
  --source-volume-id <volume_id> \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id> \
  --restore-size 8K \
  --restore-drill-id restore-drill-readback-pass \
  --restore-drill-result kernel_readback_passed_artifact_transition_pending \
  --artifact-integrity-rechecked \
  --userspace-readback-matched \
  --kernel-readback-matched
```

Before purge, create holds and inspect the dry-run guardrail:

``` bash
sbsctl backup hold create \
  --hold-id hold-a \
  --target-kind artifact \
  --target-id artifact-a

sbsctl backup purge plan \
  --artifact-id artifact-a \
  --output json
```

The Backup/DR control plane does not add a destructive purge executor, background copy scheduler, or automated remote DR. Enterprise security/compliance controls wrap the already valid Backup/DR state; they do not make an artifact available without integrity and restore-drill rules. Any deployment that changes `sbs-service` must restart/redeploy `sbs-service` before interpreting these API results in a lab.

### 4.6 Enterprise Security Control Plane <span class="edition-boundary-inline">Enterprise edition only</span>

Use `sbsctl security` for enterprise provider, policy, key, lease, audit, and crypto erase operations. Community builds must hide this surface.

``` bash
sbsctl security provider create \
  --provider-id provider-a \
  --provider-type local_fixture \
  --endpoint-ref fixture:local \
  --output json

sbsctl security policy create \
  --policy-id policy-a \
  --key-provider-id provider-a \
  --output json

sbsctl security policy bind \
  --policy-id policy-a \
  --volume-id <volume_id> \
  --output json

sbsctl security audit verify --output json
```

### 4.7 Enterprise Governance/WORM Boundary <span class="edition-boundary-inline">Enterprise edition only</span>

Governance/WORM is officially limited to scoped support. The closed scope covers block-native derived objects, local Governance/WORM fixtures, and userspace gateway sealed-target write rejection. Ordinary writable live volumes are not WORM while they accept arbitrary overwrites.

Do not treat scoped Governance/WORM support as regulatory certification or object-store API compatibility. Public governance API/CLI, kernel/iSCSI/NVMe protected-state support, ransomware recovery support, and remote DR support remain unsupported until their owning evidence gates close.

Before a destructive security action, inspect the plan and blocking reasons:

``` bash
sbsctl security key destroy-plan --data-key-id <data_key_id> --output json
sbsctl security crypto-erase plan --target-type volume --target-id <volume_id> --output json
```

### 4.7 iSCSI Target Operations <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

Start an SBS-backed iSCSI LUN only after the volume and backend endpoint are prepared. The current basic iSCSI product wording is conservative: one target with one LUN is the initial model, Persistent Reservation product semantics are rejected, and MPIO/ALUA is not advertised.

``` bash
namrbd-iscsi-gateway \
  --backend=sbs \
  --portal <gateway_ip>:3260 \
  --serve \
  --sbs-endpoint <sbs_volume_service_host>:9460 \
  --volume-id <volume_id> \
  --export-id <export_id> \
  --target-iqn <target_iqn> \
  --active-iscsi-gateway-id <iscsi_gateway_id> \
  --export-lease-id <lease_id> \
  --export-epoch <epoch> \
  --attachment-id <attachment_id> \
  --generation <generation> \
  --allow-gotgt-wildcard-listen \
  --summary-json ./namrbd-output/gateway-summary.json \
  --operation-jsonl ./namrbd-output/gateway-operations.jsonl \
  --json
```

`--allow-gotgt-wildcard-listen` reflects the current gotgt listener limitation. Use it only in an isolated fixture or controlled lab/deployment network; it is not an initiator source-IP ACL.

Production checklist before allowing initiators:

- Pin each exported LUN to an explicit portal address, target IQN, export id, active iSCSI gateway id, lease id, attachment id, and generation.
- Protect TCP/3260 with host firewall, network policy, or an equivalent access-control layer. The wildcard listener flag does not authenticate initiators.
- Run the target gateway under a service manager, rotate operation JSONL according to local log policy, and preserve summary JSON for support/debug evidence.
- Restart the target gateway after any binary, portal, SBS endpoint, target, export, attachment, or command-mapping change before interpreting initiator evidence.
- Keep Community deployments within the 3 distinct iSCSI-exported-volume cap. Use Enterprise planning for larger export scale, HA, MPIO/ALUA, advanced security/audit, or scale observability.

For Linux initiator validation, use the maintained iSCSI smoke harness instead of hand-interpreting a partial login. The accepted evidence must include the gateway summary JSON, operation JSONL, initiator session details, readback success, `ok_count`, `error_count`, and the exact gateway restart state for the run.

The live iSCSI gateway process must be restarted after changes to `cmd/namrbd-iscsi-gateway`, the `iscsi` package, gotgt fork patches, backend adapter semantics, or iSCSI command mapping before interpreting initiator evidence.

## 5. Discard/UNMAP Operations <span class="edition-boundary-inline">Enterprise edition only true reclaim</span>

Enterprise Discard Reclaim true discard rules remain the Enterprise Service UNMAP/discard baseline:

- Reclaim-aligned replicated discard can report `policy=true_reclaim`.
- Full-stripe/page-aligned EC discard can report `policy=true_reclaim`.
- Partial or unaligned filesystem-style discard is accepted as `policy=zero_fallback`, not advertised as true reclaim.
- Snapshot/clone/read-view contracts must survive discard.
- Kernel `discard` and `write zeroes` are distinct operation paths.

Volume delete success is not physical reclaim evidence by itself. Treat `/api/v1/sbs/reclaim` as the operator-facing summary: it must show pending retired payload chunks/bytes, failed batches, blocked reasons, protected-reference status, and whether before/after free-byte evidence is still required.

Live validation must exercise replicated reclaim, EC full-stripe reclaim, and kernel discard/write-zeroes paths separately, then record the policy decision, reclaimed or zero-filled byte count, protected read-view result, `ok_count`, `error_count`, and the kernel module build/reload state used for the test.

Kubernetes discard exposure remains disabled by default. Only enable manifest discard mount options with explicit evidence:

``` bash
export NAMRBD_CSI_ENABLE_DISCARD=1
export NAMRBD_CSI_DISCARD_VALIDATION_PROFILE="<current validation evidence id>"
```

## 6. CSI/Kubernetes Operations

Current CSI surface:

- Identity, Controller, and Node services.
- Create/DeleteVolume.
- Create/Delete/ListSnapshot.
- Restore from snapshot.
- Controller expansion.
- Node stage/publish and node expansion.
- RWOP conflict validation in smoke.

Operational checks must confirm CSI controller/node readiness, dynamic provision/delete, snapshot/restore, expansion, RWOP conflict behavior, and discard-wrapper evidence when discard exposure is explicitly enabled.

When discard exposure is enabled, the Kubernetes wrapper should record node readiness, whether manifests were rendered, applied, or linted, the delegated CSI smoke result, `ok_count`, `error_count`, first error, last error, and the work directory where the summary was written.

## 7. Release Guardrails

Run the current metadata-retirement guardrail and edition-boundary check before release packaging:

``` bash
make test-community
```

Expected:

- `regression_count=0`.
- `archived core repositories` is historical-only.
- No active source, build target, smoke dependency, release manifest, or public/community export includes the archived legacy metadata command surface.
- Community builds include `namrbd-csi-driver`, Kubernetes CSI manifests, basic `namrbd-iscsi-gateway`, `sbsctl iscsi`, and LUN export, enforce the 3 distinct iSCSI-exported-volume cap, and hide enterprise-only iSCSI HA, MPIO/ALUA, advanced security/audit, and scale observability operations.
- Community builds hide enterprise `backup`, `performance`, and `security` command surfaces while keeping manual replicated snapshot/restore visible.

Release guardrail evidence should record the scanned surfaces, hit counts, historical-only matches, and `regression_count=0` for the exact checkout being packaged.

Generated public/community export artifact validation remains a separate release artifact check unless it is explicitly run and recorded.

## 8. Closure And Validation

Closure means the deployed product path has fresh evidence for the exact source revision, binaries, images, service restarts, and kernel module state under review. Do not reuse private lab paths, historical hostnames, or cached artifacts as public support claims.

Basic iSCSI target access uses Linux open-iscsi as the required compatibility baseline. The validation package should include fixture startup, SBS-backed Linux initiator discovery/login, guarded LUN selection, write/readback, flush or UNMAP observation when applicable, logout, cleanup, Community edition-boundary status, and unsupported initiator exclusions.

Full Enterprise Discard Reclaim closure requires required-mode Kubernetes smoke evidence, replicated reclaim evidence, EC reclaim evidence when Enterprise EC is enabled, and live kernel discard evidence.

Enterprise Backup/DR, Performance, Security, and iSCSI closure packages should be separate records. Each package should state whether scheduler execution, destructive purge execution, live external KMS provider/destroy, active/standby iSCSI HA, full SBS-backed Windows support, macOS support, and remote DR automation were validated, skipped, or remain future work.

Minimum closure summary fields:

- `result`, `ok_count`, `error_count`, `skipped_count`, first error, and last error.
- git revision, binary checksums, image tags, kernel module version, and service restart/reload status.
- gateway, `sbs-service`, `sbs-data`, CSI driver, iSCSI gateway, and kernel processes that were changed or deliberately left unchanged.
- feature mode evidence, such as Community vs Enterprise build, replicated vs EC backend, Kubernetes discard gate, iSCSI portal/IQN/LUN identity, and backup/security policy id.
- readback evidence for host block devices, CSI pods, snapshot restores, backup artifacts, and iSCSI LUNs where those paths are claimed.
- explicit exclusions for unsupported or future compatibility paths.

A validation package that changes `namrbd-gateway`, `sbs-service`, `sbs-data`, CSI, iSCSI, or kernel code must include the matching deploy, restart, or reload step before interpreting smoke or workload results.

## 9. Troubleshooting Checklist

When a smoke or workload fails, capture:

- failed command/target.
- layer: script/env, CLI, gateway, kernel, `sbs-service`, `sbs-data`, metadata.
- first error and last error.
- summary `result`, `ok_count`, `error_count`, `skipped_count`.
- attachment id, generation, path plan revision, device size, and runtime path status for kernel/gateway failures.
- Kubernetes PVC/PV/pod/events and CSI logs for CSI failures.
- iSCSI target IQN, portal, LUN id, initiator IQN/vendor/version, SCSI status/sense, session logs, and gateway operation JSONL for iSCSI target failures.
- deploy/restart/reload state.

Common boundaries:

- Metadata CAS conflicts must be classified as stale writer/fencing vs ordinary retryable same-volume concurrency.
- Two-gateway tests must prove request distribution and correctness.
- Snapshot/clone/read-view changes must preserve source, snapshot, clone delta, protected refs, and payload GC.
- Retired payload observation/reporting must never mix human logs into JSON data flow.

## 10. Security Notes

Enterprise security implements the integrated mock provider security/compliance baseline: key policy, data-key metadata, key access leases, encrypted replicated/EC/backup payload evidence, disabled-key fail-closed behavior, rotation, audit, and crypto erase. Current operational security focus:

- keep admin endpoints protected by environment/network policy.
- preserve auditability of manual repair, break-glass, key, backup, restore, attach, and crypto erase operations.
- keep public/community export surfaces free of archived source.
- never expose the archived legacy metadata command surface as an active operator command.
- never emit plaintext key material, provider credentials, CHAP secrets, or payload samples in JSON/logs/summaries.
- do not claim live external KMS provider availability or real external-provider key destroy until those conditional follow-up gates are run.

[\<- Architecture Index](architecture-manual/index.md) [User Manual -\>](user-manual.md)
