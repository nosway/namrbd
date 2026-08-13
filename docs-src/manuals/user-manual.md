Operations Manual

Edition boundary: Community edition user flows and Enterprise edition only feature sections are both present.

# NAMRBD Unified User Manual (Production Release Baseline)

This manual provides comprehensive guidelines for production-grade NAMRBD operations, including Community block access, basic iSCSI target access, and the Enterprise-only boundaries for backup/remote DR policies, performance throttling (QoS), Vault KMS-backed payload encryption, and iSCSI HA/scale features. For installation and bring-up, please refer first to the [Installation Guide](installation-guide.md). For day-2 operations and troubleshooting, refer to the [Admin Guide](admin-guide.md).

Current command surface:

- `namrbdctl`: host device create/attach/detach/status.
- `sbsctl`: volume, snapshot, restore, topology, maintenance, guardrail, enterprise backup/performance/security.
- `namrbd-debug`: low-level inspect and diagnostics.
- `namrbd-csi-driver`: Kubernetes CSI integration.
- `namrbd-iscsi-gateway`: Community basic iSCSI target gateway for standard Linux open-iscsi initiators.
- `sbsctl iscsi`: Community iSCSI product control surface for SBS-cluster-backed status/list/get. HA/failover product support is Enterprise-only.

The legacy metadata command surface is archived and is not a primary command in the current user/operator workflow.

## 1. What NAMRBD Provides

NAMRBD provides network-backed Linux block devices with:

- replicated and enterprise EC-backed volumes.
- snapshot, clone, restore, and read-view safety.
- grow-only volume expansion.
- CSI/Kubernetes dynamic provisioning, VolumeSnapshot restore, block/filesystem PVC use, and RWOP conflict validation.
- Enterprise Discard Reclaim true discard for reclaim-aligned replicated ranges and aligned EC ranges.
- observable zero vs discard operation identity.
- Enterprise Backup & Remote Disaster Recovery control-plane state for targets, policies, runs, restore-drilled artifacts, holds, purge plans, and status.
- Enterprise Performance enterprise performance and operations tiers for policies, budgets, restore warmup, diff index, and guarded EC journal evidence.
- Enterprise Cryptographic Security & Vault KMS for key policy, encrypted payload evidence, key admission, disabled-key fail-closed behavior, rotation, audit, and crypto erase.
- Community basic iSCSI target access for standard Linux open-iscsi initiators.

Current boundaries:

- Kubernetes discard exposure is disabled by default and requires explicit gate evidence before mount options are enabled.
- RWX is not a current block-core feature.
- EC profile conversion is not online metadata flipping; future work should use controlled migration/repack.
- The current security baseline uses integrated mock provider evidence, while scoped Governance/WORM support separately covers block-native derived objects and userspace gateway sealed-target write rejection. Live external KMS network/provider destroy and dedupe remain future or conditional tracks.
- Basic iSCSI support claims Linux open-iscsi as the required compatibility baseline. Windows has optional memory-backend and SBS-backed connection/log-cleanup evidence only; macOS is excluded.
- Community edition remains limited to manual replicated snapshot, manual restore-from-snapshot, read-only snapshot/read-view safety, restore-size validation, basic delete/reference guardrails, and basic iSCSI gateway/CLI/LUN export with at most 3 distinct iSCSI-exported volumes. Enterprise Backup & DR automation, Enterprise Performance tiers, Enterprise Security & Vault KMS, scoped Governance/WORM, more than 3 iSCSI exports, unlimited export scale, iSCSI HA, MPIO/ALUA, advanced security/audit, scale observability, release/access claim packages, and remote DR features are enterprise-only or future-gated.

## 2. Quick Start

This section assumes the administrator has installed and started:

- `etcd`
- `sbs-service`
- `sbs-data`
- `namrbd-gateway`
- host kernel modules

### 2.1 Create A Volume

``` bash
sbsctl volume create \
  --volume-id 00000065 \
  --size 1G \
  --block-size 4K \
  --replication-factor 3 \
  --policy-name spread-3az \
  --topology-mode strict

sbsctl volume status --volume-id 00000065 --output json
```

Expected checkpoint:

- `sbsctl volume status` returns the requested volume id, size, block size, replication factor, and a usable lifecycle state such as created or available.
- The placement or topology fields match the requested policy, or the command reports a clear validation error before any host attachment is attempted.

### 2.2 Create And Configure A Host Device

``` bash
sudo insmod kernel/module/namrbd_blk.ko no_path_retry=fail
sudo insmod kernel/module/namrbd_ctrl.ko

namrbdctl create-device
namrbdctl config-rest --device 0 --server "1,gw01,9899,false,/api/v1"
```

### 2.3 Attach

``` bash
namrbdctl attach \
  --device 0 \
  --host host-a \
  --volume 00000065 \
  --gateway http://gw01:9899

namrbdctl status --device 0
lsblk /dev/namrbd0
```

Expected checkpoint:

- `namrbdctl status` shows the attached volume id, attachment id, generation, and at least one usable gateway path.
- `lsblk` shows `/dev/namrbd0` with the expected size before a filesystem is created.

### 2.4 Use The Device

Filesystem example:

``` bash
sudo mkfs.ext4 /dev/namrbd0
sudo mkdir -p /mnt/namrbd-demo
sudo mount /dev/namrbd0 /mnt/namrbd-demo
echo "hello namrbd" | sudo tee /mnt/namrbd-demo/hello.txt
sync
cat /mnt/namrbd-demo/hello.txt
```

Expected checkpoint:

- `cat /mnt/namrbd-demo/hello.txt` prints `hello namrbd`.
- No gateway, SBS, or kernel log should show a failed write, failed flush, stale attachment, or path-plan generation mismatch for this device.

Cleanup:

``` bash
sudo umount /mnt/namrbd-demo
namrbdctl detach --device 0 --host host-a --volume 00000065
namrbdctl destroy-device --device 0
sudo rmmod namrbd_ctrl
sudo rmmod namrbd_blk
```

## 3. Snapshot And Restore

Snapshot and restore are storage-owned operations; CSI and CLI paths translate to the same backend semantics.

Important semantics:

- Snapshot read-view is immutable after the cut.
- Parent overwrites do not change snapshot content.
- Clone delta writes do not mutate the source snapshot.
- Restore from snapshot produces an ordinary attachable volume.
- Restored volume size must be at least the snapshot restore size.

Operator restore command:

``` bash
sbsctl volume restore-from-snapshot \
  --snapshot-id <snapshot_id> \
  --volume-id <new_volume_id> \
  --size <size>
```

Kubernetes users restore by creating a PVC from a `VolumeSnapshot`; the CSI driver maps that to `CreateVolumeFromSnapshot`.

## 4. Enterprise Backup & Remote Disaster Recovery <span class="edition-boundary-inline">Enterprise edition only</span>

The enterprise Backup/DR control plane records state in `sbs-service`; it does not run a background copy scheduler, destructive purge executor, or remote DR automation. Enterprise security controls can protect backup artifacts, but artifact availability still depends on integrity and restore-drill evidence.

Create a backup target and policy:

``` bash
sbsctl backup target create \
  --target-id target-a \
  --type local_filesystem \
  --root /var/lib/namrbd-backup/target-a \
  --capacity-status ok

sbsctl backup policy create \
  --policy-id policy-a \
  --source-volume-id 00000065 \
  --target-id target-a \
  --schedule every:24h \
  --retention-count 2 \
  --retention-age-days 7
```

Record a run and mark the artifact available after restore drill evidence:

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
  --source-volume-id 00000065 \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id> \
  --restore-size 8K \
  --restore-drill-id restore-drill-readback-pass \
  --restore-drill-result kernel_readback_passed_artifact_transition_pending \
  --artifact-integrity-rechecked \
  --userspace-readback-matched \
  --kernel-readback-matched
```

Check status and purge guardrails:

``` bash
sbsctl backup status --policy-id policy-a --output json

sbsctl backup hold create \
  --hold-id hold-a \
  --target-kind artifact \
  --target-id artifact-a

sbsctl backup purge plan \
  --artifact-id artifact-a \
  --output json
```

`artifact_available=true` means the artifact passed integrity recheck, userspace gateway readback, and kernel readback. A purge plan is dry-run only at this stage.

## 5. Enterprise Cryptographic Security & Vault KMS <span class="edition-boundary-inline">Enterprise edition only</span>

Enterprise security and compliance controls wrap already-correct storage paths. Security policy decides whether encrypted data can be accessed and how key, attach, backup, restore, audit, rotation, and crypto erase operations are recorded.

Common inspect commands:

``` bash
sbsctl security provider list --output json
sbsctl security policy list --output json
sbsctl security key list --output json
sbsctl security audit list --output json
sbsctl security crypto-erase list --output json
```

Typical policy setup in an enterprise environment:

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
  --volume-id 00000065 \
  --output json
```

Important semantics:

- Disabled, missing, or destroyed keys fail closed; they must not fall back to plaintext reads or writes.
- Plaintext data keys, raw provider credentials, and payload samples must not be emitted in JSON, logs, or summaries.
- Rotation preserves old-object readability until re-encrypt or crypto erase intentionally changes access.
- Crypto erase removes access through key authority only after holds, protected artifacts, active attachments, and pending operations allow it.
- Live external KMS network/provider destroy evidence is not part of the closed integrated mock provider baseline.

## 6. Basic iSCSI Standard Access <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

NAMRBD includes an optional iSCSI target gateway for standard block initiators. It is a protocol frontend; volume lifecycle, committed metadata, read-view identity, discard/reclaim, and security decisions remain in NAMRBD/SBS authority.

Community edition includes the basic gateway, `sbsctl iscsi`, and LUN export for up to 3 distinct iSCSI-exported volumes. More than 3 exported volumes, unlimited export scale, iSCSI HA, MPIO/ALUA, advanced security/audit operations, and scale-oriented observability are Enterprise-only.

Operators export a prepared LUN with `namrbd-iscsi-gateway`:

``` bash
namrbd-iscsi-gateway \
  --backend=sbs \
  --portal <gateway_ip>:3260 \
  --serve \
  --sbs-endpoint <sbs_volume_service_host>:9460 \
  --volume-id 00000065 \
  --export-id iscsi-00000065 \
  --target-iqn iqn.2026-06.io.namrbd:iscsi.00000065 \
  --active-iscsi-gateway-id gw-iscsi-a \
  --export-lease-id lease-iscsi-00000065 \
  --export-epoch 1 \
  --attachment-id att-iscsi-00000065 \
  --generation 1 \
  --allow-gotgt-wildcard-listen \
  --summary-json ./namrbd-output/gateway-summary.json \
  --operation-jsonl ./namrbd-output/gateway-operations.jsonl \
  --json
```

`--allow-gotgt-wildcard-listen` is required by the current gotgt listener behavior and should be used only in a controlled lab or deployment network.

A Linux user with open-iscsi can discover and log in after the operator confirms the target IQN and portal:

``` bash
sudo systemctl enable --now iscsid
sudo iscsiadm -m discovery -t sendtargets -p <gateway_ip>:3260
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --login
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

The current iSCSI compatibility closure claims Linux open-iscsi only. Windows native initiator has optional memory-backend success and SBS-backed connection/log-cleanup evidence, but not full SBS-backed read/write/readback/flush/cleanup support. macOS support is not claimed.

Cleanup on the initiator:

``` bash
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --logout
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --op delete
```

## 7. Kubernetes User Flow

The current Kubernetes baseline includes:

- replicated and enterprise EC StorageClasses.
- VolumeSnapshotClass.
- block and filesystem PVCs.
- snapshot restore.
- larger restore target and source PVC expansion.
- RWOP conflict smoke.

Typical user objects:

``` yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: namrbd-demo
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: namrbd-replicated
  resources:
    requests:
      storage: 1Gi
```

Snapshot restore follows the standard Kubernetes `VolumeSnapshot` and `dataSource` PVC flow. See the [Installation Guide](installation-guide.md) for install-time manifest rendering and the [Admin Guide](admin-guide.md) for operational checks.

Discard note:

- Default NAMRBD CSI manifests render no filesystem discard mount option.
- Administrators must set `NAMRBD_CSI_ENABLE_DISCARD=1` and provide `NAMRBD_CSI_DISCARD_VALIDATION_PROFILE` before enabling discard exposure.

## 8. Discard And WRITE_ZEROES <span class="edition-boundary-inline">Enterprise edition only true reclaim</span>

Enterprise Discard Reclaim distinguishes:

- `discard`: storage reclaim intent.
- `write zeroes`: logical zero write intent.

True reclaim is advertised only when the operation is aligned to the backend reclaim geometry:

- replicated reclaim-aligned ranges.
- EC full-stripe/page-aligned ranges.

Partial or unaligned filesystem-style discard is accepted as observable `zero_fallback`; it is not advertised as true reclaim.

Validated Enterprise Discard Reclaim gates cover replicated reclaim, EC reclaim, and live kernel discard behavior. Record fresh `blkdiscard` or filesystem `fstrim` evidence for the deployed kernel module before advertising true reclaim support.

## 9. Validation Commands

Use validation commands to prove the exact product path you installed. A public or release-facing validation record should include the build revision, binaries or image tags, restarted services, kernel module version, and the smallest executable gate that exercised the changed behavior.

Required record fields:

| Field | Purpose |
|----|----|
| `ok_count` / `error_count` | Shows whether the smoke or fixture completed successfully. |
| first error / last error | Keeps failures actionable without requiring private logs. |
| deployment state | Records whether gateway, SBS service, SBS data, CSI, iSCSI, or kernel components were rebuilt or restarted. |
| mode evidence | Identifies the enabled backend, edition, StorageClass, reclaim policy, iSCSI portal, or initiator used by the test. |
| readback evidence | Proves snapshot restore, backup artifact, iSCSI LUN, or block-device writes were read back through the intended path. |
| unsupported scope | Lists skipped, future, or Enterprise-only paths without turning them into support claims. |

Recommended gates by feature:

- Community block path: volume create/status, attach/status, filesystem write/readback, detach, and cleanup.
- CSI path: manifest render/lint, Kubernetes apply state, PVC bind, pod write/readback, snapshot restore when enabled, and node readiness.
- iSCSI path: Linux open-iscsi discovery, login, guarded LUN selection, write/readback, flush observation, logout, and target cleanup.
- Discard/reclaim path: replicated reclaim, EC reclaim when Enterprise EC is enabled, kernel `blkdiscard` or filesystem `fstrim`, and alignment evidence.
- Enterprise Backup/DR and security paths: restore-drill readback, artifact availability, hold/purge guardrails, key admission, fail-closed reads, rotation, audit, and crypto erase evidence.

Generated public/community export artifact validation remains a separate release artifact check unless explicitly run and recorded.

## 10. Troubleshooting

### 10.1 Kernel Module Build

Symptoms:

- missing kernel headers.
- `/lib/modules/$(uname -r)/build` does not exist.

Checks:

``` bash
uname -r
ls /lib/modules/$(uname -r)/build
make -C kernel/module
```

### 10.2 Attach Or Device Status

Checks:

``` bash
namrbdctl status --device 0
lsblk /dev/namrbd0
dmesg | tail -n 100
```

Capture attachment id, generation, path plan revision, device size, and runtime path status when reporting an issue.

### 10.3 Gateway Or SBS Authority

Checks:

``` bash
curl -fsS http://gw01:9899/healthz
sbsctl cluster status --output json
sbsctl volume status --volume-id <volume_id> --output json
```

The gateway should use `--sbs-admin-endpoint` to reach `sbs-service`. Raw SBS TiKV metadata flags are legacy/dev bootstrap, not the primary runtime path.

### 10.4 Kubernetes

Checks:

``` bash
kubectl get nodes
kubectl -n namrbd-system get pods
kubectl get pvc,pv,volumesnapshot -A
```

Collect PVC/PV handles, pod events, CSI controller logs, CSI node logs, and the Enterprise Discard Reclaim summary path.

### 10.5 iSCSI

Checks:

``` bash
sbsctl iscsi status gateway --json
sbsctl iscsi status target --target-iqn <target_iqn> --json
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

Collect target IQN, portal, LUN id, initiator IQN/vendor/version, SCSI status/sense, gateway summary JSON, operation JSONL, and whether `iscsi_gateway_restarted=true` for the run.

### 10.6 Smoke Failure

Record:

- failed command.
- summary `result`, `ok_count`, `error_count`, `skipped_count`.
- `first_error` and `last_error`.
- stdout/stderr log paths.
- deploy/reload/restart state.

## 11. FAQ

### Is the legacy metadata CLI still used?

No. It remains archived for historical reference and guardrail scans. Use `namrbdctl`, `sbsctl`, and `namrbd-debug`.

### Does current Kubernetes install enable discard by default?

No. Backend and kernel discard are validated, but CSI manifests keep discard disabled by default. Enable it only with explicit gate evidence.

### Can I change an EC profile online? <span class="edition-boundary-inline">Enterprise edition only EC</span>

No. EC profile/geometry is effectively create-time immutable in the current baseline. Treat profile changes as future controlled migration/repack work.

### Is RWX supported?

Not as a current block-core feature. RWX should be evaluated later as a separate filesystem/share-manager layer.

### What is the encryption/KMS boundary? <span class="edition-boundary-inline">Enterprise edition only</span>

The enterprise security baseline closes integrated mock provider evidence and deployed integrated mock provider follow-up gates. Do not claim live external KMS network/provider destroy, dedupe, or broader kernel readback beyond the recorded gates unless fresh evidence is attached.

### What is the Governance/WORM boundary? <span class="edition-boundary-inline">Enterprise edition only</span>

Governance/WORM is scoped support. The supported scope is block-native derived objects and userspace gateway sealed-target write rejection. It does not claim regulatory certification, S3/Azure API compatibility, ordinary writable live-volume WORM semantics, public governance API/CLI registration, kernel/iSCSI/NVMe protected-state support, ransomware recovery support, or remote DR support.

### Is iSCSI a full non-Linux support claim?

No. The current iSCSI support claim requires Linux open-iscsi evidence. Windows native initiator has optional memory-backend success and SBS-backed connection/log-cleanup evidence, but full SBS-backed Windows I/O and macOS support are future evidence tracks.

## 12. Legacy Notes

The following content is historical or development-only:

- Redis payload backend examples.
- legacy pre-release smoke scripts.
- raw gateway SBS metadata bootstrap flags.
- legacy metadata CLI active workflows.

Do not present these as the standard Enterprise Service user path. Historical references may remain in archived docs and guardrail inventories.

## 13. Offline Copy

Use the browser print or PDF export command on this HTML page when an offline copy is needed. The public Community documentation is published as HTML.

## 14. Related Public Documents

| Document | Purpose |
|----|----|
| [Installation Guide](installation-guide.md) | Community and Enterprise install and bring-up |
| [Admin Guide](admin-guide.md) | Operations, observability, troubleshooting |
| [Interface Specifications](architecture-manual/chapters/appendix-interface-specifications.md) | Gateway, iSCSI, SBS, and operator surface boundaries |
| [Edition Boundaries](architecture-manual/chapters/17-edition-and-release-boundaries.md) | Community and Enterprise product scope, including iSCSI limits |
| [Kubernetes/CSI Case](architecture-manual/chapters/16-kubernetes-csi-integration-case.md) | CSI provisioning, snapshot restore, and Kubernetes usage boundaries |
| [Zero, Discard, And Reclaim](architecture-manual/chapters/12-zero-discard-and-reclaim.md) | Discard, write-zeroes, and reclaim behavior |
| [Read Views, Snapshots, And Clones](architecture-manual/chapters/10-read-views-snapshots-and-clones.md) | Snapshot/clone read-view behavior |

[\<- Architecture Index](architecture-manual/index.md) [Installation Guide -\>](installation-guide.md)
