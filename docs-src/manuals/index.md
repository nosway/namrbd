Distributed Block Storage Docs

Edition boundary: Community edition entry points and Enterprise edition only capability summaries are both present.

# NAMRBD Distributed Block Storage

<div class="summary" markdown="1">

NAMRBD (Network Attached Multipath Resilient Block Device) is a distributed block storage system with native Linux block-device paths, Kubernetes CSI integration, and an optional standard iSCSI target gateway.

The Community edition includes standard single-path block attachments, Kubernetes CSI integration, basic manual snapshot routines, `namrbd-iscsi-gateway`, `sbsctl iscsi`, and basic LUN export for up to 3 distinct iSCSI-exported volumes. The Enterprise edition adds remote DR orchestration and replication automation, dynamic QoS rate limiting, Vault KMS integration, iSCSI HA/MPIO/ALUA, advanced security/audit, and scale-oriented observability surfaces.

</div>

## 1. Product Positioning

NAMRBD is a low-latency distributed block storage system sharing the SBS storage substrate with the NAMROS object storage engine. Unlike NAMROS, which is optimized for standard S3 API workflows, NAMRBD provisions raw blocks directly via out-of-tree kernel modules (mounting as `/dev/namrbdX`) or via the native Kubernetes CSI provider to act as ultra-resilient container storage volumes.

## 2. Supported Deployment Shapes

| Shape | Purpose | Primary Dependencies | Edition Scope |
|----|----|----|----|
| Local Small Lab | Developer virtual testing and basic harness smoke | Single `namrbd-gateway`, local memory/Pebble metadata store | <span class="badge">Community</span> |
| Kubernetes CSI Cluster | Dynamic persistent volume provisioning | TiKV distributed metadata, sbsctl, `namrbd-csi-driver` | <span class="badge">Community</span> |
| Basic iSCSI Target Access | Standard Linux open-iscsi LUN export through a single target path | `namrbd-iscsi-gateway`, `sbsctl iscsi`, TCP/3260 | <span class="badge">Community</span> capped at 3 distinct exported volumes |
| Enterprise iSCSI HA/Scale | High-scale redundant target gateway integrations | etcd, `namrbd-iscsi-gateway`, host multipath tooling | <span class="badge enterprise">Enterprise</span> |
| Remote DR Automation | Cross-region replication, failover planning, and recovery orchestration | Remote gateways, policy automation, SBS-EC storage nodes | <span class="badge enterprise">Enterprise</span> |

## 3. Community And Enterprise Capabilities

| Capability | Community Edition | Enterprise Edition |
|----|----|----|
| Block Volume & Mount (`namrbdctl`) | Included (local dev only) | Included (optimized kernel pathing) |
| Manual Snapshots & Rollbacks | Included | Included |
| Kubernetes CSI Provisioning | Basic cluster driver | Advanced StorageClass driver with dynamic QoS metadata |
| Remote DR & Policy Automation | Not available | <span class="badge enterprise">Enterprise</span> Remote replication, failover workflow, and recovery policy automation |
| KMS integration & Payload Encryption | Not available | <span class="badge enterprise">Enterprise</span> Vault KMS with hardware fail-closed posture |
| Basic iSCSI Target Access | Included: `namrbd-iscsi-gateway`, `sbsctl iscsi`, basic LUN export, max 3 distinct exported volumes | Included with larger export scale |
| iSCSI HA / MPIO / ALUA / Scale Operations | Not available | <span class="badge enterprise">Enterprise</span> HA, MPIO/ALUA, advanced security/audit, and scale observability |

## 4. Persona-Based Navigation

Select the optimal reader path based on your operational goals and responsibilities.

<div class="cards" markdown="1">

<div id="developer-path" class="section card" markdown="1">

### GitHub Developer

Source, Build & Contribution Path

Start with the Community source tree, build the command binaries, run the edition-boundary checks, and use the architecture manual to understand the storage contracts before changing code.

<a href="installation-guide.md#2-developer-build-and-test" class="btn">Open Developer Build Path →</a>

</div>

<div class="section card" markdown="1">

### Kubernetes Operator

Container Dev & Deployment Path

Deploy the NAMRBD CSI driver, fine-tune StorageClass metrics, and master persistent volume reclaiming via Discard/WRITE_ZEROES and VolumeSnapshot restore YAML templates.

<a href="user-manual.md" class="btn">Open User Manual →</a>

</div>

<div class="section card" markdown="1">

### Storage Infra Administrator

Cluster Platform Engineer Path

Compile out-of-tree linux modules using DKMS automation, manage etcd HA clusters, configure basic iSCSI target access, review Enterprise-only iSCSI HA boundaries, and orchestrate sbsctl volume healing.

<a href="admin-guide.md" class="btn">Open Admin Guide →</a>

</div>

</div>
