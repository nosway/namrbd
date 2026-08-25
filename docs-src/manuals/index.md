Distributed Block Storage Docs

# NAMRBD Distributed Block Storage

<div class="summary" markdown="1">

NAMRBD (Network Attached Multipath Resilient Block Device) is an open-source distributed block storage platform with native Linux block-device paths, Kubernetes CSI integration, and an optional standard iSCSI target gateway.

The public source provides the replicated storage platform and its main host, gateway, SBS, CSI, iSCSI, and operations surfaces. Source availability and v1.0 support validation are distinct; see the [Feature Status](../feature-status.md) page before choosing a deployment shape. Advanced Enterprise capabilities are under development and validation and are not general-availability commitments.

</div>

## 1. Product Positioning

NAMRBD is a low-latency distributed block storage system sharing the SBS storage substrate with the NAMROS object storage engine. Unlike NAMROS, which is optimized for standard S3 API workflows, NAMRBD provisions raw blocks directly via out-of-tree kernel modules (mounting as `/dev/namrbdX`) or via the native Kubernetes CSI provider to act as ultra-resilient container storage volumes.

## 2. Deployment And Evaluation Shapes

| Shape | Purpose | Primary dependencies | Current status |
|----|----|----|----|
| Local single-node quickstart | Developer evaluation and smoke validation | `namrbd-gateway`, `sbs-service`, `sbs-data`, local metadata | Public development workflow |
| Replicated userspace gateway | Replicated block-volume service | SBS cluster, metadata authority, `namrbd-gateway` | Validated v1.0 volume path |
| Kubernetes CSI cluster | Dynamic persistent volume provisioning | SBS cluster, `sbsctl`, `namrbd-csi-driver` | Public integration preview; not validated for v1.0 support |
| Basic iSCSI target access | Linux open-iscsi LUN export through a single target path | `namrbd-iscsi-gateway`, `sbsctl iscsi`, TCP/3260 | Public integration preview, capped at three distinct exported volumes |
| Linux kernel block path | Native `/dev/namrbdX` attachment | Matching kernel headers, kernel module, gateways | Source available; kernel I/O is outside the v1.0 support boundary |

## 3. Advanced Features Under Development

NAMRBD is developing and validating erasure-coded storage, automated backup
and recovery, security/KMS and governance workflows, performance/QoS controls,
advanced iSCSI HA and scale, remote replication/DR, data mobility/repack, and
deduplication for the Enterprise edition. These are development directions,
not general-availability, compatibility, performance, or support commitments.
See [Feature Status](../feature-status.md) for concise capability descriptions
and current limitations.

The [Community and Enterprise Edition Boundary](edition-boundary.md) defines
the required `[Enterprise Edition Only]` label and the fail-closed CLI/API
behavior when an advanced capability is unavailable. Canonical maintainers
also maintain seven Enterprise operations manuals covering the eight feature
families listed above. Those manuals are excluded from the Community source
export. Every page is marked Enterprise-only and describes work under
development and validation, not a published support promise.

## 4. Persona-Based Navigation

Select the optimal reader path based on your operational goals and responsibilities.

<div class="cards" markdown="1">

<div id="developer-path" class="section card" markdown="1">

### GitHub Developer

Source, Build & Contribution Path

Start with the public source tree, build the command binaries, run the public validation checks, and use the architecture manual to understand the storage contracts before changing code.

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

Compile out-of-tree Linux modules using DKMS automation, manage etcd HA clusters, configure basic iSCSI target access, review its current validation boundary, and operate SBS volumes.

<a href="admin-guide.md" class="btn">Open Admin Guide →</a>

</div>

</div>

## 5. Operations References

- [Troubleshooting and FAQ](troubleshooting-and-faq.md): incident safety,
  evidence collection, suspected split-brain, metadata quorum loss, CSI mount
  failure, and iSCSI disconnect runbooks.
- [OS, Kernel, and Kubernetes Compatibility Matrix](compatibility-matrix.md):
  current support, build-only, unvalidated, and integration-preview boundaries.
- [Administrator Guide](admin-guide.md): ownership, observability, maintenance,
  and release guardrails.
- [Developer Onboarding](developer-onboarding.md): codebase ownership, package
  tests, local validation, Delve, and contribution boundaries.
- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md) and
  [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md): metadata backend
  topology and member operations.
