Compatibility Reference

# OS, Kernel, and Kubernetes Compatibility Matrix

This page separates source availability, a compile boundary, integration
validation, and public v1.0 support. A component appearing in the repository or
building successfully does not by itself make every operating-system, kernel,
or Kubernetes combination supported.

The exact release tag or commit and its published evidence take precedence
over this summary. See [Feature Status](../feature-status.md) for feature-level
status and [Troubleshooting and FAQ](troubleshooting-and-faq.md) for incident
response.

## 1. Status Vocabulary

| Status | Meaning |
| --- | --- |
| **Supported** | The exact feature path has named release evidence and documented limits. |
| **Unvalidated** | The implementation exists, but evidence for the exact platform/version combination is missing. |
| **Build-only** | Source compilation is checked; runtime I/O and recovery behavior are not established. |
| **Integration preview** | Assets and workflows are public for evaluation, but not part of the v1.0 support claim. |
| **Unsupported** | The combination is outside the declared implementation or compatibility boundary. |

## 2. Operating System Matrix

| Platform or distribution | Surface | Current status | Requirements and limits |
| --- | --- | --- | --- |
| Linux userspace | Replicated `namrbd-gateway` plus SBS volume path | **Supported feature path**; distribution/version matrix **unvalidated** | This is the current v1.0 volume claim. The release evidence is for the replicated userspace path, not every Linux distribution. |
| GitHub-hosted `ubuntu-latest` | Community binaries, tests, docs, Helm rendering, kernel module compilation | CI-validated at each commit; not a stable distro support range | The runner image changes over time. Use the exact workflow log to identify its Ubuntu and kernel versions. Kernel CI is compile-only. |
| Ubuntu 20.04+ LTS | etcd or TiKV/PD host examples | Deployment-guide baseline, not a blanket NAMRBD kernel qualification | Follow the backend vendor lifecycle and security policy. Matching NAMRBD runtime evidence is still required. |
| RHEL 8+ / Rocky Linux 8+ | TiKV/PD host examples | Deployment-guide baseline, not a blanket NAMRBD kernel qualification | The backend guide describes host preparation; it does not validate every NAMRBD kernel and CSI combination. |
| Other Linux distributions | Userspace source build | **Unvalidated** | Go and runtime dependencies may work, but qualify the exact distro, architecture, service manager, filesystem utilities, and network policy before use. |
| Windows | Native NAMRBD kernel/CSI node | **Unsupported** | The out-of-tree block modules and CSI node path are Linux-specific. Windows native iSCSI has incomplete evidence and is not a current support claim. |
| macOS | Native NAMRBD kernel/CSI node or iSCSI initiator | **Unsupported** | macOS can be used as a Docker-based development host where Docker supports the workflow; it is not a native data-path target or a validated iSCSI initiator. |

The etcd guide also contains a historical CentOS host example. Treat it as an
installation example rather than a current operating-system support promise;
an end-of-life distribution must not be selected solely because it appears in
that example.

## 3. Linux Kernel Matrix

| Kernel range or condition | Status | Notes |
| --- | --- | --- |
| Linux 5.10 or later | **Unvalidated runtime; provisional compile boundary** | The source declares 5.10 as the minimum and no upper bound. Exact tested kernel versions have not been published as a supported range. |
| Current `ubuntu-latest` workflow kernel | **Build-only** | CI installs headers matching `uname -r` and runs `make kernel-module`; it does not prove attach, I/O, failover, or recovery. |
| Linux earlier than 5.10 | **Unsupported** | Outside the declared compile boundary. |
| Kernel without matching headers/build tree | **Unsupported build environment** | `/lib/modules/$(uname -r)/build` must resolve to headers/build output matching the running kernel. |
| Secure Boot or locked-down kernel | **Unvalidated** | Module signing, enrollment, and local kernel policy are operator responsibilities; the public workflow does not qualify them. |

The shipped module version must match the NAMRBD release version. Before
loading a module, record:

```bash
uname -r
test -e "/lib/modules/$(uname -r)/build"
make kernel-module
modinfo kernel/module/namrbd_ctrl.ko
modinfo kernel/module/namrbd_blk.ko
```

A build result alone must be labeled build-only. Runtime qualification requires
module load/unload, attach/detach, size and generation checks, write, flush,
readback, surviving-path failover, cleanup, and kernel logs for each exact
kernel version.

## 4. Kubernetes and CSI Matrix

| Kubernetes/CSI combination | Current status | Scope and limits |
| --- | --- | --- |
| Kubernetes 1.29+ style APIs on Linux nodes | **Integration preview / unvalidated** | The installation guide uses this as the design baseline, but no exact supported minor-version range is frozen. |
| Public kind CSI PVC demo | Development validation only | Creates a fresh kind cluster and proves controller provisioning/PVC binding. The node is disabled; it does not prove mount or workload I/O. The kind node image is not pinned as a compatibility claim. |
| Dynamic provisioning, attach, and node mount | **Integration preview / unvalidated** | Controller and node services exist. Exact Kubernetes, kubelet, kernel, and sidecar combinations require qualification. |
| `VolumeSnapshot` and restore | **Integration preview / unvalidated** | Snapshot CRDs and an external snapshot controller are required. Backend read-view correctness does not by itself validate Kubernetes integration. |
| Online expansion | **Integration preview / unvalidated** | Requires controller expansion, node/device size reload, and filesystem growth evidence for the exact environment. |
| RWOP fencing | **Integration preview / unvalidated** | The implementation contains conflict validation, but the platform/version combination must be tested. RWX is not a current block-core support claim. |
| EC StorageClass | Enterprise development | Disabled by default in the Community Helm values and not a Community support claim. |
| Kubernetes earlier than 1.29 | **Unvalidated** | No compatibility promise is published. API availability and the selected CSI sidecars must be reviewed before testing. |

### Shipped Helm sidecar defaults

These are deployment defaults, not a declaration that every Kubernetes release
supports the combination:

| Component | Default image version |
| --- | --- |
| `csi-provisioner` | `v5.2.0` |
| `csi-attacher` | `v4.8.1` |
| `csi-snapshotter` | `v8.2.0` |
| `csi-resizer` | `v1.13.2` |
| `csi-node-driver-registrar` | `v2.13.0` |
| `livenessprobe` | `v2.15.0` |

Use the versions in the Helm chart values for the exact checkout being
deployed. If a sidecar is overridden, record the override as a new
qualification combination.

## 5. iSCSI Initiator Compatibility

| Initiator | Current status | Notes |
| --- | --- | --- |
| Linux open-iscsi | **Integration preview / unvalidated for v1.0 support** | It is the required compatibility baseline for current validation. Record discovery, login, guarded LUN selection, write/readback, flush, logout, and cleanup. |
| Windows native initiator | **Unvalidated** | Connection and limited backend evidence exist, but full SBS-backed write/readback/flush/cleanup is not a support claim. |
| macOS initiator | **Unsupported** | Excluded until an approved initiator and complete validation environment are available. |
| MPIO/ALUA or automatic target failover | Enterprise development | Do not infer this behavior from basic single-target access. |

## 6. Metadata Backend Compatibility

| Backend | Baseline | Status and limits |
| --- | --- | --- |
| Local Compose etcd | etcd `v3.6.8` image | Development/evaluation topology; no host install required. |
| External etcd | etcd v3-compatible, three members recommended | Required for gateway/control-plane HA. The HA guide's `v3.5.9` is an installation example, not the complete support range. |
| Local SBS metadata | Embedded Pebble | Development/evaluation topology; no TiKV/PD required. |
| Primary multi-node SBS metadata | Three PD members and at least three TiKV stores, RF=3 baseline | `sbs-service` uses PD endpoints and its own keyspace. The TiKV guide's `v6.5.2` is an installation example, not the complete support range. |

etcd and PD are different authorities even when both use TCP `2379`. Never put
their addresses in the same endpoint list.

## 7. Qualification Checklist for a New Combination

Record the exact operating system, architecture, kernel, Kubernetes, kubelet,
container runtime, Helm chart, CSI sidecars, NAMRBD commit/image, etcd, PD/TiKV,
and restart/reload state.

### Linux userspace

- Build and package the exact commit.
- Verify gateway, `sbs-service`, and `sbs-data` readiness.
- Exercise create, write, flush, readback, expansion, and cleanup.
- Record `ok_count`, `error_count`, first error, last error, and backend mode.

### Linux kernel

- Build against headers matching the running kernel.
- Verify module version, load, attach identity/generation/path plan, and size.
- Exercise write, flush, readback, multipath failover, detach, unload, and
  kernel-log cleanup.

### Kubernetes

- Render and lint the Helm chart for the exact values.
- Verify controller and node registration/readiness on every selected node.
- Exercise PVC bind, workload mount, write/flush/readback, delete/reclaim.
- Separately test snapshot/restore, expansion, RWOP conflict, and any enabled
  discard behavior.
- Record events and controller/node logs even for a passing run.

A version row should move from **unvalidated** to **supported** only when the
release evidence names that exact combination, known limits, rollback or
disable path, and cleanup result.

## 8. Related Documents

- [Feature Status](../feature-status.md)
- [Installation Guide](installation-guide.md)
- [Administrator Guide](admin-guide.md)
- [Troubleshooting and FAQ](troubleshooting-and-faq.md)
- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md)
- [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md)

