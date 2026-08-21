Operations Manual

Edition boundary: Community edition install paths and Enterprise edition only sections are both present.

# NAMRBD Installation Guide (Enterprise Service Baseline)

This document details the standard installation, bring-up, and deployment verification paths for NAMRBD Community and Enterprise deployments. Operators should use this as the primary reference for installation, and consult the [Admin Guide](admin-guide.md) for day-2 operations and troubleshooting.

Current install surface:

- `sbs-service`: SBS cluster authoritative metadata, placement, topology, repair/rebalance/drain, volume lifecycle authority.
- `sbs-data`: node-local payload store and replica/EC shard I/O endpoint.
- `namrbd-gateway`: forwards host I/O to SBS targets and records attach/path-plan control-plane metadata in `etcd`.
- `namrbdctl`: host device create/attach/detach/status and volume-facing primary CLI surface.
- `sbsctl`: SBS cluster, node, topology, volume, snapshot/restore, maintenance, guardrail, enterprise backup/security/performance operations CLI.
- `namrbd-debug`: low-level inspect, validation, break-glass diagnostics.
- `namrbd-csi-driver`: Kubernetes CSI Identity/Controller/Node service.
- `namrbd-iscsi-gateway`: Community basic iSCSI target frontend for standard Linux open-iscsi initiators.
- `sbsctl iscsi`: Community iSCSI product control surface for SBS-cluster-backed status/list/get. HA/failover product support is Enterprise-only.

The legacy metadata command surface is archived and is not part of the current installation, build, smoke testing, or daily operations path.

Related Public Documents:

- [Admin Guide](admin-guide.md): Day-2 operations, observability, and troubleshooting.
- [User Manual](user-manual.md): User quick start and command reference.
- [Interface Specifications](architecture-manual/chapters/appendix-interface-specifications.md): gateway, iSCSI, SBS, and operator surface boundaries.
- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md): high availability etcd operations.
- [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md): high availability TiKV/PD operations.

## 1. Prerequisites

Host requirements:

- Linux host with kernel headers/build tree for `namrbd_blk.ko`.
- Go 1.26.4.
- `make`, `curl`, `jq`, `sudo`.
- `etcd` for gateway/control-plane metadata.
- `TiKV/PD` for `sbs-service` authoritative SBS metadata in primary multi-node runtime.
- Kubernetes 1.29+ style cluster and snapshot CRDs when CSI/Kubernetes paths are installed.

Recommended validation/operator variables:

``` bash
export NAMRBD_ETCD_ENDPOINTS="data-01.example.com:2379,data-02.example.com:2379,data-03.example.com:2379"
export NAMRBD_ETCD_ROOT="/namrbd/prod"
export NAMRBD_TIKV_PD_ENDPOINTS="pd01:2379"
export NAMRBD_TIKV_API_VERSION="v1"
export NAMRBD_TIKV_KEYSPACE="namrbd-prod-001"
export NAMRBD_SBS_SERVICE_ENDPOINT="service-01.example.com:9443"
```

Ownership rules:

- Gateway uses `--metadata-backend=etcd` for gateway/control-plane state.
- `sbs-service` owns SBS authoritative metadata through TiKV/PD.
- Gateway talks to SBS authority through `--sbs-service-endpoint`; it does not open raw SBS TiKV metadata in primary runtime.
- Local Pebble SBS metadata and `--sbs-cluster-bootstrap-metadata` are legacy/dev bootstrap paths only.

## 2. Developer Build And Test

Use this section when working from a public source checkout or preparing a code contribution. Operators installing prebuilt packages can continue with [Primary Multi-Node Runtime](#3-primary-multi-node-runtime).

The source tree is organized around these public-facing surfaces:

- `cmd/`: command binaries such as `namrbd-gateway`, `namrbdctl`, `sbs-service`, `sbs-data`, `sbsctl`, `namrbd-csi-driver`, `namrbd-iscsi-gateway`, and `sbsctl iscsi`.
- `internal/` and `sbs/`: gateway, metadata, storage, SBS authority, replication, placement, and Community-safe runtime implementation.
- `kernel/module/`: out-of-tree Linux block device module.
- `deploy/observability/`: public health, metrics, Grafana, alert, and metric catalog assets.
- `docs-src/` and `mkdocs.yml`: public MkDocs documentation source, published
  to <https://nosway.github.io/namrbd/>.

From the repository root, build and test the Community command bundle:

``` bash
make build-community
make test-community
```

For the container quickstart, render the Compose file and run the local SBS smoke. It starts one `sbs-service` and one `sbs-data`, creates a small replicated volume, and verifies `sbsctl` write/read I/O:

``` bash
make quickstart-compose-config
make quickstart-local-sbs-smoke
make quickstart-local-down
```

Validate the public operations and documentation assets:

``` bash
make observability-assets-check
make docs-source-check
```

Build the editable public docs when MkDocs is installed:

``` bash
make docs-build
```

Build the kernel module separately on a Linux host with matching kernel headers:

``` bash
make kernel-module
```

Before proposing a change, run the smallest executable gate that covers the modified path. For broad source or edition-boundary changes, start with:

``` bash
mkdir -p .build-cache/go-build .build-cache/go-mod
GOCACHE=$PWD/.build-cache/go-build GOMODCACHE=$PWD/.build-cache/go-mod go test ./...
make test-community
```

For shell, manifest, or generated-summary changes, run the exact changed path and record the output field that proves the intended mode was active. Do not treat a syntax-only check as a complete validation gate.

## 3. Primary Multi-Node Runtime

The primary runtime separates gateway metadata, SBS authority, and payload I/O:

| Layer | Component | Authority |
|----|----|----|
| Gateway control-plane | `etcd` + `namrbd-gateway` | attach, path-plan, gateway registry |
| SBS metadata authority | `sbs-service` + TiKV/PD | cluster membership, placement, volume/snapshot/restore, EC profile, maintenance |
| Payload datapath | `sbs-data` | local stores, replica chunks, EC shards |
| Host/device | kernel modules + `namrbdctl` | device lifecycle and attach |
| Standard protocol frontend | `namrbd-iscsi-gateway` + `sbsctl iscsi` | iSCSI target sessions, portals, LUN export status, and evidence summaries; storage authority remains SBS-backed |

### 3.1 etcd

Prepare an HA etcd cluster and expose the same endpoints to all gateways:

``` bash
export NAMRBD_ETCD_ENDPOINTS="10.10.0.11:2379,10.10.0.12:2379,10.10.0.13:2379"
export NAMRBD_ETCD_ROOT="/namrbd/prod"
```

All gateways in one environment must use the same `--etcd-endpoints` and `--etcd-root`. Separate dev/stage/prod roots.

### 3.2 TiKV/PD

Prepare TiKV/PD for SBS authoritative metadata:

``` bash
export NAMRBD_TIKV_PD_ENDPOINTS="10.20.0.10:2379"
export NAMRBD_TIKV_API_VERSION="v1"
export NAMRBD_TIKV_KEYSPACE="namrbd-prod-001"
```

All `sbs-service` instances for one SBS cluster must use the same PD endpoint set and keyspace.

### 3.3 `sbs-data`

Start `sbs-data` on each storage node:

``` bash
./sbs-data \
  --node-id data-01 \
  --path /var/lib/namrbd/sbs-data \
  --sbs-data-listen 0.0.0.0:9444 \
  --sbs-data-http-listen 0.0.0.0:9082
```

Health and store checks:

``` bash
curl -fsS http://data-01.example.com:9082/healthz
sbsctl store status --sbs-service-http-endpoint http://data-01.example.com:9082
```

For multi-store nodes, pass explicit store/shard definitions and keep store IDs stable across restarts.

### 3.4 `sbs-service`

Start `sbs-service` on service nodes. Example for `service-01`:

``` bash
./sbs-service \
  --cluster-id namrbd-prod \
  --sbs-cluster-id sbs-prod-9n \
  --node-id service-01 \
  --metadata-backend tikv \
  --tikv-pd-endpoints "$NAMRBD_TIKV_PD_ENDPOINTS" \
  --tikv-api-version "$NAMRBD_TIKV_API_VERSION" \
  --tikv-keyspace "$NAMRBD_TIKV_KEYSPACE" \
  --sbs-service-listen 0.0.0.0:9443 \
  --sbs-service-http-listen 0.0.0.0:9081
```

Set an admin endpoint for operator commands:

``` bash
export NAMRBD_SBS_SERVICE_ENDPOINTS="service-01.example.com:9443"
curl -fsS http://service-01.example.com:9081/healthz
```

Initialize cluster authority and declare topology:

``` bash
sbsctl cluster init
sbsctl topology zone create --zone zone-a
sbsctl topology zone create --zone zone-b
sbsctl topology zone create --zone zone-c
sbsctl cluster status --output json
```

Join storage nodes:

``` bash
sbsctl node join --node-id data-01 --grpc-endpoint data-01.example.com:9444 --sbs-service-http-endpoint http://data-01.example.com:9082 --zone zone-a
sbsctl node join --node-id data-02 --grpc-endpoint data-02.example.com:9444 --sbs-service-http-endpoint http://data-02.example.com:9082 --zone zone-a
sbsctl node join --node-id data-03 --grpc-endpoint data-03.example.com:9444 --sbs-service-http-endpoint http://data-03.example.com:9082 --zone zone-a
sbsctl node join --node-id data-04 --grpc-endpoint data-04.example.com:9444 --sbs-service-http-endpoint http://data-04.example.com:9082 --zone zone-b
sbsctl node join --node-id data-05 --grpc-endpoint data-05.example.com:9444 --sbs-service-http-endpoint http://data-05.example.com:9082 --zone zone-b
sbsctl node join --node-id data-06 --grpc-endpoint data-06.example.com:9444 --sbs-service-http-endpoint http://data-06.example.com:9082 --zone zone-b
sbsctl node join --node-id data-07 --grpc-endpoint data-07.example.com:9444 --sbs-service-http-endpoint http://data-07.example.com:9082 --zone zone-c
sbsctl node join --node-id data-08 --grpc-endpoint data-08.example.com:9444 --sbs-service-http-endpoint http://data-08.example.com:9082 --zone zone-c
sbsctl node join --node-id data-09 --grpc-endpoint data-09.example.com:9444 --sbs-service-http-endpoint http://data-09.example.com:9082 --zone zone-c
```

Confirm:

``` bash
sbsctl cluster status --output json
sbsctl node status --node-id data-01 --output json
```

### 3.5 Gateway

Start each gateway with `etcd` plus the `sbs-service` admin endpoint:

``` bash
./namrbd-gateway \
  --gateway-id gw-gw01 \
  --control-http-listen 0.0.0.0:9899 \
  --data-listen 0.0.0.0:9898 \
  --advertise-control-address 10.30.0.11 \
  --advertise-data-address 10.30.0.11 \
  --metadata-backend etcd \
  --etcd-endpoints "$NAMRBD_ETCD_ENDPOINTS" \
  --etcd-root "$NAMRBD_ETCD_ROOT" \
  --volume-cache-ttl 30s \
  --data-backend-mode sbs \
  --sbs-service-endpoint "$NAMRBD_SBS_SERVICE_ENDPOINT"
```

Additional gateways use unique `--gateway-id` and advertised addresses while sharing the same `etcd` root and SBS admin authority.

### 3.6 Host Device

Build and load kernel modules on attach hosts:

``` bash
sudo insmod kernel/module/namrbd_blk.ko no_path_retry=fail
sudo insmod kernel/module/namrbd_ctrl.ko
```

`no_path_retry` may be `fail`, `queue`, or a seconds value depending on the availability policy. Enterprise Service inherits the current kernel discard/WRITE_ZEROES, security admission, and gateway v1 dataplane framing baseline.

Create a device and configure gateway endpoints:

``` bash
namrbdctl create-device
namrbdctl config-rest --device 0 --server "1,gw01,9899,false,/api/v1"
```

### 3.7 Volume Create And Attach

Create a replicated volume:

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

Attach from a host:

``` bash
namrbdctl attach \
  --device 0 \
  --host host-a \
  --volume 00000065 \
  --gateway http://gw01:9899

namrbdctl status --device 0
lsblk /dev/namrbd0
```

Detach and cleanup:

``` bash
namrbdctl detach --device 0 --host host-a --volume 00000065
namrbdctl destroy-device --device 0
sudo rmmod namrbd_ctrl
sudo rmmod namrbd_blk
```

### 3.8 Basic iSCSI Standard Initiator Access <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

NAMRBD includes an optional iSCSI target gateway beside the Linux kernel-module path. The iSCSI gateway is a protocol frontend: it owns target sessions and SCSI/iSCSI status mapping, but it must not become SBS metadata, placement, repair, security, discard, or lifecycle authority.

Community edition includes `namrbd-iscsi-gateway`, `sbsctl iscsi`, and basic LUN export for up to 3 distinct iSCSI-exported volumes. More than 3 exported volumes, unlimited export scale, iSCSI HA, MPIO/ALUA, advanced security/audit operations, and scale-oriented observability are Enterprise-only.

Controlled validation start example for an SBS-backed LUN:

``` bash
export NAMRBD_ISCSI_PORTAL="10.30.0.21:3260"
export NAMRBD_ISCSI_TARGET_IQN="iqn.2026-06.io.namrbd:iscsi.00000065"

./namrbd-iscsi-gateway \
  --backend=sbs \
  --portal "$NAMRBD_ISCSI_PORTAL" \
  --serve \
  --sbs-endpoint data-01.example.com:9444 \
  --volume-id 00000065 \
  --export-id iscsi-00000065 \
  --target-iqn "$NAMRBD_ISCSI_TARGET_IQN" \
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

`--allow-gotgt-wildcard-listen` is required by the current gotgt v0.2.2 listener behavior and should be limited to isolated fixtures or controlled validation networks. Do not treat it as a source-IP ACL.

Production bring-up checklist:

- Use a dedicated portal address and firewall policy for TCP/3260; keep initiator access control outside the wildcard listener flag.
- Run `namrbd-iscsi-gateway` under a service manager and restart it after binary, portal, target, export, or SBS endpoint changes.
- Record the target IQN, export id, active gateway id, export lease id, attachment id, generation, summary JSON path, and operation JSONL path for each exported LUN.
- Keep Community deployments within 3 distinct iSCSI-exported volumes. Plan an Enterprise deployment for larger export scale, HA, MPIO/ALUA, advanced security/audit, or scale observability.
- Validate each deployed build from a Linux open-iscsi initiator with discovery, login, guarded LUN selection, write/readback, flush observation, logout, and cleanup.

On a Linux initiator with open-iscsi installed:

``` bash
sudo systemctl enable --now iscsid
sudo iscsiadm -m discovery -t sendtargets -p "$NAMRBD_ISCSI_PORTAL"
sudo iscsiadm -m node -T "$NAMRBD_ISCSI_TARGET_IQN" -p "$NAMRBD_ISCSI_PORTAL" --login
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

The current iSCSI compatibility closure claims Linux open-iscsi compatibility only. Windows native initiator has post-closure memory-backend success evidence and SBS-backed connection/log-cleanup evidence, but SBS-backed Windows read/write/readback/flush/cleanup remains a future compatibility track. macOS support is excluded until a licensed initiator validation environment exists.

## 4. CSI/Kubernetes Install

Community includes the current CSI baseline: CSI Identity, Controller, and Node services plus Kubernetes manifests for namespace, RBAC, CSIDriver, controller Deployment, node DaemonSet, replicated StorageClass, and VolumeSnapshotClass. EC StorageClass usage remains Enterprise-only.

Prerequisites:

- Snapshot CRDs and snapshot controller are installed.
- `namrbd-csi-driver` image is built and available to the cluster.
- Controller can reach the configured gateway and SBS admin endpoints.
- Nodes have current kernel modules loaded.

Render and lint manifests, then apply and run the Kubernetes CSI e2e smoke in a prepared validation environment. Record the manifest rendering result, node readiness, object application result, and smoke `ok_count`/`error_count`.

When discard exposure is enabled, record a fresh required-mode Kubernetes smoke result for the exact deployed image and kernel module. The record should include `ok_count`, `error_count`, first error, last error, node readiness, and whether manifests were rendered, applied, or only linted.

### 4.1 Discard Exposure Gate <span class="edition-boundary-inline">Enterprise edition only reclaim validation</span>

Kubernetes discard exposure is disabled by default. Default manifests render `mountOptions: []` and annotate the exposure state. Do not add filesystem `discard` mount options unless both of these are true:

``` bash
export NAMRBD_CSI_ENABLE_DISCARD=1
export NAMRBD_CSI_DISCARD_VALIDATION_PROFILE="<current kernel discard or validation evidence id>"
```

Enterprise Discard Reclaim validates true discard through backend and kernel smokes, but default Kubernetes manifests remain conservative.

## 5. Snapshot, Restore, Expansion, And EC <span class="edition-boundary-inline">Includes Enterprise edition only EC</span>

Enterprise Service inherits the current snapshot, EC, CSI, and discard/reclaim product semantics:

- `sbsctl volume restore-from-snapshot` is the operator-facing restore command.
- CSI `CreateVolume` with `VolumeContentSource.snapshot` maps to the backend `CreateVolumeFromSnapshot` wrapper.
- Restored volumes are ordinary attachable volumes and support grow-only expansion.
- EC is an enterprise capability. EC profile and geometry are create-time immutable; profile migration is a future controlled migration/repack topic.
- RWX is not a current block-core feature.

Use the architecture manual for detailed snapshot, clone, restore, EC, rebuild, scrub, and GC semantics.

## 6. Install Verification

Run the smallest validation gate for the path you installed. At minimum, record syntax/edition-boundary checks, CSI sanity, Kubernetes manifest validation, replicated and EC discard/reclaim evidence, kernel discard evidence, performance/security validation evidence when those features are enabled, and basic iSCSI fixture evidence when the iSCSI gateway is installed.

For public or release-facing validation, record evidence as product facts rather than private environment facts:

- the exact git revision, build artifact, image tag, kernel module version, and restarted processes;
- `ok_count`, `error_count`, first error, last error, and skipped gate count;
- CSI smoke and manifest application state when Kubernetes is installed;
- replicated and EC discard/reclaim evidence only when the relevant edition and backend are enabled;
- Linux open-iscsi discovery, login, guarded LUN selection, readback, flush, logout, and cleanup when the iSCSI gateway is installed;
- unsupported or future compatibility tracks, such as macOS initiator validation or full SBS-backed Windows I/O, as exclusions rather than support claims.

Generated public/community export artifact validation remains a separate release artifact check unless it is explicitly run and recorded.

## 7. Legacy/Dev Bootstrap

The following surfaces are not primary Enterprise Service runtime:

- legacy metadata CLI active command paths.
- Redis payload backend.
- Gateway raw SBS metadata flags such as `--sbs-cluster-metadata-path`, `--sbs-cluster-metadata-backend`, `--tikv-pd-endpoints`, and `--sbs-cluster-bootstrap-metadata` when used directly by gateway.
- Local Pebble SBS metadata for multi-node production.

These may remain useful for historical tests, isolated development, or archived references, but current install guides should not present them as the standard operator path.

[\<- Architecture Index](architecture-manual/index.md) [Admin Guide -\>](admin-guide.md)
