Operations Runbook

# Troubleshooting and FAQ

This guide provides first-response procedures for NAMRBD operational failures.
It uses a **Symptom - Cause - Resolution - Verification** structure and keeps
data-preserving, read-only inspection ahead of mutation.

The replicated userspace gateway path is the current validated v1.0 volume
path. Kernel, Kubernetes CSI, and iSCSI paths are available in public source but
have narrower validation boundaries. Check the
[Compatibility Matrix](compatibility-matrix.md) and
[Feature Status](../feature-status.md) before treating a recovery result as a
support claim.

## 1. Incident Safety Rules

Before changing cluster state:

1. Stop or pause application writes when writer identity, metadata authority,
   or quorum is uncertain.
2. Prefer read-only status, health, logs, and metrics collection.
3. Record the exact commit or tag, binary and image versions, configuration
   profile, and restart or reload state.
4. Record the first error and last error. Later retries often hide the original
   failure.
5. Do not delete a PVC/PV, clear an attachment, force a generation, remove a
   metadata member, or force-disconnect an active session merely to make an
   error disappear.
6. Preserve snapshots, protected references, gateway summaries, and operation
   records until data correctness is confirmed.
7. Redact credentials, bearer tokens, TLS private keys, CHAP secrets, and
   payload data before sharing an evidence bundle.

!!! danger "Quorum recovery is not a routine restart"

    Never form two independent metadata clusters from members that may later
    reconnect. Do not use etcd force-new-cluster, PD force recovery, or manual
    key edits as a first response. If a majority cannot be restored, use a
    reviewed backup-restore procedure in an isolated environment and obtain
    storage-administrator approval.

## 2. Common Triage and Evidence Bundle

Classify the failed layer before attempting recovery:

| Layer | Primary evidence |
| --- | --- |
| Client or workload | Exact command, exit status, first/last error, readback result |
| Linux host/kernel | `namrbdctl status`, `lsblk`, `uname -r`, loaded modules, `dmesg` |
| Kubernetes CSI | PVC/PV/pod state, events, controller/node logs, scheduled node |
| Gateway | `/healthz`, `/readyz`, metrics, attachment/generation/path-plan state |
| `sbs-service` | cluster and volume status, source authority, placement/transition state |
| `sbs-data` | readiness, store availability, local I/O errors and capacity |
| etcd | endpoint health/status, member list, alarms, configured root |
| PD/TiKV | PD member/store state, TiKV store health, configured keyspace |
| iSCSI | portal/IQN/LUN identity, initiator session, target session and operation records |

Start with the public health surfaces:

```bash
curl -fsS "$GATEWAY_URL/healthz"
curl -fsS "$GATEWAY_URL/readyz"
curl -fsS "$SBS_SERVICE_HTTP_ENDPOINT/healthz"
curl -fsS "$SBS_SERVICE_HTTP_ENDPOINT/readyz"
curl -fsS "$SBS_DATA_HTTP_ENDPOINT/healthz"
curl -fsS "$SBS_DATA_HTTP_ENDPOINT/readyz"
sbsctl cluster status --output json
```

For a host-attached volume, collect the identifiers together:

```bash
namrbdctl status --device "$DEVICE_ID"
sbsctl volume status --volume-id "$VOLUME_ID" --output json
lsblk "/dev/namrbd${DEVICE_ID}"
dmesg | tail -n 200
```

Do not interpret a single green health endpoint as full recovery. A useful
incident record includes `result`, `ok_count`, `error_count`, first error, last
error, attachment id, generation, path-plan revision, volume size, runtime path
status, and every process that was restarted or left unchanged.

## 3. Suspected Split-Brain or Multiple Writers

NAMRBD uses attachment identity, generation, path-plan revision, and fencing to
prevent stale writers. “Split-brain” in this runbook means a **suspected
multiple-writer condition** or conflicting authority observation; it is not an
expected steady state.

### Symptoms

- Two hosts or gateways appear to own write authority for one volume.
- `namrbdctl` and gateway status disagree on attachment id, generation, or
  path-plan revision.
- Repeated stale-writer, fencing, generation, or ownership errors occur.
- Reads differ by path after acknowledged writes, or I/O succeeds through a
  path that should have been retired.
- A gateway reconnects with an older control-plane view after an outage.

### Likely causes

- A stale gateway or host continued running after attachment authority changed.
- Gateways use different etcd endpoint sets or `etcd-root` values.
- Services point to different SBS authority endpoint sets or keyspaces.
- A network partition separated the writer from metadata authority.
- A forced detach, generation change, or manual metadata edit bypassed normal
  fencing.
- Ordinary retryable same-volume metadata contention was misclassified as a
  stale-writer failure. These are different failure classes.

### Resolution

1. Pause writes on every affected host. If that is not possible, isolate the
   suspected stale host or gateway without modifying metadata.
2. Capture `namrbdctl status`, volume status, gateway readiness/metrics, and
   recent kernel and gateway logs from every path.
3. Compare attachment id, generation, path-plan revision, device size, and
   runtime path state. Do not compare only a hostname or process id.
4. Confirm that all gateways use the same etcd endpoint set and `etcd-root`,
   and that all `sbs-service` clients use the same PD endpoint set and keyspace.
5. Restore metadata quorum first if etcd or PD/TiKV is unhealthy.
6. Select the current writer only from authoritative attachment/generation
   state. Stop the stale process through its service manager or orchestrator.
7. Use the documented detach/attach workflow only after the old writer is
   fenced and no application can issue I/O through it. Do not clear attachment
   or generation records by hand.
8. If acknowledged data differs between paths, keep writes stopped, preserve
   all snapshots and evidence, and escalate for data-consistency review.

### Verification

- Exactly one current attachment/generation owns write authority.
- Every active gateway reports the same path-plan revision and volume size.
- The retired path rejects I/O or is absent from the active path plan.
- Write, flush, and readback succeed through the surviving path with no
  stale-writer or fencing errors.
- A second gateway test, when used, proves both request distribution and equal
  readback rather than only process availability.

## 4. etcd Quorum Loss

etcd owns gateway/control-plane metadata. In a three-member cluster, one member
may fail while quorum remains; losing two members removes quorum.

### Symptoms

- New attach, detach, or control-plane changes fail closed or time out.
- Gateways report etcd deadline, leader, lease, or watch errors.
- `endpoint health` succeeds for fewer than a majority of members.
- Existing data-path behavior may differ from new control-plane operations;
  do not assume a successful read proves metadata health.

### Likely causes

- Two members are stopped or unreachable.
- Peer port `2380` or client port `2379` is blocked.
- Disk latency, full disk, quota alarms, or WAL/database corruption prevents
  progress.
- Time synchronization or certificate validity disrupts peer communication.
- Clients use an incomplete endpoint set or the wrong environment root.

### Diagnosis

Use the same TLS flags and endpoint list as the gateways. Do not print private
key contents.

```bash
export ETCDCTL_API=3
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" endpoint status --write-out=table
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" endpoint health
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" member list --write-out=table
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" alarm list
```

### Resolution

1. Stop control-plane mutations and retain the current endpoint/root
   configuration.
2. Restore network, disk space, certificates, time synchronization, or the
   service on enough **existing** members to regain majority.
3. With quorum healthy, replace a failed member one at a time using the etcd HA
   guide. Do not remove a member while the cluster lacks majority.
4. If the majority is permanently lost, keep surviving old members isolated.
   Restore a verified snapshot into one new, consistent cluster using the etcd
   disaster-recovery procedure, then update clients in a controlled rollout.
5. Never start a second cluster with the same logical root while an old member
   can reconnect.

### Verification

```bash
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" endpoint health
etcdctl --endpoints="$NAMRBD_ETCD_ENDPOINTS" endpoint status --write-out=table
curl -fsS "$GATEWAY_URL/readyz"
namrbdctl status --device "$DEVICE_ID"
```

Confirm one leader, majority health, no active alarms, stable gateway leases,
and unchanged attachment/generation state before resuming mutations.

## 5. PD/TiKV Quorum or Store Loss

PD/TiKV is the authoritative SBS metadata backend for the primary multi-node
runtime. PD endpoints are not etcd endpoints even though both commonly use TCP
`2379`.

### Symptoms

- Volume allocation, placement, snapshot, restore, or maintenance operations
  freeze or fail.
- `sbs-service` is unhealthy or reports transaction, region, timestamp, leader,
  or PD connectivity errors.
- Placement and repair transitions stop progressing.
- A TiKV store is offline or a PD majority is unavailable.

### Likely causes

- Loss of two PD members in a three-member PD cluster.
- Loss of enough TiKV replicas to remove quorum for one or more regions.
- Network partition between PD, TiKV, and `sbs-service`.
- Full/slow disks, clock problems, or a store left offline beyond the
  configured threshold.
- `sbs-service` was pointed at etcd instead of PD, or at the wrong keyspace.

### Diagnosis

Use the TiUP cluster name and a control utility version matching the deployed
TiKV release:

```bash
tiup cluster display "$TIKV_CLUSTER_NAME"
tiup ctl:"$TIKV_VERSION" pd -u "http://$PD_ENDPOINT" member
tiup ctl:"$TIKV_VERSION" pd -u "http://$PD_ENDPOINT" store
sbsctl cluster status --output json
sbsctl volume status --volume-id "$VOLUME_ID" --output json
```

Also inspect PD and TiKV service logs, host disk capacity/latency, and the
configured PD endpoint set and keyspace. Do not mix etcd and PD addresses into
one endpoint list.

### Resolution

1. Pause SBS metadata mutations and retain current cluster identity/keyspace.
2. Restore connectivity or service on enough existing PD members to regain
   majority.
3. For a single failed TiKV store with healthy region quorum, restore or
   replace the store using the TiUP procedure and let PD drive recovery.
4. Do not scale in, tombstone, manually recreate regions, or force-recover
   during an unresolved partition.
5. If PD majority or region quorum is permanently lost, use a reviewed TiKV
   backup/recovery procedure with the old cluster isolated. Do not bootstrap an
   empty cluster against the existing NAMRBD keyspace and call it recovery.
6. Restart `sbs-service` only if its endpoint/configuration changed or it did
   not reconnect after backend health returned; record the restart.

### Verification

- PD has one leader and a healthy majority.
- TiKV stores and regions are healthy or explicitly in a monitored recovery
  state.
- `sbs-service` readiness and source authority are current.
- Volume placement/transition state advances again.
- A replicated userspace write, flush, and readback succeeds before workloads
  resume.

## 6. Kubernetes CSI Provision, Attach, or Mount Failure

First determine the failing stage. A `Pending` PVC is not the same failure as a
`Bound` PVC with `FailedMount`.

| Observation | Failure stage | Common causes |
| --- | --- | --- |
| PVC remains `Pending` | Provisioning | Controller/sidecar unavailable, bad StorageClass, endpoint or credential failure |
| PVC is `Bound`, pod shows `FailedAttachVolume` | Attach | Gateway/SBS unavailable, stale writer/generation, node identity conflict |
| PVC is `Bound`, pod shows `FailedMount` | Node stage/publish | Node plugin absent, kernel modules unavailable, device missing, filesystem tool or mount propagation failure |
| Mount succeeds but application I/O fails | Data path | Gateway path health, kernel runtime path, backend quorum, filesystem/device error |

### Diagnosis

```bash
kubectl -n "$APP_NAMESPACE" get pvc "$PVC_NAME" -o wide
kubectl get pv
kubectl -n "$APP_NAMESPACE" describe pvc "$PVC_NAME"
kubectl -n "$APP_NAMESPACE" describe pod "$POD_NAME"
kubectl -n "$APP_NAMESPACE" get events --sort-by=.lastTimestamp
kubectl -n "$NAMRBD_NAMESPACE" get deployment/namrbd-csi-controller daemonset/namrbd-csi-node
kubectl -n "$NAMRBD_NAMESPACE" get pods -o wide
kubectl -n "$NAMRBD_NAMESPACE" logs -l app.kubernetes.io/name=namrbd-csi -c namrbd-csi-driver --prefix --tail=200
```

On the node selected for the workload, also collect:

```bash
uname -r
test -d "/lib/modules/$(uname -r)"
lsmod | grep -E '^namrbd_(blk|ctrl)'
dmesg | tail -n 200
```

Inspect the `namrbd-csi-config` ConfigMap for endpoint and cluster identifiers,
but do not print the credentials Secret. Confirm that the node has access to
`/dev`, `/sys`, `/lib/modules`, and the configured kubelet root.

### Resolution

1. Restore gateway, SBS, or metadata readiness before restarting CSI pods.
2. For provisioning failures, correct the StorageClass, endpoint, cluster id,
   or Secret reference and restart only the failing controller pod if needed.
3. For attach failures, reconcile attachment id/generation and fence any stale
   writer before retrying the pod.
4. For mount failures, install modules built for the node's running kernel,
   load `namrbd_ctrl` and `namrbd_blk`, verify the device, filesystem utility,
   kubelet root, and bidirectional mount propagation, then restart the node CSI
   pod on that node.
5. Do not delete a bound PVC/PV or manually remove kubelet staging directories
   before confirming reclaim policy and data ownership.

### Verification

- Controller and node CSI pods are Ready on the relevant nodes.
- The PVC is `Bound`; the workload pod reaches `Running` without new attach or
  mount events.
- The workload writes, flushes, and reads back a test value.
- Snapshot/restore, expansion, or RWOP is claimed only when that exact scenario
  is separately verified.

The public kind demo stops at PVC binding. It does not prove node mount, kernel
module loading, or workload I/O.

## 7. iSCSI Session Disconnect or Reconnect Loop

Basic iSCSI is an integration preview. The current compatibility baseline is a
Linux initiator using open-iscsi; automatic HA, MPIO/ALUA, and large export
scale are Advanced features under Enterprise development.

### Symptoms

- `iscsiadm -m session` no longer lists the target.
- The session remains visible but block I/O times out or reports SCSI errors.
- Discovery works but login repeatedly disconnects.
- Target and initiator disagree on portal, IQN, LUN, attachment, generation, or
  export lease.

### Likely causes

- Portal/network interruption or target process restart.
- The iSCSI gateway cannot reach the SBS volume backend.
- Stale attachment generation, export lease, or LUN mapping.
- Target IQN, portal, ACL, or CHAP configuration mismatch.
- An operator expected automatic failover from a Community single-target
  deployment.

### Diagnosis

On the initiator:

```bash
iscsiadm -m session -P 3
journalctl -u iscsid -n 200 --no-pager
dmesg | tail -n 200
```

On the NAMRBD control side:

```bash
sbsctl iscsi status gateway --json
sbsctl iscsi status target --target-iqn "$TARGET_IQN" --json
sbsctl iscsi session list --target-iqn "$TARGET_IQN" --connected-only
sbsctl volume status --volume-id "$VOLUME_ID" --output json
```

Capture gateway summary JSON, operation JSONL, portal, target IQN, initiator
IQN, LUN id, backend mode, export id/epoch/lease, attachment id, generation,
SCSI status/sense, and the exact gateway restart state. Never include a raw
CHAP secret.

### Resolution

1. Stop application I/O and unmount the filesystem before logging out an
   active session. Never force-disconnect a mounted filesystem or root device.
2. Restore portal reachability and iSCSI gateway/SBS readiness.
3. Confirm target, LUN, export lease, attachment, and generation agree with the
   current authoritative state.
4. If the target binary, portal, backend endpoint, target mapping, or command
   mapping changed, restart the iSCSI gateway and record that restart.
5. After the target is healthy and the old writer is fenced, perform a normal
   initiator logout/login:

```bash
iscsiadm -m node -T "$TARGET_IQN" -p "$PORTAL" --logout
iscsiadm -m node -T "$TARGET_IQN" -p "$PORTAL" --login
```

6. Do not use forced registry disconnect as routine cleanup. It requires an
   approved mutation, the expected registry revision, an idempotency key, and
   proof that the old initiator cannot still issue I/O.

### Verification

- Initiator and target each show one expected connected session.
- Portal/IQN/LUN and attachment/generation/export identity agree.
- The correct by-path device is selected; no unrelated LUN is used.
- Write, flush, readback, logout, and cleanup succeed with `error_count=0`.
- Reconnect loops and new SCSI sense errors are absent from both sides' logs.

## 8. FAQ

### Should I restart every NAMRBD process after an incident?

No. Restore the failed authority first, then restart only a process whose
configuration or binary changed, or one that demonstrably failed to reconnect.
Record every restart because it changes the evidence boundary.

### Can I increase retry counts or add a sleep to clear timeouts?

Not without identifying the failed contract. Retries may be correct for
ordinary transient metadata contention, but they must not hide stale-writer,
fencing, quorum, or data-path errors.

### Is a healthy `/healthz` sufficient?

No. Check `/readyz`, authoritative status, attachment/generation/path-plan
identity, backend quorum, and an end-to-end write/flush/readback appropriate to
the claimed path.

### Can I force a new etcd or PD cluster after quorum loss?

Not as routine recovery. First restore a majority of the existing cluster. If
that is impossible, isolate the old cluster and use a reviewed snapshot or
backup recovery procedure. Two authorities for one environment can create
irreconcilable state.

### Should I delete a stuck Kubernetes PVC and retry?

Not until the failing stage, reclaim policy, and data ownership are known.
Deleting a bound PVC may trigger backend deletion. Preserve the PVC/PV and
collect events and CSI logs first.

### Should I force-disconnect an iSCSI session that looks stale?

Only after applications are stopped, filesystems are unmounted, the old
initiator is isolated, and the current attachment/export authority is known.
Prefer normal logout and login.

### When should I open an issue?

For a reproducible public-platform problem, follow the repository
[support policy](https://github.com/nosway/namrbd/blob/main/SUPPORT.md) and
include the sanitized evidence bundle. Use the private process in the
[security policy](https://github.com/nosway/namrbd/blob/main/SECURITY.md) for
vulnerabilities.

## 9. Related Guides

- [Administrator Guide](admin-guide.md)
- [Compatibility Matrix](compatibility-matrix.md)
- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md)
- [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md)
- [Observability](../observability.md)
- [Feature Status](../feature-status.md)
