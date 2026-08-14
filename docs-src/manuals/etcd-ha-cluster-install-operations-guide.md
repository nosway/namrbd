Operations Guide

# etcd High Availability (HA) Cluster Installation & Operations Guide

This document details the procedures for deploying and maintaining a robust, multi-member **etcd cluster across distributed servers** to absorb single-point-of-failure (SPOF) scenarios.

### Scope:

- HA Topology and Raft quorum requirements.
- etcd v3 multi-member bootstrap and systemd operations.
- Network, firewall, and disk storage preparation.
- NAMRBD gateway / `namrbdctl` integration.
- Daily inspections, backups, fault responses, and member rotation.

### Out of Scope (Refer to other documents):

- Single-node local testing (MacBook/Homebrew): See [installation-guide.html](installation-guide.md) summary.
- Control-plane vs storage metadata separation: See TiKV Architecture Guide.
- TiKV/PD HA deployment: See [tikv-ha-cluster-install-operations-guide.html](tikv-ha-cluster-install-operations-guide.md).

## 1. Role of etcd in NAMRBD

etcd acts as the \*\*authoritative gateway control-plane\*\* for NAMRBD. The underlying `sbs-service` does not utilize etcd.

| Item                                          | Authority                  |
|-----------------------------------------------|----------------------------|
| Volume Spec & Control-Plane Status            | `etcd`                     |
| Attachment Ownership & Generation             | `etcd`                     |
| Gateway Membership & Liveness (Lease)         | `etcd`                     |
| Gateway Discovery Endpoint                    | `etcd`                     |
| Extent/Chunk SBS Placement (SBS Cluster Mode) | `TiKV` (via `sbs-service`) |

Common rules:

- All gateways and `namrbdctl` must point to the \*\*same `--etcd-endpoints`\*\* and utilize the \*\*same `--etcd-root`\*\*.
- Dev/Stage/Prod environments are \*\*logically isolated via `--etcd-root` prefixes\*\* even if sharing the same physical etcd cluster.
- PD (TiKV) and etcd are separate processes. Although both default to port `2379`, they must never be confused.

## 2. Recommended HA Topology

### 2.1 Minimum Operational Baseline

- **etcd 3-member cluster** (Raft quorum = 2/3).
- Members deployed on **distinct hosts** (separate racks/zones).
- Co-locating etcd on the same host as `namrbd-gateway` is discouraged in production to ensure failure domain isolation.

```
              +---------------------------+
              | namrbd-gateway, namrbdctl |
              +-------------+-------------+
                            |
            client URLs (comma-separated, :2379)
                            |
     +----------+-----------+-----------+----------+
     |          |                       |          |
+----+----+ +----+----+             +----+----+ +----+----+
| etcd-1  | | etcd-2  |<-- Raft --> | etcd-3  | | (spare) |
| :2379   | | :2379   |    :2380    | :2379   | |  N/A    |
+---------+ +---------+             +---------+ +---------+
```

### 2.2 Deprecated Configurations

- **2-member cluster:** Quorum remains 2. If 1 node fails, the cluster hangs immediately. This offers zero HA benefit.
- **Even-numbered nodes:** 4 nodes require a quorum of 3. This has worse fault tolerance than a 3-node cluster.

### 2.3 Five-Member Expansion

For high-density production tiers, a 5-member cluster (quorum = 3) can withstand up to 2 concurrent node failures.

## 3. Prerequisites

### 3.1 Host Requirements

- CentOS 7+, RHEL 8+, or Ubuntu 20.04+ LTS.
- Time synchronization (Chrony/NTP) is critical. Raft drift can cause leadership loss.

### 3.2 Network Ports

- `2379`: Client Port (Gateways, CLI, namrbdctl).
- `2380`: Peer Port (Raft internode communications).

### 3.3 Directory Structure

- Binary: `/usr/local/bin/etcd`, `/usr/local/bin/etcdctl`.
- Configuration: `/etc/etcd/etcd.yml`.
- Data Directory: `/var/lib/etcd/namrbd.etcd` (SSD storage recommended).

## 4. etcd Binary Installation

### 4.1 Version and Installation (Per Node)

``` bash
ETCD_VER=v3.5.9
GITHUB_URL=https://github.com/etcd-io/etcd/releases/download
curl -L ${GITHUB_URL}/${ETCD_VER}/etcd-${ETVER}-linux-amd64.tar.gz -o /tmp/etcd.tar.gz
tar -xzvf /tmp/etcd.tar.gz -C /tmp
sudo cp /tmp/etcd-${ETVER}-linux-amd64/etcd* /usr/local/bin/
```

### 4.2 Cluster Variables Configuration

Ensure client/peer bindings are strictly mapped in `/etc/etcd/etcd.yml` or environment variables prior to bootstrap.

## 5. Three-Member Cluster Bootstrap

### 5.1 Member 1 (etcd-1)

Initialize the first member with initial cluster state configured to `new`.

### 5.2 Members 2 and 3

Complete mutual registration by adding peers to `--initial-cluster`.

### 5.3 systemd Unit Example (etcd-1)

```
[Unit]
Description=etcd service for NAMRBD
After=network.target

[Service]
Type=notify
ExecStart=/usr/local/bin/etcd --config-file=/etc/etcd/etcd.yml
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 5.4 Post-Bootstrap Validation

``` bash
export ETCDCTL_API=3
etcdctl --endpoints=10.10.10.11:2379,10.10.10.12:2379,10.10.10.13:2379 endpoint health
etcdctl --endpoints=10.10.10.11:2379,10.10.10.12:2379,10.10.10.13:2379 member list
```

## 6. NAMRBD Integration

### 6.1 Environment Variables

Add endpoints to the gateway execution parameters:

``` bash
namrbd-gateway --etcd-endpoints="10.10.10.11:2379,10.10.10.12:2379,10.10.10.13:2379" --etcd-root="/namrbd/prod"
```

### 6.2 Bootstrap Sequence

Always boot the physical/logical etcd cluster **prior to starting the gateway process**. If etcd is unreachable, gateways will fail-closed and block host I/O binds.

## 7. Daily Operations

### 7.1 Health Checklist

- Confirm key index rate and watch-latency profiles.
- Review disk wal/db commit latencies under 10ms.

### 7.2 Prefix & Environment Isolation

Use distinct namespaces: `/namrbd/dev`, `/namrbd/staging`, and `/namrbd/prod`.

### 7.3 Planned Rolling Maintenance

Upgrade cluster components sequentially. Restart one node at a time to retain Raft quorum.

### 7.4 Compaction & Defragmentation

Perform key space maintenance routinely to reclaim storage:

``` bash
etcdctl compact 50000
etcdctl defrag
```

## 8. Backup & Restore

### 8.1 Snapshot Backup

``` bash
etcdctl snapshot save /backups/etcd-namrbd-$(date +%F).db
```

### 8.2 Snapshot Restore Summary

To restore, stop all etcd services, run `etcdctl snapshot restore` specifying initial configurations, and spin up daemon engines.

## 9. Failure Scenarios & Mitigation

### 9.1 Single Member Down (1 of 3)

Raft quorum remains intact. Write path continues operating. Replace failed node immediately.

### 9.2 Quorum Loss (2 of 3 Down)

The cluster is frozen. Read/Write actions are denied.

### 9.2.1 NAMRBD Symptoms

Gateways will fail-closed, blocking mount requests and preventing new attachments.

### 9.3 Slow Disks & Quota Exceeded

Sluggish SSD/HDD causes watch timeouts. Quota violations trigger alarm blockages.

### 9.4 Member Replacement Summary

Add a replacement node using `etcdctl member add` before starting the new daemon.

## 10. Security & TLS Checklist

Always enable peer-to-peer and client-to-server mTLS in production tiers.

## 11. NAMRBD Verification

Run simulated partition drills to guarantee system resilient failover behavior.

## 12. Quick Reference

Use `etcdctl endpoint status` and check alarm statuses regularly.

## 13. Related Documents

- [NAMRBD Installation Guide](installation-guide.md)
- [PD/TiKV Operations Manual](tikv-ha-cluster-install-operations-guide.md)
