Operations Guide

# TiKV High Availability (HA) Cluster Installation & Operations Guide

This operations guide outlines the procedures for deploying, managing, and maintaining a robust, distributed \*\*TiKV/PD storage cluster across multiple nodes\*\* to support authoritative database quorum and transactional meta configurations.

### Scope:

- HA Topology and multi-node Raft quorum requirements.
- Production deployment runbooks using the TiUP package manager.
- OS configurations, sysctls, and disk specifications.
- NAMRBD (`sbs-service`, gateway) client configuration mapping.
- Maintenance, failure remediation, and member scaling workflows.

### Out of Scope (Refer to other documents):

- Local MacBook playground setups: See local development documents.
- SBS metadata logical repair policies: See SBS Architecture guides.
- Comprehensive multi-node SBS cluster bring-up: See multi-node integration runbooks.

## 1. Role of TiKV in NAMRBD

TiKV fulfills two central database roles in NAMRBD deployments. To maximize isolation, placing metadata on separate dedicated logical clusters is highly recommended.

| Deployment Role | Consuming Process | Database API | Authority Scope |
|----|----|----|----|
| SBS Cluster Metadata | `sbs-service`, (legacy/dev) gateway raw metadata | TxnKV | volume maps, chunk placement, and logical topology tables |
| Legacy RawKV Payload Store | `namrbd-gateway` (`store-backend=tikv`) | RawKV | compressed volume binary payload segments |

Common operational rules:

- All SBS clients locate the active storage topology by querying the \*\*Placement Driver (PD)\*\* service endpoints.
- PD endpoints and etcd control-plane endpoints are distinct physical processes. Although both default to client port `2379`, they must never be grouped together. Pointing metadata clients to etcd or gateways to PD will crash operations.
- All clients within a given SBS cluster must connect to the \*\*same PD client URL set\*\* and keyspace prefixes.

## 2. Recommended HA Topology

### 2.1 Minimum Operational Baseline

For high-reliability test environments and production baselines, the following configuration is the default standard:

- **3 Placement Driver (PD) nodes** (Raft quorum = 2/3).
- **3+ TiKV store nodes** (defaulting region replication factor RF = 3).
- PD and TiKV processes allocated on \*\*distinct physical hosts\*\* to ensure failure domain containment (individual racks/zones).

```
                    +------------------+
                    |  NAMRBD clients  |
                    | sbs-service, gw  |
                    +--------+---------+
                             |
              PD client URLs (comma-separated)
                             |
         +--------------------+--------------------+
         |                    |                    |
    +----+----+          +----+----+          +----+----+
    |  PD-1   |<-------->|  PD-2   |<-------->|  PD-3   |
    +----+----+          +----+----+          +----+----+
         |                    |                    |
         +--------------------+--------------------+
                              |
                     schedule / heartbeat
                              |
         +--------------------+--------------------+
         |                    |                    |
    +----+----+          +----+----+          +----+----+
    | TiKV-1  |          | TiKV-2  |          | TiKV-3  |
    | :20160  |          | :20160  |          | :20160  |
    +---------+          +---------+          +---------+
```

### 2.2 Production-Density Deployments

For large production installations, expand storage nodes linearly. TiKV automatically distributes region ranges and splits metadata chunk placements based on PD storage balance algorithms.

## 3. Prerequisites

### 3.1 Host Operating Systems

- RHEL 8+, Rocky Linux 8+, or Ubuntu 20.04+ LTS.
- Strict time synchronization (Chrony) is required to prevent timestamp ordering deviations.

### 3.2 Network Firewall Ports

- PD: `2379` (client port), `2380` (peer port).
- TiKV: `20160` (storage endpoint).

### 3.3 Storage Disk Systems

- Use local enterprise NVMe SSD arrays. Traditional HDDs or shared NFS endpoints cause critical write blockages under load.

### 3.4 System Limits & CPU Governor

Prior to executing cluster installation, verify CPU is configured to `performance` governor and file descriptors are tuned to high limits.

## 4. Cluster Installation via TiUP

### 4.1 Preparing the TiUP Control Host

Install TiUP on the management host and ensure passwordless SSH key authentication is established to all remote nodes:

``` bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.bashrc
tiup cluster
```

### 4.2 topology.yaml Blueprint Configuration

Configure the physical deployment mapping in `topology.yaml` prior to provisioning.

### 4.3 Execute Deploy & Start Runbooks

``` bash
tiup cluster deploy namrbd-db v6.5.2 ./topology.yaml --user root -p
tiup cluster start namrbd-db
```

### 4.4 Post-Install Cluster State Verification

``` bash
tiup cluster display namrbd-db
```

## 5. NAMRBD/SBS Service Integration

### 5.1 Configuration Parameters

Incorporate the PD targets inside the `sbs-service` execution arguments:

``` bash
sbs-service --pd-endpoints="10.10.10.21:2379,10.10.10.22:2379,10.10.10.23:2379" --keyspace="namrbd-prod"
```

### 5.2 Sequential Stack Bootstrap Order

Ensure PD/TiKV databases are fully responsive **before launching `sbs-service`**. If storage metadata is inaccessible, SBS clusters will refuse requests.

## 6. Daily Inspections & Operational Lifecycles

### 6.1 Grafana Dashboards & Metrics

Monitor transaction run latencies and keep write queues below critical threshold levels.

### 6.2 Advanced pd-ctl Administrative Operations

Utilize the `pd-ctl` CLI utility to monitor member lists, inspect scheduler queues, and manage region allocations.

### 6.3 Storage Compaction & Region Rebalance

TiKV handles background compactions automatically. Keep region density consistent across stores to maximize throughput.

## 7. Failure Scenarios & Administrative Remediation

### 7.1 Single PD Node Down

PD cluster maintains Raft quorum. The storage service continues processing volume actions seamlessly. Rebuild the failed node.

### 7.2 Single TiKV Node Down

Metadata stays readable. Replicated region ranges handle operations. Replace stores if recovery exceeds offline timeout parameters.

### 7.3 Loss of PD Quorum (2 of 3 Down)

Metadata updates freeze. Cluster remains locked down until single-node force recovery procedures are executed.

### 7.3.1 SBS Service Failures & Symptoms

Active volume allocations fail, path mappings freeze, and background repair engines suspend task loops.

### 7.4 Member Rotation and Node Scaling Runbooks

Safely scale your infrastructure out or decommission active storage instances using `tiup cluster scale-out` and `scale-in` commands.

## 8. Related Documents

- [NAMRBD Installation & Bring-up Guide](installation-guide.md)
- [etcd HA Architecture & Operations Guide](etcd-ha-cluster-install-operations-guide.md)
