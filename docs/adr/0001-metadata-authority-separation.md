# ADR 0001: Separate Metadata Authorities By Ownership Scope

- Status: Accepted
- Date: 2026-08-25
- Deciders: NAMRBD maintainers
- Supersedes: none
- Superseded by: none

## Context

NAMRBD coordinates host attachments and gateway liveness, cluster-wide volume
geometry and placement, and node-local payload execution. Those states differ
in consistency domain, scale, failure recovery, and writer ownership. Placing
all of them in one generic key-value namespace would make a gateway or data
node capable of bypassing the service that owns storage transitions.

Gateway discovery also needs fast lease/watch semantics, while cluster storage
metadata needs transactional range-oriented records and node-local execution
needs an embedded durable store.

## Decision

Use three explicit authority scopes:

- etcd owns gateway/control-plane state such as attachments, writer generation,
  gateway registry, and leases.
- TiKV owns primary multi-node SBS cluster metadata such as volume geometry,
  allocation pages, topology, placement, and operation state. `sbs-service` is
  its authoritative writer.
- local Pebble owns each `sbs-data` node's materialized state, idempotency
  records, store metadata, and local payload objects. Pebble may also provide a
  single-process development metadata backend, but that does not make it a
  multi-node cluster authority.

Gateways consume published, gateway-facing SBS views from `sbs-service`; they
do not open raw TiKV metadata to derive placement. Kernel and CSI components do
not become metadata authorities merely because they initiate operations.

## Alternatives Considered

### One etcd cluster for all metadata

This simplifies dependency inventory but combines short-lived gateway leases
with storage allocation and maintenance state, expands transaction and scan
pressure on one quorum, and weakens ownership separation.

### Gateway reads TiKV directly

This can reduce one request hop, but it couples gateway release and cache logic
to internal SBS schemas and can turn a replaceable router into a second source
of placement truth.

### One Pebble database per deployment

This is useful for local evaluation but cannot provide the distributed
transaction and failover authority required by a multi-node SBS cluster.

## Consequences

### Positive

- Failure and writer ownership are explicit.
- Gateway processes remain replaceable and caches remain non-authoritative.
- Cluster maintenance can transact through one SBS authority without coupling
  node-local payload persistence to the control-plane lease store.

### Negative And Operational Cost

- A primary multi-node deployment operates both etcd and PD/TiKV.
- Similar endpoint ports can be confused even though the authorities differ.
- Backup, monitoring, TLS, quorum response, and capacity planning are required
  separately for each dependency.
- Cross-authority workflows need generations, idempotency, and reconciliation;
  there is no atomic transaction spanning all three stores.

## Validation And Revisit Triggers

Configuration and architecture checks must keep the ownership matrix visible.
Tests must reject gateway raw-metadata authority and must distinguish the local
development backend from primary TiKV mode. Revisit through a new ADR if one
store can satisfy all ownership, transaction, watch, scale, and independent
failure requirements without broadening writer authority.

