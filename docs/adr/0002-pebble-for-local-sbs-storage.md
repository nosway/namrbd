# ADR 0002: Use Pebble For Local SBS Storage

- Status: Accepted
- Date: 2026-08-25
- Deciders: NAMRBD maintainers
- Supersedes: none
- Superseded by: none

## Context

Each `sbs-data` process needs an embedded, ordered, durable engine for
node-local volume materialization, idempotency records, store metadata, and
chunk or shard payload objects. The engine must support atomic batches,
prefix/range iteration, synchronous commit boundaries, predictable local
deployment, and unit-test isolation without introducing a separate daemon.

The same primitives are useful for a disposable single-process
`sbs-service` metadata backend, but production cluster metadata remains a
different authority.

## Decision

Use CockroachDB Pebble as the local SBS key-value and payload-object engine.
Local mutations that acknowledge durability use synchronous Pebble commits.
Key prefixes encode store and object ownership, while the storage service
retains responsibility for request fencing, generations, idempotency, and
logical reference safety.

Pebble is also allowed as the explicitly labeled local/development
`sbs-service` metadata backend. It is not a substitute for TiKV in a primary
multi-node topology and must not be described as cluster consensus.

## Alternatives Considered

### Store payloads only as ordinary files

Files simplify large sequential-object inspection but require separate atomic
metadata, idempotency, and namespace handling. Crash consistency across file and
metadata updates becomes another protocol.

### Use an external database on every data node

An external service can offer operational tooling, but it adds process,
deployment, network, and failure dependencies to a node-local execution path.

### Use TiKV for local payload objects

This unifies APIs but moves payload traffic and node-local failure domains into
the cluster metadata dependency. It also blurs the difference between local
execution state and cluster authority.

## Consequences

### Positive

- One embedded engine provides atomic batch, iterator, and durability
  primitives for local services and tests.
- A node can own and recover its local store without a second local database
  daemon.
- Repository interfaces can hide Pebble-specific details from cluster and
  gateway callers.

### Negative And Operational Cost

- Compaction, write amplification, cache, file descriptors, and disk-space
  pressure must be observed and bounded.
- Large payload behavior and filesystem durability depend on the storage device
  and mount configuration; an acknowledged Pebble commit is not a universal
  hardware durability claim.
- Key layout and value formats require migration discipline.
- Backup of raw Pebble files is not automatically a consistent NAMRBD volume
  backup.

## Validation And Revisit Triggers

Package tests exercise reopen, sync, iteration, idempotency, and payload
readback. Deployment evidence must also cover crash/restart, disk-full behavior,
compaction pressure, and the target filesystem/device. Revisit if payload size
or workload evidence shows that a file/object engine plus a transactional local
index produces materially safer or more predictable operation.

