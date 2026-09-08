# Changelog

All notable changes to the public NAMRBD source distribution are documented in
this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and NAMRBD product versioning follows semantic versioning once public releases
begin.

## [Unreleased]

### Added

### Changed

### Fixed

### Deprecated

### Removed

### Security

### Edition: Community

### Edition: Enterprise only

### Support & Evidence

### Upgrade & Migration

### Known Limits

## [1.1.1] - 2026-09-08

### Fixed

- Restored Community `sbs-service` startup. The v1.1.0 Community build treated
  the unavailable Enterprise authenticated admin listener as a fatal startup
  error even though that optional listener was disabled.

### Edition: Community

- The optional authenticated admin transport now resolves to no separate
  runtime in Community builds. AdminService and OperationsService remain
  registered on the product listener; no Enterprise-only flags or transport
  surface are exposed.

### Edition: Enterprise only

- No Enterprise authenticated admin transport behavior changed. Its optional
  split mTLS listener, validation, RBAC, and fail-closed admission remain
  covered by the Enterprise test profile.

### Support & Evidence

- Added a Community regression test for the disabled authenticated admin
  transport constructor. Real Community `sbs-service`, `sbs-data`, and `sbsctl`
  binaries reached readiness, initialized and joined a cluster, created a
  volume, and completed open/write/flush/read with server version v1.1.1.

### Compatibility

- NAMROS integrations should consume the v1.1.1 source tag for all SBS image
  builds. Do not mix v1.1.0 and v1.1.1 gateway or SBS processes because runtime
  compatibility requires an exact product SemVer match.

### Upgrade & Migration

- Metadata migration required: no.
- Rolling upgrade: unsupported between v1.1.0 and v1.1.1 because runtime
  compatibility requires exact product versions. Restart gateway, SBS service,
  and SBS data processes as one coordinated maintenance operation.
- `kernel_module_compatible: unchanged from v1.1.0`; the separately versioned
  1.0.0 kernel modules are unchanged and remain outside the supported userspace
  volume claim.

### Known Limits

- This hotfix restores Community service startup but does not widen the v1.1
  support matrix or add a public container-image artifact.

## [1.1.0] - 2026-09-07

### Added

- Added a declarative cluster manifest workflow with schema validation,
  deterministic canonicalization and digesting, canonical export/render/plan,
  signed host preflight admission, and file-backed rollout, standby activation,
  and host-maintenance guardrails.
- Added an exact logical `node1..node160` fixture covering eight zones with 20
  nodes per zone and active-3/standby-2 placement validation. Validation and
  planning remain pure: they perform no TiKV mutation or daemon action.
- Added bounded cluster aggregates, revision-pinned node and volume page/point
  views, bounded operation and maintenance views, fleet metrics and alerts, a
  Grafana overview, an operations console, and observe-first MCP tools.

### Changed

- Set the product and release binary identity to `v1.1.0`; gateway and SBS
  runtime compatibility continues to require an exact product SemVer match.
- Normal fleet reads use bounded summary, page, batch, or point queries and
  expose stale, partial, or rebuild-required state instead of falling back to
  raw cluster-wide completion scans.
- Updated CSI spec to 1.13.0, reedsolomon to 1.14.2, etcd client to 3.7.1,
  gRPC to 1.83.1, and protobuf to 1.36.12 with their resolved transitive
  dependencies.
- Updated the pinned setup-go, Helm setup, Pages deployment, dependency review,
  and build-provenance GitHub Actions revisions.

### Fixed

- Made the Community export self-contained for the logical fleet fixture,
  dependency-budget contract, module metadata, format checks, and Community
  build flags inherited from an Enterprise-default canonical checkout.
- Updated the gateway fleet watch fixture for the etcd 3.7 response-header
  pointer API.

### Deprecated

- No new deprecations. Environment names deprecated in v1.0.x reach their
  previously announced removal boundary in this release.

### Removed

- Removed acceptance of the legacy environment-variable aliases listed in
  `docs-src/reference/config/index.md`. A v1.1.0 process or `sbsctl` command
  fails with the canonical replacement name when one is present.

### Security

- Retained immutable commit pins for all updated GitHub Actions and verified the
  exported Go call graph with `govulncheck`.
- Updated the root and bundled gotgt module from logrus 1.9.0 to 1.9.3 to
  resolve GHSA-4f99-4q7p-p3gh / CVE-2025-65637.

### Edition: Community

- Assigned root and bundled `gotgt` Go module metadata to each repository
  independently. Canonical-to-public sync preserves the public module files,
  and public-to-canonical import does not copy dependency metadata.
- The manifest, bounded fleet operations, observability, console, and
  observe-first MCP surfaces described above are included in public source.

### Edition: Enterprise only

- No Enterprise-only capability is promoted to general availability or public
  support by this release.

### Support & Evidence

- The supported v1.1 volume claim remains the replicated userspace gateway and
  SBS path.
- The exact 160-node logical fixture and an 18-host/160-process software run
  provide workflow and process-scale evidence only. They do not qualify 160
  independent physical servers or create a scale/performance support claim.

### Upgrade & Migration

- Metadata migration required: no destructive or one-way metadata migration.
  Fleet summaries and bounded indexes are derived state and can report
  rebuild-required until reconstructed.
- Rolling upgrade: mixed v1.0.0/v1.1.0 gateway and SBS serving is unsupported
  because runtime compatibility requires exact product versions. Use a
  coordinated maintenance restart.
- Replace every v1.0.x legacy environment name with its documented canonical
  name before starting a v1.1.0 process.
- `kernel_module_not_required` for the supported userspace deployment. The
  separately versioned 1.0.0 kernel modules are unchanged and remain outside
  the supported userspace volume claim.

### Known Limits

- The release makes no general IOPS, bandwidth, latency, or fleet-scale
  performance claim.
- Snapshot/restore, CSI, kernel datapath I/O, basic iSCSI, and external
  initiator integrations remain available in source but outside the supported
  v1.1 release surface unless their feature-status row says otherwise.
- Qualification of 160 independent physical servers is deferred to a separate
  hardware qualification.


## [1.0.0] - 2026-08-21

### Added

- Added the replicated userspace gateway and SBS volume path, host control
  tools, kernel module source, Kubernetes CSI assets, basic iSCSI gateway,
  snapshot/restore building blocks, and public operations assets.
- Added reviewed config-file authority for long-running public services and
  bounded etcd/TiKV dependency handling.
- Embedded the `v1.0.0` GA identity, Git commit, build date, and dirty-state
  metadata into release binaries.

### Changed

- Gateway/SBS runtime version checks compare the exact product SemVer string.
- Stable daemon settings now come from versioned configuration files; legacy
  command-line names remain compatibility aliases with deprecation notices.

### Public Source Boundary

- Public binaries retain the three-distinct-iSCSI-exported-volume limit and do
  not register advanced Enterprise command surfaces.
- Public Makefile and container builds inject the v1.0.0 build identity.

### Support & Evidence

- The supported v1.0 volume claim is the replicated userspace gateway path.
- Snapshot/restore, CSI, kernel datapath I/O, basic iSCSI, and external
  initiator integrations are available in source but are not validated as
  supported v1.0 release surfaces.

### Upgrade & Migration

- Upgrade into NAMRBD 1.0 is unsupported. Install cleanly and restore from a
  reviewed backup; 1.0 becomes the first supported source version for later
  upgrade work.

### Known Limits

- The release makes no public benchmark claim.
- Kernel datapath and larger-than-recorded topology claims remain outside the
  v1.0 support boundary.
- Dependency loss is fail-open for already-admitted serving and fail-closed for
  membership changes, new export admission, promotion, and failover.
