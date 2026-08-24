# Changelog

All notable changes to the public NAMRBD source distribution are documented in
this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and NAMRBD product versioning follows semantic versioning once public releases
begin.

## [Unreleased]

### Added

### Changed

- Updated CSI spec to 1.13.0, reedsolomon to 1.14.2, etcd client to 3.7.1,
  gRPC to 1.83.1, and protobuf to 1.36.12 with their resolved transitive
  dependencies.
- Updated the pinned setup-go, Helm setup, Pages deployment, dependency review,
  and build-provenance GitHub Actions revisions.

### Fixed

- Updated the gateway fleet watch fixture for the etcd 3.7 response-header
  pointer API.

### Deprecated

### Removed

### Security

- Retained immutable commit pins for all updated GitHub Actions and verified the
  exported Go call graph with `govulncheck`.
- Updated the root and bundled gotgt module from logrus 1.9.0 to 1.9.3 to
  resolve GHSA-4f99-4q7p-p3gh / CVE-2025-65637.

### Public Source Boundary

- Assigned root and bundled `gotgt` Go module metadata to each repository
  independently. Canonical-to-public sync now preserves the public module
  files, and public-to-canonical import does not copy dependency metadata.

### Support & Evidence

### Upgrade & Migration

### Known Limits


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
