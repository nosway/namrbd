# Changelog

All notable changes to the public NAMRBD Community edition are documented in
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

### Support & Evidence

### Upgrade & Migration

### Known Limits


## [1.0.0] - 2026-08-21

### Added

- Added reviewed config-file authority for all long-running services, bounded
  etcd/TiKV membership and dependency handling, and enterprise `t2_large`
  operation at 100 SBS nodes, 32 block gateways, 32 iSCSI gateways, 1,000
  volumes, and 1,000 exports.
- Added receiver-enforced iSCSI writer fencing, bounded registry live reload,
  multi-export serving, and audited membership lifecycle operations.
- Embedded the `v1.0.0` GA identity, Git commit, build date, and dirty-state
  metadata into release binaries.

### Changed

- Gateway/SBS runtime version checks compare the exact product SemVer string.
- Stable daemon settings now come from versioned configuration files; legacy
  command-line names remain compatibility aliases with deprecation notices.

### Edition: Community

- Community binaries retain the three-distinct-iSCSI-exported-volume limit and
  fail closed for Enterprise-only EC and `t2_large` export-scale claims.
- Community Makefile and container builds inject the same GA build identity as
  the canonical release.

### Support & Evidence

- Phase Z release-readiness evidence and the Phase AA `t2_large` closure are
  consumed by the `v1.0.0` release package. The support matrix remains the
  authority for edition, topology, and feature scope.

### Upgrade & Migration

- Upgrade into NAMRBD 1.0 is unsupported. Install cleanly and restore from a
  reviewed backup; 1.0 becomes the first supported source version for later
  upgrade work.

### Known Limits

- Enterprise `t2_large` evidence does not widen Community export limits.
- Dependency loss is fail-open for already-admitted serving and fail-closed for
  membership changes, new export admission, promotion, and failover.
- No public benchmark claim is made by this release.
