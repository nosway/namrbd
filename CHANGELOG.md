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

### Public Source Boundary

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
