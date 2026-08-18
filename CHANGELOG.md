# Changelog

All notable changes to the public NAMRBD Community edition are documented in
this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and NAMRBD product versioning follows semantic versioning once public releases
begin.

## [Unreleased]

### Added

- `v1.0.0-rc` release-candidate version identity is now embedded into Community
  binaries together with the Git commit, build date, and dirty-state metadata.

### Changed

- Gateway/SBS runtime version checks now compare the exact product SemVer
  string instead of a separate Major.Minor compatibility value.

### Fixed

### Deprecated

### Removed

### Security

### Edition: Community

- Community Makefile and container builds pass release metadata into built
  binaries through Go linker flags.

### Support & Evidence

### Upgrade & Migration

- NAMRBD `v1.0.0` has not been released yet. Public release readiness is still
  being completed.

### Known Limits

- Pre-1.0 development continues on `main`. Milestone tags are engineering
  markers, not product releases.
