# Release Artifacts

## v1.1.1

The `v1.1.1` GitHub release publishes the same Linux AMD64 Community artifact
family as v1.1.0: the public command binaries and license metadata, SHA-256
checksums, an SPDX JSON SBOM, and GitHub build provenance.

This patch release restores Community `sbs-service` startup when the optional
Enterprise authenticated admin listener is unavailable. It does not add an
authenticated admin surface to Community or widen the supported feature set.
Community AdminService and OperationsService remain on the product listener,
as they did before the optional Enterprise listener split.

Container images are not v1.1.1 release outputs. Build them from the tagged
source and record an immutable registry digest before deployment. All gateway
and SBS processes in one deployment must report the exact same v1.1.1 product
version.

## v1.1.0

The `v1.1.0` GitHub release publishes a Linux AMD64 archive containing the
public command binaries and license metadata, SHA-256 checksums, an SPDX JSON
SBOM, and GitHub build provenance. The release workflow first verifies that the
tag, `version.Current`, this artifact section, and the dated changelog section
all identify the same version.

Container images are not v1.1.0 release outputs. Build them from the tagged
source and record an immutable registry digest before deployment; do not infer
an image digest from a source tag or from the CSI chart's unpublished evidence
candidate.

The supported v1.1 volume claim is the replicated userspace gateway and SBS
path. The exact logical 160-node fixture and 18-host/160-process software
evidence do not qualify 160 independent physical servers. See
[`docs-src/feature-status.md`](docs-src/feature-status.md) and
[`CHANGELOG.md`](CHANGELOG.md) for integration status, migration requirements,
and known limits.

## v1.0.0

`v1.0.0` is a source release. GitHub provides automatic source archives for
the tag, but the project did not attach generated binaries, container digests,
checksums, an SBOM, or provenance to that release. Do not infer binary or image
provenance from the tag alone.

The supported v1.0 volume claim is the replicated userspace gateway path. See
[`docs-src/feature-status.md`](docs-src/feature-status.md) and
[`CHANGELOG.md`](CHANGELOG.md) for integration status and known limits.

## Tagged Release Contract

The public release workflow runs the exported-source test boundary and creates:

- a Linux AMD64 archive containing the public command binaries and license
  metadata;
- SHA-256 checksums;
- an SPDX JSON SBOM for the archive contents; and
- GitHub build provenance for the release archive.

The workflow uploads the same files as a CI artifact and to the matching GitHub
Release. Container images are a separate artifact family; a release must list
their immutable digests explicitly before users should treat them as release
outputs.

Before tagging a release, maintainers must also verify the public source export,
documentation render, support boundary, migration notes, security policy, and
known limitations. `make release-version-check RELEASE_TAG=vX.Y.Z` guards the
tag-to-source identity. A passing source build does not promote an integration
or advanced feature to supported status.
