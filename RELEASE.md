# Release Artifacts

## v1.0.0

`v1.0.0` is a source release. GitHub provides automatic source archives for
the tag, but the project did not attach generated binaries, container digests,
checksums, an SBOM, or provenance to that release. Do not infer binary or image
provenance from the tag alone.

The supported v1.0 volume claim is the replicated userspace gateway path. See
[`docs-src/feature-status.md`](docs-src/feature-status.md) and
[`CHANGELOG.md`](CHANGELOG.md) for integration status and known limits.

## Future Tagged Releases

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
known limitations. A passing source build does not promote an integration or
Advanced feature to supported status.
