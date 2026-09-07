#!/usr/bin/env bash
set -euo pipefail

root="${RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
tag="${1:-${RELEASE_TAG:-}}"

fail() {
	printf '[release-version-check] %s\n' "$*" >&2
	exit 1
}

[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]] ||
	fail "RELEASE_TAG must be a full v-prefixed SemVer, got ${tag:-<empty>}"

version_file="$root/version/version.go"
changelog="$root/CHANGELOG.md"
release_notes="$root/RELEASE.md"

[[ -f "$version_file" ]] || fail "missing version/version.go"
[[ -f "$changelog" ]] || fail "missing CHANGELOG.md"
[[ -f "$release_notes" ]] || fail "missing RELEASE.md"

current="$(sed -n 's/^var Current = "\(v[^"]*\)"$/\1/p' "$version_file")"
[[ -n "$current" ]] || fail "could not read version.Current"
[[ "$current" == "$tag" ]] ||
	fail "tag $tag does not match version.Current $current"

release_version="${tag#v}"
grep -Eq "^## \\[${release_version//./\\.}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" "$changelog" ||
	fail "CHANGELOG.md has no dated [$release_version] release section"
grep -Fxq "## $tag" "$release_notes" ||
	fail "RELEASE.md has no $tag artifact section"

printf '[release-version-check] ok tag=%s version=%s\n' "$tag" "$current"
