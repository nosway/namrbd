#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checker="$root/tools/check-release-version.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/version"
printf 'package version\n\nvar Current = "v1.1.0"\n' >"$work/version/version.go"
printf '# Changelog\n\n## [1.1.0] - 2026-09-07\n' >"$work/CHANGELOG.md"
printf '# Release Artifacts\n\n## v1.1.0\n' >"$work/RELEASE.md"

RELEASE_ROOT="$work" "$checker" v1.1.0 >/dev/null

if RELEASE_ROOT="$work" "$checker" v1.1.1 >/dev/null 2>&1; then
	printf '[release-version-fixture] mismatched tag was accepted\n' >&2
	exit 1
fi

if RELEASE_ROOT="$work" "$checker" 1.1.0 >/dev/null 2>&1; then
	printf '[release-version-fixture] tag without v prefix was accepted\n' >&2
	exit 1
fi

printf '# Changelog\n' >"$work/CHANGELOG.md"
if RELEASE_ROOT="$work" "$checker" v1.1.0 >/dev/null 2>&1; then
	printf '[release-version-fixture] missing changelog section was accepted\n' >&2
	exit 1
fi

printf '[release-version-fixture] ok case_count=3 error_count=0\n'
