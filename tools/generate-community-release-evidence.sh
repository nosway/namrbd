#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${NAMRBD_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
HANDOFF_DIR="${PHASE_Y_PUBLIC_HANDOFF_OUT_DIR:-$ROOT_DIR/.cache/phase-y-public-handoff}"
OUT_DIR="${PHASE_Y_RELEASE_EVIDENCE_OUT_DIR:-$ROOT_DIR/.cache/phase-y-release-evidence}"
IMAGES_JSONL="${PHASE_Y_RELEASE_IMAGES_JSONL:-$HANDOFF_DIR/images.jsonl}"
SYFT="${SYFT:-syft}"
SOURCE_REPOSITORY="${NAMRBD_SOURCE_REPOSITORY:-https://github.com/nosway/namrbd}"
SOURCE_REVISION="${NAMRBD_SOURCE_REVISION:-$(git -C "$ROOT_DIR" rev-parse HEAD)}"
BUILDER_ID="${NAMRBD_RELEASE_BUILDER_ID:-https://github.com/nosway/namrbd/actions/workflows/release.yml}"
BUILD_STARTED_ON="${NAMRBD_BUILD_STARTED_ON:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

SBOM_DIR="$OUT_DIR/sbom"
PROVENANCE_DIR="$OUT_DIR/provenance"
CHECKSUMS="$OUT_DIR/checksums.sha256"
MANIFEST="$OUT_DIR/release-manifest.json"
SUMMARY="$OUT_DIR/summary.json"
CHECKS="$OUT_DIR/checks.jsonl"

mkdir -p "$SBOM_DIR" "$PROVENANCE_DIR"
: >"$CHECKS"

ok_count=0
error_count=0
first_error=""
last_error=""

record() {
	local id="$1" result="$2" detail="$3" artifact="${4:-}"
	jq -cn --arg id "$id" --arg result "$result" --arg detail "$detail" --arg artifact "$artifact" \
		'{check_id:$id,result:$result,detail:$detail,artifact_path:$artifact}' >>"$CHECKS"
	if [[ "$result" == ok ]]; then
		ok_count=$((ok_count + 1))
	else
		error_count=$((error_count + 1))
		[[ -n "$first_error" ]] || first_error="$detail"
		last_error="$detail"
	fi
}

for command_name in jq sha256sum "$SYFT"; do
	if command -v "$command_name" >/dev/null 2>&1; then
		record "command-$command_name" ok "required command available: $command_name"
	else
		record "command-$command_name" fail "required command not found: $command_name"
	fi
done

if [[ -s "$IMAGES_JSONL" ]] && jq -s -e 'length == 6 and all(.[]; .inspected == true and .digest_recorded == true and (.image_id | startswith("sha256:")))' "$IMAGES_JSONL" >/dev/null; then
	record images-input ok "six inspected Community images have immutable IDs" "$IMAGES_JSONL"
else
	record images-input fail "images.jsonl must contain six inspected images with immutable IDs" "$IMAGES_JSONL"
fi

if [[ "$error_count" -eq 0 ]]; then
	while IFS= read -r row; do
		name="$(jq -r '.name' <<<"$row")"
		ref="$(jq -r '.ref' <<<"$row")"
		image_id="$(jq -r '.image_id' <<<"$row")"
		sbom="$SBOM_DIR/$name.spdx.json"
		provenance="$PROVENANCE_DIR/$name.intoto.json"

		if "$SYFT" "$ref" -o spdx-json >"$sbom" 2>"$SBOM_DIR/$name.stderr.log" \
			&& jq -e --arg name "$name" '.spdxVersion == "SPDX-2.3" and (.packages | type == "array") and (.packages | length > 0)' "$sbom" >/dev/null; then
			record "sbom-$name" ok "SPDX 2.3 SBOM generated for $ref" "${sbom#"$ROOT_DIR/"}"
		else
			record "sbom-$name" fail "failed to generate non-empty SPDX 2.3 SBOM for $ref" "${sbom#"$ROOT_DIR/"}"
			continue
		fi

		jq -n \
			--arg subject "$ref" --arg digest "${image_id#sha256:}" \
			--arg source "$SOURCE_REPOSITORY" --arg revision "$SOURCE_REVISION" \
			--arg builder "$BUILDER_ID" --arg started "$BUILD_STARTED_ON" \
			'{
			  _type:"https://in-toto.io/Statement/v1",
			  subject:[{name:$subject,digest:{sha256:$digest}}],
			  predicateType:"https://slsa.dev/provenance/v1",
			  predicate:{
			    buildDefinition:{buildType:"https://github.com/nosway/namrbd/phase-y-community-container@v1",externalParameters:{image:$subject},internalParameters:{},resolvedDependencies:[{uri:$source,digest:{gitCommit:$revision}}]},
			    runDetails:{builder:{id:$builder},metadata:{invocationId:("phase-y-"+$revision),startedOn:$started,finishedOn:(now|todateiso8601)}}
			  }
			}' >"$provenance"
		if jq -e --arg digest "${image_id#sha256:}" '.subject[0].digest.sha256 == $digest and .predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit != ""' "$provenance" >/dev/null; then
			record "provenance-$name" ok "in-toto SLSA provenance generated for $ref" "${provenance#"$ROOT_DIR/"}"
		else
			record "provenance-$name" fail "invalid provenance for $ref" "${provenance#"$ROOT_DIR/"}"
		fi
	done <"$IMAGES_JSONL"
fi

if [[ "$error_count" -eq 0 ]]; then
	(
		cd "$OUT_DIR"
		find sbom provenance -type f \( -name '*.json' \) -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
	) >"$CHECKSUMS"
	if (cd "$OUT_DIR" && sha256sum -c "$(basename "$CHECKSUMS")" >/dev/null); then
		record checksums ok "all SBOM and provenance checksums verify" "${CHECKSUMS#"$ROOT_DIR/"}"
	else
		record checksums fail "release evidence checksum verification failed" "${CHECKSUMS#"$ROOT_DIR/"}"
	fi
fi

jq -n \
	--arg schema_version "namrbd.community.release-manifest.v1" \
	--arg source_repository "$SOURCE_REPOSITORY" --arg source_revision "$SOURCE_REVISION" \
	--arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--slurpfile images "$IMAGES_JSONL" \
	--argjson sbom_recorded "$([[ "$error_count" -eq 0 ]] && echo true || echo false)" \
	--argjson provenance_recorded "$([[ "$error_count" -eq 0 ]] && echo true || echo false)" \
	'{schema_version:$schema_version,source:{repository:$source_repository,revision:$source_revision},generated_at:$generated_at,images:$images,sbom_recorded:$sbom_recorded,provenance_recorded:$provenance_recorded,checksum_algorithm:"sha256",checksums_file:"checksums.sha256"}' >"$MANIFEST"

result=ok
[[ "$error_count" -eq 0 ]] || result=fail
jq -n --arg result "$result" --arg manifest "${MANIFEST#"$ROOT_DIR/"}" --arg checksums "${CHECKSUMS#"$ROOT_DIR/"}" \
	--arg first_error "$first_error" --arg last_error "$last_error" --argjson ok_count "$ok_count" --argjson error_count "$error_count" \
	--argjson sbom_count "$(find "$SBOM_DIR" -name '*.spdx.json' -type f | wc -l | tr -d ' ')" \
	--argjson provenance_count "$(find "$PROVENANCE_DIR" -name '*.intoto.json' -type f | wc -l | tr -d ' ')" \
	'{result:$result,entrypoint:"phase-y-release-evidence",release_manifest_ready:($error_count==0),sbom_recorded:($error_count==0),provenance_recorded:($error_count==0),checksum_verified:($error_count==0),sbom_count:$sbom_count,provenance_count:$provenance_count,ok_count:$ok_count,error_count:$error_count,first_error:$first_error,last_error:$last_error,release_manifest_path:$manifest,checksums_path:$checksums}' | tee "$SUMMARY"

[[ "$error_count" -eq 0 ]]
