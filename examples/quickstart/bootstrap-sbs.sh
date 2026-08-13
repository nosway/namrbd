#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
docker_bin="${DOCKER:-docker}"
compose_file="${NAMRBD_QUICKSTART_COMPOSE_FILE:-$script_dir/compose.yaml}"
env_file="${NAMRBD_QUICKSTART_ENV_FILE:-$script_dir/.env.example}"

log() {
	printf '[quickstart-sbs] %s\n' "$*" >&2
}

require_cmd() {
	command -v "$1" >/dev/null 2>&1 || {
		printf '[quickstart-sbs] error: missing required command: %s\n' "$1" >&2
		exit 1
	}
}

env_setting() {
	local key="$1" default_value="$2" value
	value="${!key-}"
	if [[ -n "$value" ]]; then
		printf '%s' "$value"
		return 0
	fi
	if [[ -f "$env_file" ]]; then
		value="$(awk -F= -v key="$key" '$1 == key { line = $0; sub(/^[^=]*=/, "", line); print line }' "$env_file" | tail -n 1)"
		value="${value%$'\r'}"
		if [[ -n "$value" ]]; then
			printf '%s' "$value"
			return 0
		fi
	fi
	printf '%s' "$default_value"
}

is_true() {
	case "$1" in
		1|true|TRUE|yes|YES|on|ON)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

require_cmd "$docker_bin"
require_cmd curl
require_cmd jq

[[ -f "$compose_file" ]] || {
	printf '[quickstart-sbs] error: missing compose file: %s\n' "$compose_file" >&2
	exit 1
}
[[ -f "$env_file" ]] || {
	printf '[quickstart-sbs] error: missing env file: %s\n' "$env_file" >&2
	exit 1
}

project_name="$(env_setting COMPOSE_PROJECT_NAME namrbd-quickstart)"
cluster_id="$(env_setting NAMRBD_QUICKSTART_CLUSTER_ID namrbd-quickstart)"
sbs_cluster_id="$(env_setting NAMRBD_QUICKSTART_SBS_CLUSTER_ID sbs-quickstart)"
data_node_id="$(env_setting NAMRBD_QUICKSTART_DATA_NODE_ID sbs-data-1)"
admin_http_port="$(env_setting NAMRBD_QUICKSTART_SBS_ADMIN_HTTP_PORT 19081)"
data_http_port="$(env_setting NAMRBD_QUICKSTART_SBS_DATA_HTTP_PORT 19082)"
gateway_http_port="$(env_setting NAMRBD_QUICKSTART_GATEWAY_HTTP_PORT 19701)"
volume_id="$(env_setting NAMRBD_QUICKSTART_VOLUME_ID '')"
skip_build="$(env_setting NAMRBD_QUICKSTART_SKIP_BUILD 0)"
cleanup_on_exit="$(env_setting NAMRBD_QUICKSTART_CLEANUP_ON_EXIT 0)"
include_gateway="$(env_setting NAMRBD_QUICKSTART_INCLUDE_GATEWAY 1)"
payload_hex="$(env_setting NAMRBD_QUICKSTART_PAYLOAD_HEX 4e414d5242442d717569636b7374617274)"
volume_size="$(env_setting NAMRBD_QUICKSTART_VOLUME_SIZE 8M)"
work_dir="$(env_setting NAMRBD_QUICKSTART_WORK_DIR "${TMPDIR:-/tmp}/namrbd-quickstart-${project_name}")"
summary_json="$(env_setting NAMRBD_QUICKSTART_SUMMARY_JSON "$work_dir/summary.json")"

if [[ -z "$volume_id" ]]; then
	volume_id="$(printf '%08x' "$(( ($(date +%s) + $$) % 4294967295 ))")"
fi

mkdir -p "$work_dir"
cluster_status_json="$work_dir/cluster-status.json"
volume_status_json="$work_dir/volume-status.json"
volume_create_output="$work_dir/volume-create.txt"
open_json="$work_dir/open.json"
read_output="$work_dir/read.txt"
materialize_json="$work_dir/materialize.json"
errors_file="$work_dir/errors.txt"
checks_file="$work_dir/checks.txt"
: >"$errors_file"
: >"$checks_file"

ok_count=0
error_count=0
first_error=""
last_error=""
readback_matched=false
volume_size_bytes=""
block_size_bytes=""
chunk_size_bytes=""
extent_page_bytes=""

compose() {
	"$docker_bin" compose --env-file "$env_file" -f "$compose_file" -p "$project_name" "$@"
}

record_ok() {
	ok_count=$((ok_count + 1))
	printf '%s\n' "$*" >>"$checks_file"
	log "$*"
}

record_error() {
	error_count=$((error_count + 1))
	if [[ -z "$first_error" ]]; then
		first_error="$*"
	fi
	last_error="$*"
	printf '%s\n' "$*" >>"$errors_file"
	log "error: $*"
}

write_summary() {
	local result="$1"
	jq -n \
		--arg result "$result" \
		--arg validation_boundary "quickstart_local_sbs_compose" \
		--arg project_name "$project_name" \
		--arg compose_file "$compose_file" \
		--arg env_file "$env_file" \
		--arg work_dir "$work_dir" \
		--arg cluster_id "$cluster_id" \
		--arg sbs_cluster_id "$sbs_cluster_id" \
		--arg data_node_id "$data_node_id" \
		--arg volume_id "$volume_id" \
		--arg admin_http_url "http://127.0.0.1:${admin_http_port}" \
		--arg data_http_url "http://127.0.0.1:${data_http_port}" \
		--arg gateway_http_url "http://127.0.0.1:${gateway_http_port}" \
		--arg volume_size_bytes "$volume_size_bytes" \
		--arg block_size_bytes "$block_size_bytes" \
		--arg chunk_size_bytes "$chunk_size_bytes" \
		--arg extent_page_bytes "$extent_page_bytes" \
		--arg first_error "$first_error" \
		--arg last_error "$last_error" \
		--argjson ok_count "$ok_count" \
		--argjson error_count "$error_count" \
		--argjson readback_matched "$readback_matched" \
		--argjson cleanup_on_exit "$(jq -n --arg v "$cleanup_on_exit" '$v == "1" or $v == "true" or $v == "TRUE" or $v == "yes" or $v == "YES" or $v == "on" or $v == "ON"')" \
		--rawfile checks "$checks_file" \
		--rawfile errors "$errors_file" \
		'{
		  result: $result,
		  validation_boundary: $validation_boundary,
		  project_name: $project_name,
		  compose_file: $compose_file,
		  env_file: $env_file,
		  work_dir: $work_dir,
		  cluster_id: $cluster_id,
		  sbs_cluster_id: $sbs_cluster_id,
		  data_node_id: $data_node_id,
		  volume_id: $volume_id,
		  admin_http_url: $admin_http_url,
		  data_http_url: $data_http_url,
		  gateway_http_url: $gateway_http_url,
		  volume_size_bytes: $volume_size_bytes,
		  block_size_bytes: $block_size_bytes,
		  chunk_size_bytes: $chunk_size_bytes,
		  extent_page_bytes: $extent_page_bytes,
		  ok_count: $ok_count,
		  error_count: $error_count,
		  first_error: $first_error,
		  last_error: $last_error,
		  readback_matched: $readback_matched,
		  cleanup_on_exit: $cleanup_on_exit,
		  checks: ($checks | split("\n") | map(select(length > 0))),
		  errors: ($errors | split("\n") | map(select(length > 0)))
		}' | tee "$summary_json"
}

fatal() {
	record_error "$*"
	write_summary error
	exit 1
}

cleanup() {
	if is_true "$cleanup_on_exit"; then
		if ! compose down --volumes --remove-orphans >"$work_dir/cleanup.log" 2>&1; then
			log "cleanup command returned non-zero; see $work_dir/cleanup.log"
		fi
	fi
}
trap cleanup EXIT

run_sbsctl() {
	compose run --rm --no-deps sbsctl "$@"
}

wait_ready() {
	local name="$1" url="$2" attempt
	for attempt in $(seq 1 60); do
		if curl -fsS "$url" >/dev/null 2>&1; then
			record_ok "$name ready"
			return 0
		fi
		sleep 1
	done
	fatal "$name did not become ready at $url"
}

if ! compose config >/dev/null; then
	fatal "Docker Compose config render failed"
fi
record_ok "Docker Compose config rendered"

if ! is_true "$skip_build"; then
	build_services=(sbs-data sbs-service sbsctl)
	if is_true "$include_gateway"; then
		build_services+=(namrbd-gateway)
	fi
	if ! compose build "${build_services[@]}" >&2; then
		fatal "quickstart image build failed"
	fi
	record_ok "quickstart images built"
fi

up_services=(sbs-data sbs-service)
if is_true "$include_gateway"; then
	up_services+=(namrbd-gateway)
fi
if ! compose up -d "${up_services[@]}" >&2; then
	fatal "quickstart services failed to start"
fi
record_ok "quickstart services started"

wait_ready "sbs-data" "http://127.0.0.1:${data_http_port}/readyz"
wait_ready "sbs-service" "http://127.0.0.1:${admin_http_port}/readyz"
if is_true "$include_gateway"; then
	wait_ready "namrbd-gateway" "http://127.0.0.1:${gateway_http_port}/readyz"
	if curl -fsS "http://127.0.0.1:${gateway_http_port}/metrics" | grep -q '^namrbd_gateway_ready 1$'; then
		record_ok "namrbd-gateway metrics exported"
	else
		fatal "namrbd-gateway metrics did not report readiness"
	fi
fi

if ! run_sbsctl cluster init \
	--cluster-id "$cluster_id" \
	--sbs-cluster-id "$sbs_cluster_id" \
	--admin-endpoint sbs-service:9443 >/dev/null; then
	fatal "SBS cluster init failed"
fi
record_ok "SBS cluster initialized"

if ! run_sbsctl node join \
	--cluster-id "$cluster_id" \
	--sbs-cluster-id "$sbs_cluster_id" \
	--admin-endpoint sbs-service:9443 \
	--node-id "$data_node_id" \
	--grpc-endpoint sbs-data:9444 \
	--admin-http-endpoint http://sbs-data:9082 \
	--zone zone-a \
	--auto-create-zone >/dev/null; then
	fatal "SBS data node join failed"
fi
record_ok "SBS data node joined"

if ! run_sbsctl cluster status \
	--admin-endpoint sbs-service:9443 \
	--output json >"$cluster_status_json"; then
	fatal "cluster status failed"
fi

if jq -e '.active_nodes == 1 and .quorum_health == 1' "$cluster_status_json" >/dev/null; then
	record_ok "cluster status reports one active node and quorum"
else
	fatal "cluster status did not report one active node and quorum"
fi

if run_sbsctl volume status \
	--admin-endpoint sbs-service:9443 \
	--volume-id "$volume_id" \
	--output json >"$volume_status_json" 2>/dev/null; then
	record_ok "volume already exists"
else
	if ! run_sbsctl volume create \
		--admin-endpoint sbs-service:9443 \
		--volume-id "$volume_id" \
		--size "$volume_size" \
		--block-size 4K \
		--replication-factor 1 \
		--policy-name quickstart >"$volume_create_output"; then
		fatal "volume create failed"
	fi
	if ! run_sbsctl volume status \
		--admin-endpoint sbs-service:9443 \
		--volume-id "$volume_id" \
		--output json >"$volume_status_json"; then
		fatal "volume status failed after create"
	fi
	record_ok "volume created"
fi

volume_size_bytes="$(jq -r '.volume.size_bytes // empty' "$volume_status_json")"
block_size_bytes="$(jq -r '.volume.block_size // empty' "$volume_status_json")"
chunk_size_bytes="$(jq -r '.volume.chunk_size_bytes // empty' "$volume_status_json")"
extent_page_bytes="$(jq -r '.volume.extent_page_bytes // .volume.extent_size_bytes // empty' "$volume_status_json")"
[[ -n "$volume_size_bytes" && -n "$block_size_bytes" && -n "$chunk_size_bytes" && -n "$extent_page_bytes" ]] || \
	fatal "volume status did not include required geometry"
record_ok "volume geometry captured"

if ! curl -fsS -X POST \
	"http://127.0.0.1:${data_http_port}/debug/materialize-volume?volume_id=${volume_id}&size_bytes=${volume_size_bytes}&block_size=${block_size_bytes}&allocation_chunk_size_bytes=${chunk_size_bytes}&allocation_page_bytes=${extent_page_bytes}&prefix=quickstart-" \
	>"$materialize_json"; then
	fatal "volume materialize failed"
fi
record_ok "volume materialized on local sbs-data"

if ! run_sbsctl testio open \
	--volume-id "$volume_id" \
	--data-endpoint sbs-data:9444 \
	--gateway-id quickstart-gateway >"$open_json"; then
	fatal "testio open failed"
fi
volume_handle="$(jq -r '.volume_handle // empty' "$open_json")"
[[ -n "$volume_handle" ]] || fatal "testio open did not return a volume handle"
record_ok "testio open returned a volume handle"

if ! run_sbsctl testio write \
	--volume-id "$volume_id" \
	--handle "$volume_handle" \
	--offset 0 \
	--data-hex "$payload_hex" >/dev/null; then
	fatal "testio write failed"
fi
record_ok "testio write completed"

if ! run_sbsctl testio flush \
	--volume-id "$volume_id" \
	--handle "$volume_handle" >/dev/null; then
	fatal "testio flush failed"
fi
record_ok "testio flush completed"

if ! run_sbsctl testio read \
	--volume-id "$volume_id" \
	--handle "$volume_handle" \
	--offset 0 \
	--length "$(( ${#payload_hex} / 2 ))" >"$read_output"; then
	fatal "testio read failed"
fi

actual_hex="$(awk -F': ' '$1 == "data_hex" { print $2 }' "$read_output")"
actual_hex="${actual_hex%$'\r'}"
if [[ "$actual_hex" == "$payload_hex" ]]; then
	readback_matched=true
	record_ok "testio readback matched"
else
	fatal "testio readback mismatch"
fi

write_summary ok
