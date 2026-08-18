#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

docker_bin="${DOCKER:-docker}"
kind_bin="${KIND:-kind}"
kubectl_bin="${KUBECTL:-kubectl}"
helm_bin="${HELM:-helm}"
jq_bin="${JQ:-jq}"

cluster_name="${NAMRBD_KIND_CLUSTER_NAME:-namrbd-csi-demo}"
context_name="kind-${cluster_name}"
namespace="${NAMRBD_KIND_CSI_NAMESPACE:-namrbd-system}"
release_name="${NAMRBD_KIND_CSI_RELEASE:-namrbd-csi}"
pvc_name="${NAMRBD_KIND_PVC_NAME:-namrbd-demo-pvc}"
pvc_namespace="${NAMRBD_KIND_PVC_NAMESPACE:-default}"
image_repository="${NAMRBD_CSI_DRIVER_IMAGE:-ghcr.io/nosway/namrbd-csi-driver}"
image_tag="${NAMRBD_IMAGE_TAG:-local}"
csi_image="${image_repository}:${image_tag}"
quickstart_bind_address="${NAMRBD_KIND_QUICKSTART_BIND_ADDRESS:-0.0.0.0}"
quickstart_admin_port="${NAMRBD_QUICKSTART_SBS_ADMIN_GRPC_PORT:-19443}"
quickstart_gateway_port="${NAMRBD_QUICKSTART_GATEWAY_HTTP_PORT:-19701}"
cluster_id="${NAMRBD_QUICKSTART_CLUSTER_ID:-namrbd-quickstart}"
sbs_cluster_id="${NAMRBD_QUICKSTART_SBS_CLUSTER_ID:-sbs-quickstart}"
timeout="${NAMRBD_KIND_WAIT_TIMEOUT:-180s}"
work_dir="${NAMRBD_KIND_WORK_DIR:-${TMPDIR:-/tmp}/namrbd-kind-csi-pvc-${cluster_name}}"
summary_json="${NAMRBD_KIND_SUMMARY_JSON:-$work_dir/summary.json}"
reset_quickstart="${NAMRBD_KIND_RESET_QUICKSTART:-1}"
start_quickstart="${NAMRBD_KIND_START_QUICKSTART:-1}"
delete_cluster_on_cleanup="${NAMRBD_KIND_DELETE_CLUSTER_ON_CLEANUP:-0}"
cleanup_quickstart_on_cleanup="${NAMRBD_KIND_CLEANUP_QUICKSTART_ON_CLEANUP:-0}"

mkdir -p "$work_dir"
checks_file="$work_dir/checks.txt"
errors_file="$work_dir/errors.txt"
: >"$checks_file"
: >"$errors_file"

ok_count=0
error_count=0
first_error=""
last_error=""
admin_endpoint=""
gateway_url=""
pvc_phase=""
pv_name=""
volume_handle=""

usage() {
	cat <<'USAGE'
Usage:
  examples/kind-csi-pvc/run.sh [run|check|cleanup]

Environment:
  NAMRBD_KIND_CLUSTER_NAME              kind cluster name
  NAMRBD_KIND_HOST_ADDRESS              host address reachable from kind pods
  NAMRBD_KIND_QUICKSTART_BIND_ADDRESS   Compose host bind address, default 0.0.0.0
  NAMRBD_KIND_RESET_QUICKSTART          reset quickstart volumes before run, default 1
  NAMRBD_IMAGE_TAG                      local CSI image tag, default local
USAGE
}

log() {
	printf '[kind-csi-pvc] %s\n' "$*" >&2
}

require_cmd() {
	command -v "$1" >/dev/null 2>&1 || {
		printf '[kind-csi-pvc] error: missing required command: %s\n' "$1" >&2
		exit 1
	}
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
	"$jq_bin" -n \
		--arg result "$result" \
		--arg validation_boundary "kind_csi_pvc_binding_demo" \
		--arg cluster_name "$cluster_name" \
		--arg context_name "$context_name" \
		--arg namespace "$namespace" \
		--arg release_name "$release_name" \
		--arg pvc_namespace "$pvc_namespace" \
		--arg pvc_name "$pvc_name" \
		--arg pvc_phase "$pvc_phase" \
		--arg pv_name "$pv_name" \
		--arg volume_handle "$volume_handle" \
		--arg csi_image "$csi_image" \
		--arg admin_endpoint "$admin_endpoint" \
		--arg gateway_url "$gateway_url" \
		--arg first_error "$first_error" \
		--arg last_error "$last_error" \
		--argjson ok_count "$ok_count" \
		--argjson error_count "$error_count" \
		--rawfile checks "$checks_file" \
		--rawfile errors "$errors_file" \
		'{
		  result: $result,
		  validation_boundary: $validation_boundary,
		  cluster_name: $cluster_name,
		  context_name: $context_name,
		  namespace: $namespace,
		  release_name: $release_name,
		  pvc_namespace: $pvc_namespace,
		  pvc_name: $pvc_name,
		  pvc_phase: $pvc_phase,
		  pv_name: $pv_name,
		  volume_handle: $volume_handle,
		  csi_image: $csi_image,
		  admin_endpoint: $admin_endpoint,
		  gateway_url: $gateway_url,
		  ok_count: $ok_count,
		  error_count: $error_count,
		  first_error: $first_error,
		  last_error: $last_error,
		  checks: ($checks | split("\n") | map(select(length > 0))),
		  errors: ($errors | split("\n") | map(select(length > 0)))
		}' | tee "$summary_json"
}

fatal() {
	record_error "$*"
	write_summary error
	exit 1
}

compose() {
	"$docker_bin" compose \
		--env-file "$repo_root/examples/quickstart/.env.example" \
		-f "$repo_root/examples/quickstart/compose.yaml" \
		-p "${COMPOSE_PROJECT_NAME:-namrbd-quickstart}" \
		"$@"
}

kind_cluster_exists() {
	"$kind_bin" get clusters 2>/dev/null | grep -Fxq "$cluster_name"
}

detect_kind_host_address() {
	if [[ -n "${NAMRBD_KIND_HOST_ADDRESS:-}" ]]; then
		printf '%s' "$NAMRBD_KIND_HOST_ADDRESS"
		return 0
	fi
	local gateway
	gateway="$("$docker_bin" network inspect kind --format '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true)"
	if [[ -n "$gateway" && "$gateway" != "<no value>" ]]; then
		printf '%s' "$gateway"
		return 0
	fi
	printf 'host.docker.internal'
}

format_uri_host() {
	local host="$1"
	case "$host" in
		\[*\]) printf '%s' "$host" ;;
		*:* ) printf '[%s]' "$host" ;;
		*) printf '%s' "$host" ;;
	esac
}

format_host_port() {
	local host="$1" port="$2"
	printf '%s:%s' "$(format_uri_host "$host")" "$port"
}

format_http_url() {
	local host="$1" port="$2"
	printf 'http://%s' "$(format_host_port "$host" "$port")"
}

run_address_fixture() {
	[[ "$(format_host_port 192.0.2.10 19443)" == "192.0.2.10:19443" ]]
	[[ "$(format_host_port host.docker.internal 19443)" == "host.docker.internal:19443" ]]
	[[ "$(format_host_port 'fc00:f853:ccd:e793::1' 19443)" == "[fc00:f853:ccd:e793::1]:19443" ]]
	[[ "$(format_http_url 'fc00:f853:ccd:e793::1' 19701)" == "http://[fc00:f853:ccd:e793::1]:19701" ]]
	[[ "$(format_http_url '[fc00:f853:ccd:e793::1]' 19701)" == "http://[fc00:f853:ccd:e793::1]:19701" ]]
	log "address formatting fixture passed"
}

run_check() {
	require_cmd "$docker_bin"
	require_cmd "$kind_bin"
	require_cmd "$kubectl_bin"
	require_cmd "$helm_bin"
	require_cmd "$jq_bin"
	test -f "$script_dir/kind-cluster.yaml"
	test -f "$script_dir/pvc.yaml"
	test -f "$repo_root/deploy/kubernetes/csi/helm/namrbd-csi/Chart.yaml"
	record_ok "demo prerequisites found"
	write_summary ok
}

start_quickstart_stack() {
	if ! is_true "$start_quickstart"; then
		record_ok "quickstart startup skipped"
		return 0
	fi
	if is_true "$reset_quickstart"; then
		NAMRBD_QUICKSTART_BIND_ADDRESS="$quickstart_bind_address" compose down --volumes --remove-orphans >/dev/null 2>&1 || true
		record_ok "quickstart reset completed"
	fi
	NAMRBD_QUICKSTART_BIND_ADDRESS="$quickstart_bind_address" \
		NAMRBD_QUICKSTART_INCLUDE_GATEWAY=1 \
		NAMRBD_QUICKSTART_CLEANUP_ON_EXIT=0 \
		NAMRBD_QUICKSTART_VOLUME_ID="${NAMRBD_KIND_SEED_VOLUME_ID:-0000c501}" \
		"$repo_root/examples/quickstart/bootstrap-sbs.sh" >"$work_dir/quickstart-summary.json"
	record_ok "quickstart Compose stack is running"
}

create_kind_cluster() {
	if kind_cluster_exists; then
		record_ok "kind cluster already exists"
		return 0
	fi
	"$kind_bin" create cluster --name "$cluster_name" --config "$script_dir/kind-cluster.yaml" >&2
	record_ok "kind cluster created"
}

build_and_load_csi_image() {
	"$docker_bin" build \
		-f "$repo_root/packaging/docker/Dockerfile.sbs" \
		--target namrbd-csi-driver \
		-t "$csi_image" \
		"$repo_root" >&2
	record_ok "CSI image built"
	"$kind_bin" load docker-image "$csi_image" --name "$cluster_name" >&2
	record_ok "CSI image loaded into kind"
}

install_csi_chart() {
	local host_address
	host_address="$(detect_kind_host_address)"
	admin_endpoint="${NAMRBD_KIND_SBS_ADMIN_ENDPOINT:-$(format_host_port "$host_address" "$quickstart_admin_port")}"
	gateway_url="${NAMRBD_KIND_GATEWAY_URL:-$(format_http_url "$host_address" "$quickstart_gateway_port")}"
	"$helm_bin" upgrade --install "$release_name" "$repo_root/deploy/kubernetes/csi/helm/namrbd-csi" \
		--kube-context "$context_name" \
		--namespace "$namespace" \
		--create-namespace \
		--wait \
		--timeout "$timeout" \
		--set "image.repository=${image_repository}" \
		--set "image.tag=${image_tag}" \
		--set "image.pullPolicy=IfNotPresent" \
		--set "config.clusterID=${cluster_id}" \
		--set "config.sbsClusterID=${sbs_cluster_id}" \
		--set "config.adminEndpoint=${admin_endpoint}" \
		--set "config.gatewayURL=${gateway_url}" \
		--set "storageClasses.replicated.volumeBindingMode=Immediate" \
		--set-string "storageClasses.replicated.parameters.replication_factor=1" \
		--set "volumeSnapshotClass.create=false" \
		--set "sidecars.csiAttacher.enabled=false" \
		--set "sidecars.csiSnapshotter.enabled=false" \
		--set "sidecars.csiResizer.enabled=false" \
		--set "node.enabled=false" >&2
	record_ok "CSI chart installed for PVC binding demo"
}

bind_pvc() {
	"$kubectl_bin" --context "$context_name" apply -f "$script_dir/pvc.yaml" >&2
	record_ok "demo PVC applied"
	if ! "$kubectl_bin" --context "$context_name" -n "$pvc_namespace" wait \
		--for=jsonpath='{.status.phase}'=Bound \
		"pvc/${pvc_name}" \
		--timeout="$timeout" >&2; then
		"$kubectl_bin" --context "$context_name" -n "$namespace" get pods -o wide >&2 || true
		"$kubectl_bin" --context "$context_name" -n "$pvc_namespace" describe "pvc/${pvc_name}" >&2 || true
		fatal "PVC did not become Bound"
	fi
	pvc_phase="$("$kubectl_bin" --context "$context_name" -n "$pvc_namespace" get "pvc/${pvc_name}" -o jsonpath='{.status.phase}')"
	pv_name="$("$kubectl_bin" --context "$context_name" -n "$pvc_namespace" get "pvc/${pvc_name}" -o jsonpath='{.spec.volumeName}')"
	volume_handle="$("$kubectl_bin" --context "$context_name" get "pv/${pv_name}" -o jsonpath='{.spec.csi.volumeHandle}')"
	[[ -n "$volume_handle" ]] || fatal "bound PV did not include a CSI volume handle"
	record_ok "PVC bound to PV ${pv_name}"
	if compose run --rm --no-deps sbsctl volume status \
		--admin-endpoint sbs-service:9443 \
		--volume-id "$volume_handle" \
		--output json >"$work_dir/sbs-volume-status.json"; then
		record_ok "SBS volume exists for CSI handle ${volume_handle}"
	else
		fatal "SBS volume status failed for CSI handle ${volume_handle}"
	fi
}

cleanup_demo() {
	if kind_cluster_exists; then
		"$kubectl_bin" --context "$context_name" -n "$pvc_namespace" delete "pvc/${pvc_name}" --ignore-not-found >&2 || true
		"$helm_bin" --kube-context "$context_name" -n "$namespace" uninstall "$release_name" >&2 || true
		record_ok "CSI demo Kubernetes objects cleaned"
		if is_true "$delete_cluster_on_cleanup"; then
			"$kind_bin" delete cluster --name "$cluster_name" >&2 || true
			record_ok "kind cluster deleted"
		fi
	fi
	if is_true "$cleanup_quickstart_on_cleanup"; then
		NAMRBD_QUICKSTART_BIND_ADDRESS="$quickstart_bind_address" compose down --volumes --remove-orphans >&2 || true
		record_ok "quickstart Compose stack cleaned"
	fi
	write_summary ok
}

run_demo() {
	run_check >/dev/null
	start_quickstart_stack
	create_kind_cluster
	build_and_load_csi_image
	install_csi_chart
	bind_pvc
	write_summary ok
}

command_name="${1:-run}"
case "$command_name" in
	run)
		run_demo
		;;
	check)
		run_check
		;;
	cleanup)
		cleanup_demo
		;;
	address-fixture)
		run_address_fixture
		;;
	-h|--help|help)
		usage
		;;
	*)
		usage >&2
		exit 2
		;;
esac
