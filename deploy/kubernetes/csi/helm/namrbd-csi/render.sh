#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
env_file="${1:-${chart_dir}/values.env}"

if [[ -f "$env_file" ]]; then
	# shellcheck disable=SC1090
	source "$env_file"
fi

if ! command -v helm >/dev/null 2>&1; then
	printf '[namrbd-csi-render] error: helm is required for rendering\n' >&2
	exit 1
fi

release_name="${RELEASE_NAME:-namrbd-csi}"
namespace="${NAMESPACE:-namrbd-system}"
discard_mount_options="${DISCARD_MOUNT_OPTIONS:-}"

set_args=(
	--namespace "$namespace"
	--set "namespaceOverride=$namespace"
	--set "driverName=${DRIVER_NAME:-csi.namrbd.io}"
	--set "image.repository=${CSI_DRIVER_IMAGE_REPOSITORY:-ghcr.io/nosway/namrbd-csi-driver}"
	--set "image.tag=${CSI_DRIVER_IMAGE_TAG:-local}"
	--set "image.pullPolicy=${IMAGE_PULL_POLICY:-IfNotPresent}"
	--set "config.clusterID=${CLUSTER_ID:-namrbd-community}"
	--set "config.sbsClusterID=${SBS_CLUSTER_ID:-sbs-community}"
	--set "config.adminEndpoint=${ADMIN_ENDPOINT:-namrbd-sbs-service:9897}"
	--set "config.gatewayURL=${GATEWAY_URL:-http://namrbd-gateway:9701}"
	--set "controller.replicas=${CONTROLLER_REPLICAS:-1}"
	--set "sidecars.csiProvisioner.timeout=${CSI_PROVISIONER_TIMEOUT:-60s}"
	--set "sidecars.csiAttacher.enabled=${CSI_ATTACHER_ENABLED:-true}"
	--set "sidecars.csiSnapshotter.enabled=${CSI_SNAPSHOTTER_ENABLED:-true}"
	--set "sidecars.csiResizer.enabled=${CSI_RESIZER_ENABLED:-true}"
	--set "node.enabled=${CSI_NODE_ENABLED:-true}"
	--set "credentials.enabled=${CREDENTIALS_ENABLED:-false}"
	--set "credentials.existingSecret=${CREDENTIALS_EXISTING_SECRET:-namrbd-csi-credentials}"
	--set "storageClasses.replicated.discard.exposureState=${DISCARD_EXPOSURE_STATE:-disabled}"
	--set "storageClasses.replicated.discard.validationProfile=${DISCARD_VALIDATION_PROFILE:-operator-validated}"
)

if [[ -n "${ADMIN_ENDPOINTS:-}" ]]; then
	set_args+=(--set "config.adminEndpoints=${ADMIN_ENDPOINTS}")
fi

if [[ -n "$discard_mount_options" ]]; then
	set_args+=(--set-json "storageClasses.replicated.discard.mountOptions=${discard_mount_options}")
fi

helm template "$release_name" "$chart_dir" "${set_args[@]}"
