#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${PHASE_Y_BROWSER_QA_OUT_DIR:-$ROOT_DIR/.cache/phase-y-browser-qa}"
PORT="${PHASE_Y_BROWSER_QA_PORT:-18080}"
PLAYWRIGHT_IMAGE="${PLAYWRIGHT_IMAGE:-mcr.microsoft.com/playwright:v1.55.0-noble}"
RUNTIME_PROFILE="${PHASE_Y_BROWSER_QA_RUNTIME_PROFILE:-}"
if [[ -z "$RUNTIME_PROFILE" ]]; then
	if docker --version 2>&1 | grep -qi podman; then RUNTIME_PROFILE=podman; else RUNTIME_PROFILE=docker; fi
fi
BASELINE="$ROOT_DIR/web/operations-dashboard/browser/screenshots.$RUNTIME_PROFILE.sha256"
UPDATE_BASELINE="${PHASE_Y_BROWSER_QA_UPDATE_BASELINE:-0}"

mkdir -p "$OUT_DIR/screenshots"
rm -f "$OUT_DIR/screenshots"/*.png "$OUT_DIR/browser-evidence.json" "$OUT_DIR/actual.sha256"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$ROOT_DIR/web/operations-dashboard/static" >"$OUT_DIR/http-server.log" 2>&1 &
server_pid=$!
cleanup() { kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; }
trap cleanup EXIT

ready=false
for _ in $(seq 1 30); do
	if curl -fsS "http://127.0.0.1:$PORT/index.html" >/dev/null 2>&1; then ready=true; break; fi
	sleep 0.1
done
if [[ "$ready" != true ]]; then
	printf '{"result":"fail","entrypoint":"phase-y-browser-qa","error_count":1,"first_error":"dashboard HTTP server did not become ready","last_error":"dashboard HTTP server did not become ready"}\n'
	exit 1
fi

docker run --rm --network host \
	-v "$ROOT_DIR/web/operations-dashboard/browser:/qa:ro" -v "$OUT_DIR:/output" \
	-e "DASHBOARD_QA_URL=http://127.0.0.1:$PORT" "$PLAYWRIGHT_IMAGE" bash -lc \
	'npm install --silent --prefix /tmp/qa playwright@1.55.0 && node /qa/browser-qa.cjs'

(cd "$OUT_DIR" && find screenshots -name '*.png' -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) >"$OUT_DIR/actual.sha256"
if [[ "$UPDATE_BASELINE" == 1 ]]; then cp "$OUT_DIR/actual.sha256" "$BASELINE"; fi
if [[ ! -s "$BASELINE" ]]; then
	printf '{"result":"fail","entrypoint":"phase-y-browser-qa","error_count":1,"first_error":"screenshot checksum baseline missing","last_error":"screenshot checksum baseline missing"}\n'; exit 1
fi
if ! diff -u "$BASELINE" "$OUT_DIR/actual.sha256" >"$OUT_DIR/screenshot-diff.txt"; then
	printf '{"result":"fail","entrypoint":"phase-y-browser-qa","browser_render_executed":true,"screenshot_count":3,"screenshot_checksum_verified":false,"error_count":1,"first_error":"screenshot checksum baseline mismatch","last_error":"screenshot checksum baseline mismatch"}\n'; exit 1
fi
jq --arg runtime_profile "$RUNTIME_PROFILE" --arg baseline_path "web/operations-dashboard/browser/screenshots.$RUNTIME_PROFILE.sha256" '. + {result:"ok",entrypoint:"phase-y-browser-qa",browser_render_executed:true,screenshot_count:3,screenshot_checksum_verified:true,runtime_profile:$runtime_profile,baseline_path:$baseline_path,error_count:0,first_error:"",last_error:""}' "$OUT_DIR/browser-evidence.json" | tee "$OUT_DIR/summary.json"
