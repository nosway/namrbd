.DEFAULT_GOAL := help

GO ?= go
GOFMT ?= gofmt
GOVULNCHECK ?= govulncheck
GOVULNCHECK_COMMUNITY_FLAGS ?=
GOFLAGS_BASE ?= -buildvcs=false
GOFLAGS_COMMUNITY ?= $(GOFLAGS_BASE)
DOCKER ?= docker
BIN_DIR ?= $(CURDIR)/bin
COMMUNITY_BIN_DIR ?= $(BIN_DIR)/community
CACHE_DIR ?= $(CURDIR)/.cache
GOCACHE ?= $(CACHE_DIR)/go-build
GOMODCACHE ?= $(CACHE_DIR)/gomod
CMD_DIR := ./cmd

COMMUNITY_CMDS := namrbd-gateway namrbdctl sbs-service sbs-data sbsctl namrbd-debug namrbd-iscsi-gateway namrbd-csi-driver namrbd-mcp
COMMUNITY_TEST_PACKAGES := \
	./cmd/namrbd-debug \
	./cmd/namrbd-gateway \
	./cmd/namrbdctl \
	./cmd/sbs-data \
	./cmd/sbs-service \
	./cmd/sbsctl \
	./cmd/namrbd-iscsi-gateway \
	./cmd/namrbd-csi-driver \
	./cmd/namrbd-mcp \
	./gateway/service \
	./internal/csi/driver \
	./internal/mcpops \
	./iscsi \
	./sbs/cluster \
	./sbs/cluster/control \
	./sbs/cluster/metadata \
	./sbs/cluster/payload \
	./sbs/cluster/placement \
	./sbs/cluster/replication \
	./sbs/internalapi/v1 \
	./sbs/local \
	./web/operations-dashboard

CONTAINER_DOCKERFILE_SBS ?= packaging/docker/Dockerfile.sbs
NAMRBD_IMAGE_REGISTRY ?= ghcr.io/nosway
NAMRBD_IMAGE_TAG ?= local
NAMRBD_IMAGE_VERSION ?= $(shell sed -n 's/^const Current = "\(.*\)"/\1/p' version/version.go 2>/dev/null || printf dev)
NAMRBD_IMAGE_REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf unknown)
NAMRBD_IMAGE_CREATED ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
NAMRBD_GATEWAY_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-gateway
NAMRBD_ISCSI_GATEWAY_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-iscsi-gateway
NAMRBD_CSI_DRIVER_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-csi-driver
NAMRBD_SBS_SERVICE_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-sbs-service
NAMRBD_SBS_DATA_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-sbs-data
NAMRBD_SBSCTL_IMAGE ?= $(NAMRBD_IMAGE_REGISTRY)/namrbd-sbsctl
DOCKER_COMPOSE ?= $(DOCKER) compose
QUICKSTART_COMPOSE_FILE ?= examples/quickstart/compose.yaml
QUICKSTART_ENV_FILE ?= examples/quickstart/.env.example
QUICKSTART_PROJECT_NAME ?= namrbd-quickstart
KIND_CSI_DEMO_DIR ?= examples/kind-csi-pvc
KIND_CSI_DEMO_SCRIPT ?= $(KIND_CSI_DEMO_DIR)/run.sh
KIND_CSI_DEMO_CLUSTER_CONFIG ?= $(KIND_CSI_DEMO_DIR)/kind-cluster.yaml
KIND_CSI_DEMO_PVC ?= $(KIND_CSI_DEMO_DIR)/pvc.yaml
JQ ?= jq
GREP ?= grep
MKDOCS ?= mkdocs
MKDOCS_CONFIG ?= mkdocs.yml
MKDOCS_SITE_DIR ?= site
DOCS_SOURCE_DIR ?= docs-src
DOCS_PUBLIC_MANUAL_SOURCE_DIR ?= $(DOCS_SOURCE_DIR)/manuals
CSI_HELM_CHART ?= deploy/kubernetes/csi/helm/namrbd-csi
CSI_LEGACY_TEMPLATE_DIR ?= deploy/kubernetes/csi/templates
CSI_HELM_REQUIRE_HELM ?= false
OBSERVABILITY_SCRAPE_CONFIG ?= deploy/observability/prometheus/namrbd-community-scrape.json
OBSERVABILITY_ALERT_RULES ?= deploy/observability/prometheus/namrbd-community-alerts.json
OBSERVABILITY_METRICS_CATALOG ?= deploy/observability/metrics/namrbd-community-metrics-catalog.json
OBSERVABILITY_GRAFANA_DASHBOARD ?= deploy/observability/grafana/namrbd-community-overview.json

.PHONY: help
help:
	@printf 'NAMRBD Community targets:\n'
	@printf '  make build-community\n'
	@printf '  make test-community\n'
	@printf '  make format-community-check\n'
	@printf '  make vet-community\n'
	@printf '  make govulncheck-community\n'
	@printf '  make kernel-module\n'
	@printf '  make build-namrbd-csi-driver\n'
	@printf '  make build-namrbd-iscsi-gateway\n'
	@printf '  make container-build-community-images\n'
	@printf '  make container-build-sbs\n'
	@printf '  make container-print-namrbd-image-env\n'
	@printf '  make container-print-namros-sbs-env\n'
	@printf '  make quickstart-compose-config\n'
	@printf '  make quickstart-local-sbs-smoke\n'
	@printf '  make quickstart-local-all-smoke\n'
	@printf '  make quickstart-local-down\n'
	@printf '  make quickstart-local-reset\n'
	@printf '  make kind-csi-pvc-demo-check\n'
	@printf '  make kind-csi-pvc-demo\n'
	@printf '  make csi-helm-chart-check\n'
	@printf '  make observability-assets-check\n'
	@printf '  make docs-source-check\n'
	@printf '  make docs-build\n'
	@printf '  make docs-render-check\n'
	@printf '  make web-operations-dashboard-test\n'
	@printf '  make clean\n'

.PHONY: build-community
build-community: $(COMMUNITY_CMDS:%=$(COMMUNITY_BIN_DIR)/%)

$(COMMUNITY_BIN_DIR):
	mkdir -p "$(COMMUNITY_BIN_DIR)"

$(COMMUNITY_BIN_DIR)/%: $(COMMUNITY_BIN_DIR)
	mkdir -p "$(GOCACHE)" "$(GOMODCACHE)"
	GOCACHE="$(GOCACHE)" GOMODCACHE="$(GOMODCACHE)" $(GO) build $(GOFLAGS_COMMUNITY) -o "$@" "$(CMD_DIR)/$(@F)"

.PHONY: test-community
test-community:
	mkdir -p "$(GOCACHE)" "$(GOMODCACHE)"
	GOCACHE="$(GOCACHE)" GOMODCACHE="$(GOMODCACHE)" $(GO) test $(GOFLAGS_COMMUNITY) $(COMMUNITY_TEST_PACKAGES)

.PHONY: format-community-check
format-community-check:
	@files="$$(git ls-files '*.go' | grep -v '^third_party/' || true)"; \
	if [ -z "$$files" ]; then exit 0; fi; \
	unformatted="$$(printf '%s\n' "$$files" | xargs $(GOFMT) -l)"; \
	if [ -n "$$unformatted" ]; then printf '%s\n' "$$unformatted"; exit 1; fi

.PHONY: vet-community
vet-community:
	mkdir -p "$(GOCACHE)" "$(GOMODCACHE)"
	GOCACHE="$(GOCACHE)" GOMODCACHE="$(GOMODCACHE)" $(GO) vet $(GOFLAGS_COMMUNITY) $(COMMUNITY_TEST_PACKAGES)

.PHONY: govulncheck-community
govulncheck-community:
	mkdir -p "$(GOCACHE)" "$(GOMODCACHE)"
	GOCACHE="$(GOCACHE)" GOMODCACHE="$(GOMODCACHE)" $(GOVULNCHECK) $(GOVULNCHECK_COMMUNITY_FLAGS) $(COMMUNITY_TEST_PACKAGES)

.PHONY: build-namrbd-csi-driver
build-namrbd-csi-driver: $(COMMUNITY_BIN_DIR)/namrbd-csi-driver

.PHONY: build-namrbd-iscsi-gateway
build-namrbd-iscsi-gateway: $(COMMUNITY_BIN_DIR)/namrbd-iscsi-gateway

.PHONY: kernel-module
kernel-module:
	$(MAKE) -C kernel/module

.PHONY: web-operations-dashboard-test
web-operations-dashboard-test:
	$(GO) test $(GOFLAGS_COMMUNITY) ./web/operations-dashboard -run TestOperationsDashboardHandler -count=1

.PHONY: container-build-community-images
container-build-community-images: container-build-namrbd-gateway container-build-namrbd-iscsi-gateway container-build-namrbd-csi-driver container-build-sbs

.PHONY: container-build-namrbd-gateway
container-build-namrbd-gateway:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target namrbd-gateway \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_GATEWAY_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-build-namrbd-iscsi-gateway
container-build-namrbd-iscsi-gateway:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target namrbd-iscsi-gateway \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_ISCSI_GATEWAY_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-build-namrbd-csi-driver
container-build-namrbd-csi-driver:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target namrbd-csi-driver \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_CSI_DRIVER_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-build-sbs
container-build-sbs: container-build-sbs-service container-build-sbs-data container-build-sbsctl

.PHONY: container-build-sbs-service
container-build-sbs-service:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target sbs-service \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_SBS_SERVICE_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-build-sbs-data
container-build-sbs-data:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target sbs-data \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_SBS_DATA_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-build-sbsctl
container-build-sbsctl:
	$(DOCKER) build \
		--file "$(CONTAINER_DOCKERFILE_SBS)" \
		--target sbsctl \
		--build-arg VERSION="$(NAMRBD_IMAGE_VERSION)" \
		--build-arg VCS_REF="$(NAMRBD_IMAGE_REVISION)" \
		--build-arg BUILD_DATE="$(NAMRBD_IMAGE_CREATED)" \
		--tag "$(NAMRBD_SBSCTL_IMAGE):$(NAMRBD_IMAGE_TAG)" \
		.

.PHONY: container-print-namros-sbs-env
container-print-namros-sbs-env:
	@printf 'NAMROS_USE_NAMRBD_SBS_IMAGES=1\n'
	@printf 'NAMROS_SBS_SERVICE_IMAGE=%s\n' "$(NAMRBD_SBS_SERVICE_IMAGE)"
	@printf 'NAMROS_SBS_DATA_IMAGE=%s\n' "$(NAMRBD_SBS_DATA_IMAGE)"
	@printf 'NAMROS_SBSCTL_IMAGE=%s\n' "$(NAMRBD_SBSCTL_IMAGE)"
	@printf 'NAMROS_SBS_IMAGE_TAG=%s\n' "$(NAMRBD_IMAGE_TAG)"

.PHONY: container-print-namrbd-image-env
container-print-namrbd-image-env:
	@printf 'NAMRBD_IMAGE_REGISTRY=%s\n' "$(NAMRBD_IMAGE_REGISTRY)"
	@printf 'NAMRBD_IMAGE_TAG=%s\n' "$(NAMRBD_IMAGE_TAG)"
	@printf 'NAMRBD_IMAGE_REVISION=%s\n' "$(NAMRBD_IMAGE_REVISION)"
	@printf 'NAMRBD_GATEWAY_IMAGE=%s\n' "$(NAMRBD_GATEWAY_IMAGE)"
	@printf 'NAMRBD_ISCSI_GATEWAY_IMAGE=%s\n' "$(NAMRBD_ISCSI_GATEWAY_IMAGE)"
	@printf 'NAMRBD_CSI_DRIVER_IMAGE=%s\n' "$(NAMRBD_CSI_DRIVER_IMAGE)"
	@printf 'NAMRBD_SBS_SERVICE_IMAGE=%s\n' "$(NAMRBD_SBS_SERVICE_IMAGE)"
	@printf 'NAMRBD_SBS_DATA_IMAGE=%s\n' "$(NAMRBD_SBS_DATA_IMAGE)"
	@printf 'NAMRBD_SBSCTL_IMAGE=%s\n' "$(NAMRBD_SBSCTL_IMAGE)"

.PHONY: quickstart-compose-config
quickstart-compose-config:
	$(DOCKER_COMPOSE) --env-file "$(QUICKSTART_ENV_FILE)" -f "$(QUICKSTART_COMPOSE_FILE)" -p "$(QUICKSTART_PROJECT_NAME)" config >/dev/null

.PHONY: quickstart-local-sbs-smoke
quickstart-local-sbs-smoke:
	bash examples/quickstart/bootstrap-sbs.sh

.PHONY: quickstart-local-all-smoke
quickstart-local-all-smoke:
	bash examples/quickstart/bootstrap-sbs.sh

.PHONY: quickstart-local-down
quickstart-local-down:
	$(DOCKER_COMPOSE) --env-file "$(QUICKSTART_ENV_FILE)" -f "$(QUICKSTART_COMPOSE_FILE)" -p "$(QUICKSTART_PROJECT_NAME)" down --remove-orphans

.PHONY: quickstart-local-reset
quickstart-local-reset:
	$(DOCKER_COMPOSE) --env-file "$(QUICKSTART_ENV_FILE)" -f "$(QUICKSTART_COMPOSE_FILE)" -p "$(QUICKSTART_PROJECT_NAME)" down --volumes --remove-orphans

.PHONY: kind-csi-pvc-demo-check
kind-csi-pvc-demo-check:
	test -f "$(KIND_CSI_DEMO_SCRIPT)"
	test -x "$(KIND_CSI_DEMO_SCRIPT)"
	test -f "$(KIND_CSI_DEMO_CLUSTER_CONFIG)"
	test -f "$(KIND_CSI_DEMO_PVC)"
	bash -n "$(KIND_CSI_DEMO_SCRIPT)"
	$(GREP) -F 'load docker-image' "$(KIND_CSI_DEMO_SCRIPT)" >/dev/null
	$(GREP) -F 'upgrade --install' "$(KIND_CSI_DEMO_SCRIPT)" >/dev/null
	$(GREP) -F 'storageClasses.replicated.volumeBindingMode=Immediate' "$(KIND_CSI_DEMO_SCRIPT)" >/dev/null
	$(GREP) -F 'node.enabled=false' "$(KIND_CSI_DEMO_SCRIPT)" >/dev/null
	$(GREP) -F 'wait' "$(KIND_CSI_DEMO_SCRIPT)" >/dev/null

.PHONY: kind-csi-pvc-demo
kind-csi-pvc-demo: kind-csi-pvc-demo-check
	bash "$(KIND_CSI_DEMO_SCRIPT)" run

.PHONY: csi-helm-chart-check
csi-helm-chart-check:
	test -f "$(CSI_HELM_CHART)/Chart.yaml"
	test -f "$(CSI_HELM_CHART)/values.yaml"
	test -x "$(CSI_HELM_CHART)/render.sh"
	test -f "$(CSI_HELM_CHART)/values.env.example"
	$(GREP) -R "credentials.enabled" "$(CSI_HELM_CHART)" >/dev/null
	$(GREP) -R "credentials.existingSecret" "$(CSI_HELM_CHART)" >/dev/null
	$(GREP) -R "node.enabled" "$(CSI_HELM_CHART)" >/dev/null
	$(GREP) -R "sidecars.csiSnapshotter.enabled" "$(CSI_HELM_CHART)" >/dev/null
	$(GREP) -R "namrbd.io/discard-validation-profile" "$(CSI_HELM_CHART)" "$(CSI_LEGACY_TEMPLATE_DIR)" >/dev/null
	! $(GREP) -R "DISCARD_GATE_EVIDENCE" deploy/kubernetes/csi
	! $(GREP) -R "__BEARER_TOKEN__\|__TLS_CA_PEM__" "$(CSI_LEGACY_TEMPLATE_DIR)"
	@if command -v helm >/dev/null 2>&1; then \
		helm template namrbd-csi "$(CSI_HELM_CHART)" --namespace namrbd-system >/dev/null; \
	elif [ "$(CSI_HELM_REQUIRE_HELM)" = "true" ]; then \
		printf 'helm is required for csi-helm-chart-check when CSI_HELM_REQUIRE_HELM=true\n' >&2; \
		exit 1; \
	fi

.PHONY: observability-assets-check
observability-assets-check:
	$(JQ) -e '.scrape_configs | length >= 4' "$(OBSERVABILITY_SCRAPE_CONFIG)" >/dev/null
	$(JQ) -e '[.scrape_configs[].job_name] | index("namrbd-sbs-service") and index("namrbd-sbs-data") and index("namrbd-gateway") and index("namrbd-iscsi-gateway")' "$(OBSERVABILITY_SCRAPE_CONFIG)" >/dev/null
	$(JQ) -e '.groups | length >= 2' "$(OBSERVABILITY_ALERT_RULES)" >/dev/null
	$(JQ) -e '[.groups[].rules[].alert] | index("NAMRBDSBSServiceNotReady") and index("NAMRBDSBSDataNotReady") and index("NAMRBDGatewayNotReady") and index("NAMRBDISCSIGatewayNotReady") and index("NAMRBDSBSVolumeDegraded") and index("NAMRBDSBSRepairBacklog") and index("NAMRBDSBSRebalanceBacklog") and index("NAMRBDSBSPlacementApplyFailures")' "$(OBSERVABILITY_ALERT_RULES)" >/dev/null
	! $(JQ) -e '.. | strings | select(. == "sbs_data_store_available_bytes < 10737418240")' "$(OBSERVABILITY_ALERT_RULES)" >/dev/null
	$(JQ) -e '.metrics | length >= 56' "$(OBSERVABILITY_METRICS_CATALOG)" >/dev/null
	$(JQ) -e '[.metrics[].name] | index("sbs_service_ready") and index("sbs_service_leader") and index("sbs_service_repair_backlog_current") and index("sbs_service_rebalance_backlog_current") and index("sbs_service_retired_payload_backlog_chunks") and index("sbs_service_placement_apply_duration_seconds_total") and index("sbs_data_ready") and index("sbs_data_store_available_bytes") and index("namrbd_gateway_ready") and index("namrbd_gateway_io_requests_total") and index("namrbd_iscsi_gateway_ready")' "$(OBSERVABILITY_METRICS_CATALOG)" >/dev/null
	@tmp_dir="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	$(GREP) -Eho '# HELP sbs_(service|data)_[A-Za-z0-9_]+' cmd/sbs-service/main.go cmd/sbs-data/main.go | awk '{ print $$3 }' | sort -u > "$$tmp_dir/emitted"; \
	$(JQ) -r '.metrics[].name' "$(OBSERVABILITY_METRICS_CATALOG)" | sort -u > "$$tmp_dir/catalog"; \
	missing="$$(comm -23 "$$tmp_dir/emitted" "$$tmp_dir/catalog")"; \
	if [ -n "$$missing" ]; then printf 'missing metrics catalog entries:\n%s\n' "$$missing" >&2; exit 1; fi
	$(JQ) -e '.uid == "namrbd-community-overview" and (.panels | length >= 4)' "$(OBSERVABILITY_GRAFANA_DASHBOARD)" >/dev/null

.PHONY: docs-source-check
docs-source-check:
	test -f "$(MKDOCS_CONFIG)"
	test -f "$(DOCS_SOURCE_DIR)/index.md"
	test -f "$(DOCS_SOURCE_DIR)/community-scope.md"
	test -f "$(DOCS_SOURCE_DIR)/quickstart.md"
	test -f "$(DOCS_SOURCE_DIR)/operations.md"
	test -f "$(DOCS_SOURCE_DIR)/observability.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/index.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/installation-guide.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/user-manual.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/admin-guide.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/architecture-manual/index.md"
	test -f "$(DOCS_PUBLIC_MANUAL_SOURCE_DIR)/architecture-manual/chapters/00-reading-guide.md"
	test -f "$(DOCS_SOURCE_DIR)/requirements.txt"
	test -f "$(DOCS_SOURCE_DIR)/assets/namrbd-docs.css"
	$(GREP) -Eq '^docs_dir:[[:space:]]+docs-src$$' "$(MKDOCS_CONFIG)"
	@# md_in_html renders Markdown inside the component <div> blocks the manual
	@# sources rely on. Without it the manual bodies publish as raw text.
	$(GREP) -Eq '^[[:space:]]+- md_in_html$$' "$(MKDOCS_CONFIG)"
	$(GREP) -Eq '^[[:space:]]+- assets/namrbd-docs\.css$$' "$(MKDOCS_CONFIG)"
	@# Page chrome belongs to the theme; embedded wrappers duplicate it.
	@if $(GREP) -rlE 'class="(layout|aside sidebar|section content)"' "$(DOCS_SOURCE_DIR)" --include='*.md' >/dev/null 2>&1; then \
		printf 'embedded page-chrome wrapper found in docs-src; the theme owns layout\n' >&2; \
		$(GREP) -rlE 'class="(layout|aside sidebar|section content)"' "$(DOCS_SOURCE_DIR)" --include='*.md' >&2; \
		exit 1; \
	fi
	@# mkdocs resolves .md sources, not the published .html paths.
	@if $(GREP) -rlE '\]\([^)]*\.html' "$(DOCS_SOURCE_DIR)" --include='*.md' >/dev/null 2>&1; then \
		printf 'internal .html link found in docs-src; link the .md source instead\n' >&2; \
		$(GREP) -rlE '\]\([^)]*\.html' "$(DOCS_SOURCE_DIR)" --include='*.md' >&2; \
		exit 1; \
	fi

.PHONY: docs-build
docs-build: docs-source-check
	$(MKDOCS) build --strict --config-file "$(MKDOCS_CONFIG)"

.PHONY: docs-render-check
docs-render-check: docs-build
	@# `mkdocs build --strict` validates links and nav but not rendered output.
	@# Assert that no Markdown source markers survive into the published pages.
	@python3 tools/check-docs-render.py "$(MKDOCS_SITE_DIR)"

.PHONY: clean
clean:
	rm -rf "$(BIN_DIR)"
