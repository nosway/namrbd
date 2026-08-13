const ENDPOINTS = {
  sbs_cluster: "/api/v1/sbs/cluster",
  sbs_nodes: "/api/v1/sbs/nodes",
  sbs_volumes: "/api/v1/sbs/volumes",
  sbs_maintenance: "/api/v1/sbs/maintenance",
  sbs_capacity: "/api/v1/sbs/capacity",
  sbs_reclaim: "/api/v1/sbs/reclaim",
  membership_status: "/api/v1/membership/status",
  operations_summary: "/api/v1/operations/summary",
  operations_warnings: "/api/v1/operations/warnings",
  query_views: "/api/v1/query/views",
  mcp_tools: "/api/v1/mcp/tools",
  gui_summary: "/api/v1/gui/summary",
  workflow_hardening: "/api/v1/workflow/hardening"
};

const VIEWS = [
  ["overview", "Overview"],
  ["sbs", "SBS"],
  ["capacity", "Capacity"],
  ["volumes", "Volumes"],
  ["maintenance", "Maintenance"],
  ["membership", "Membership"],
  ["warnings", "Warnings"],
  ["evidence", "Evidence"],
  ["settings", "Settings"]
];

const SETTINGS_KEY = "namrbd.operations.dashboard.settings.v1";
const DEFAULT_SETTINGS = {
  refreshIntervalSeconds: 60,
  density: "normal",
  timezone: "local",
  endpointDisplay: "compact"
};

const state = {
  view: currentView(),
  settings: loadSettings(),
  fixture: new URLSearchParams(window.location.search).get("fixture") || "",
  cluster: null,
  lastGoodCluster: null,
  loading: false,
  error: "",
  lastRefreshStartedAt: "",
  lastRefreshCompletedAt: "",
  lastRefreshDurationMs: 0,
  selectedNodeID: "",
  timer: null
};

const app = document.getElementById("app");

init();

function init() {
  window.addEventListener("hashchange", () => {
    state.view = currentView();
    render();
  });
  render();
  refreshDashboard();
  scheduleRefresh();
}

function currentView() {
  const hash = window.location.hash.replace(/^#\/?/, "");
  return VIEWS.some(([id]) => id === hash) ? hash : "overview";
}

function loadSettings() {
  try {
    return { ...DEFAULT_SETTINGS, ...JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}") };
  } catch {
    return { ...DEFAULT_SETTINGS };
  }
}

function saveSettings(next) {
  state.settings = { ...state.settings, ...next };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(state.settings));
  scheduleRefresh();
  render();
}

function scheduleRefresh() {
  if (state.timer) {
    clearInterval(state.timer);
  }
  const seconds = Math.max(Number(state.settings.refreshIntervalSeconds) || 60, 60);
  state.timer = setInterval(refreshDashboard, seconds * 1000);
}

async function refreshDashboard() {
  if (state.loading) {
    return;
  }
  state.loading = true;
  state.error = "";
  state.lastRefreshStartedAt = new Date().toISOString();
  render();
  try {
    const result = await requestJSON("sbs_cluster");
    state.cluster = result.json;
    state.lastGoodCluster = result.json;
    state.lastRefreshDurationMs = result.durationMs;
    state.lastRefreshCompletedAt = new Date().toISOString();
  } catch (err) {
    state.error = err instanceof Error ? err.message : String(err);
    if (state.lastGoodCluster) {
      state.cluster = state.lastGoodCluster;
    }
  } finally {
    state.loading = false;
    render();
  }
}

async function requestJSON(endpointKey) {
  const path = ENDPOINTS[endpointKey];
  const started = performance.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  const url = state.fixture ? `fixtures/${encodeURIComponent(state.fixture)}/${endpointKey}.json` : path;
  try {
    const response = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
      cache: state.fixture ? "no-store" : "default"
    });
    if (!response.ok) {
      throw new Error(`${path} status=${response.status}`);
    }
    const json = await response.json();
    return { json, durationMs: Math.round(performance.now() - started) };
  } finally {
    clearTimeout(timeout);
  }
}

function render() {
  const snapshot = state.cluster || state.lastGoodCluster;
  document.body.classList.toggle("compact", state.settings.density === "compact");
  app.innerHTML = [
    renderTopbar(snapshot),
    `<div class="layout">${renderNav()}<main class="content">${renderCurrentView(snapshot)}</main></div>`
  ].join("");
  attachEvents();
  renderBillboardCharts(snapshot);
}

function renderTopbar(snapshot) {
  const status = statusValue(snapshot);
  const generated = snapshot?.generated_at || "unavailable";
  const cluster = snapshot?.cluster_id || "unknown cluster";
  const sbs = snapshot?.sbs_cluster_id || "unknown SBS";
  const loading = state.loading ? `<span class="pill partial">refreshing</span>` : "";
  const fixture = state.fixture ? `<span class="pill unsupported">fixture ${escapeHTML(state.fixture)}</span>` : "";
  return `
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand">
          <img class="brand-logo" src="assets/namrbd-logo.svg" alt="NAMRBD" />
          <div>
            <div class="brand-title">NAMRBD Operations</div>
            <div class="brand-subtitle">
              <span class="mono">${escapeHTML(cluster)}</span>
              <span>${escapeHTML(sbs)}</span>
              <span>generated ${escapeHTML(formatTime(generated))}</span>
            </div>
          </div>
        </div>
        <div class="top-actions">
          ${statusBadge(status)}
          <span class="pill ok">read-only</span>
          ${fixture}
          ${loading}
          <button class="button" data-action="refresh" title="Refresh dashboard">Refresh</button>
        </div>
      </div>
    </header>
  `;
}

function renderNav() {
  return `
    <nav class="nav" aria-label="Operations dashboard">
      ${VIEWS.map(([id, label]) => `
        <button class="nav-button ${state.view === id ? "active" : ""}" data-view="${id}">
          <span>${escapeHTML(label)}</span>
        </button>
      `).join("")}
    </nav>
  `;
}

function renderCurrentView(snapshot) {
  if (!snapshot) {
    return renderLoadingPage();
  }
  const banner = renderGlobalBanner(snapshot);
  const body = ({
    overview: renderOverview,
    sbs: renderSBS,
    capacity: renderCapacity,
    volumes: renderVolumes,
    maintenance: renderMaintenance,
    membership: renderMembership,
    warnings: renderWarnings,
    evidence: renderEvidence,
    settings: renderSettings
  }[state.view] || renderOverview)(snapshot);
  return banner + body;
}

function renderLoadingPage() {
  const detail = state.error ? `<p class="muted">${escapeHTML(state.error)}</p>` : "";
  return `
    <section class="page-title">
      <h1>Overview</h1>
      <div class="meta">primary API ${ENDPOINTS.sbs_cluster}</div>
    </section>
    <div class="panel">
      <h2>Loading</h2>
      ${detail}
    </div>
  `;
}

function renderGlobalBanner(snapshot) {
  const status = statusValue(snapshot);
  const messages = [];
  if (state.error) {
    messages.push(`refresh error: ${state.error}`);
  }
  if (status !== "ok") {
    messages.push(`collection status: ${status}`);
  }
  if (snapshot.warning_count > 0) {
    messages.push(`warnings: ${snapshot.warning_count}`);
  }
  if (snapshot.first_error) {
    messages.push(`first error: ${snapshot.first_error}`);
  }
  if (snapshot.last_error && snapshot.last_error !== snapshot.first_error) {
    messages.push(`last error: ${snapshot.last_error}`);
  }
  if (!messages.length) {
    return "";
  }
  return `<div class="banner">${messages.map(escapeHTML).join(" | ")}</div>`;
}

function renderOverview(snapshot) {
  const capacity = snapshot.capacity || {};
  const maintenance = snapshot.maintenance || {};
  return `
    ${pageTitle("Overview", `primary snapshot ${ENDPOINTS.sbs_cluster}`)}
    <section class="status-strip">
      ${statusTile("Cluster", statusValue(snapshot), snapshot.ready ? "ready" : "not ready")}
      ${statusTile("SBS Nodes", nodeHealthStatus(snapshot), `${count(snapshot.nodes)} nodes`)}
      ${statusTile("Volumes", volumeStatus(snapshot), `${count(snapshot.volumes)} volumes`)}
      ${statusTile("Capacity", capacityStatus(capacity), bytes(capacity.physical_free_bytes) + " free")}
      ${statusTile("Reclaim", reclaimStatus(snapshot.reclaim), bytes(snapshot.reclaim?.pending_bytes || 0) + " pending")}
      ${statusTile("Membership", membershipStatus(snapshot.membership), `${snapshot.membership?.healthy_nodes || 0} healthy`)}
      ${statusTile("Operations", operationsStatus(snapshot.operations), `${snapshot.operations?.running || 0} running`)}
      ${statusTile("Workflow", workflowStatus(snapshot.workflow), "dangerous actions blocked")}
    </section>
    <section class="grid two" style="margin-top:12px">
      ${panel("Cluster Health", `
        <div class="chart-row">
          ${healthRing(statusValue(snapshot), snapshot.warning_count || 0)}
          <dl class="kv">
            ${kv("Source", snapshot.source_authority)}
            ${kv("Freshness", `${round(snapshot.collector_freshness_seconds)}s`)}
            ${kv("Leader", snapshot.leader_node_id || "unknown")}
            ${kv("Runtime", snapshot.runtime_mode || "unknown")}
            ${kv("RBAC/redaction", boolLabel(snapshot.rbac_checked && snapshot.redaction_applied))}
          </dl>
        </div>
      `)}
      ${panel("Capacity", `
        <div class="bb-chart" data-bb-chart="capacity">${capacityStack(capacity)}</div>
        ${capacityLegend(capacity)}
      `)}
    </section>
    <section class="grid two" style="margin-top:12px">
      ${panel("SBS Node Topology", nodeGrid(snapshot.nodes || []))}
      ${panel("Maintenance Backlog", `
        <div class="bb-chart" data-bb-chart="maintenance">
          ${barList([
            ["repair", maintenance.repair_backlog || 0],
            ["rebalance", maintenance.rebalance_backlog || 0],
            ["drain", maintenance.drain_backlog || 0],
            ["failed", maintenance.transition_failed_batches || 0]
          ])}
        </div>
      `)}
    </section>
    <section class="grid two" style="margin-top:12px">
      ${panel("Warnings", warningCards(snapshot))}
      ${panel("Read-Only Envelope", safetyEnvelope(snapshot))}
    </section>
  `;
}

function renderSBS(snapshot) {
  const nodes = snapshot.nodes || [];
  const stores = snapshot.stores || [];
  const selected = nodes.find((node) => node.node_id === state.selectedNodeID) || nodes[0];
  return `
    ${pageTitle("SBS", "node and store evidence")}
    <section class="grid two">
      ${panel("Nodes", nodeGrid(nodes))}
      ${panel("Selected Node", selected ? nodeDetail(selected) : `<span class="muted">No node records</span>`)}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Stores</h2>
      ${storeTable(stores)}
    </section>
  `;
}

function renderCapacity(snapshot) {
  const capacity = snapshot.capacity || {};
  return `
    ${pageTitle("Capacity", capacity.source || "capacity evidence")}
    <section class="grid two">
      ${panel("Physical Capacity", `
        <div class="bb-chart" data-bb-chart="capacity">${capacityStack(capacity)}</div>
        ${capacityLegend(capacity)}
      `)}
      ${panel("Reclaim Evidence", reclaimDetail(snapshot.reclaim || {}))}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Node Pressure</h2>
      ${nodeCapacityTable(snapshot.nodes || [])}
    </section>
  `;
}

function renderVolumes(snapshot) {
  return `
    ${pageTitle("Volumes", "volume status and protocol boundary")}
    <section class="grid two">
      ${panel("Backend Distribution", backendDistribution(snapshot.volumes || []))}
      ${panel("Protocol Boundary", protocolBoundary(snapshot.volumes || []))}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Volumes</h2>
      ${volumeTable(snapshot.volumes || [])}
    </section>
  `;
}

function renderMaintenance(snapshot) {
  const maintenance = snapshot.maintenance || {};
  const operations = snapshot.operations || {};
  return `
    ${pageTitle("Maintenance", "repair, rebalance, drain, and operations")}
    <section class="grid two">
      ${panel("Backlog", barList([
        ["repair", maintenance.repair_backlog || 0],
        ["rebalance", maintenance.rebalance_backlog || 0],
        ["drain", maintenance.drain_backlog || 0],
        ["transition failures", maintenance.transition_failed_batches || 0],
        ["probe failures", maintenance.nodes_with_probe_failures || 0]
      ]))}
      ${panel("Operations", operationCounters(operations))}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Failure Signals</h2>
      ${failureSignals(snapshot)}
    </section>
  `;
}

function renderMembership(snapshot) {
  const membership = snapshot.membership || {};
  return `
    ${pageTitle("Membership", membership.source_authority || "membership status")}
    <section class="grid two">
      ${panel("Source Authority", sourceAuthorityDiagram(membership))}
      ${panel("Counts", membershipCounts(membership))}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Runbook Envelope</h2>
      ${runbookStepper(membership.steps || [])}
      <div style="margin-top:12px">${actionEnvelope(membership)}</div>
    </section>
  `;
}

function renderWarnings(snapshot) {
  return `
    ${pageTitle("Warnings", "warning and error drill-down")}
    <section class="grid two">
      ${panel("Warning Summary", warningCards(snapshot))}
      ${panel("Limitations", listBlock(snapshot.limitations || ["No limitations reported"]))}
    </section>
    <section class="panel" style="margin-top:12px">
      <h2>Warnings</h2>
      ${listBlock((snapshot.warnings || []).length ? snapshot.warnings : ["No warnings reported"])}
    </section>
  `;
}

function renderEvidence(snapshot) {
  const query = snapshot.query || {};
  const mcp = snapshot.mcp || {};
  const gui = snapshot.gui || {};
  const workflow = snapshot.workflow || {};
  return `
    ${pageTitle("Evidence", "query, MCP, GUI, and workflow descriptors")}
    <section class="grid two">
      ${panel("Query API", descriptorTable({
        registered: query.query_api_registered,
        contract: query.data_contract_version,
        view: query.observability_view_id,
        read_only: query.read_only,
        raw_log_fallback: query.raw_log_fallback
      }))}
      ${panel("MCP", descriptorTable({
        registered: mcp.mcp_tool_registered,
        read_only: mcp.read_only,
        mutating_tools_enabled: mcp.mutating_tools_enabled,
        human_approval_required: mcp.human_approval_required,
        transport: mcp.transport
      }))}
      ${panel("GUI", descriptorTable({
        route: gui.gui_route || "/console/",
        ready: gui.gui_console_ready,
        contract_ready: gui.gui_view_contract_ready,
        read_only: gui.read_only_mode_enforced,
        mutation_controls_hidden: gui.mutation_controls_hidden
      }))}
      ${panel("Workflow", descriptorTable({
        hardened: workflow.operator_workflow_hardened,
        evidence_bundle_ready: workflow.evidence_bundle_ready,
        dangerous_actions_blocked: workflow.dangerous_actions_blocked,
        ai_context_redacted: workflow.ai_context_redacted
      }))}
    </section>
  `;
}

function renderSettings() {
  return `
    ${pageTitle("Settings", "local dashboard preferences")}
    <section class="panel">
      <h2>Preferences</h2>
      ${settingSelect("Refresh interval", "refreshIntervalSeconds", [
        ["60", "60 seconds"],
        ["120", "120 seconds"],
        ["300", "300 seconds"]
      ], String(state.settings.refreshIntervalSeconds))}
      ${settingSelect("Density", "density", [
        ["normal", "Normal"],
        ["compact", "Compact"]
      ], state.settings.density)}
      ${settingSelect("Timezone", "timezone", [
        ["local", "Local"],
        ["utc", "UTC"]
      ], state.settings.timezone)}
      ${settingSelect("Endpoint display", "endpointDisplay", [
        ["compact", "Compact"],
        ["full", "Full"]
      ], state.settings.endpointDisplay)}
    </section>
  `;
}

function attachEvents() {
  document.querySelectorAll("[data-view]").forEach((button) => {
    button.addEventListener("click", () => {
      window.location.hash = button.dataset.view;
    });
  });
  document.querySelectorAll("[data-action='refresh']").forEach((button) => {
    button.addEventListener("click", refreshDashboard);
  });
  document.querySelectorAll("[data-node-id]").forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedNodeID = node.dataset.nodeId || "";
      render();
    });
  });
  document.querySelectorAll("[data-setting]").forEach((select) => {
    select.addEventListener("change", () => {
      const key = select.dataset.setting;
      const value = key === "refreshIntervalSeconds" ? Number(select.value) : select.value;
      saveSettings({ [key]: value });
    });
  });
}

function renderBillboardCharts(snapshot) {
  if (!snapshot || !window.bb || typeof window.bb.generate !== "function") {
    return;
  }
  const capacityTarget = document.querySelector("[data-bb-chart='capacity']");
  if (capacityTarget) {
    const capacity = snapshot.capacity || {};
    window.bb.generate({
      bindto: capacityTarget,
      data: {
        type: "donut",
        columns: [
          ["used", capacity.physical_used_bytes || 0],
          ["free", capacity.physical_free_bytes || 0],
          ["unknown", capacity.unknown_bytes || 0],
          ["reclaimable", capacity.reclaimable_bytes || 0]
        ]
      },
      donut: { title: "Capacity" },
      legend: { position: "right" },
      size: { height: 210 }
    });
  }
  const maintenanceTarget = document.querySelector("[data-bb-chart='maintenance']");
  if (maintenanceTarget) {
    const maintenance = snapshot.maintenance || {};
    window.bb.generate({
      bindto: maintenanceTarget,
      data: {
        type: "bar",
        columns: [
          ["backlog", maintenance.repair_backlog || 0, maintenance.rebalance_backlog || 0, maintenance.drain_backlog || 0, maintenance.transition_failed_batches || 0]
        ]
      },
      axis: {
        x: {
          type: "category",
          categories: ["repair", "rebalance", "drain", "failed"]
        }
      },
      legend: { show: false },
      size: { height: 210 }
    });
  }
}

function pageTitle(title, meta) {
  return `
    <section class="page-title">
      <h1>${escapeHTML(title)}</h1>
      <div class="meta">${escapeHTML(meta || "")}</div>
    </section>
  `;
}

function panel(title, body) {
  return `<section class="panel"><h2>${escapeHTML(title)}</h2>${body}</section>`;
}

function metric(label, value, detail) {
  return `
    <div class="metric">
      <div class="metric-label">${escapeHTML(label)}</div>
      <div class="metric-value">${escapeHTML(String(value))}</div>
      <div class="metric-detail">${escapeHTML(detail || "")}</div>
    </div>
  `;
}

function statusTile(name, status, value) {
  return `
    <div class="status-tile">
      <div class="name">${escapeHTML(name)}</div>
      <div class="value">${escapeHTML(value)}</div>
      <div style="margin-top:8px">${statusBadge(status)}</div>
    </div>
  `;
}

function statusBadge(status) {
  const clean = statusClass(status);
  return `<span class="badge ${clean}">${escapeHTML(clean)}</span>`;
}

function capacityStack(capacity) {
  const items = [
    ["used", capacity.physical_used_bytes || 0, "#1f6fb2"],
    ["free", capacity.physical_free_bytes || 0, "#16835b"],
    ["unknown", capacity.unknown_bytes || 0, "#a96905"],
    ["reclaimable", capacity.reclaimable_bytes || 0, "#087d8f"]
  ];
  const total = items.reduce((sum, item) => sum + item[1], 0) || 1;
  return `
    <div class="stack" role="img" aria-label="capacity stacked bar">
      ${items.map(([label, value, color]) => {
        const width = Math.max((value / total) * 100, value > 0 ? 1 : 0);
        return `<span title="${escapeHTML(label)} ${bytes(value)}" style="width:${width}%;background:${color}"></span>`;
      }).join("")}
    </div>
  `;
}

function capacityLegend(capacity) {
  const rows = [
    ["used", capacity.physical_used_bytes || 0, "#1f6fb2"],
    ["free", capacity.physical_free_bytes || 0, "#16835b"],
    ["unknown", capacity.unknown_bytes || 0, "#a96905"],
    ["reclaimable", capacity.reclaimable_bytes || 0, "#087d8f"],
    ["logical", capacity.logical_bytes || 0, "#6750a4"]
  ];
  return `<div class="legend">${rows.map(([label, value, color]) => `
    <span class="legend-item"><span class="swatch" style="background:${color}"></span>${escapeHTML(label)} ${bytes(value)}</span>
  `).join("")}</div>`;
}

function healthRing(status, warnings) {
  const color = statusClass(status) === "ok" ? "#16835b" : statusClass(status) === "error" ? "#bf3d33" : "#a96905";
  return `
    <svg class="ring" viewBox="0 0 160 160" role="img" aria-label="cluster health ring">
      <circle cx="80" cy="80" r="62" fill="none" stroke="#d8e0e5" stroke-width="16"></circle>
      <circle cx="80" cy="80" r="62" fill="none" stroke="${color}" stroke-width="16" stroke-dasharray="389" stroke-dashoffset="0" transform="rotate(-90 80 80)"></circle>
      <text x="80" y="76" text-anchor="middle">${escapeHTML(statusClass(status))}</text>
      <text x="80" y="96" text-anchor="middle">${warnings} warnings</text>
    </svg>
  `;
}

function nodeGrid(nodes) {
  if (!nodes.length) {
    return `<span class="muted">No node records</span>`;
  }
  return `<div class="node-grid">${nodes.map((node) => {
    const selected = node.node_id === state.selectedNodeID;
    return `
      <div class="node ${selected ? "selected" : ""}" role="button" tabindex="0" data-node-id="${escapeAttr(node.node_id)}">
        <div class="node-id">${escapeHTML(node.node_id)}</div>
        <div class="node-sub">${statusBadge(node.health || "unknown")} ${escapeHTML(node.lifecycle || "unknown")}</div>
      </div>
    `;
  }).join("")}</div>`;
}

function nodeDetail(node) {
  return `
    <div class="drawer">
      <dl class="kv">
        ${kv("Node", node.node_id)}
        ${kv("Health", node.health)}
        ${kv("Lifecycle", node.lifecycle)}
        ${kv("Zone", node.zone || "unavailable")}
        ${kv("Host", node.host || "unavailable")}
        ${kv("Stores", `${node.healthy_store_count || 0}/${node.store_count || 0} healthy`)}
        ${kv("Writable", String(node.writable_store_count || 0))}
        ${kv("Allocatable", String(node.allocatable_store_count || 0))}
        ${kv("Capacity", bytes(node.capacity_bytes || 0))}
        ${kv("Used", bytes(node.used_bytes || 0))}
        ${kv("Admin HTTP", boolLabel(node.admin_http_endpoint_configured))}
      </dl>
    </div>
  `;
}

function storeTable(stores) {
  return table(["Node", "Stores", "Healthy", "Writable", "Allocatable", "Used", "Free", "Eligible"], stores.map((store) => [
    store.node_id,
    store.store_count,
    store.healthy_store_count,
    store.writable_store_count,
    store.allocatable_store_count,
    bytes(store.used_bytes || 0),
    bytes(store.available_bytes || 0),
    boolLabel(store.placement_eligible)
  ]));
}

function nodeCapacityTable(nodes) {
  return table(["Node", "Health", "Lifecycle", "Used", "Capacity", "Pressure"], nodes.map((node) => {
    const capacity = node.capacity_bytes || 0;
    const used = node.used_bytes || 0;
    const pressure = capacity > 0 ? Math.round((used / capacity) * 100) : 0;
    return [node.node_id, node.health, node.lifecycle, bytes(used), bytes(capacity), `${pressure}%`];
  }));
}

function volumeTable(volumes) {
  return table(["Volume", "Status", "Backend", "Logical", "Chunk", "Replication", "Policy"], volumes.map((volume) => [
    volume.volume_id,
    volume.status,
    volume.redundancy_backend || "replicated",
    bytes(volume.size_bytes || 0),
    bytes(volume.chunk_size_bytes || 0),
    volume.replication_factor || "n/a",
    volume.protection_policy || "none"
  ]));
}

function backendDistribution(volumes) {
  const counts = {};
  for (const volume of volumes) {
    const key = volume.redundancy_backend || "replicated";
    counts[key] = (counts[key] || 0) + 1;
  }
  const rows = Object.entries(counts);
  if (!rows.length) {
    return `<span class="muted">No volume records</span>`;
  }
  return barList(rows);
}

function protocolBoundary(volumes) {
  const exportedVolumes = volumes.filter((volume) => volume.iscsi_exported).length;
  const exportCount = exportedVolumes || 0;
  const communityStatus = exportCount <= 3 ? "ok" : "unsupported";
  return `
    <dl class="kv">
      ${kv("Basic iSCSI gateway", "Community")}
      ${kv("sbsctl iscsi", "Community")}
      ${kv("Basic LUN export", "Community")}
      ${kv("Distinct exports", `${exportCount}/3 Community cap`)}
      ${kv("3+ exports", "Enterprise only")}
      ${kv("HA MPIO ALUA", "Enterprise only")}
      ${kv("Boundary", statusClass(communityStatus))}
    </dl>
  `;
}

function operationCounters(operations) {
  return `
    <div class="grid four">
      ${metric("Total", operations.total || 0, "all operations")}
      ${metric("Running", operations.running || 0, "in progress")}
      ${metric("Failed", operations.failed || 0, "needs triage")}
      ${metric("Completed", operations.completed || 0, "finished")}
    </div>
  `;
}

function reclaimDetail(reclaim) {
  return `
    <dl class="kv">
      ${kv("Source", reclaim.source || "sbs-service retired payload backlog")}
      ${kv("Pending chunks", String(reclaim.pending_chunks || 0))}
      ${kv("Pending bytes", bytes(reclaim.pending_bytes || 0))}
      ${kv("Failed batches", String(reclaim.failed_batches || 0))}
      ${kv("Protected refs", boolLabel(reclaim.protected_reference_check_passed))}
      ${kv("Completed claimed", boolLabel(reclaim.completed_claimed))}
      ${kv("Evidence required", boolLabel(reclaim.evidence_required))}
      ${kv("Blocked reason", reclaim.blocked_reason || "none")}
    </dl>
  `;
}

function warningCards(snapshot) {
  return `
    <div class="grid two">
      ${metric("Warning count", snapshot.warning_count || 0, snapshot.collection_status || "unknown")}
      ${metric("First error", snapshot.first_error || "none", "source collector")}
      ${metric("Last error", snapshot.last_error || "none", "source collector")}
      ${metric("Last refresh", state.lastRefreshCompletedAt ? formatTime(state.lastRefreshCompletedAt) : "none", `${state.lastRefreshDurationMs || 0}ms`)}
    </div>
  `;
}

function failureSignals(snapshot) {
  const maintenance = snapshot.maintenance || {};
  return descriptorTable({
    first_error: snapshot.first_error || "none",
    last_error: snapshot.last_error || "none",
    transition_failed_batches: maintenance.transition_failed_batches || 0,
    transition_oldest_failed_batch_age_seconds: maintenance.transition_oldest_failed_batch_age_seconds || 0,
    nodes_with_probe_failures: maintenance.nodes_with_probe_failures || 0,
    max_consecutive_probe_failures: maintenance.max_consecutive_probe_failures || 0
  });
}

function sourceAuthorityDiagram(membership) {
  return `
    <div class="source-diagram">
      <div class="source-box">
        <strong>Gateway control plane</strong>
        <p class="muted">${escapeHTML(membership.gateway_membership_source_authority || "gateway membership and liveness")}</p>
      </div>
      <div class="source-box">
        <strong>sbs-service AdminService</strong>
        <p class="muted">${escapeHTML(membership.sbs_membership_source_authority || "node and topology membership")}</p>
      </div>
      <div class="source-box">
        <strong>sbs-data health</strong>
        <p class="muted">capacity and health evidence only</p>
      </div>
    </div>
  `;
}

function membershipCounts(membership) {
  return `
    <div class="grid three">
      ${metric("Active", membership.active_nodes || 0, "lifecycle")}
      ${metric("Draining", membership.draining_nodes || 0, "lifecycle")}
      ${metric("Removed", membership.removed_nodes || 0, "lifecycle")}
      ${metric("Healthy", membership.healthy_nodes || 0, "health")}
      ${metric("Suspect", membership.suspect_nodes || 0, "health")}
      ${metric("Down", membership.down_nodes || 0, "health")}
    </div>
  `;
}

function runbookStepper(steps) {
  return `<div class="stepper">${steps.map((step, index) => `
    <div class="step">
      <div class="index">${index + 1}</div>
      <strong>${escapeHTML(step)}</strong>
      <div class="muted">read-only</div>
    </div>
  `).join("")}</div>`;
}

function actionEnvelope(membership) {
  return `
    <dl class="kv">
      ${kv("Mutation apply", membership.mutation_apply_enabled ? "enabled" : "disabled")}
      ${kv("Human approval", membership.human_approval_required ? "required" : "not required")}
      ${kv("Emergency lock", "read-only")}
      ${kv("Apply control", "unavailable")}
    </dl>
  `;
}

function safetyEnvelope(snapshot) {
  return descriptorTable({
    read_only_mode_enforced: snapshot.read_only_mode_enforced,
    unsupported_claim_visible: snapshot.unsupported_claim_visible,
    rbac_checked: snapshot.rbac_checked,
    tenant_scope_checked: snapshot.tenant_scope_checked,
    redaction_applied: snapshot.redaction_applied,
    support_claimed: snapshot.support_claimed,
    public_gui_claimed: snapshot.public_gui_claimed,
    public_benchmark_claimed: snapshot.public_benchmark_claimed
  });
}

function descriptorTable(values) {
  return `
    <dl class="kv">
      ${Object.entries(values).map(([key, value]) => kv(key, value)).join("")}
    </dl>
  `;
}

function settingSelect(label, key, options, selected) {
  return `
    <div class="settings-row">
      <label for="setting-${escapeAttr(key)}">${escapeHTML(label)}</label>
      <select id="setting-${escapeAttr(key)}" data-setting="${escapeAttr(key)}">
        ${options.map(([value, text]) => `<option value="${escapeAttr(value)}" ${value === selected ? "selected" : ""}>${escapeHTML(text)}</option>`).join("")}
      </select>
    </div>
  `;
}

function table(headers, rows) {
  if (!rows.length) {
    return `<span class="muted">No records</span>`;
  }
  return `
    <div class="table-wrap">
      <table>
        <thead><tr>${headers.map((head) => `<th>${escapeHTML(head)}</th>`).join("")}</tr></thead>
        <tbody>${rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHTML(String(cell ?? ""))}</td>`).join("")}</tr>`).join("")}</tbody>
      </table>
    </div>
  `;
}

function barList(rows) {
  const max = Math.max(1, ...rows.map((row) => Number(row[1]) || 0));
  return `<div class="bar-list">${rows.map(([label, value]) => {
    const n = Number(value) || 0;
    return `
      <div class="bar-row">
        <div>${escapeHTML(String(label))}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${Math.max(0, (n / max) * 100)}%"></div></div>
        <div class="mono">${escapeHTML(String(n))}</div>
      </div>
    `;
  }).join("")}</div>`;
}

function listBlock(items) {
  return `<ul>${items.map((item) => `<li>${escapeHTML(String(item))}</li>`).join("")}</ul>`;
}

function kv(key, value) {
  return `<dt>${escapeHTML(String(key))}</dt><dd>${escapeHTML(String(value ?? ""))}</dd>`;
}

function bytes(value) {
  const n = Number(value) || 0;
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let scaled = n;
  let index = 0;
  while (scaled >= 1024 && index < units.length - 1) {
    scaled /= 1024;
    index++;
  }
  return `${scaled >= 10 || index === 0 ? Math.round(scaled) : scaled.toFixed(1)} ${units[index]}`;
}

function count(items) {
  return Array.isArray(items) ? items.length : 0;
}

function statusValue(snapshot) {
  if (!snapshot) {
    return "partial";
  }
  if (state.error) {
    return "stale";
  }
  return snapshot.collection_status || "partial";
}

function statusClass(status) {
  const value = String(status || "partial").toLowerCase();
  if (["ok", "partial", "degraded", "error", "stale", "unsupported"].includes(value)) {
    return value;
  }
  if (["healthy", "active", "ready", "true"].includes(value)) {
    return "ok";
  }
  if (["down", "failed", "false"].includes(value)) {
    return "error";
  }
  if (["suspect", "draining", "warning"].includes(value)) {
    return "degraded";
  }
  return "partial";
}

function nodeHealthStatus(snapshot) {
  const nodes = snapshot.nodes || [];
  if (!nodes.length) {
    return "partial";
  }
  if (nodes.some((node) => node.health === "down")) {
    return "error";
  }
  if (nodes.some((node) => node.health === "suspect")) {
    return "degraded";
  }
  return "ok";
}

function volumeStatus(snapshot) {
  const volumes = snapshot.volumes || [];
  if (!volumes.length) {
    return "partial";
  }
  return volumes.some((volume) => volume.status && volume.status !== "healthy") ? "degraded" : "ok";
}

function capacityStatus(capacity) {
  const total = capacity.total_bytes || 0;
  const free = capacity.physical_free_bytes || 0;
  if (!total) {
    return "partial";
  }
  return free / total < 0.1 ? "degraded" : "ok";
}

function reclaimStatus(reclaim) {
  if (!reclaim) {
    return "partial";
  }
  if (reclaim.failed_batches > 0 || reclaim.blocked_reason) {
    return "degraded";
  }
  return "ok";
}

function membershipStatus(membership) {
  if (!membership) {
    return "partial";
  }
  if (membership.down_nodes > 0) {
    return "error";
  }
  if (membership.suspect_nodes > 0 || membership.draining_nodes > 0) {
    return "degraded";
  }
  return "ok";
}

function operationsStatus(operations) {
  if (!operations) {
    return "partial";
  }
  if (operations.failed > 0) {
    return "degraded";
  }
  return "ok";
}

function workflowStatus(workflow) {
  if (!workflow) {
    return "partial";
  }
  return workflow.dangerous_actions_blocked ? "ok" : "error";
}

function boolLabel(value) {
  return value ? "true" : "false";
}

function round(value) {
  const n = Number(value) || 0;
  return Math.round(n * 10) / 10;
}

function formatTime(value) {
  if (!value || value === "unavailable") {
    return "unavailable";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  if (state.settings.timezone === "utc") {
    return date.toISOString();
  }
  return date.toLocaleString();
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHTML(value);
}
