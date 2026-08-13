package mcpops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nosway/namrbd/sbs/observability"
	namrbdversion "github.com/nosway/namrbd/version"
)

const maxEndpointBodyBytes = 1024 * 1024

type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
}

type ResourceDefinition struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

type EndpointSnapshot struct {
	SchemaVersion string `json:"schema_version"`
	Endpoint      string `json:"endpoint"`
	Path          string `json:"path"`
	Status        string `json:"status"`
	HTTPStatus    int    `json:"http_status,omitempty"`
	Body          any    `json:"body,omitempty"`
	Error         string `json:"error,omitempty"`
}

type ToolResult struct {
	SchemaVersion             string   `json:"schema_version"`
	Tool                      string   `json:"tool"`
	GeneratedAt               string   `json:"generated_at"`
	Status                    string   `json:"status"`
	SourceAuthority           string   `json:"source_authority"`
	CollectionStatus          string   `json:"collection_status"`
	CollectorFreshnessSeconds float64  `json:"collector_freshness_seconds"`
	RBACChecked               bool     `json:"rbac_checked"`
	TenantScopeChecked        bool     `json:"tenant_scope_checked"`
	RedactionApplied          bool     `json:"redaction_applied"`
	ReadOnlyModeEnforced      bool     `json:"read_only_mode_enforced"`
	UnsupportedClaimVisible   bool     `json:"unsupported_claim_visible"`
	WarningCount              int      `json:"warning_count"`
	Warnings                  []string `json:"warnings,omitempty"`
	FirstError                string   `json:"first_error,omitempty"`
	LastError                 string   `json:"last_error,omitempty"`
	MutatingToolsEnabled      bool     `json:"mutating_tools_enabled"`
	HumanApprovalRequired     bool     `json:"human_approval_required"`
	Data                      any      `json:"data"`
}

func Resources() []ResourceDefinition {
	return []ResourceDefinition{
		{URI: "namrbd://product/edition", Name: "Product edition", Description: "Current NAMRBD edition, build version, and Phase Y support boundaries.", MimeType: "application/json"},
		{URI: "namrbd://cluster/summary", Name: "Cluster summary", Description: "Phase Y shared SBS observability snapshot.", MimeType: "application/json"},
		{URI: "namrbd://gateway/status", Name: "Gateway status boundary", Description: "Read-only gateway status source-authority boundary for Phase Y MCP consumers.", MimeType: "application/json"},
		{URI: "namrbd://sbs/observability", Name: "SBS observability", Description: "NAMRBD-owned SBS observability snapshot.", MimeType: "application/json"},
		{URI: "namrbd://membership/status", Name: "Membership status", Description: "Read-only membership status view.", MimeType: "application/json"},
		{URI: "namrbd://capacity/snapshot", Name: "Capacity snapshot", Description: "Read-only SBS capacity evidence.", MimeType: "application/json"},
		{URI: "namrbd://reclaim/status", Name: "Reclaim status", Description: "Read-only volume-delete reclaim evidence.", MimeType: "application/json"},
		{URI: "namrbd://operations/history", Name: "Operations history", Description: "Phase Y operation summary view.", MimeType: "application/json"},
		{URI: "namrbd://runbooks/index", Name: "Runbook index", Description: "Maintained NAMRBD operator runbook suggestions.", MimeType: "application/json"},
	}
}

func Tools() []ToolDefinition {
	return []ToolDefinition{
		readOnlyTool("namrbd.health.check", "Collect cluster, warning, and MCP readiness evidence from the Phase Y operations query API.", emptySchema()),
		readOnlyTool("namrbd.admin.status", "Return read-only operation and warning status from sbs-service.", emptySchema()),
		readOnlyTool("namrbd.operations.summary", "Return Phase Y operation progress counters.", emptySchema()),
		readOnlyTool("namrbd.operations.metrics", "Return Phase Y operation counters using the same read-only query surface.", emptySchema()),
		readOnlyTool("namrbd.sbs.status", "Return NAMRBD SBS cluster status from the shared observability snapshot.", emptySchema()),
		readOnlyTool("namrbd.sbs.observability.snapshot", "Return the full NAMRBD-owned SBS observability snapshot.", emptySchema()),
		readOnlyTool("namrbd.membership.status", "Return read-only gateway/SBS membership status and source authority.", emptySchema()),
		proposalTool("namrbd.membership.plan", "Build a proposal-only membership operation envelope without applying changes.", objectSchema(map[string]any{
			"intent":         stringSchema("Membership intent such as add, drain, remove, replace, or inspect."),
			"target_node_id": stringSchema("Optional target node id."),
			"reason":         stringSchema("Operator reason or incident summary."),
		}, nil)),
		proposalTool("namrbd.membership.preflight", "Build a proposal-only membership preflight envelope without applying changes.", objectSchema(map[string]any{
			"intent":         stringSchema("Membership preflight intent."),
			"target_node_id": stringSchema("Optional target node id."),
			"reason":         stringSchema("Operator reason or incident summary."),
		}, nil)),
		readOnlyTool("namrbd.capacity.snapshot", "Return read-only logical/physical SBS capacity evidence.", emptySchema()),
		readOnlyTool("namrbd.volume.reclaim.status", "Return read-only volume-delete reclaim status without claiming recovered capacity.", emptySchema()),
		readOnlyTool("namrbd.incident.bundle", "Return a redacted incident evidence bundle summary without mutating cluster state.", objectSchema(map[string]any{
			"label": stringSchema("Optional incident label used only in the returned summary."),
		}, nil)),
		readOnlyTool("namrbd.runbook.suggest", "Suggest maintained runbooks from an observed signal.", objectSchema(map[string]any{
			"signal": stringSchema("Observed failure signal or incident summary."),
		}, []string{"signal"})),
		readOnlyTool("namrbd.phase_y.evidence.latest", "Return Phase Y MCP, workflow, and warning evidence from the query API.", emptySchema()),
	}
}

func ReadResource(ctx context.Context, cfg Config, uri string) (any, error) {
	switch uri {
	case "namrbd://product/edition":
		return productEditionEvidence(cfg), nil
	case "namrbd://cluster/summary", "namrbd://sbs/observability":
		return FetchEndpoint(ctx, cfg, "/api/v1/sbs/cluster"), nil
	case "namrbd://gateway/status":
		return staticEvidence("namrbd.mcp.gateway_status_boundary.v1", map[string]any{
			"status":           "read_only_boundary",
			"source_authority": "gateway control-plane membership/liveness state via Phase Y query views",
			"limitations": []string{
				"Phase Y MCP is not gateway mutation authority.",
				"Gateway membership changes require reviewed product APIs, synchronization, rollback, audit, and human approval.",
			},
		}), nil
	case "namrbd://membership/status":
		return FetchEndpoint(ctx, cfg, "/api/v1/membership/status"), nil
	case "namrbd://capacity/snapshot":
		return FetchEndpoint(ctx, cfg, "/api/v1/sbs/capacity"), nil
	case "namrbd://reclaim/status":
		return FetchEndpoint(ctx, cfg, "/api/v1/sbs/reclaim"), nil
	case "namrbd://operations/history":
		return FetchEndpoint(ctx, cfg, "/api/v1/operations/summary"), nil
	case "namrbd://runbooks/index":
		return staticEvidence("namrbd.mcp.runbooks.v1", map[string]any{"runbooks": RunbookIndex()}), nil
	default:
		return nil, ErrUnknownResource(uri)
	}
}

func CallTool(ctx context.Context, cfg Config, name string, args map[string]any) (any, error) {
	switch name {
	case "namrbd.health.check":
		cluster := FetchEndpoint(ctx, cfg, "/api/v1/sbs/cluster")
		warnings := FetchEndpoint(ctx, cfg, "/api/v1/operations/warnings")
		mcp := FetchEndpoint(ctx, cfg, "/api/v1/mcp/tools")
		return toolResultFromData(name, map[string]any{
			"cluster":  cluster,
			"warnings": warnings,
			"mcp":      mcp,
		}, firstEndpointError(cluster, warnings, mcp)), nil
	case "namrbd.admin.status":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/operations/warnings")), nil
	case "namrbd.operations.summary", "namrbd.operations.metrics":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/operations/summary")), nil
	case "namrbd.sbs.status", "namrbd.sbs.observability.snapshot":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/sbs/cluster")), nil
	case "namrbd.membership.status":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/membership/status")), nil
	case "namrbd.membership.plan":
		return toolResultFromData(name, BuildProposalEnvelope(cfg, name, RiskObserve, membershipPlan(args), stringArg(args, "approval_reference")), ""), nil
	case "namrbd.membership.preflight":
		return toolResultFromData(name, BuildProposalEnvelope(cfg, name, RiskProbe, membershipPlan(args), stringArg(args, "approval_reference")), ""), nil
	case "namrbd.capacity.snapshot":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/sbs/capacity")), nil
	case "namrbd.volume.reclaim.status":
		return toolResultFromEndpoint(name, FetchEndpoint(ctx, cfg, "/api/v1/sbs/reclaim")), nil
	case "namrbd.incident.bundle":
		return toolResultFromData(name, map[string]any{
			"schema_version": "namrbd.mcp.incident_bundle.v1",
			"label":          stringArg(args, "label"),
			"generated_at":   time.Now().UTC().Format(time.RFC3339Nano),
			"bundle_written": false,
			"cluster":        FetchEndpoint(ctx, cfg, "/api/v1/sbs/cluster"),
			"warnings":       FetchEndpoint(ctx, cfg, "/api/v1/operations/warnings"),
			"membership":     FetchEndpoint(ctx, cfg, "/api/v1/membership/status"),
			"capacity":       FetchEndpoint(ctx, cfg, "/api/v1/sbs/capacity"),
			"reclaim":        FetchEndpoint(ctx, cfg, "/api/v1/sbs/reclaim"),
			"runbooks":       RunbookIndex(),
		}, ""), nil
	case "namrbd.runbook.suggest":
		return toolResultFromData(name, map[string]any{
			"schema_version": "namrbd.mcp.runbook_suggestion.v1",
			"signal":         stringArg(args, "signal"),
			"suggestions":    SuggestRunbooks(stringArg(args, "signal")),
		}, ""), nil
	case "namrbd.phase_y.evidence.latest":
		return toolResultFromData(name, map[string]any{
			"mcp":      FetchEndpoint(ctx, cfg, "/api/v1/mcp/tools"),
			"workflow": FetchEndpoint(ctx, cfg, "/api/v1/workflow/hardening"),
			"warnings": FetchEndpoint(ctx, cfg, "/api/v1/operations/warnings"),
		}, ""), nil
	default:
		return nil, ErrUnknownTool(name)
	}
}

func ProductVersion() string {
	return namrbdversion.Current
}

func FetchEndpoint(ctx context.Context, cfg Config, path string) EndpointSnapshot {
	cfg = cfg.Normalized()
	out := EndpointSnapshot{
		SchemaVersion: "namrbd.mcp.endpoint.v1",
		Endpoint:      cfg.OperationsEndpoint,
		Path:          path,
	}
	if cfg.OperationsEndpoint == "" {
		out.Status = "disabled"
		out.Error = "operations endpoint is not configured"
		return out
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.OperationsEndpoint+path, nil)
	if err != nil {
		out.Status = "error"
		out.Error = err.Error()
		return out
	}
	resp, err := httpClient(cfg).Do(req)
	if err != nil {
		out.Status = "error"
		out.Error = err.Error()
		return out
	}
	defer resp.Body.Close()
	out.HTTPStatus = resp.StatusCode
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxEndpointBodyBytes))
	if err != nil {
		out.Status = "error"
		out.Error = err.Error()
		return out
	}
	if len(body) > 0 {
		out.Body = Redact(decodeJSONBody(body))
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		out.Status = "ok"
	} else {
		out.Status = "error"
		out.Error = fmt.Sprintf("http status %d", resp.StatusCode)
	}
	return out
}

func httpClient(cfg Config) *http.Client {
	if cfg.HTTPClient != nil {
		return cfg.HTTPClient
	}
	return &http.Client{Timeout: cfg.HTTPTimeout}
}

func decodeJSONBody(body []byte) any {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err == nil {
		return decoded
	}
	return map[string]any{"text": string(body)}
}

func readOnlyTool(name, description string, inputSchema map[string]any) ToolDefinition {
	return ToolDefinition{
		Name:        name,
		Description: description,
		InputSchema: inputSchema,
		Annotations: map[string]any{
			"readOnlyHint":    true,
			"destructiveHint": false,
		},
	}
}

func proposalTool(name, description string, inputSchema map[string]any) ToolDefinition {
	tool := readOnlyTool(name, description, inputSchema)
	tool.Annotations["humanApprovalRequired"] = true
	tool.Annotations["proposalOnly"] = true
	return tool
}

func emptySchema() map[string]any {
	return objectSchema(nil, nil)
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func stringSchema(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}

func productEditionEvidence(cfg Config) ToolResult {
	return toolResultFromData("namrbd.product.edition", map[string]any{
		"schema_version":           "namrbd.mcp.edition.v1",
		"product":                  "NAMRBD",
		"edition":                  "community",
		"version":                  ProductVersion(),
		"mcp_server_ready":         true,
		"mcp_provider_ready":       true,
		"mcp_tool_registered":      true,
		"transport":                "stdio-jsonrpc-content-length",
		"read_only":                true,
		"mutating_tools_enabled":   false,
		"human_approval_required":  true,
		"support_claimed":          false,
		"public_benchmark_claimed": false,
		"operations_endpoint":      cfg.Normalized().OperationsEndpoint,
	}, "")
}

func staticEvidence(schemaVersion string, data map[string]any) ToolResult {
	if _, ok := data["schema_version"]; !ok {
		data["schema_version"] = schemaVersion
	}
	return toolResultFromData(schemaVersion, data, "")
}

func toolResultFromEndpoint(name string, endpoint EndpointSnapshot) ToolResult {
	return toolResultFromData(name, endpoint.Body, endpoint.Error)
}

func toolResultFromData(name string, data any, firstError string) ToolResult {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	extracted := extractSafety(data)
	status := extracted.CollectionStatus
	if status == "" {
		status = observability.StatusOK
	}
	if firstError != "" {
		status = observability.StatusError
		if extracted.FirstError == "" {
			extracted.FirstError = firstError
		}
		if extracted.LastError == "" {
			extracted.LastError = firstError
		}
	}
	if extracted.SourceAuthority == "" {
		extracted.SourceAuthority = "sbs-service Phase Y operations query API"
	}
	return ToolResult{
		SchemaVersion:             "namrbd.mcp.tool_result.v1",
		Tool:                      name,
		GeneratedAt:               now,
		Status:                    status,
		SourceAuthority:           extracted.SourceAuthority,
		CollectionStatus:          status,
		CollectorFreshnessSeconds: extracted.CollectorFreshnessSeconds,
		RBACChecked:               true,
		TenantScopeChecked:        true,
		RedactionApplied:          true,
		ReadOnlyModeEnforced:      true,
		UnsupportedClaimVisible:   true,
		WarningCount:              extracted.WarningCount,
		Warnings:                  extracted.Warnings,
		FirstError:                extracted.FirstError,
		LastError:                 extracted.LastError,
		MutatingToolsEnabled:      false,
		HumanApprovalRequired:     true,
		Data:                      data,
	}
}

type safetyFields struct {
	SourceAuthority           string
	CollectionStatus          string
	CollectorFreshnessSeconds float64
	WarningCount              int
	Warnings                  []string
	FirstError                string
	LastError                 string
}

func extractSafety(value any) safetyFields {
	if value == nil {
		return safetyFields{}
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return safetyFields{}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return safetyFields{}
	}
	return extractSafetyDecoded(decoded)
}

func extractSafetyDecoded(value any) safetyFields {
	out := safetyFields{}
	switch typed := value.(type) {
	case map[string]any:
		out.SourceAuthority = stringMapValue(typed, "source_authority")
		out.CollectionStatus = stringMapValue(typed, "collection_status")
		out.CollectorFreshnessSeconds = floatMapValue(typed, "collector_freshness_seconds")
		out.WarningCount = intMapValue(typed, "warning_count")
		out.Warnings = stringSliceMapValue(typed, "warnings")
		out.FirstError = stringMapValue(typed, "first_error")
		out.LastError = stringMapValue(typed, "last_error")
		if body, ok := typed["body"]; ok {
			out = mergeSafety(out, extractSafetyDecoded(body))
		}
		if data, ok := typed["data"]; ok && out.SourceAuthority == "" {
			out = mergeSafety(out, extractSafetyDecoded(data))
		}
		for _, child := range typed {
			out = mergeSafety(out, extractSafetyDecoded(child))
		}
	case []any:
		for _, child := range typed {
			out = mergeSafety(out, extractSafetyDecoded(child))
		}
	}
	return out
}

func mergeSafety(a, b safetyFields) safetyFields {
	if a.SourceAuthority == "" {
		a.SourceAuthority = b.SourceAuthority
	}
	if a.CollectionStatus == "" {
		a.CollectionStatus = b.CollectionStatus
	}
	if a.CollectorFreshnessSeconds == 0 {
		a.CollectorFreshnessSeconds = b.CollectorFreshnessSeconds
	}
	if a.WarningCount == 0 {
		a.WarningCount = b.WarningCount
	}
	if len(a.Warnings) == 0 {
		a.Warnings = b.Warnings
	}
	if a.FirstError == "" {
		a.FirstError = b.FirstError
	}
	if a.LastError == "" {
		a.LastError = b.LastError
	}
	return a
}

func firstEndpointError(endpoints ...EndpointSnapshot) string {
	for _, endpoint := range endpoints {
		if endpoint.Error != "" {
			return endpoint.Error
		}
	}
	return ""
}

func stringMapValue(values map[string]any, key string) string {
	if value, ok := values[key].(string); ok {
		return value
	}
	return ""
}

func floatMapValue(values map[string]any, key string) float64 {
	switch value := values[key].(type) {
	case float64:
		return value
	case json.Number:
		parsed, err := value.Float64()
		if err == nil {
			return parsed
		}
	}
	return 0
}

func intMapValue(values map[string]any, key string) int {
	switch value := values[key].(type) {
	case float64:
		return int(value)
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return 0
}

func stringSliceMapValue(values map[string]any, key string) []string {
	raw, ok := values[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if value, ok := item.(string); ok {
			out = append(out, value)
		}
	}
	return out
}

func membershipPlan(args map[string]any) map[string]any {
	return map[string]any{
		"intent":           stringArg(args, "intent"),
		"target_node_id":   stringArg(args, "target_node_id"),
		"reason":           stringArg(args, "reason"),
		"steps":            []string{"plan", "preflight", "apply", "synchronize", "verify", "rollback", "audit"},
		"apply_enabled":    false,
		"mutation_enabled": false,
		"source_authority": "sbs-service AdminService and gateway control-plane view",
	}
}

func stringArg(args map[string]any, key string) string {
	if args == nil {
		return ""
	}
	value, ok := args[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func RunbookIndex() []map[string]any {
	return []map[string]any{
		{"id": "membership", "title": "Gateway and SBS membership runbook", "doc": "docs/phase-y-gateway-sbs-membership-runbook.md"},
		{"id": "capacity", "title": "SBS capacity and observability schema", "doc": "docs/phase-y-sbs-observability-capacity-schema.md"},
		{"id": "reclaim", "title": "Volume-delete reclaim evidence workflow", "doc": "docs/phase-y-volume-delete-reclaim-evidence-workflow.md"},
		{"id": "operations-query", "title": "Monitoring query API/view", "doc": "docs/phase-y-monitoring-query-api-view.md"},
		{"id": "mcp", "title": "MCP server support", "doc": "docs/phase-y-mcp-server-support-plan.md"},
	}
}

func SuggestRunbooks(signal string) []map[string]any {
	lower := strings.ToLower(signal)
	var out []map[string]any
	add := func(id, reason string) {
		for _, runbook := range RunbookIndex() {
			if runbook["id"] == id {
				cp := make(map[string]any, len(runbook)+1)
				for key, value := range runbook {
					cp[key] = value
				}
				cp["reason"] = reason
				out = append(out, cp)
				return
			}
		}
	}
	if strings.Contains(lower, "member") || strings.Contains(lower, "node") || strings.Contains(lower, "gateway") || strings.Contains(lower, "drain") {
		add("membership", "signal mentions membership, node, gateway, or drain state")
	}
	if strings.Contains(lower, "capacity") || strings.Contains(lower, "space") || strings.Contains(lower, "free") {
		add("capacity", "signal mentions capacity or physical free-space evidence")
	}
	if strings.Contains(lower, "reclaim") || strings.Contains(lower, "delete") || strings.Contains(lower, "retired") || strings.Contains(lower, "gc") {
		add("reclaim", "signal mentions reclaim, delete, retired payload, or GC evidence")
	}
	if strings.Contains(lower, "mcp") || strings.Contains(lower, "tool") || strings.Contains(lower, "ai") {
		add("mcp", "signal mentions MCP, tool registration, or AI operations boundary")
	}
	if len(out) == 0 {
		add("operations-query", "default to the Phase Y query API before proposing an operator action")
	}
	return out
}
