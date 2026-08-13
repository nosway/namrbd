package mcpops

import (
	"fmt"
	"strings"
	"time"
)

const (
	RiskObserve     = "observe"
	RiskProbe       = "probe"
	RiskRepair      = "repair"
	RiskProtect     = "protect"
	RiskDestructive = "destructive"
)

type OperationEnvelope struct {
	SchemaVersion string         `json:"schema_version"`
	OperationID   string         `json:"operation_id"`
	Tool          string         `json:"tool"`
	RiskClass     string         `json:"risk_class"`
	Mode          string         `json:"mode"`
	Approval      ApprovalState  `json:"approval"`
	Plan          map[string]any `json:"plan"`
	Preflight     map[string]any `json:"preflight"`
	Result        map[string]any `json:"result"`
	Verification  map[string]any `json:"verification"`
	Audit         map[string]any `json:"audit"`
}

type ApprovalState struct {
	Required  bool   `json:"required"`
	Policy    string `json:"policy"`
	Reference string `json:"reference,omitempty"`
}

func BuildProposalEnvelope(cfg Config, tool, riskClass string, plan map[string]any, approvalReference string) OperationEnvelope {
	cfg = cfg.Normalized()
	return OperationEnvelope{
		SchemaVersion: "namrbd.mcp.operation.v1",
		OperationID:   newOperationID(tool),
		Tool:          tool,
		RiskClass:     riskClass,
		Mode:          cfg.Mode,
		Approval: ApprovalState{
			Required:  true,
			Policy:    cfg.ApprovalPolicy,
			Reference: approvalReference,
		},
		Plan: plan,
		Preflight: map[string]any{
			"status":                  "proposal_only",
			"reviewed_api_available":  false,
			"apply_enabled":           false,
			"human_approval_required": true,
		},
		Result: map[string]any{
			"status":  "blocked",
			"message": "Phase Y MCP tools are read-only/proposal-only until a reviewed NAMRBD API, RBAC rule, audit record, rollback behavior, and human approval gate exist.",
		},
		Verification: map[string]any{"status": "not_run"},
		Audit: map[string]any{
			"status":           "not_written",
			"local_output_dir": cfg.OperationOutputDir,
		},
	}
}

func newOperationID(tool string) string {
	return fmt.Sprintf("op-%s-%d", sanitizeToolName(tool), time.Now().UTC().UnixNano())
}

func sanitizeToolName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	out := make([]rune, 0, len(value))
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			out = append(out, r)
			continue
		}
		out = append(out, '-')
	}
	return string(out)
}
