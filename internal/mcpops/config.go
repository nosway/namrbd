package mcpops

import (
	"net/http"
	"strings"
	"time"
)

const (
	ModeObserve = "observe"
	ModeOperate = "operate"

	ApprovalPolicyDryRun            = "dry-run"
	ApprovalPolicyExternalToken     = "external-token"
	ApprovalPolicyLocalConfirmation = "local-confirmation"
)

type Config struct {
	OperationsEndpoint string
	Mode               string
	ApprovalPolicy     string
	OperationOutputDir string
	HTTPTimeout        time.Duration
	HTTPClient         *http.Client
}

func DefaultConfig() Config {
	return Config{
		OperationsEndpoint: "http://127.0.0.1:9081",
		Mode:               ModeObserve,
		ApprovalPolicy:     ApprovalPolicyDryRun,
		OperationOutputDir: ".cache/namrbd-mcp-operations",
		HTTPTimeout:        3 * time.Second,
	}
}

func (c Config) Normalized() Config {
	c.OperationsEndpoint = trimEndpoint(c.OperationsEndpoint)
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		c.Mode = ModeObserve
	}
	c.ApprovalPolicy = strings.ToLower(strings.TrimSpace(c.ApprovalPolicy))
	if c.ApprovalPolicy == "" {
		c.ApprovalPolicy = ApprovalPolicyDryRun
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 3 * time.Second
	}
	return c
}

func trimEndpoint(value string) string {
	value = strings.TrimSpace(value)
	return strings.TrimRight(value, "/")
}
