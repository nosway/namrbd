package mcpops

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRunStdioProtocolSmoke(t *testing.T) {
	cfg := DefaultConfig()
	cfg.OperationsEndpoint = "http://sbs-service.test"
	cfg.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body string
		switch r.URL.Path {
		case "/api/v1/sbs/cluster":
			body = `{"schema_version":"namrbd.sbs.observability.v1","source_authority":"sbs-service AdminService","collection_status":"ok","collector_freshness_seconds":0.12,"rbac_checked":true,"tenant_scope_checked":true,"redaction_applied":true,"read_only_mode_enforced":true,"unsupported_claim_visible":true,"warning_count":0,"mcp":{"mcp_server_ready":true,"mcp_provider_ready":true,"mcp_tool_registered":true,"read_only":true,"transport":"stdio-jsonrpc-content-length","mutating_tools_enabled":false,"human_approval_required":true}}`
		case "/api/v1/operations/warnings":
			body = `{"schema_version":"namrbd.sbs.observability.v1","view_id":"operations.warnings","source_authority":"sbs-service AdminService","collection_status":"ok","collector_freshness_seconds":0.12,"rbac_checked":true,"tenant_scope_checked":true,"redaction_applied":true,"read_only_mode_enforced":true,"unsupported_claim_visible":true,"warning_count":0,"data":{"warnings":[]}}`
		case "/api/v1/mcp/tools":
			body = `{"schema_version":"namrbd.sbs.observability.v1","view_id":"mcp.tools","source_authority":"sbs-service AdminService","collection_status":"ok","collector_freshness_seconds":0.12,"rbac_checked":true,"tenant_scope_checked":true,"redaction_applied":true,"read_only_mode_enforced":true,"unsupported_claim_visible":true,"warning_count":0,"data":{"mcp_server_ready":true,"mcp_provider_ready":true,"mcp_tool_registered":true,"read_only":true,"transport":"stdio-jsonrpc-content-length","mutating_tools_enabled":false,"human_approval_required":true}}`
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	})}

	requests := []map[string]any{
		{"jsonrpc": "2.0", "id": 1, "method": "initialize"},
		{"jsonrpc": "2.0", "id": 2, "method": "resources/list"},
		{"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
		{
			"jsonrpc": "2.0",
			"id":      4,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      "namrbd.health.check",
				"arguments": map[string]any{},
			},
		},
		{
			"jsonrpc": "2.0",
			"id":      5,
			"method":  "tools/call",
			"params": map[string]any{
				"name": "namrbd.membership.plan",
				"arguments": map[string]any{
					"intent":         "drain",
					"target_node_id": "node-a",
					"reason":         "operator review",
				},
			},
		},
	}
	input := bytes.NewBuffer(nil)
	for _, request := range requests {
		input.Write(mustMCPFrame(t, request))
	}
	output := bytes.NewBuffer(nil)
	if err := RunStdio(context.Background(), cfg, input, output); err != nil {
		t.Fatalf("RunStdio() error = %v", err)
	}

	reader := bufio.NewReader(output)
	responses := make([]rpcResponse, 0, len(requests))
	for range requests {
		payload, err := readFrame(reader)
		if err != nil {
			t.Fatalf("read response frame: %v", err)
		}
		var response rpcResponse
		if err := json.Unmarshal(payload, &response); err != nil {
			t.Fatalf("response JSON decode: %v", err)
		}
		if response.JSONRPC != "2.0" {
			t.Fatalf("jsonrpc = %q, want 2.0", response.JSONRPC)
		}
		if response.Error != nil {
			t.Fatalf("response error = %+v", response.Error)
		}
		responses = append(responses, response)
	}
	if !strings.Contains(string(mustJSON(t, responses[1].Result)), "namrbd://sbs/observability") {
		t.Fatalf("resources/list result = %s", mustJSON(t, responses[1].Result))
	}
	if !strings.Contains(string(mustJSON(t, responses[2].Result)), "namrbd.health.check") {
		t.Fatalf("tools/list result = %s", mustJSON(t, responses[2].Result))
	}
	healthText := firstToolText(t, responses[3].Result)
	if !strings.Contains(healthText, `"source_authority": "sbs-service AdminService"`) ||
		!strings.Contains(healthText, `"redaction_applied": true`) ||
		!strings.Contains(healthText, `"mutating_tools_enabled": false`) {
		t.Fatalf("health tool result missing safety evidence: %s", healthText)
	}
	planText := firstToolText(t, responses[4].Result)
	if !strings.Contains(planText, `"schema_version": "namrbd.mcp.operation.v1"`) ||
		!strings.Contains(planText, `"apply_enabled": false`) ||
		!strings.Contains(planText, `"human_approval_required": true`) {
		t.Fatalf("membership plan did not return proposal envelope: %s", planText)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRunStdioUnknownToolReturnsInvalidParams(t *testing.T) {
	request := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "namrbd.volume.reclaim.run",
			"arguments": map[string]any{},
		},
	}
	input := bytes.NewBuffer(mustMCPFrame(t, request))
	output := bytes.NewBuffer(nil)
	if err := RunStdio(context.Background(), DefaultConfig(), input, output); err != nil {
		t.Fatalf("RunStdio() error = %v", err)
	}
	payload, err := readFrame(bufio.NewReader(output))
	if err != nil {
		t.Fatalf("read response frame: %v", err)
	}
	var response rpcResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("response JSON decode: %v", err)
	}
	if response.Error == nil || response.Error.Code != -32602 {
		t.Fatalf("response error = %+v, want invalid params", response.Error)
	}
}

func firstToolText(t *testing.T, result any) string {
	t.Helper()
	payload := mustJSON(t, result)
	var decoded struct {
		Content []contentBlock `json:"content"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if len(decoded.Content) == 0 {
		t.Fatalf("tool result has no content: %s", payload)
	}
	return decoded.Content[0].Text
}

func mustMCPFrame(t *testing.T, value any) []byte {
	t.Helper()
	payload := mustJSON(t, value)
	return []byte(fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(payload), payload))
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return payload
}
