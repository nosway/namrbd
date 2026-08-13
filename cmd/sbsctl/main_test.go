package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

type storeStatusRoundTripFunc func(*http.Request) (*http.Response, error)

func (f storeStatusRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDefaultAdminEndpointPrefersSBSPrefix(t *testing.T) {
	t.Setenv("SBS_ADMIN_ENDPOINTS", "sbs-admin-a:8443,sbs-admin-b:8443")
	t.Setenv("NAMRBD_SBS_ADMIN_ENDPOINTS", "legacy-admin:8443")

	if got := defaultAdminEndpoint(); got != "sbs-admin-a:8443" {
		t.Fatalf("defaultAdminEndpoint=%q want=%q", got, "sbs-admin-a:8443")
	}
}

func TestDefaultDataEndpointSupportsAliases(t *testing.T) {
	t.Setenv("SBS_DATA_ENDPOINTS", "node-a:9460,node-b:9460")

	if got := defaultDataEndpoint(); got != "node-a:9460" {
		t.Fatalf("defaultDataEndpoint=%q want=%q", got, "node-a:9460")
	}
}

func TestDefaultTimeoutSupportsSBSAlias(t *testing.T) {
	t.Setenv("SBS_TIMEOUT", "7s")

	if got := defaultTimeout(); got != 7*time.Second {
		t.Fatalf("defaultTimeout=%v want=%v", got, 7*time.Second)
	}
}

func TestFirstEnv(t *testing.T) {
	t.Setenv("SECONDARY", "two")
	if got := firstEnv("PRIMARY", "SECONDARY"); got != "two" {
		t.Fatalf("firstEnv=%q want=%q", got, "two")
	}
}

func TestParseOperationStateFilter(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  adminv1.OperationState
	}{
		{"", adminv1.OperationState_OPERATION_STATE_UNSPECIFIED},
		{"all", adminv1.OperationState_OPERATION_STATE_UNSPECIFIED},
		{"queued", adminv1.OperationState_OPERATION_STATE_QUEUED},
		{"running", adminv1.OperationState_OPERATION_STATE_RUNNING},
		{"completed", adminv1.OperationState_OPERATION_STATE_COMPLETED},
		{"failed", adminv1.OperationState_OPERATION_STATE_FAILED},
		{"canceled", adminv1.OperationState_OPERATION_STATE_CANCELED},
		{"cancelled", adminv1.OperationState_OPERATION_STATE_CANCELED},
	} {
		got, err := parseOperationStateFilter(tc.input)
		if err != nil {
			t.Fatalf("parseOperationStateFilter(%q): %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("parseOperationStateFilter(%q)=%v want=%v", tc.input, got, tc.want)
		}
	}
}

func TestParseOperationStateFilterRejectsUnknownState(t *testing.T) {
	if _, err := parseOperationStateFilter("mystery"); err == nil {
		t.Fatalf("parseOperationStateFilter should reject unknown state")
	}
}

func TestParseBinarySize(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{"256k", 256 << 10},
		{"64K", 64 << 10},
		{"4M", 4 << 20},
		{"4m", 4 << 20},
		{"10G", 10 << 30},
		{"10g", 10 << 30},
		{"100T", 100 << 40},
		{"100t", 100 << 40},
	} {
		got, err := parseBinarySize(tc.input, "size")
		if err != nil {
			t.Fatalf("parseBinarySize(%q): %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("parseBinarySize(%q)=%d want=%d", tc.input, got, tc.want)
		}
	}
}

func TestParseBinarySizeRejectsUnsafeForms(t *testing.T) {
	for _, input := range []string{"", "266144", "1P", "0K"} {
		if _, err := parseBinarySize(input, "size"); err == nil {
			t.Fatalf("parseBinarySize(%q) should fail", input)
		}
	}
}

func TestParseUint32BinarySizeRejectsOverflow(t *testing.T) {
	if _, err := parseUint32BinarySize("5G", "allocation-page-size"); err == nil {
		t.Fatalf("parseUint32BinarySize should reject values larger than uint32")
	}
}

func TestValidateVolumePurgeArgs(t *testing.T) {
	for _, tc := range []struct {
		name              string
		volumeID          string
		yes               bool
		confirmedDeletion bool
		wantErr           string
	}{
		{
			name:              "missing yes",
			volumeID:          "0000007c",
			confirmedDeletion: true,
			wantErr:           "--yes is required for volume purge",
		},
		{
			name:     "missing explicit confirmation",
			volumeID: "0000007c",
			yes:      true,
			wantErr:  "--i-confirmed-deletion is required for volume purge",
		},
		{
			name:              "missing volume id",
			yes:               true,
			confirmedDeletion: true,
			wantErr:           "--volume-id is required",
		},
		{
			name:              "valid",
			volumeID:          "0000007c",
			yes:               true,
			confirmedDeletion: true,
		},
	} {
		err := validateVolumePurgeArgs(tc.volumeID, tc.yes, tc.confirmedDeletion)
		if tc.wantErr == "" {
			if err != nil {
				t.Fatalf("%s: unexpected err=%v", tc.name, err)
			}
			continue
		}
		if err == nil || err.Error() != tc.wantErr {
			t.Fatalf("%s: err=%v want=%q", tc.name, err, tc.wantErr)
		}
	}
}

func TestRunStoreStatusRemoteWithClient(t *testing.T) {
	client := &http.Client{
		Transport: storeStatusRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "http://node-a:9082/debug/summary" {
				t.Fatalf("url=%q", req.URL.String())
			}
			body := `{"path":"/var/sbs","build_version":"test","open_sessions":1,"volumes":2,"stores":[{"id":"fast","state":"failed","path":"/data/fast","shards":2,"weight":100,"capacity_bytes":1000,"available_bytes":700,"pebble_disk_usage_bytes":123,"compaction_pending_bytes":9,"compaction_in_progress_bytes":0},{"id":"bulk","state":"healthy","path":"/data/bulk","shards":2,"weight":50,"capacity_bytes":2000,"available_bytes":1800,"pebble_disk_usage_bytes":42,"compaction_pending_bytes":0,"compaction_in_progress_bytes":0}]}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	summary, err := runStoreStatusRemoteWithClient(context.Background(), client, "http://node-a:9082")
	if err != nil {
		t.Fatalf("runStoreStatusRemoteWithClient: %v", err)
	}
	if got := transitionAnyString(summary["process_state"]); got != "healthy" {
		t.Fatalf("process_state=%q want=healthy", got)
	}
	stores, _ := summary["stores"].([]any)
	if len(stores) != 2 {
		t.Fatalf("stores=%d want=2", len(stores))
	}
	first, _ := stores[0].(map[string]any)
	if transitionAnyString(first["id"]) != "fast" || transitionAnyString(first["state"]) != "failed" {
		t.Fatalf("unexpected first store=%v", first)
	}
	if transitionAnyUint64(first["capacity_bytes"]) != 1000 || transitionAnyUint64(first["compaction_pending_bytes"]) != 9 {
		t.Fatalf("unexpected first store capacity/compaction fields=%v", first)
	}
	if transitionAnyUint64(first["allocation_weight"]) != 100 {
		t.Fatalf("allocation_weight=%v want 100", first["allocation_weight"])
	}
}

func TestRunStoreStatusRemoteWithClientRejectsHTTPError(t *testing.T) {
	client := &http.Client{
		Transport: storeStatusRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     "400 Bad Request",
				Body:       io.NopCloser(bytes.NewBufferString("bad store state")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	if _, err := runStoreStatusRemoteWithClient(context.Background(), client, "http://node-a:9082"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseStoreTuningSpec(t *testing.T) {
	spec, err := parseStoreTuningSpec("store_id=fast,allocation_weight=200")
	if err != nil {
		t.Fatalf("parseStoreTuningSpec: %v", err)
	}
	if spec.GetStoreId() != "fast" || spec.GetWeight() != 200 {
		t.Fatalf("unexpected spec: %+v", spec)
	}
}

func TestParseStoreTuningSpecAcceptsIDAlias(t *testing.T) {
	spec, err := parseStoreTuningSpec("id=bulk,weight=80")
	if err != nil {
		t.Fatalf("parseStoreTuningSpec: %v", err)
	}
	if spec.GetStoreId() != "bulk" || spec.GetWeight() != 80 {
		t.Fatalf("unexpected spec: %+v", spec)
	}
}

func TestParseStoreTuningSpecRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{
		"",
		"weight=10",
		"store_id=fast,weight=-1",
		"store_id=fast,weight=10,allocation_policy=deny",
		"store_id=fast,foo=bar",
	} {
		if _, err := parseStoreTuningSpec(raw); err == nil {
			t.Fatalf("expected parseStoreTuningSpec(%q) to fail", raw)
		}
	}
}
