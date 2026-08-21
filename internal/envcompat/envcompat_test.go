package envcompat

import (
	"bytes"
	"strings"
	"testing"
)

func lookup(values map[string]string) Lookup {
	return func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	}
}

func TestLegacyAcceptedThroughV10(t *testing.T) {
	res, err := Resolve(SBSServiceNodeID, lookup(map[string]string{"NAMRBD_NODE_ID": "svc-1"}), "v1.0.9")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if res.Value != "svc-1" || res.Source != "NAMRBD_NODE_ID" || !res.LegacyUsed {
		t.Fatalf("resolution=%+v", res)
	}
	if len(res.Warnings) == 0 || !strings.Contains(res.Warnings[0], "removed in v1.1.0") {
		t.Fatalf("warnings=%v", res.Warnings)
	}
}

func TestCanonicalWinsLegacyConflict(t *testing.T) {
	res, err := Resolve(SBSDataPath, lookup(map[string]string{
		"NAMRBD_SBS_DATA_PATH": "/canonical",
		"NAMRBD_SBS_DATA_DIR":  "/legacy",
	}), "v1.0.9")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if res.Value != "/canonical" || res.Source != "NAMRBD_SBS_DATA_PATH" || !res.Conflict {
		t.Fatalf("resolution=%+v", res)
	}
}

func TestLegacyRejectedAtV11(t *testing.T) {
	_, err := Resolve(ISCSISBSDataEndpoint, lookup(map[string]string{
		"NAMRBD_ISCSI_SBS_ENDPOINT": "sbs-data:9444",
	}), "v1.1.0-rc.1")
	if err == nil || !strings.Contains(err.Error(), "removed environment variable") {
		t.Fatalf("error=%v", err)
	}
}

func TestCanonicalStillAcceptedAtV11(t *testing.T) {
	res, err := Resolve(ISCSISBSDataEndpoint, lookup(map[string]string{
		"NAMRBD_ISCSI_SBS_DATA_ENDPOINT": "sbs-data:9444",
	}), "v1.1.0")
	if err != nil || res.Value != "sbs-data:9444" {
		t.Fatalf("resolution=%+v error=%v", res, err)
	}
}

func TestWarningsStayOffStdoutByConstruction(t *testing.T) {
	res, err := Resolve(SBSCTLOutput, lookup(map[string]string{"SBS_OUTPUT": "json"}), "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	WriteWarnings(&stderr, res.Warnings)
	if !strings.Contains(stderr.String(), "deprecated environment variable SBS_OUTPUT") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestGatewayAndCSIEndpointCatalogUsesSharedServiceNames(t *testing.T) {
	for name, spec := range map[string]Spec{
		"gateway service": GatewaySBSServiceEndpoint,
		"CSI service":     CSISBSServiceEndpoint,
	} {
		if spec.Canonical != "NAMRBD_SBS_SERVICE_ENDPOINT" || len(spec.Legacy) != 1 {
			t.Errorf("%s spec=%+v", name, spec)
		}
	}
	if GatewayControlListen.Canonical != "NAMRBD_GATEWAY_CONTROL_LISTEN" ||
		!GatewayControlListen.Matches("NAMRBD_GATEWAY_LISTEN") {
		t.Fatalf("gateway control listener spec=%+v", GatewayControlListen)
	}
	if CSISBSServiceEndpoints.Canonical != "NAMRBD_SBS_SERVICE_ENDPOINTS" ||
		!CSISBSServiceEndpoints.Matches("NAMRBD_ADMIN_ENDPOINTS") {
		t.Fatalf("CSI service endpoint list spec=%+v", CSISBSServiceEndpoints)
	}
}
