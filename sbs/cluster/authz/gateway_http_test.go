package authz

import (
	"net/http"
	"testing"
)

func TestGatewayHTTPInventoryAndRequestClassification(t *testing.T) {
	inventory := GatewayHTTPInventory()
	if len(inventory) != 3 {
		t.Fatalf("gateway HTTP inventory=%d want=3", len(inventory))
	}
	cases := []struct {
		path, route string
	}{
		{"/api/v1/volumes/00000065/reload-size", GatewayHTTPRouteReloadVolumeSize},
		{"/api/v1/debug/discovery/volumes/00000065/path-plan", GatewayHTTPRouteApplyPathPlan},
		{"/api/v1/debug/sbs-cluster/nodes/node-a", GatewayHTTPRouteUpdateNodeHealth},
	}
	for _, test := range cases {
		permission, ok := GatewayHTTPPermissionForRequest(http.MethodPost, test.path)
		if !ok || permission.RouteID != test.route || permission.Permission == "" || permission.QACaseID == "" || !permission.Mutation {
			t.Fatalf("path=%s permission=%+v ok=%t", test.path, permission, ok)
		}
	}
	for _, path := range []string{
		"/api/v1/volumes/00000065/attach",
		"/api/v1/volumes/00000065/write",
		"/api/v1/debug/discovery/volumes/00000065/runtime-feedback",
		"/healthz",
	} {
		if permission, ok := GatewayHTTPPermissionForRequest(http.MethodPost, path); ok {
			t.Fatalf("non-admin path=%s classified=%+v", path, permission)
		}
	}
	if _, ok := GatewayHTTPPermissionForRequest(http.MethodGet, "/api/v1/volumes/00000065/reload-size"); ok {
		t.Fatal("GET reload-size unexpectedly classified as a mutation")
	}
}

func TestGatewayHTTPRoleMatrix(t *testing.T) {
	for _, permission := range GatewayHTTPInventory() {
		allowed := 0
		for _, role := range BuiltinRoles() {
			if RoleAllowsGatewayHTTP(role, permission) {
				allowed++
			}
		}
		if allowed == 0 || RoleAllowsGatewayHTTP(RoleObserver, permission) {
			t.Fatalf("permission=%+v allowed=%d observer_allowed=%t", permission, allowed, RoleAllowsGatewayHTTP(RoleObserver, permission))
		}
	}
}
