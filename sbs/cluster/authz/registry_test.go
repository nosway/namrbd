package authz

import (
	"strings"
	"testing"
)

func TestAdmittedGRPCInventoryCoversEveryMethod(t *testing.T) {
	inventory, err := AdmittedGRPCInventory()
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory) == 0 {
		t.Fatal("AdminService inventory is empty")
	}
	seen := make(map[string]struct{}, len(inventory))
	seenQA := make(map[string]struct{}, len(inventory))
	seenServices := map[string]bool{}
	for _, item := range inventory {
		if !item.Covered || item.Resource == "" || item.Action == "" || item.Permission == "" || item.QACaseID == "" {
			t.Errorf("uncovered method: %+v", item)
		}
		if _, ok := seen[item.FullMethod]; ok {
			t.Errorf("duplicate method inventory: %s", item.FullMethod)
		}
		seen[item.FullMethod] = struct{}{}
		if _, ok := seenQA[item.QACaseID]; ok {
			t.Errorf("duplicate QA case id: %s", item.QACaseID)
		}
		seenQA[item.QACaseID] = struct{}{}
		for _, service := range []string{"AdminService", "OperationsService", "PlacementResolverService"} {
			if strings.Contains(item.FullMethod, service+"/") {
				seenServices[service] = true
			}
		}
		if item.Mutation && contains(item.Roles, RoleObserver) {
			t.Errorf("observer owns mutation: %+v", item)
		}
		if !strings.HasPrefix(item.Permission, item.Resource+":") {
			t.Errorf("unstable permission: %+v", item)
		}
	}
	for _, service := range []string{"AdminService", "OperationsService", "PlacementResolverService"} {
		if !seenServices[service] {
			t.Errorf("admitted inventory is missing %s", service)
		}
	}
}

func TestRoleAllowsUnknownAndMutationFailClosed(t *testing.T) {
	unknown := MethodPermission{FullMethod: "/unknown.Service/Mutate", Mutation: true}
	for _, role := range BuiltinRoles() {
		if RoleAllows(role, unknown) {
			t.Fatalf("role %q admitted unknown permission", role)
		}
	}
	permission, ok := PermissionForFullMethod("/sbs.admin.v1.AdminService/CreateSecurityProvider")
	if !ok || !permission.Mutation {
		t.Fatalf("security provider permission=%+v ok=%v", permission, ok)
	}
	if !RoleAllows(RoleSecurityAdmin, permission) || RoleAllows(RoleStorageOperator, permission) || RoleAllows(RoleObserver, permission) {
		t.Fatalf("unexpected role matrix for %+v", permission)
	}
	read, ok := PermissionForFullMethod("/sbs.admin.v1.AdminService/GetVolume")
	if !ok || read.Mutation || !RoleAllows(RoleObserver, read) {
		t.Fatalf("observer read permission=%+v ok=%v", read, ok)
	}
}
