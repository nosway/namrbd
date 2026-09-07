package authz

import "testing"

func TestServiceActorInventoryRequiresRBACOnlyForNewAdminIntent(t *testing.T) {
	inventory := ServiceActorInventory()
	if len(inventory) != 5 {
		t.Fatalf("service actor inventory=%d want=5", len(inventory))
	}
	qaCases := map[string]struct{}{}
	rbacRequired, exclusions := 0, 0
	for _, item := range inventory {
		if item.ActorID == "" || item.Classification == "" || item.QACaseID == "" {
			t.Fatalf("incomplete service actor entry=%+v", item)
		}
		if _, duplicate := qaCases[item.QACaseID]; duplicate {
			t.Fatalf("duplicate service actor QA case=%s", item.QACaseID)
		}
		qaCases[item.QACaseID] = struct{}{}
		if item.RBACRequired {
			rbacRequired++
			permission, covered := PermissionForFullMethod(item.FullMethod)
			if !covered || !permission.Mutation || permission.Permission != item.Permission || len(item.Roles) == 0 || !item.AuditRequired {
				t.Fatalf("RBAC service actor entry=%+v permission=%+v covered=%t", item, permission, covered)
			}
		} else {
			exclusions++
			if item.ExclusionReason == "" || item.FullMethod != "" || item.Permission != "" || item.AuditRequired {
				t.Fatalf("product exclusion entry=%+v", item)
			}
		}
	}
	if rbacRequired != 1 || exclusions != 4 {
		t.Fatalf("rbac_required=%d exclusions=%d", rbacRequired, exclusions)
	}
}
