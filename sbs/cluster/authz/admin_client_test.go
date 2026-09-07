package authz

import "testing"

func TestAdminClientConsumerInventoryUsesOneAuthenticatedTransport(t *testing.T) {
	inventory := AdminClientConsumerInventory()
	if len(inventory) != 7 {
		t.Fatalf("admin client consumer inventory=%d want=7", len(inventory))
	}
	seenID := map[string]struct{}{}
	seenQA := map[string]struct{}{}
	splitCount := 0
	for _, item := range inventory {
		if item.ConsumerID == "" || item.SourcePath == "" || item.Transport != "internal/adminclient" ||
			!item.RBACRequired || !item.AuditRequired || item.QACaseID == "" {
			t.Fatalf("incomplete admin client consumer=%+v", item)
		}
		if _, duplicate := seenID[item.ConsumerID]; duplicate {
			t.Fatalf("duplicate consumer=%s", item.ConsumerID)
		}
		if _, duplicate := seenQA[item.QACaseID]; duplicate {
			t.Fatalf("duplicate QA case=%s", item.QACaseID)
		}
		seenID[item.ConsumerID] = struct{}{}
		seenQA[item.QACaseID] = struct{}{}
		if item.SplitRequired {
			splitCount++
		}
	}
	if splitCount != 3 {
		t.Fatalf("split admin consumer count=%d want=3", splitCount)
	}
}
