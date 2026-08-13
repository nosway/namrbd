package metadata

import "testing"

func TestEtcdKeyLayout(t *testing.T) {
	repo := NewEtcdRepository(nil, "/namrbd")
	if got := repo.volumeSpecKey(101); got != "/namrbd/volumes/00000065/spec" {
		t.Fatalf("unexpected volume spec key: %s", got)
	}
	if got := repo.volumeStatusKey(101); got != "/namrbd/volumes/00000065/status" {
		t.Fatalf("unexpected volume status key: %s", got)
	}
	if got := repo.attachmentKey(101); got != "/namrbd/volumes/00000065/attachments/current" {
		t.Fatalf("unexpected attachment key: %s", got)
	}
	if got := repo.gatewayStatusKey("gw-a"); got != "/namrbd/gateways/gw-a/status" {
		t.Fatalf("unexpected gateway status key: %s", got)
	}
	if got := repo.extentPageKey(101, 7); got != "/namrbd/volumes/00000065/extents/pages/7" {
		t.Fatalf("unexpected extent page key: %s", got)
	}
	if got := repo.chunkNextIDKey(101); got != "/namrbd/volumes/00000065/chunks/next_id" {
		t.Fatalf("unexpected chunk next-id key: %s", got)
	}
	if got := repo.chunkGarbageKey(101, 9); got != "/namrbd/volumes/00000065/chunks/garbage/9" {
		t.Fatalf("unexpected chunk garbage key: %s", got)
	}
}

func TestParsePageNoFromKey(t *testing.T) {
	pageNo, err := parsePageNoFromKey("/namrbd/volumes/00000065/extents/pages/12", "/namrbd/volumes/00000065/extents/pages/")
	if err != nil {
		t.Fatalf("parsePageNoFromKey failed: %v", err)
	}
	if pageNo != 12 {
		t.Fatalf("unexpected page no: %d", pageNo)
	}
}
