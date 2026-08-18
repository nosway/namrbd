package version

import (
	"strings"
	"testing"
)

func TestCurrentUsesProductSemVer(t *testing.T) {
	if Current != "v1.0.0-rc" {
		t.Fatalf("Current=%q want product SemVer release candidate", Current)
	}
	if ProductVersion() != "v1.0.0-rc" {
		t.Fatalf("ProductVersion=%q", ProductVersion())
	}
}

func TestBuildSummaryIncludesVersionAndCommit(t *testing.T) {
	oldCurrent, oldCommit, oldBuildDate, oldDirty := Current, Commit, BuildDate, Dirty
	defer func() {
		Current, Commit, BuildDate, Dirty = oldCurrent, oldCommit, oldBuildDate, oldDirty
	}()

	Current = "v1.0.0-rc"
	Commit = "abcdef123456"
	BuildDate = "2026-08-18T00:00:00Z"
	Dirty = "false"

	got := BuildSummary()
	wantParts := []string{
		"v1.0.0-rc",
		"commit=abcdef123456",
		"build_date=2026-08-18T00:00:00Z",
		"dirty=false",
	}
	for _, part := range wantParts {
		if !containsWord(got, part) {
			t.Fatalf("BuildSummary=%q missing %q", got, part)
		}
	}
}

func containsWord(s, want string) bool {
	for _, part := range strings.Fields(s) {
		if part == want {
			return true
		}
	}
	return false
}
