package version

import (
	"strings"
	"testing"
)

func TestCurrentUsesProductSemVer(t *testing.T) {
	if Current != "v1.0.0" {
		t.Fatalf("Current=%q want GA product SemVer", Current)
	}
	if ProductVersion() != "v1.0.0" {
		t.Fatalf("ProductVersion=%q", ProductVersion())
	}
}

func TestBuildSummaryIncludesVersionAndCommit(t *testing.T) {
	oldCurrent, oldCommit, oldBuildDate, oldDirty := Current, Commit, BuildDate, Dirty
	defer func() {
		Current, Commit, BuildDate, Dirty = oldCurrent, oldCommit, oldBuildDate, oldDirty
	}()

	Current = "v1.0.0"
	Commit = "abcdef123456"
	BuildDate = "2026-08-18T00:00:00Z"
	Dirty = "false"

	got := BuildSummary()
	wantParts := []string{
		"v1.0.0",
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

func TestAtLeastUsesSemVerCore(t *testing.T) {
	for _, tc := range []struct {
		current, minimum string
		want             bool
	}{
		{"v1.0.9", "v1.1.0", false},
		{"v1.1.0-rc.1", "v1.1.0", true},
		{"v1.1.0", "v1.1.0", true},
		{"v1.2.0+build.4", "v1.1.0", true},
	} {
		got, err := AtLeast(tc.current, tc.minimum)
		if err != nil {
			t.Fatalf("AtLeast(%q, %q): %v", tc.current, tc.minimum, err)
		}
		if got != tc.want {
			t.Errorf("AtLeast(%q, %q)=%v want=%v", tc.current, tc.minimum, got, tc.want)
		}
	}
	if _, err := AtLeast("dev", "v1.1.0"); err == nil {
		t.Fatal("AtLeast accepted an unversioned build")
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
