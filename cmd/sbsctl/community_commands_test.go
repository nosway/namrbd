//go:build !enterprise

package main

import (
	"strings"
	"testing"
)

func TestCommunityBuildHidesEnterpriseUsage(t *testing.T) {
	if lines := enterpriseUsageLines(); len(lines) != 0 {
		t.Fatalf("enterpriseUsageLines length=%d want=0", len(lines))
	}
	if runEnterpriseTopLevel([]string{"snapshot"}) {
		t.Fatalf("community build should dispatch snapshot through common commands")
	}
	if runEnterpriseTopLevel([]string{"clone"}) {
		t.Fatalf("community build should not dispatch enterprise clone command")
	}
	if got := enterpriseCapabilityRequiredMessage("ec"); got != "enterprise_capability_required: ec requires an enterprise build" {
		t.Fatalf("enterprise capability message=%q", got)
	}
}

func TestCommunityBackupCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "backup")
	if exitCode != 1 {
		t.Fatalf("sbsctl backup exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: backup requires an enterprise build") {
		t.Fatalf("sbsctl backup did not report enterprise requirement: %s", output)
	}
}

func TestCommunityDRCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "dr")
	if exitCode != 1 {
		t.Fatalf("sbsctl dr exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: dr requires an enterprise build") {
		t.Fatalf("sbsctl dr did not report enterprise requirement: %s", output)
	}
}

func TestCommunityPerformanceCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "performance")
	if exitCode != 1 {
		t.Fatalf("sbsctl performance exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: performance requires an enterprise build") {
		t.Fatalf("sbsctl performance did not report enterprise requirement: %s", output)
	}
}

func TestCommunitySecurityCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "security")
	if exitCode != 1 {
		t.Fatalf("sbsctl security exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: security requires an enterprise build") {
		t.Fatalf("sbsctl security did not report enterprise requirement: %s", output)
	}
}

func TestCommunityMobilityCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "mobility")
	if exitCode != 1 {
		t.Fatalf("sbsctl mobility exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: mobility requires an enterprise build") {
		t.Fatalf("sbsctl mobility did not report enterprise requirement: %s", output)
	}
}

func TestCommunityDedupeCommandRequiresEnterpriseBuild(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "dedupe")
	if exitCode != 1 {
		t.Fatalf("sbsctl dedupe exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "enterprise_capability_required: dedupe requires an enterprise build") {
		t.Fatalf("sbsctl dedupe did not report enterprise requirement: %s", output)
	}
}

func TestCommunityTopLevelHelpDoesNotLeakEnterpriseOnlySurface(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t)
	if exitCode != 2 {
		t.Fatalf("sbsctl exit=%d want=2 output=%s", exitCode, output)
	}
	lower := strings.ToLower(output)
	for _, forbidden := range []string{
		"ec profile",
		"backup",
		"clone",
		"dr volume",
		"promote",
		"demote",
		"performance",
		"security",
		"kms",
		"crypto-erase",
		"performance-tier",
		"qos",
		"restore warmup",
		"diff-index",
		"ec-journal",
		"mobility",
		"repack",
		"dedupe",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("community help leaked enterprise-only surface %q: %s", forbidden, output)
		}
	}
}
