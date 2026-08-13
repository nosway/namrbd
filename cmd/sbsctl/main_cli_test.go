package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestTopLevelHelpIncludesReplicatedSnapshotRestore(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t)
	if exitCode != 2 {
		t.Fatalf("sbsctl exit=%d want=2 output=%s", exitCode, output)
	}
	for _, want := range []string{
		"volume create|restore-from-snapshot",
		"snapshot create|get|list|delete",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("top-level help missing %q: %s", want, output)
		}
	}
}

func TestTopLevelSnapshotDispatchesToSnapshotUsage(t *testing.T) {
	exitCode, output := runSBSCTLForTest(t, "snapshot")
	if exitCode != 1 {
		t.Fatalf("sbsctl snapshot exit=%d want=1 output=%s", exitCode, output)
	}
	if !strings.Contains(output, "usage: sbsctl snapshot create|get|list|delete ...") {
		t.Fatalf("snapshot command did not dispatch to snapshot usage: %s", output)
	}
	if strings.Contains(output, "commands:") {
		t.Fatalf("snapshot command fell through to top-level usage: %s", output)
	}
}

func runSBSCTLForTest(t *testing.T, args ...string) (int, string) {
	t.Helper()
	cmdArgs := append([]string{"-test.run=TestSBSCTLMainHelper", "--"}, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), "SBSCTL_TEST_MAIN_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), string(out)
	}
	t.Fatalf("failed to run sbsctl helper: %v output=%s", err, string(out))
	return -1, string(out)
}

func TestSBSCTLMainHelper(t *testing.T) {
	if os.Getenv("SBSCTL_TEST_MAIN_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			os.Args = append([]string{"sbsctl"}, args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	t.Fatalf("missing helper separator")
}
