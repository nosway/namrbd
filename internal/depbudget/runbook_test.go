package depbudget

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const runbookPath = "docs/phase-aa-dependency-budget-runbook.md"

func readRunbook(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), runbookPath))
	if err != nil {
		t.Fatalf("read %s: %v", runbookPath, err)
	}
	return string(raw)
}

// counterSources are the files whose json tags define what a process publishes.
var counterSources = []string{
	"gateway/metadata/pressure.go",
	"gateway/service/reconcile_gate.go",
	"gateway/service/chunk_gc_bounded.go",
	"sbs/cluster/metadata/tikv_pressure.go",
	"iscsi/reload_budget.go",
	"iscsi/live_reload.go",
	"internal/depavail/status.go",
}

func emittedCounters(t *testing.T) map[string]string {
	t.Helper()
	root := repoRoot(t)
	tag := regexp.MustCompile(`json:"([a-z][a-z0-9_]*)"`)
	out := map[string]string{}
	for _, rel := range counterSources {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, m := range tag.FindAllStringSubmatch(string(raw), -1) {
			out[m[1]] = rel
		}
	}
	return out
}

// A runbook that names a counter the code does not emit sends an operator
// looking for a number that is not there.
func TestRunbookNamesOnlyCountersTheCodeEmits(t *testing.T) {
	doc := readRunbook(t)
	emitted := emittedCounters(t)

	// Counters appear in the document as `backtick_quoted` table cells.
	named := regexp.MustCompile("`([a-z][a-z0-9_]*_(?:count|revision|generation|attached))`")
	seen := map[string]bool{}
	for _, m := range named.FindAllStringSubmatch(doc, -1) {
		name := m[1]
		if seen[name] {
			continue
		}
		seen[name] = true
		if _, ok := emitted[name]; !ok {
			t.Errorf("the runbook names counter %s, which no process emits", name)
		}
	}
	if len(seen) == 0 {
		t.Fatal("the runbook names no counters; the extraction is broken")
	}
}

// The reverse: a counter the code emits but the runbook never mentions is a
// number an operator will see with no guidance attached.
func TestEveryEmittedCounterAppearsInTheRunbook(t *testing.T) {
	doc := readRunbook(t)
	for name, file := range emittedCounters(t) {
		if !strings.Contains(doc, "`"+name+"`") {
			t.Errorf("%s emits %s but the runbook does not mention it; an operator would see it with no guidance",
				file, name)
		}
	}
}

// A threshold in prose that disagrees with the constant it came from is worse
// than no threshold, because it looks authoritative.
func TestRunbookThresholdsMatchTheConstants(t *testing.T) {
	doc := readRunbook(t)
	for _, c := range []struct {
		constant string
		value    int
	}{
		{"BudgetEtcdStatusWritesPerSecond", BudgetEtcdStatusWritesPerSecond},
		{"BudgetEtcdWatchConsumers", BudgetEtcdWatchConsumers},
		{"BudgetEtcdSteadyStatePrefixScans", BudgetEtcdSteadyStatePrefixScans},
		{"BudgetTiKVHotPathFullScans", BudgetTiKVHotPathFullScans},
		{"BudgetTiKVBatchGetKeys", BudgetTiKVBatchGetKeys},
		{"BudgetExportsPerProcess", BudgetExportsPerProcess},
	} {
		if !strings.Contains(doc, "`"+c.constant+"`") {
			t.Errorf("the runbook does not cite %s, so its threshold cannot be traced to code", c.constant)
			continue
		}
		// The value must appear somewhere in the same table row as the constant.
		if !rowMentions(doc, c.constant, fmt.Sprintf("%d", c.value)) {
			t.Errorf("the runbook row citing %s does not carry its value %d", c.constant, c.value)
		}
	}
}

// rowMentions matches the value as a whole number rather than a substring. A
// substring match would accept 500 where the budget is 50, which is exactly the
// drift this check exists to catch.
func rowMentions(doc, constant, value string) bool {
	whole := regexp.MustCompile(`(^|[^0-9])` + regexp.QuoteMeta(value) + `([^0-9]|$)`)
	for _, line := range strings.Split(doc, "\n") {
		if strings.Contains(line, "`"+constant+"`") && whole.MatchString(line) {
			return true
		}
	}
	return false
}

// The tier the thresholds are sized for must be stated, or a reader cannot tell
// what the numbers apply to.
func TestRunbookStatesTheTier(t *testing.T) {
	doc := readRunbook(t)
	topo := T2Large()
	for _, want := range []string{
		"t2_large",
		fmt.Sprintf("%d SBS nodes", topo.SBSNodes),
		fmt.Sprintf("%d gateways", topo.Gateways),
		fmt.Sprintf("%d volumes", topo.Volumes),
		fmt.Sprintf("%d exports", topo.Exports),
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("the runbook does not state %q", want)
		}
	}
}

// The known limits must stay in the document while they are still true. A
// runbook that quietly drops them reads as though the system is clean.
func TestRunbookRecordsTheKnownLimits(t *testing.T) {
	doc := readRunbook(t)

	sim := Simulate(T2Large())
	if sim.WatchConsumerHeadroomPercent == 0 && !strings.Contains(doc, "no headroom") {
		t.Error("the watch budget has no headroom but the runbook does not say so")
	}
	if len(ViolationsAtT2Large()) > 0 && !strings.Contains(doc, "standing prefix scan remains") {
		t.Error("a standing prefix scan violation exists but the runbook does not record it")
	}
	if len(RisksAtT2Large()) > 0 {
		for _, a := range RisksAtT2Large() {
			if !strings.Contains(doc, a.Func) {
				t.Errorf("%s is an unbounded on-demand scan but the runbook does not name it", a.Func)
			}
		}
	}
	// The document must not imply the evidence is more than it is.
	if !strings.Contains(doc, "AA-IMPL-013") {
		t.Error("the runbook does not point at the live evidence gate, so a reader could take the model for a deployment")
	}
	if !strings.Contains(doc, "must not describe large-scale operations as supported") {
		t.Error("the runbook does not state the support boundary Phase Z has to respect")
	}
}
