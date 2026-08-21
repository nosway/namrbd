package depbudget

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot walks up to the module root so the test reads the real sources.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not find the module root")
	return ""
}

var depOp = regexp.MustCompile(`\b(?:client|base|txn|tx\.txn|batcher)\.(Get|Put|Delete|DeleteRange|Txn|Watch|Grant|KeepAlive|Revoke|BatchGet|Scan|Commit)\(`)
var funcDecl = regexp.MustCompile(`^func (?:\([^)]*\) )?(\w+)`)

// scanSource returns the functions in a file that touch a dependency.
func scanSource(t *testing.T, root, rel string) map[string]bool {
	t.Helper()
	f, err := os.Open(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("open %s: %v", rel, err)
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	fn := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if m := funcDecl.FindStringSubmatch(line); m != nil {
			fn = m[1]
		}
		if fn != "" && depOp.MatchString(line) {
			out[fn] = true
		}
	}
	return out
}

// The gate for AA-IMPL-003A: no unknown hot path and no unowned prefix scan.
// A dependency access nobody classified is one nobody has sized.
func TestInventoryCoversEveryDependencyAccessInSource(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range InventoriedFiles() {
		found := scanSource(t, root, rel)
		if len(found) == 0 {
			t.Errorf("%s: the inventory claims to cover this file but no dependency access was found; "+
				"either the file moved or the scanner needs updating", rel)
		}
		for fn := range found {
			if _, ok := Lookup(rel, fn); !ok {
				t.Errorf("%s: %s touches a dependency but is not in the inventory; "+
					"classify it before it ships", rel, fn)
			}
		}
		for _, a := range All() {
			if a.File != rel {
				continue
			}
			if !found[a.Func] {
				t.Errorf("%s: the inventory lists %s but the source no longer has a dependency access there; "+
					"remove the stale record", rel, a.Func)
			}
		}
	}
}

// Every access must be fully classified. A partially filled record is not an
// inventory entry, it is a placeholder.
func TestEveryAccessIsFullyClassified(t *testing.T) {
	for _, a := range All() {
		where := a.File + ":" + a.Func
		if a.Store == "" || a.Owner == "" || a.Class == "" || a.Cadence == "" || a.Bound == "" {
			t.Errorf("%s is not fully classified: %+v", where, a)
		}
		// An unbounded scan has to say what it scales with, or its cost cannot
		// be estimated at any tier.
		if a.Bound == UnboundedPrefix && a.GrowsWith == "" {
			t.Errorf("%s is an unbounded prefix scan with no stated growth dimension", where)
		}
		// The accesses that are a problem have to explain themselves.
		if a.Bound == UnboundedPrefix && strings.TrimSpace(a.Note) == "" {
			t.Errorf("%s is an unbounded prefix scan with no note", where)
		}
		if a.Cadence == CadenceDataPath && strings.TrimSpace(a.Note) == "" {
			t.Errorf("%s is on the data path with no note about what keeps it off every I/O", where)
		}
	}
}

// No unbounded prefix scan runs on a timer any more, and the inventory has to
// keep saying so. The two that did, the path-plan reconcile loop and the
// chunk-GC sweep, were the standing cost this budget existed to remove; a scan
// returning to a timer brings it straight back.
func TestNoUnboundedScanRunsOnATimer(t *testing.T) {
	if v := ViolationsAtT2Large(); len(v) != 0 {
		for _, x := range v {
			t.Errorf("%s is timer-driven again: %s", x.Access.Func, x.Why)
		}
	}
	for _, name := range []string{"ListVolumes"} {
		a, ok := Lookup("gateway/metadata/etcd.go", name)
		if !ok {
			t.Errorf("%s left the inventory", name)
			continue
		}
		if a.Cadence == CadenceTimer {
			t.Errorf("%s is classified as timer-driven again", name)
		}
		if !strings.Contains(a.Note, "AA-IMPL-003B") {
			t.Errorf("the %s note no longer records why it left the timer: %q", name, a.Note)
		}
	}
}

// They are still unbounded, so they stay visible as risks. A scan that stops
// being standing load has not become free.
func TestFormerTimerScansRemainVisibleAsRisks(t *testing.T) {
	risks := map[string]bool{}
	for _, a := range RisksAtT2Large() {
		risks[a.Func] = true
	}
	for _, name := range []string{"ListVolumes"} {
		if !risks[name] {
			t.Errorf("%s is unbounded but is no longer reported as a risk", name)
		}
	}
	if a, ok := Lookup("gateway/metadata/gateway_fleet.go", "ListGatewayFleetPage"); !ok || a.Bound != BoundedLimit {
		t.Errorf("gateway fleet list is not revision-paged and bounded: %+v", a)
	}
}

// An unbounded scan that is not on a timer is a risk, not standing load. Both
// have to be visible, and they must not be conflated.
func TestOnDemandUnboundedScansAreRisksNotViolations(t *testing.T) {
	for _, a := range RisksAtT2Large() {
		if a.Cadence == CadenceTimer {
			t.Errorf("%s is on a timer but was classified as an on-demand risk", a.Func)
		}
	}
	for _, v := range ViolationsAtT2Large() {
		if v.Access.Cadence != CadenceTimer {
			t.Errorf("%s is not on a timer but was reported as a violation", v.Access.Func)
		}
	}
	if len(RisksAtT2Large()) == 0 {
		t.Error("ListExtentPages is an unbounded on-demand scan and should be visible")
	}
	// A scan moved to startup only is no longer a standing risk, but it must
	// still be counted somewhere rather than disappearing from the report.
	for _, a := range RisksAtT2Large() {
		if a.Cadence == CadenceStartup {
			t.Errorf("%s is startup-only but was counted as a standing risk", a.Func)
		}
	}
	// No unbounded scan is startup-only today. The count exists so one that
	// becomes startup-only is still reported rather than vanishing from the
	// summary, and the totals must agree either way.
	s := Summarize()
	if s.ViolationCount+s.RiskCount+s.StartupOnlyScanCount != s.UnboundedScanCount {
		t.Errorf("unbounded scans do not add up: %d violations + %d risks + %d startup-only != %d total",
			s.ViolationCount, s.RiskCount, s.StartupOnlyScanCount, s.UnboundedScanCount)
	}
}

// TiKV has no prefix scan at all today, which is worth pinning: a scan added
// later would otherwise slip in unnoticed.
func TestTiKVHasNoUnboundedScans(t *testing.T) {
	for _, a := range All() {
		if a.Store == StoreTiKV && a.Bound == UnboundedPrefix {
			t.Errorf("%s scans a TiKV prefix without a limit; SBS metadata access is point and batch only", a.Func)
		}
	}
}

func TestSummaryCountsAgree(t *testing.T) {
	s := Summarize()
	if s.AccessCount != len(All()) {
		t.Errorf("access count %d does not match the inventory size %d", s.AccessCount, len(All()))
	}
	if s.EtcdAccessCount+s.TiKVAccessCount != s.AccessCount {
		t.Error("store counts do not add up to the total")
	}
	if s.ViolationCount != len(ViolationsAtT2Large()) || s.RiskCount != len(RisksAtT2Large()) {
		t.Error("summary counts disagree with the reported violations and risks")
	}
	if s.TimerDrivenScanCount > s.UnboundedScanCount {
		t.Error("more timer-driven scans than unbounded scans")
	}
}
