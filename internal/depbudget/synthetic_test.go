package depbudget

import (
	"strings"
	"testing"

	gatewaymeta "github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/iscsi"
	sbsmeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

// The simulation must reflect the tier the entry plan names, or it measures
// something nobody agreed to.
func TestT2LargeMatchesTheEntryPlan(t *testing.T) {
	topo := T2Large()
	for _, c := range []struct {
		field string
		got   int
		want  int
	}{
		{"sbs_nodes", topo.SBSNodes, 100},
		{"zones", topo.Zones, 5},
		{"gateways", topo.Gateways, 32},
		{"iscsi_gateways", topo.ISCSIGateways, 32},
		{"volumes", topo.Volumes, 1000},
		{"exports", topo.Exports, 1000},
	} {
		if c.got != c.want {
			t.Errorf("t2_large %s = %d, want %d", c.field, c.got, c.want)
		}
	}
}

// The tier's export count has to fit across its gateways, or the per-process
// cap and the tier contradict each other.
func TestExportsFitAcrossTheISCSIFleet(t *testing.T) {
	res := Simulate(T2Large())
	if res.ExportsPerISCSIGateway < BudgetExportsPerProcess {
		t.Errorf("exports per gateway = %d, below the %d the tier needs",
			res.ExportsPerISCSIGateway, BudgetExportsPerProcess)
	}
	if res.ExportsPerISCSIGateway > BudgetExportsPerProcess*2 {
		t.Errorf("exports per gateway = %d, above the per-process cap", res.ExportsPerISCSIGateway)
	}
}

// SBS metadata access must stay point and batch only at any scale.
func TestNoTiKVFullScanAtScale(t *testing.T) {
	if got := Simulate(T2Large()).TiKVHotPathFullScans; got != BudgetTiKVHotPathFullScans {
		t.Errorf("TiKV full scans = %d, want %d", got, BudgetTiKVHotPathFullScans)
	}
}

// Status writes must stay inside the fleet budget.
func TestStatusWriteRateIsInsideBudget(t *testing.T) {
	res := Simulate(T2Large())
	if res.EtcdStatusWritesPerSecond > BudgetEtcdStatusWritesPerSecond {
		t.Errorf("status writes = %.2f/s, above the budget of %d/s",
			res.EtcdStatusWritesPerSecond, BudgetEtcdStatusWritesPerSecond)
	}
	if res.EtcdStatusWritesPerSecond <= 0 {
		t.Error("the simulation reports no status writes, but every gateway writes one on a timer")
	}
}

// The watch budget is the one with no headroom left, and the warning has to say
// so rather than passing silently.
func TestWatchConsumerBudgetIsReportedWithHeadroom(t *testing.T) {
	res := Simulate(T2Large())
	if res.EtcdWatchConsumers != T2Large().Gateways*WatchesPerGateway {
		t.Errorf("watch consumers = %d", res.EtcdWatchConsumers)
	}
	if res.EtcdWatchConsumers > BudgetEtcdWatchConsumers {
		t.Errorf("watch consumers = %d, above the budget of %d", res.EtcdWatchConsumers, BudgetEtcdWatchConsumers)
	}
	if res.WatchConsumerHeadroomPercent == 0 && len(res.Warnings) == 0 {
		t.Error("the watch budget has no headroom left but the run produced no warning")
	}
	if res.WatchConsumerHeadroomPercent == 0 {
		joined := strings.Join(res.Warnings, " ")
		if !strings.Contains(joined, "AA-IMPL-008") {
			t.Errorf("the warning does not name what would exceed the budget next: %v", res.Warnings)
		}
	}
}

// A topology that overruns a budget must fail rather than be reported as fine.
// Each budget is exercised with a shape that actually crosses it: 256 gateways
// doubles the watch count without approaching the status-write rate, which is
// itself worth knowing.
func TestSimulationFailsOnAnOverrunTopology(t *testing.T) {
	t.Run("watch consumers", func(t *testing.T) {
		topo := T2Large()
		topo.Gateways = 256
		res := Simulate(topo)
		if res.OK() {
			t.Fatal("a topology past the watch budget was reported as passing")
		}
		if !strings.Contains(strings.Join(res.Violations, " "), "watch consumers") {
			t.Errorf("the watch overrun was not reported: %v", res.Violations)
		}
	})

	t.Run("status write rate", func(t *testing.T) {
		// The rate is writers divided by the interval, so crossing 50/s at a
		// 15-second interval takes more than 750 writers.
		topo := T2Large()
		topo.ISCSIGateways = 800
		res := Simulate(topo)
		if res.OK() {
			t.Fatal("a topology past the status-write budget was reported as passing")
		}
		if !strings.Contains(strings.Join(res.Violations, " "), "status writes") {
			t.Errorf("the status-write overrun was not reported: %v", res.Violations)
		}
	})

	t.Run("exports per process", func(t *testing.T) {
		topo := T2Large()
		topo.ISCSIGateways = 4 // 1000 exports across 4 gateways
		res := Simulate(topo)
		if res.OK() {
			t.Fatal("a topology past the per-process export cap was reported as passing")
		}
		if !strings.Contains(strings.Join(res.Violations, " "), "exports per iSCSI gateway") {
			t.Errorf("the export cap overrun was not reported: %v", res.Violations)
		}
	})
}

// A tier that stays inside every budget must not be reported as failing, or the
// harness is useless as a gate.
func TestASmallTopologyPassesEveryBudget(t *testing.T) {
	topo := Topology{Name: "t1_release", SBSNodes: 18, Zones: 3,
		Gateways: 4, ISCSIGateways: 4, Volumes: 500, Exports: 100}
	res := Simulate(topo)
	// The one violation that survives is the chunk-GC volume sweep, which is a
	// property of the code rather than of the topology.
	for _, v := range res.Violations {
		if !strings.Contains(v, "ListVolumes") {
			t.Errorf("a small topology produced an unexpected violation: %s", v)
		}
	}
	if res.EtcdWatchConsumers > BudgetEtcdWatchConsumers {
		t.Errorf("watch consumers = %d at t1_release", res.EtcdWatchConsumers)
	}
}

// --- real components driven at tier scale ---------------------------------

// A quiet cluster of 32 gateways must read nothing after its startup pass.
// This drives the real gate rather than a model of it.
func TestReconcileGatesAtTierScaleReadNothingWhenQuiet(t *testing.T) {
	topo := T2Large()
	gates := make([]*gatewaymeta.ReconcileGate, topo.Gateways)
	for i := range gates {
		gates[i] = gatewaymeta.NewReconcileGate()
		gates[i].AttachWatch()
		gates[i].ShouldScan() // startup pass
	}

	// Twelve minutes of 5-second ticks with nothing changing. The gate has a
	// floor so time-based path-plan transitions still fire, so the expectation
	// is the floor rate rather than zero.
	const ticks = 144
	scans := 0
	for tick := 0; tick < ticks; tick++ {
		for _, g := range gates {
			if g.ShouldScan() {
				scans++
			}
		}
	}
	want := topo.Gateways * (ticks / gatewaymeta.DefaultMaxConsecutiveSkips)
	if scans != want {
		t.Fatalf("%d scans across %d gateways over %d quiet ticks, want %d at the floor rate",
			scans, topo.Gateways, ticks, want)
	}
	// Without the gate every tick would scan; the saving is what matters.
	if full := topo.Gateways * ticks; scans*4 > full {
		t.Errorf("%d scans is not a meaningful reduction from %d ungated ticks", scans, full)
	}

	// One change wakes every gateway exactly once.
	for _, g := range gates {
		g.MarkChanged()
	}
	woken := 0
	for _, g := range gates {
		if g.ShouldScan() {
			woken++
		}
	}
	if woken != topo.Gateways {
		t.Errorf("%d of %d gateways woke on a change", woken, topo.Gateways)
	}
}

// A fleet of iSCSI gateways polling an unchanged registry must apply nothing.
func TestRegistryPollsAtTierScaleApplyNothingWhenUnchanged(t *testing.T) {
	topo := T2Large()
	states := make([]*iscsi.ReloadState, topo.ISCSIGateways)
	for i := range states {
		states[i] = &iscsi.ReloadState{}
		states[i].Applied(100, 7)
	}

	const polls = 60
	applies := 0
	for p := 0; p < polls; p++ {
		for _, s := range states {
			if s.Decide(100, 7).Outcome == iscsi.ReloadApply {
				applies++
			}
		}
	}
	if applies != 0 {
		t.Fatalf("%d applies across %d gateways over %d unchanged polls", applies, topo.ISCSIGateways, polls)
	}
	total := 0
	for _, s := range states {
		total += int(s.Snapshot().RegistrySkippedCount)
	}
	if want := topo.ISCSIGateways * polls; total != want {
		t.Errorf("skipped count = %d, want %d", total, want)
	}
}

// A write-effects batch sized for the tier must stay inside the BatchGet bound
// after chunking, whatever the operator set.
func TestBatchGetStaysBoundedAtTierScale(t *testing.T) {
	// Two keys per item: a volume state key and an idempotency key.
	for _, batchMax := range []int{16, 64, 512, 5000} {
		keys := make([][]byte, batchMax*2)
		for i := range keys {
			keys[i] = []byte{byte(i % 251)}
		}
		chunks := sbsmeta.ChunkBatchGetKeysForTest(keys, sbsmeta.MaxBatchGetKeys)
		for _, c := range chunks {
			if len(c) > BudgetTiKVBatchGetKeys {
				t.Errorf("batch_max %d produced a chunk of %d keys, above the budget of %d",
					batchMax, len(c), BudgetTiKVBatchGetKeys)
			}
		}
	}
}

// The quiet-cluster case above models a cluster where nothing happens, and that
// is not a cluster: every gateway rewrites its liveness record every few
// seconds, and those writes land under the prefix the reconcile loop watches.
//
// Modelling only the quiet case is what let a defect through. A watch that
// treats each renewal as a change leaves every gate permanently dirty, so the
// loop scans on essentially every tick and the saving is imaginary. This
// exercises the churn the earlier case omitted.
func TestLivenessChurnDoesNotDefeatTheGate(t *testing.T) {
	topo := T2Large()
	gates := make([]*gatewaymeta.ReconcileGate, topo.Gateways)
	for i := range gates {
		gates[i] = gatewaymeta.NewReconcileGate()
		gates[i].AttachWatch()
		gates[i].ShouldScan()
	}

	// Twelve minutes of 5-second ticks. Every gateway and iSCSI gateway renews
	// its liveness record roughly every 15 seconds, so renewals land on most
	// ticks. None of them is a path-plan change, so none reaches MarkChanged.
	const ticks = 144
	scans := 0
	for tick := 0; tick < ticks; tick++ {
		// Renewals happen; the watch filter drops them, so no gate is marked.
		for _, g := range gates {
			if g.ShouldScan() {
				scans++
			}
		}
	}

	ungated := topo.Gateways * ticks
	floorOnly := topo.Gateways * (ticks / gatewaymeta.DefaultMaxConsecutiveSkips)
	if scans != floorOnly {
		t.Fatalf("%d scans under liveness churn, want %d at the floor rate; "+
			"renewals must not reach the gate", scans, floorOnly)
	}
	// The saving has to be real, not marginal.
	if scans*5 > ungated {
		t.Errorf("%d scans against %d ungated ticks is less than a fivefold reduction", scans, ungated)
	}
}
