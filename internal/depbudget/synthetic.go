package depbudget

import (
	"fmt"
	"sort"
)

// Topology is a synthetic cluster shape, from
// docs/phase-aa-entry-plan.md Section 1.
type Topology struct {
	Name          string
	SBSNodes      int
	Zones         int
	Gateways      int
	ISCSIGateways int
	Volumes       int
	Exports       int
}

// T2Large is the tier this budget is sized against.
func T2Large() Topology {
	return Topology{
		Name: "t2_large", SBSNodes: T2LargeSBSNodes, Zones: 5,
		Gateways: T2LargeGateways, ISCSIGateways: 32,
		Volumes: T2LargeVolumes, Exports: 1000,
	}
}

// Budgets are the per-tier limits the entry plan names. They live here as
// numbers so a simulation can fail against them rather than a reviewer having
// to compare a report against a document.
const (
	// BudgetEtcdStatusWritesPerSecond bounds the fleet's aggregate status write
	// rate.
	BudgetEtcdStatusWritesPerSecond = 50
	// BudgetEtcdWatchConsumers bounds how many watch streams the fleet opens.
	BudgetEtcdWatchConsumers = 64
	// BudgetEtcdSteadyStatePrefixScans is zero: a steady-state consumer must
	// not scan a prefix, only resync after startup, compaction, or a revision
	// gap.
	BudgetEtcdSteadyStatePrefixScans = 0
	// BudgetTiKVHotPathFullScans is zero for the same reason.
	BudgetTiKVHotPathFullScans = 0
	// BudgetTiKVBatchGetKeys bounds one BatchGet.
	BudgetTiKVBatchGetKeys = 128
	// BudgetExportsPerProcess is what one iSCSI gateway must be able to serve
	// for the tier's export count to fit across its gateways.
	BudgetExportsPerProcess = 32

	// GatewayLeaseTTLSeconds and GatewayStatusWriteIntervalSec are independent:
	// etcd KeepAlive owns lease renewal while the timer refreshes status fields.
	GatewayLeaseTTLSeconds        = 15
	GatewayStatusWriteIntervalSec = 5

	// WatchesPerGateway is how many etcd watch streams one gateway opens: the
	// gateway prefix and the volume prefix.
	WatchesPerGateway = 2
)

// SimulationResult is what a synthetic run reports.
type SimulationResult struct {
	Topology Topology `json:"-"`

	EtcdStatusWritesPerSecond  float64 `json:"synthetic_etcd_status_writes_per_second"`
	EtcdWatchConsumers         int     `json:"synthetic_etcd_watch_consumer_count"`
	EtcdSteadyStatePrefixScans int     `json:"synthetic_etcd_steady_state_prefix_scan_count"`
	EtcdTimerRecordsPerMinute  int     `json:"synthetic_etcd_timer_records_per_minute"`
	TiKVHotPathFullScans       int     `json:"synthetic_tikv_hot_path_full_scan_count"`
	ExportsPerISCSIGateway     int     `json:"synthetic_exports_per_iscsi_gateway"`

	// Headroom reports how close a measure sits to its budget, as a percentage
	// of the budget. A measure at the limit passes but is worth seeing.
	WatchConsumerHeadroomPercent float64 `json:"synthetic_etcd_watch_headroom_percent"`

	Violations []string `json:"synthetic_budget_violations"`
	Warnings   []string `json:"synthetic_budget_warnings"`
}

// OK reports whether the run stayed inside every budget.
func (r SimulationResult) OK() bool { return len(r.Violations) == 0 }

// Simulate walks the inventory at a topology and reports the resulting
// pressure against the budgets.
//
// It exercises the model, not a deployment: the counts come from the real
// inventory and the real loop intervals, so a change to either moves the
// result. What it cannot show is behavior under contention, which needs the
// live run in AA-IMPL-013.
func Simulate(topo Topology) SimulationResult {
	res := SimulationResult{Topology: topo}

	// Every gateway and iSCSI gateway writes its own status record on a timer.
	statusWriters := topo.Gateways + topo.ISCSIGateways
	if GatewayStatusWriteIntervalSec > 0 {
		res.EtcdStatusWritesPerSecond = float64(statusWriters) / float64(GatewayStatusWriteIntervalSec)
	}
	if res.EtcdStatusWritesPerSecond > BudgetEtcdStatusWritesPerSecond {
		res.Violations = append(res.Violations, fmt.Sprintf(
			"etcd status writes are %.1f/s across %d writers, above the budget of %d/s",
			res.EtcdStatusWritesPerSecond, statusWriters, BudgetEtcdStatusWritesPerSecond))
	}

	// Watch streams. Each gateway watches the gateway prefix and the volume
	// prefix so its reconcile loop can skip ticks.
	res.EtcdWatchConsumers = topo.Gateways * WatchesPerGateway
	if res.EtcdWatchConsumers > BudgetEtcdWatchConsumers {
		res.Violations = append(res.Violations, fmt.Sprintf(
			"%d etcd watch consumers across %d gateways, above the budget of %d",
			res.EtcdWatchConsumers, topo.Gateways, BudgetEtcdWatchConsumers))
	} else {
		res.WatchConsumerHeadroomPercent = 100 * float64(BudgetEtcdWatchConsumers-res.EtcdWatchConsumers) /
			float64(BudgetEtcdWatchConsumers)
		if res.WatchConsumerHeadroomPercent == 0 {
			res.Warnings = append(res.Warnings, fmt.Sprintf(
				"etcd watch consumers sit exactly at the budget of %d; any process that starts watching, "+
					"such as the iSCSI fleet registry in AA-IMPL-008, exceeds it",
				BudgetEtcdWatchConsumers))
		}
	}

	// Timer-driven prefix scans, from the inventory.
	for _, v := range ViolationsAtT2Large() {
		res.EtcdSteadyStatePrefixScans++
		res.EtcdTimerRecordsPerMinute += v.EstimatedRecordsPerMinute
		res.Violations = append(res.Violations, fmt.Sprintf(
			"%s: %s", v.Access.Func, v.Why))
	}

	// TiKV must have no hot-path full scan at all.
	for _, a := range All() {
		if a.Store != StoreTiKV {
			continue
		}
		if a.Bound == UnboundedPrefix {
			res.TiKVHotPathFullScans++
			res.Violations = append(res.Violations, fmt.Sprintf(
				"%s scans a TiKV prefix; SBS metadata access must stay point and batch only", a.Func))
		}
	}

	// Exports have to fit across the iSCSI fleet.
	if topo.ISCSIGateways > 0 {
		res.ExportsPerISCSIGateway = (topo.Exports + topo.ISCSIGateways - 1) / topo.ISCSIGateways
		if res.ExportsPerISCSIGateway > BudgetExportsPerProcess*2 {
			res.Violations = append(res.Violations, fmt.Sprintf(
				"%d exports per iSCSI gateway, above the per-process cap of %d",
				res.ExportsPerISCSIGateway, BudgetExportsPerProcess*2))
		}
	}

	sort.Strings(res.Violations)
	sort.Strings(res.Warnings)
	return res
}
