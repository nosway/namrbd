package depbudget

import (
	"fmt"
	"sort"
)

// Tier thresholds this package sizes against, from
// docs/phase-aa-entry-plan.md Section 1.
const (
	T2LargeVolumes  = 1000
	T2LargeGateways = 32
	T2LargeSBSNodes = 100

	// Loop intervals as shipped today.
	PathPlanReconcileIntervalSeconds = 5
	ChunkGCIntervalSeconds           = 30
)

// Violation is one access that cannot hold at a tier.
type Violation struct {
	Access Access
	// Why states the problem in terms an operator can act on.
	Why string
	// EstimatedRecordsPerMinute is the standing read volume this access
	// produces cluster-wide at the tier, when it can be estimated.
	EstimatedRecordsPerMinute int
}

// ViolationsAtT2Large returns the accesses that cannot hold at t2_large.
//
// The rule is narrow: an unbounded prefix scan reached on a timer is a standing
// cost proportional to the record count, paid whether or not anything changed.
// The same scan on demand is a real cost but an operator-triggered one, so it
// is reported as a risk rather than a violation.
func ViolationsAtT2Large() []Violation {
	var out []Violation
	for _, a := range All() {
		if a.Bound != UnboundedPrefix {
			continue
		}
		if a.Cadence != CadenceTimer {
			continue
		}
		records := estimateRecords(a)
		perMinute := 0
		if interval := loopIntervalSeconds(a); interval > 0 && records > 0 {
			// Every gateway runs its own loop.
			perMinute = records * T2LargeGateways * (60 / interval)
		}
		out = append(out, Violation{
			Access: a,
			Why: fmt.Sprintf(
				"an unbounded %s prefix scan on a %ds timer; at t2_large each pass reads about %d records, and every gateway runs its own loop",
				a.Store, loopIntervalSeconds(a), records),
			EstimatedRecordsPerMinute: perMinute,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Access.Func < out[j].Access.Func })
	return out
}

// RisksAtT2Large returns unbounded accesses that are neither on a timer nor
// confined to startup.
//
// A startup-only unbounded scan is paid once per process lifetime, so it is
// recorded in the inventory but is not a standing risk. Counting it alongside
// on-demand scans would make the risk number grow every time a scan is moved
// off a hot path, which is the opposite of what the number should do.
func RisksAtT2Large() []Access {
	var out []Access
	for _, a := range All() {
		if a.Bound != UnboundedPrefix {
			continue
		}
		if a.Cadence == CadenceTimer || a.Cadence == CadenceStartup {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Func < out[j].Func })
	return out
}

func estimateRecords(a Access) int {
	switch a.GrowsWith {
	case "volume count":
		return T2LargeVolumes
	case "gateway count":
		return T2LargeGateways
	case "sbs node count":
		return T2LargeSBSNodes
	default:
		return 0
	}
}

func loopIntervalSeconds(a Access) int {
	switch a.Func {
	case "ListChunkGarbage", "DeleteChunkGarbage":
		return ChunkGCIntervalSeconds
	default:
		return 0
	}
}

// DataPathAccesses returns accesses that sit on a per-I/O path. Each one needs
// a cache or projection in front of it, named in its note.
func DataPathAccesses() []Access {
	var out []Access
	for _, a := range All() {
		if a.Cadence == CadenceDataPath {
			out = append(out, a)
		}
	}
	return out
}

// Summary is the machine-readable budget result.
type Summary struct {
	AccessCount          int `json:"dependency_access_count"`
	EtcdAccessCount      int `json:"etcd_access_count"`
	TiKVAccessCount      int `json:"tikv_access_count"`
	UnboundedScanCount   int `json:"unbounded_prefix_scan_count"`
	TimerDrivenScanCount int `json:"timer_driven_unbounded_scan_count"`
	DataPathAccessCount  int `json:"data_path_access_count"`
	ViolationCount       int `json:"tikv_scan_budget_violation_count"`
	RiskCount            int `json:"unbounded_on_demand_scan_count"`
	StartupOnlyScanCount int `json:"unbounded_startup_only_scan_count"`
}

// Summarize counts the inventory.
func Summarize() Summary {
	s := Summary{}
	for _, a := range All() {
		s.AccessCount++
		switch a.Store {
		case StoreEtcd:
			s.EtcdAccessCount++
		case StoreTiKV:
			s.TiKVAccessCount++
		}
		if a.Bound == UnboundedPrefix {
			s.UnboundedScanCount++
			if a.Cadence == CadenceTimer {
				s.TimerDrivenScanCount++
			}
		}
		if a.Cadence == CadenceDataPath {
			s.DataPathAccessCount++
		}
	}
	for _, a := range All() {
		if a.Bound == UnboundedPrefix && a.Cadence == CadenceStartup {
			s.StartupOnlyScanCount++
		}
	}
	s.ViolationCount = len(ViolationsAtT2Large())
	s.RiskCount = len(RisksAtT2Large())
	return s
}
