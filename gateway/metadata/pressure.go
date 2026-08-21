package metadata

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// EtcdPressure counts what this process asks of etcd.
//
// AA-IMPL-003A found the cost by reading the source; these counters make it
// observable at runtime, which is what an operator needs when a cluster is
// already large and the question is which loop is responsible.
type EtcdPressure struct {
	prefixScans  atomic.Int64
	statusWrites atomic.Int64
	pointReads   atomic.Int64
	resyncs      atomic.Int64
	// skippedValidations counts renewals that did not re-scan the fleet
	// prefix. It exists so the saving is visible rather than asserted.
	skippedValidations atomic.Int64
}

func (p *EtcdPressure) countPrefixScan() {
	if p != nil {
		p.prefixScans.Add(1)
	}
}

func (p *EtcdPressure) countStatusWrite() {
	if p != nil {
		p.statusWrites.Add(1)
	}
}

func (p *EtcdPressure) countPointRead() {
	if p != nil {
		p.pointReads.Add(1)
	}
}

func (p *EtcdPressure) countResync() {
	if p != nil {
		p.resyncs.Add(1)
	}
}

func (p *EtcdPressure) countSkippedValidation() {
	if p != nil {
		p.skippedValidations.Add(1)
	}
}

// EtcdPressureSnapshot is the observable form.
type EtcdPressureSnapshot struct {
	PrefixScanCount        int64 `json:"etcd_prefix_scan_count"`
	StatusWriteCount       int64 `json:"etcd_status_write_count"`
	PointReadCount         int64 `json:"etcd_point_read_count"`
	ResyncCount            int64 `json:"etcd_resync_count"`
	SkippedValidationCount int64 `json:"etcd_skipped_registry_validation_count"`
}

// PressureSnapshot returns the current counts.
func (r *EtcdRepository) PressureSnapshot() EtcdPressureSnapshot {
	if r == nil || r.pressure == nil {
		return EtcdPressureSnapshot{}
	}
	return EtcdPressureSnapshot{
		PrefixScanCount:        r.pressure.prefixScans.Load(),
		StatusWriteCount:       r.pressure.statusWrites.Load(),
		PointReadCount:         r.pressure.pointReads.Load(),
		ResyncCount:            r.pressure.resyncs.Load(),
		SkippedValidationCount: r.pressure.skippedValidations.Load(),
	}
}

// StatusWriteJitterFraction is how far a renewal may drift from its nominal
// interval, as a fraction.
//
// Without it every gateway that started together renews together forever, and
// the fleet's whole status-write load lands in the same instant of each period.
// At t2_large that is 64 writes in one moment rather than spread across the
// interval. The entry plan budget names +/-20%.
const StatusWriteJitterFraction = 0.2

// jitteredInterval spreads a periodic interval by +/- StatusWriteJitterFraction.
func jitteredInterval(base time.Duration, rnd *rand.Rand) time.Duration {
	if base <= 0 {
		return base
	}
	spread := float64(base) * StatusWriteJitterFraction
	// rnd is caller-supplied so tests are deterministic.
	delta := (rnd.Float64()*2 - 1) * spread
	out := time.Duration(float64(base) + delta)
	if out <= 0 {
		return base
	}
	return out
}
