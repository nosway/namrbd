package service

import (
	"sync"
	"time"
)

type OperationMetrics struct {
	Count          uint64 `json:"count"`
	Errors         uint64 `json:"errors"`
	Bytes          uint64 `json:"bytes,omitempty"`
	TotalLatencyMS uint64 `json:"total_latency_ms"`
}

type MetricsSnapshot struct {
	ByOperation  map[string]OperationMetrics `json:"by_operation"`
	Retry        map[string]uint64           `json:"retry"`
	RetrySummary RetrySummary                `json:"retry_summary"`
	IOIdentity   *IOIdentityMetrics          `json:"io_identity,omitempty"`
}

type RetrySummary struct {
	TotalRetries           uint64 `json:"total_retries"`
	OpenUnavailableRetries uint64 `json:"open_unavailable_retries"`
	ReopenRetries          uint64 `json:"reopen_retries"`
}

type IOIdentityMetrics struct {
	DiscardBytes              uint64              `json:"discard_bytes,omitempty"`
	LogicalZeroBytes          uint64              `json:"logical_zero_bytes,omitempty"`
	DiscardZeroFallbackBytes  uint64              `json:"discard_zero_fallback_bytes,omitempty"`
	DiscardTrueReclaimBytes   uint64              `json:"discard_true_reclaim_bytes,omitempty"`
	DiscardAlignmentFallbacks uint64              `json:"discard_alignment_fallbacks,omitempty"`
	DiscardAlignedCount       uint64              `json:"discard_aligned_count,omitempty"`
	DiscardUnalignedCount     uint64              `json:"discard_unaligned_count,omitempty"`
	ByDiscardPolicy           map[string]uint64   `json:"by_discard_policy,omitempty"`
	LastObservation           *DiscardObservation `json:"last_observation,omitempty"`
}

type MetricsCollector struct {
	mu          sync.Mutex
	byOperation map[string]OperationMetrics
	ioIdentity  *IOIdentityMetrics
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		byOperation: map[string]OperationMetrics{},
	}
}

func (c *MetricsCollector) Record(op string, bytes uint64, started time.Time, err error) {
	if c == nil || op == "" {
		return
	}
	latencyMS := uint64(time.Since(started) / time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.byOperation[op]
	current.Count++
	current.Bytes += bytes
	current.TotalLatencyMS += latencyMS
	if err != nil {
		current.Errors++
	}
	c.byOperation[op] = current
}

func (c *MetricsCollector) RecordDiscardObservation(obs DiscardObservation) {
	if c == nil || obs.Operation == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ioIdentity == nil {
		c.ioIdentity = &IOIdentityMetrics{ByDiscardPolicy: map[string]uint64{}}
	}
	current := c.ioIdentity
	if current.ByDiscardPolicy == nil {
		current.ByDiscardPolicy = map[string]uint64{}
	}
	current.LogicalZeroBytes += obs.LogicalZeroBytes
	if obs.Operation == IOOperationDiscard {
		current.DiscardBytes += obs.DiscardBytes
		if obs.Policy != "" {
			current.ByDiscardPolicy[obs.Policy]++
		}
		switch obs.Policy {
		case DiscardPolicyZeroFallback:
			current.DiscardZeroFallbackBytes += obs.DiscardBytes
		case DiscardPolicyTrueReclaim:
			current.DiscardTrueReclaimBytes += obs.DiscardBytes
		}
		if obs.AlignedToReclaimGeometry {
			current.DiscardAlignedCount++
		} else {
			current.DiscardUnalignedCount++
			if obs.Policy == DiscardPolicyZeroFallback {
				current.DiscardAlignmentFallbacks++
			}
		}
	}
	last := obs
	current.LastObservation = &last
}

func (c *MetricsCollector) Snapshot() MetricsSnapshot {
	if c == nil {
		return MetricsSnapshot{ByOperation: map[string]OperationMetrics{}}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]OperationMetrics, len(c.byOperation))
	for op, metrics := range c.byOperation {
		out[op] = metrics
	}
	return MetricsSnapshot{ByOperation: out, IOIdentity: cloneIOIdentityMetrics(c.ioIdentity)}
}

func cloneIOIdentityMetrics(in *IOIdentityMetrics) *IOIdentityMetrics {
	if in == nil {
		return nil
	}
	out := *in
	if in.ByDiscardPolicy != nil {
		out.ByDiscardPolicy = make(map[string]uint64, len(in.ByDiscardPolicy))
		for key, value := range in.ByDiscardPolicy {
			out.ByDiscardPolicy[key] = value
		}
	}
	if in.LastObservation != nil {
		last := *in.LastObservation
		out.LastObservation = &last
	}
	return &out
}
