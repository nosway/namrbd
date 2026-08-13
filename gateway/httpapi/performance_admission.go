package httpapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	phaseperformance "github.com/nosway/namrbd/sbs/cluster/performance"
)

const (
	phaseOThrottleError             = "phase_o_throttle_rejected"
	phaseOThrottleReasonCap         = "cap_exceeded"
	phaseOThrottleReasonConfig      = "invalid_performance_admission_config"
	defaultPerformancePolicyID      = "gateway-local"
	defaultPerformanceGeneration    = 1
	defaultPerformanceLeaseWindowMS = 1000
	defaultPerformanceLeaseTTLMS    = 500
)

var errPerformanceAdmissionRejected = errors.New(phaseOThrottleError)

type PerformanceBudgetLeaseClient interface {
	AcquirePerformanceBudgetLease(context.Context, PerformanceBudgetLeaseRequest) (PerformanceBudgetLeaseGrant, error)
}

type PerformanceBudgetLeaseRequest struct {
	LeaseID                 string
	VolumeID                string
	PolicyID                string
	PolicyGeneration        uint64
	BudgetClass             string
	CapScope                string
	ThrottleMode            string
	RequestedTokens         uint64
	RequestedBytes          uint64
	IOPSCap                 uint64
	BandwidthCapBytesPerSec uint64
	BurstIOPS               uint64
	BurstBytes              uint64
	WindowMs                uint64
	TTLMs                   uint64
	GatewayID               string
}

type PerformanceBudgetLeaseGrant struct {
	LeaseID                 string
	LeaseGeneration         uint64
	GrantedTokens           uint64
	GrantedBytes            uint64
	DeniedTokens            uint64
	DeniedBytes             uint64
	ThrottleWaitMs          uint64
	RejectedOps             uint64
	RejectionReason         string
	OutstandingTokensBefore uint64
	OutstandingBytesBefore  uint64
	AvailableTokensBefore   uint64
	AvailableBytesBefore    uint64
	ClusterWideCapSupport   bool
	SharedBudgetAuthority   bool
}

type PerformanceAdmissionConfig struct {
	Enabled                           bool
	PolicyID                          string
	PolicyGeneration                  uint64
	CapScope                          string
	ThrottleMode                      string
	IOPSCap                           uint64
	BandwidthCapBytesPerSec           uint64
	BurstIOPS                         uint64
	BurstBytes                        uint64
	GatewayID                         string
	LeaseWindowMs                     uint64
	LeaseTTLMs                        uint64
	SharedBudgetLeaseClientConfigured bool
}

type gatewayPerformanceAdmission struct {
	cfg         PerformanceAdmissionConfig
	leaseClient PerformanceBudgetLeaseClient

	mu         sync.Mutex
	lastRefill time.Time
	opTokens   float64
	byteTokens float64
	leaseSeq   uint64

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

type gatewayPerformanceAdmissionDecision struct {
	Observed                 bool
	PolicyID                 string
	PolicyGeneration         uint64
	CapScope                 string
	ThrottleMode             string
	Operation                string
	RequestedTokens          uint64
	GrantedTokens            uint64
	RequestedBytes           uint64
	GrantedBytes             uint64
	ThrottledOps             uint64
	ThrottledBytes           uint64
	ThrottleWaitMs           uint64
	RejectedOps              uint64
	RejectionReason          string
	DeniedTokens             uint64
	DeniedBytes              uint64
	IOPSCap                  uint64
	BandwidthCapBytesPerSec  uint64
	BurstIOPS                uint64
	BurstBytes               uint64
	LeaseID                  string
	LeaseGeneration          uint64
	OutstandingTokensBefore  uint64
	OutstandingBytesBefore   uint64
	AvailableTokensBefore    uint64
	AvailableBytesBefore     uint64
	SharedBudgetAuthority    bool
	GatewayConsumesLease     bool
	EnforcedBeforeDispatch   bool
	ClusterWideCapSupport    bool
	GatewayRestartRequired   bool
	RemoteLabValidationState string
}

func newGatewayPerformanceAdmission(cfg PerformanceAdmissionConfig, leaseClient PerformanceBudgetLeaseClient) (*gatewayPerformanceAdmission, error) {
	normalized, err := normalizePerformanceAdmissionConfig(cfg, leaseClient != nil)
	if !normalized.Enabled {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	now := time.Now
	admission := &gatewayPerformanceAdmission{
		cfg:         normalized,
		leaseClient: leaseClient,
		lastRefill:  now(),
		opTokens:    tokenCapacity(normalized.IOPSCap, normalized.BurstIOPS),
		byteTokens:  tokenCapacity(normalized.BandwidthCapBytesPerSec, normalized.BurstBytes),
		now:         now,
		sleep:       sleepWithContext,
	}
	return admission, nil
}

func ValidatePerformanceAdmissionConfig(cfg PerformanceAdmissionConfig) (PerformanceAdmissionConfig, error) {
	return normalizePerformanceAdmissionConfig(cfg, cfg.SharedBudgetLeaseClientConfigured)
}

func normalizePerformanceAdmissionConfig(cfg PerformanceAdmissionConfig, sharedBudgetLeaseClientConfigured bool) (PerformanceAdmissionConfig, error) {
	cfg.PolicyID = strings.TrimSpace(cfg.PolicyID)
	cfg.CapScope = strings.ToLower(strings.TrimSpace(cfg.CapScope))
	cfg.ThrottleMode = strings.ToLower(strings.TrimSpace(cfg.ThrottleMode))
	cfg.GatewayID = strings.TrimSpace(cfg.GatewayID)
	if !cfg.Enabled {
		return cfg, nil
	}
	if cfg.PolicyID == "" {
		cfg.PolicyID = defaultPerformancePolicyID
	}
	if cfg.PolicyGeneration == 0 {
		cfg.PolicyGeneration = defaultPerformanceGeneration
	}
	if cfg.CapScope == "" {
		cfg.CapScope = phaseperformance.CapScopeLabOnly
	}
	if cfg.ThrottleMode == "" {
		cfg.ThrottleMode = phaseperformance.ThrottleModeWait
	}
	if cfg.LeaseWindowMs == 0 {
		cfg.LeaseWindowMs = defaultPerformanceLeaseWindowMS
	}
	if cfg.LeaseTTLMs == 0 {
		cfg.LeaseTTLMs = defaultPerformanceLeaseTTLMS
	}
	switch cfg.CapScope {
	case phaseperformance.CapScopeLabOnly, phaseperformance.CapScopePerGateway:
	case phaseperformance.CapScopeClusterVolume:
		if !sharedBudgetLeaseClientConfigured {
			return cfg, fmt.Errorf("%s: cap_scope=%s requires service-owned shared budget leases", phaseOThrottleReasonConfig, cfg.CapScope)
		}
	default:
		return cfg, fmt.Errorf("%s: unsupported cap_scope %q", phaseOThrottleReasonConfig, cfg.CapScope)
	}
	switch cfg.ThrottleMode {
	case phaseperformance.ThrottleModeWait, phaseperformance.ThrottleModeReject:
	default:
		return cfg, fmt.Errorf("%s: unsupported throttle_mode %q", phaseOThrottleReasonConfig, cfg.ThrottleMode)
	}
	if cfg.IOPSCap == 0 && cfg.BandwidthCapBytesPerSec == 0 {
		return cfg, fmt.Errorf("%s: iops_cap or bandwidth_cap_bytes_per_sec is required", phaseOThrottleReasonConfig)
	}
	if cfg.CapScope == phaseperformance.CapScopeClusterVolume && cfg.GatewayID == "" {
		return cfg, fmt.Errorf("%s: cap_scope=%s requires gateway_id", phaseOThrottleReasonConfig, cfg.CapScope)
	}
	return cfg, nil
}

func (a *gatewayPerformanceAdmission) admit(ctx context.Context, volumeID, operation string, bytes uint64) (gatewayPerformanceAdmissionDecision, error) {
	if a == nil {
		return gatewayPerformanceAdmissionDecision{}, nil
	}
	if operation == "" {
		operation = "io"
	}
	if a.cfg.CapScope == phaseperformance.CapScopeClusterVolume {
		return a.admitSharedBudgetLease(ctx, volumeID, operation, bytes)
	}
	decision := gatewayPerformanceAdmissionDecision{
		Observed:                 true,
		PolicyID:                 a.cfg.PolicyID,
		PolicyGeneration:         a.cfg.PolicyGeneration,
		CapScope:                 a.cfg.CapScope,
		ThrottleMode:             a.cfg.ThrottleMode,
		Operation:                operation,
		RequestedTokens:          1,
		RequestedBytes:           bytes,
		IOPSCap:                  a.cfg.IOPSCap,
		BandwidthCapBytesPerSec:  a.cfg.BandwidthCapBytesPerSec,
		BurstIOPS:                a.cfg.BurstIOPS,
		BurstBytes:               a.cfg.BurstBytes,
		EnforcedBeforeDispatch:   true,
		ClusterWideCapSupport:    false,
		GatewayRestartRequired:   true,
		RemoteLabValidationState: "required",
	}

	a.mu.Lock()
	now := a.now()
	a.refillLocked(now)
	opDeficit := deficit(a.opTokens, 1, a.cfg.IOPSCap)
	byteDeficit := deficit(a.byteTokens, float64(bytes), a.cfg.BandwidthCapBytesPerSec)
	if opDeficit == 0 && byteDeficit == 0 {
		a.opTokens -= 1
		a.byteTokens -= float64(bytes)
		decision.GrantedTokens = 1
		decision.GrantedBytes = bytes
		a.mu.Unlock()
		return decision, nil
	}

	if opDeficit > 0 {
		decision.ThrottledOps = 1
	}
	if byteDeficit > 0 {
		decision.ThrottledBytes = bytes
	}
	if a.cfg.ThrottleMode == phaseperformance.ThrottleModeReject {
		decision.RejectedOps = 1
		decision.RejectionReason = phaseOThrottleReasonCap
		a.mu.Unlock()
		return decision, errPerformanceAdmissionRejected
	}

	wait := maxDuration(
		waitForDeficit(opDeficit, a.cfg.IOPSCap),
		waitForDeficit(byteDeficit, a.cfg.BandwidthCapBytesPerSec),
	)
	a.opTokens -= 1
	a.byteTokens -= float64(bytes)
	decision.GrantedTokens = 1
	decision.GrantedBytes = bytes
	decision.ThrottleWaitMs = durationMillisCeil(wait)
	a.mu.Unlock()

	if wait <= 0 {
		return decision, nil
	}
	if err := a.sleep(ctx, wait); err != nil {
		return decision, err
	}
	return decision, nil
}

func (a *gatewayPerformanceAdmission) admitSharedBudgetLease(ctx context.Context, volumeID, operation string, bytes uint64) (gatewayPerformanceAdmissionDecision, error) {
	requestedTokens := a.sharedBudgetRequestedTokens()
	requestedBytes := a.sharedBudgetRequestedBytes(bytes)
	decision := gatewayPerformanceAdmissionDecision{
		Observed:                 true,
		PolicyID:                 a.cfg.PolicyID,
		PolicyGeneration:         a.cfg.PolicyGeneration,
		CapScope:                 a.cfg.CapScope,
		ThrottleMode:             a.cfg.ThrottleMode,
		Operation:                operation,
		RequestedTokens:          requestedTokens,
		RequestedBytes:           requestedBytes,
		IOPSCap:                  a.cfg.IOPSCap,
		BandwidthCapBytesPerSec:  a.cfg.BandwidthCapBytesPerSec,
		BurstIOPS:                a.cfg.BurstIOPS,
		BurstBytes:               a.cfg.BurstBytes,
		EnforcedBeforeDispatch:   true,
		ClusterWideCapSupport:    true,
		SharedBudgetAuthority:    true,
		GatewayConsumesLease:     true,
		GatewayRestartRequired:   true,
		RemoteLabValidationState: "required",
	}
	if a.leaseClient == nil {
		decision.RejectionReason = phaseOThrottleReasonConfig
		return decision, fmt.Errorf("%s: shared budget lease client is not configured", phaseOThrottleReasonConfig)
	}
	leaseID := a.nextLeaseID(volumeID, operation)
	grant, err := a.acquireSharedBudgetLease(ctx, leaseID, volumeID, operation, requestedTokens, requestedBytes)
	decision.applyLeaseGrant(grant)
	if err != nil {
		return decision, err
	}
	if decision.hasFullLeaseGrant() {
		return decision, nil
	}
	if a.cfg.ThrottleMode == phaseperformance.ThrottleModeReject || decision.RejectedOps > 0 {
		if decision.RejectionReason == "" {
			decision.RejectionReason = phaseOThrottleReasonCap
		}
		return decision, errPerformanceAdmissionRejected
	}
	if decision.ThrottleWaitMs == 0 {
		return decision, fmt.Errorf("shared budget lease denied without wait")
	}
	if err := a.sleep(ctx, time.Duration(decision.ThrottleWaitMs)*time.Millisecond); err != nil {
		return decision, err
	}
	grant, err = a.acquireSharedBudgetLease(ctx, leaseID, volumeID, operation, requestedTokens, requestedBytes)
	decision.applyLeaseGrant(grant)
	if err != nil {
		return decision, err
	}
	if !decision.hasFullLeaseGrant() {
		if decision.RejectionReason == "" {
			decision.RejectionReason = phaseOThrottleReasonCap
		}
		return decision, errPerformanceAdmissionRejected
	}
	return decision, nil
}

func (a *gatewayPerformanceAdmission) acquireSharedBudgetLease(ctx context.Context, leaseID, volumeID, operation string, requestedTokens, requestedBytes uint64) (PerformanceBudgetLeaseGrant, error) {
	return a.leaseClient.AcquirePerformanceBudgetLease(ctx, PerformanceBudgetLeaseRequest{
		LeaseID:                 leaseID,
		VolumeID:                volumeID,
		PolicyID:                a.cfg.PolicyID,
		PolicyGeneration:        a.cfg.PolicyGeneration,
		BudgetClass:             phaseperformance.BudgetClassForeground,
		CapScope:                phaseperformance.CapScopeClusterVolume,
		ThrottleMode:            a.cfg.ThrottleMode,
		RequestedTokens:         requestedTokens,
		RequestedBytes:          requestedBytes,
		IOPSCap:                 a.cfg.IOPSCap,
		BandwidthCapBytesPerSec: a.cfg.BandwidthCapBytesPerSec,
		BurstIOPS:               a.cfg.BurstIOPS,
		BurstBytes:              a.cfg.BurstBytes,
		WindowMs:                a.cfg.LeaseWindowMs,
		TTLMs:                   a.cfg.LeaseTTLMs,
		GatewayID:               a.cfg.GatewayID,
	})
}

func (a *gatewayPerformanceAdmission) sharedBudgetRequestedTokens() uint64 {
	if a.cfg.IOPSCap == 0 {
		return 0
	}
	return 1
}

func (a *gatewayPerformanceAdmission) sharedBudgetRequestedBytes(bytes uint64) uint64 {
	if a.cfg.BandwidthCapBytesPerSec == 0 {
		return 0
	}
	return bytes
}

func (a *gatewayPerformanceAdmission) nextLeaseID(volumeID, operation string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.leaseSeq++
	return fmt.Sprintf("%s-%s-%s-%d-%d", a.cfg.GatewayID, volumeID, operation, a.now().UnixNano(), a.leaseSeq)
}

func (d *gatewayPerformanceAdmissionDecision) applyLeaseGrant(grant PerformanceBudgetLeaseGrant) {
	d.LeaseID = grant.LeaseID
	d.LeaseGeneration = grant.LeaseGeneration
	d.GrantedTokens = grant.GrantedTokens
	d.GrantedBytes = grant.GrantedBytes
	d.DeniedTokens = grant.DeniedTokens
	d.DeniedBytes = grant.DeniedBytes
	if grant.DeniedTokens > 0 {
		d.ThrottledOps += grant.DeniedTokens
	}
	if grant.DeniedBytes > 0 {
		d.ThrottledBytes += grant.DeniedBytes
	}
	d.ThrottleWaitMs += grant.ThrottleWaitMs
	d.RejectedOps = grant.RejectedOps
	d.RejectionReason = grant.RejectionReason
	d.OutstandingTokensBefore = grant.OutstandingTokensBefore
	d.OutstandingBytesBefore = grant.OutstandingBytesBefore
	d.AvailableTokensBefore = grant.AvailableTokensBefore
	d.AvailableBytesBefore = grant.AvailableBytesBefore
	d.ClusterWideCapSupport = d.ClusterWideCapSupport || grant.ClusterWideCapSupport
	d.SharedBudgetAuthority = d.SharedBudgetAuthority || grant.SharedBudgetAuthority
}

func (d gatewayPerformanceAdmissionDecision) hasFullLeaseGrant() bool {
	tokensGranted := d.RequestedTokens == 0 || d.GrantedTokens >= d.RequestedTokens
	bytesGranted := d.RequestedBytes == 0 || d.GrantedBytes >= d.RequestedBytes
	return tokensGranted && bytesGranted
}

func (a *gatewayPerformanceAdmission) refillLocked(now time.Time) {
	if a.lastRefill.IsZero() {
		a.lastRefill = now
		return
	}
	elapsed := now.Sub(a.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	if a.cfg.IOPSCap > 0 {
		a.opTokens = math.Min(tokenCapacity(a.cfg.IOPSCap, a.cfg.BurstIOPS), a.opTokens+elapsed*float64(a.cfg.IOPSCap))
	}
	if a.cfg.BandwidthCapBytesPerSec > 0 {
		a.byteTokens = math.Min(tokenCapacity(a.cfg.BandwidthCapBytesPerSec, a.cfg.BurstBytes), a.byteTokens+elapsed*float64(a.cfg.BandwidthCapBytesPerSec))
	}
	a.lastRefill = now
}

func (d gatewayPerformanceAdmissionDecision) responsePayload() map[string]any {
	if !d.Observed {
		return nil
	}
	return map[string]any{
		"policy_id":                   d.PolicyID,
		"policy_generation":           d.PolicyGeneration,
		"cap_scope":                   d.CapScope,
		"throttle_mode":               d.ThrottleMode,
		"operation":                   d.Operation,
		"requested_tokens":            d.RequestedTokens,
		"granted_tokens":              d.GrantedTokens,
		"requested_bytes":             d.RequestedBytes,
		"granted_bytes":               d.GrantedBytes,
		"throttled_ops":               d.ThrottledOps,
		"throttled_bytes":             d.ThrottledBytes,
		"throttle_wait_ms":            d.ThrottleWaitMs,
		"rejected_ops":                d.RejectedOps,
		"rejection_reason":            d.RejectionReason,
		"denied_tokens":               d.DeniedTokens,
		"denied_bytes":                d.DeniedBytes,
		"iops_cap":                    d.IOPSCap,
		"bandwidth_cap_bytes_per_sec": d.BandwidthCapBytesPerSec,
		"burst_iops":                  d.BurstIOPS,
		"burst_bytes":                 d.BurstBytes,
		"lease_id":                    d.LeaseID,
		"lease_generation":            d.LeaseGeneration,
		"outstanding_tokens_before":   d.OutstandingTokensBefore,
		"outstanding_bytes_before":    d.OutstandingBytesBefore,
		"available_tokens_before":     d.AvailableTokensBefore,
		"available_bytes_before":      d.AvailableBytesBefore,
		"shared_budget_authority":     d.SharedBudgetAuthority,
		"gateway_consumes_lease":      d.GatewayConsumesLease,
		"enforced_before_dispatch":    d.EnforcedBeforeDispatch,
		"cluster_wide_cap_support":    d.ClusterWideCapSupport,
		"gateway_restart_required":    d.GatewayRestartRequired,
		"remote_lab_validation_state": d.RemoteLabValidationState,
	}
}

func (d gatewayPerformanceAdmissionDecision) structuredFields() []structuredThrottleField {
	if !d.Observed {
		return nil
	}
	return []structuredThrottleField{
		{"phase_o_policy_id", d.PolicyID},
		{"phase_o_policy_generation", d.PolicyGeneration},
		{"phase_o_cap_scope", d.CapScope},
		{"phase_o_throttle_mode", d.ThrottleMode},
		{"phase_o_requested_tokens", d.RequestedTokens},
		{"phase_o_granted_tokens", d.GrantedTokens},
		{"phase_o_requested_bytes", d.RequestedBytes},
		{"phase_o_granted_bytes", d.GrantedBytes},
		{"phase_o_throttled_ops", d.ThrottledOps},
		{"phase_o_throttled_bytes", d.ThrottledBytes},
		{"phase_o_throttle_wait_ms", d.ThrottleWaitMs},
		{"phase_o_rejected_ops", d.RejectedOps},
		{"phase_o_rejection_reason", d.RejectionReason},
		{"phase_o_denied_tokens", d.DeniedTokens},
		{"phase_o_denied_bytes", d.DeniedBytes},
		{"phase_o_lease_id", d.LeaseID},
		{"phase_o_lease_generation", d.LeaseGeneration},
		{"phase_o_shared_budget_authority", d.SharedBudgetAuthority},
		{"phase_o_gateway_consumes_lease", d.GatewayConsumesLease},
		{"phase_o_cluster_wide_cap_support", d.ClusterWideCapSupport},
		{"phase_o_remote_lab_validation_state", d.RemoteLabValidationState},
	}
}

type structuredThrottleField struct {
	key   string
	value any
}

func tokenCapacity(cap, burst uint64) float64 {
	if cap == 0 {
		return 0
	}
	if burst > ^uint64(0)-cap {
		return float64(^uint64(0))
	}
	return float64(cap + burst)
}

func deficit(available, requested float64, cap uint64) float64 {
	if cap == 0 {
		return 0
	}
	if available >= requested {
		return 0
	}
	return requested - available
}

func waitForDeficit(deficit float64, cap uint64) time.Duration {
	if deficit <= 0 || cap == 0 {
		return 0
	}
	seconds := deficit / float64(cap)
	return time.Duration(seconds * float64(time.Second))
}

func durationMillisCeil(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64((d + time.Millisecond - 1) / time.Millisecond)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
