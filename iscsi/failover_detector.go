package iscsi

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

const (
	FailoverDetectionBudget            = 20 * time.Second
	FailoverApplyBudget                = 10 * time.Second
	FailoverP99Budget                  = 30 * time.Second
	FailoverHardCeiling                = 60 * time.Second
	AssumedInitiatorReplacementTimeout = 120 * time.Second
)

type FailoverReview struct {
	Reviewed                 bool
	Suppressed               bool
	ActiveGatewayID          string
	CandidateGatewayID       string
	Trigger                  string
	ObservedAt               time.Time
	MembershipAuthority      string
	ServingRegistryAuthority string
	Reason                   string
}

// ReviewISCSIFailover converts the etcd fleet view into a reviewed candidate.
// It never mutates serving mappings; only sbs-service may publish the returned
// transition into the TiKV registry.
func ReviewISCSIFailover(etcdAvailable bool, records []service.GatewayRecord, activeGatewayID string, standbyGatewayIDs []string, now time.Time, staleAfter time.Duration) FailoverReview {
	review := FailoverReview{
		ActiveGatewayID: strings.TrimSpace(activeGatewayID), ObservedAt: now,
		MembershipAuthority: "etcd", ServingRegistryAuthority: "tikv",
	}
	if !etcdAvailable {
		review.Suppressed = true
		review.Reason = "etcd fleet health is unavailable"
		return review
	}
	if staleAfter <= 0 {
		staleAfter = 2 * 5 * time.Second
	}
	byID := make(map[string]service.GatewayRecord, len(records))
	for _, raw := range records {
		rec := service.NormalizeGatewayFleetRecord(raw)
		if rec.Product != service.GatewayProductISCSI || rec.Role != service.GatewayRoleISCSI {
			continue
		}
		byID[strings.TrimSpace(rec.GatewayID)] = rec
	}
	active, found := byID[review.ActiveGatewayID]
	review.Trigger = failedISCSIGatewayTrigger(active, found, now, staleAfter)
	if review.Trigger == "" {
		review.Reason = "active gateway remains eligible"
		return review
	}
	candidates := append([]string(nil), standbyGatewayIDs...)
	sort.Strings(candidates)
	for _, id := range candidates {
		id = strings.TrimSpace(id)
		candidate, ok := byID[id]
		if id == "" || !ok || failedISCSIGatewayTrigger(candidate, true, now, staleAfter) != "" {
			continue
		}
		review.CandidateGatewayID = id
		review.Reviewed = true
		review.Reason = fmt.Sprintf("active gateway %s; standby %s is eligible", review.Trigger, id)
		return review
	}
	review.Suppressed = true
	review.Reason = "active gateway failed but no healthy standby is eligible"
	return review
}

func failedISCSIGatewayTrigger(rec service.GatewayRecord, found bool, now time.Time, staleAfter time.Duration) string {
	if !found || rec.LeaseExpiresAtUnix <= 0 || rec.LeaseExpiresAtUnix <= now.Unix() {
		return "lease_expired"
	}
	if rec.DrainState != service.GatewayDrainActive {
		return "drain"
	}
	if rec.ConnectionState == service.GatewayStateDown || rec.ConnectionState == service.GatewayStateDetached {
		return "explicit_down"
	}
	if rec.Readiness != service.GatewayReadinessReady || rec.ConnectionState != service.GatewayStateUp {
		return "readiness_loss"
	}
	if rec.LastSeenUnix <= 0 || now.Sub(time.Unix(rec.LastSeenUnix, 0)) > staleAfter {
		return "stale_heartbeat"
	}
	return ""
}

type FailoverTiming struct {
	Detection time.Duration
	Apply     time.Duration
	TotalP99  time.Duration
	Maximum   time.Duration
}

func (t FailoverTiming) ValidateBudget() error {
	switch {
	case t.Detection > FailoverDetectionBudget:
		return fmt.Errorf("failover detection %s exceeds %s", t.Detection, FailoverDetectionBudget)
	case t.Apply > FailoverApplyBudget:
		return fmt.Errorf("failover apply %s exceeds %s", t.Apply, FailoverApplyBudget)
	case t.TotalP99 > FailoverP99Budget:
		return fmt.Errorf("failover total p99 %s exceeds %s", t.TotalP99, FailoverP99Budget)
	case t.Maximum > FailoverHardCeiling:
		return fmt.Errorf("failover maximum %s exceeds %s", t.Maximum, FailoverHardCeiling)
	default:
		return nil
	}
}
