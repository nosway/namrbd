package iscsi

import (
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

func TestReviewISCSIFailoverUsesEtcdHealthWithoutMutatingServingAuthority(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	records := []service.GatewayRecord{
		iscsiFleetRecord("gw-a", now.Add(-30*time.Second), now.Add(time.Minute)),
		iscsiFleetRecord("gw-b", now, now.Add(time.Minute)),
	}
	review := ReviewISCSIFailover(true, records, "gw-a", []string{"gw-b"}, now, 10*time.Second)
	if !review.Reviewed || review.Suppressed || review.Trigger != "stale_heartbeat" || review.CandidateGatewayID != "gw-b" {
		t.Fatalf("review=%+v", review)
	}
	if review.MembershipAuthority != "etcd" || review.ServingRegistryAuthority != "tikv" {
		t.Fatalf("authority boundary=%+v", review)
	}
	if records[0].ConnectionState != service.GatewayStateUp || records[1].GatewayID != "gw-b" {
		t.Fatalf("health review mutated fleet records: %+v", records)
	}
}

func TestReviewISCSIFailoverSuppressesWhenEtcdUnavailable(t *testing.T) {
	review := ReviewISCSIFailover(false, nil, "gw-a", []string{"gw-b"}, time.Now(), 10*time.Second)
	if review.Reviewed || !review.Suppressed || review.CandidateGatewayID != "" {
		t.Fatalf("review=%+v", review)
	}
}

func TestFailoverTimingBudget(t *testing.T) {
	if err := (FailoverTiming{
		Detection: 15 * time.Second, Apply: 7 * time.Second,
		TotalP99: 22 * time.Second, Maximum: 40 * time.Second,
	}).ValidateBudget(); err != nil {
		t.Fatalf("valid timing: %v", err)
	}
	if err := (FailoverTiming{Detection: 21 * time.Second}).ValidateBudget(); err == nil {
		t.Fatal("detection over budget was accepted")
	}
}

func iscsiFleetRecord(id string, seen, expires time.Time) service.GatewayRecord {
	return service.GatewayRecord{
		SchemaVersion: service.GatewayFleetSchemaVersion, GatewayID: id,
		Product: service.GatewayProductISCSI, Role: service.GatewayRoleISCSI,
		ConnectionState: service.GatewayStateUp, Readiness: service.GatewayReadinessReady,
		DrainState: service.GatewayDrainActive, LastSeenUnix: seen.Unix(), LeaseExpiresAtUnix: expires.Unix(),
	}
}
