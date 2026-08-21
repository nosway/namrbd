package fleet

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

func TestRecordFromConfigPublishesAllPortalsWithoutServingAuthority(t *testing.T) {
	rec, err := RecordFromConfig(Config{
		GatewayID:        "iscsi-gw-01",
		AdvertisePortals: []string{"10.20.0.11:3260", "[2001:db8::11]:3260", "10.20.0.11:3260"},
		BuildVersion:     "v1.0.0-rc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Product != service.GatewayProductISCSI || rec.Role != service.GatewayRoleISCSI {
		t.Fatalf("fleet identity = %s/%s", rec.Product, rec.Role)
	}
	if len(rec.AdvertisedAddresses) != 2 || len(rec.DataplaneEndpoints) != 2 {
		t.Fatalf("portal projection = %+v / %+v", rec.AdvertisedAddresses, rec.DataplaneEndpoints)
	}
	if rec.DataplaneEndpoints[1].Address != "2001:db8::11" || rec.DataplaneEndpoints[1].Port != 3260 {
		t.Fatalf("IPv6 portal = %+v", rec.DataplaneEndpoints[1])
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"target_iqn", "lun_id", "export_id", "volume_id", "export_epoch", "export_lease"} {
		if strings.Contains(string(payload), forbidden) {
			t.Errorf("etcd fleet record contains serving authority %q: %s", forbidden, payload)
		}
	}
}

func TestSummarizeThirtyTwoGatewayFleetTracksHeartbeatDrainAndExpiry(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	records := make([]service.GatewayRecord, 0, 32)
	for i := 0; i < 32; i++ {
		records = append(records, service.GatewayRecord{
			GatewayID: fmt.Sprintf("iscsi-gw-%02d", i), Product: service.GatewayProductISCSI,
			Role: service.GatewayRoleISCSI, ConnectionState: service.GatewayStateUp,
			Readiness: service.GatewayReadinessReady, DrainState: service.GatewayDrainActive,
			LastSeenUnix: now.Add(-DefaultStatusRefresh).Unix(), LeaseExpiresAtUnix: now.Add(DefaultLeaseTTL).Unix(),
		})
	}

	// A heartbeat renews gw-00. gw-30 is draining, while gw-31 has missed the
	// lease expiry boundary and remains visible only as a stale cached listing.
	records[0].LastSeenUnix = now.Unix()
	records[0].LeaseExpiresAtUnix = now.Add(DefaultLeaseTTL).Unix()
	records[30].ConnectionState = service.GatewayStateDegraded
	records[30].Readiness = service.GatewayReadinessDegraded
	records[30].DrainState = service.GatewayDrainDraining
	records[31].LeaseExpiresAtUnix = now.Add(-time.Second).Unix()

	summary := Summarize(records, now)
	if summary.MembershipAuthority != MembershipAuthority || summary.HealthAuthority != HealthAuthority {
		t.Fatalf("authority = %+v", summary)
	}
	if summary.GatewayCount != 32 || summary.ReadyCount != 30 || summary.StaleCount != 1 {
		t.Fatalf("fleet summary = %+v", summary)
	}
	ids := SortedGatewayIDs(records)
	if len(ids) != 32 || ids[0] != "iscsi-gw-00" || ids[31] != "iscsi-gw-31" {
		t.Fatalf("deterministic ids = %v", ids)
	}
}

func TestISCSIFleetRejectsMixedProductRoot(t *testing.T) {
	records := []service.GatewayRecord{{
		GatewayID: "block-gw-01", Product: service.GatewayProductNAMRBD, Role: service.GatewayRoleBlock,
		ConnectionState: service.GatewayStateUp, Readiness: service.GatewayReadinessReady,
		DrainState: service.GatewayDrainActive,
	}}
	if err := validateISCSIRecords(records); err == nil {
		t.Fatal("iSCSI fleet accepted a block gateway record")
	}
}

func TestRecordFromConfigRequiresGatewayAndPortal(t *testing.T) {
	if _, err := RecordFromConfig(Config{AdvertisePortals: []string{"10.20.0.11:3260"}}); err == nil {
		t.Fatal("empty gateway id was accepted")
	}
	if _, err := RecordFromConfig(Config{GatewayID: "iscsi-gw-01"}); err == nil {
		t.Fatal("empty portal set was accepted")
	}
}
