package metadata

import (
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nosway/namrbd/gateway/service"
)

func fleetKV(t *testing.T, key string, revision int64, rec service.GatewayRecord) *mvccpb.KeyValue {
	t.Helper()
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	return &mvccpb.KeyValue{Key: []byte(key), Value: payload, ModRevision: revision}
}

func TestDecodeGatewayFleetPagePinsRevisionAndCursor(t *testing.T) {
	prefix := "/namrbd/prod/gateways/"
	resp := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 41},
		Kvs: []*mvccpb.KeyValue{
			fleetKV(t, prefix+"gw-a/status", 39, service.GatewayRecord{GatewayID: "gw-a", ConnectionState: service.GatewayStateUp}),
			fleetKV(t, prefix+"gw-b/status", 40, service.GatewayRecord{GatewayID: "gw-b", Product: service.GatewayProductNAMRBD, Role: service.GatewayRoleBlock, Readiness: service.GatewayReadinessDegraded, DrainState: service.GatewayDrainDraining}),
		},
		More: true,
	}
	page, err := decodeGatewayFleetPage(prefix, resp)
	if err != nil {
		t.Fatal(err)
	}
	if page.Revision != 41 || page.NextCursor != prefix+"gw-b/status" || len(page.Records) != 2 {
		t.Fatalf("page = %+v", page)
	}
	if page.Records[0].RegistryRevision != 39 || page.Records[0].Readiness != service.GatewayReadinessReady {
		t.Fatalf("legacy page record was not normalized: %+v", page.Records[0])
	}
}

func TestGatewayFleetWatchCarriesCheckpointAndLifecycle(t *testing.T) {
	prefix := "/namrbd/prod/gateways/"
	put := fleetKV(t, prefix+"gw-a/status", 52, service.GatewayRecord{
		GatewayID: "gw-a", Product: service.GatewayProductNAMRBD, Role: service.GatewayRoleBlock,
		Readiness: service.GatewayReadinessDegraded, DrainState: service.GatewayDrainDraining,
	})
	resp := clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 53},
		Events: []*clientv3.Event{
			{Type: clientv3.EventTypePut, Kv: put},
			{Type: clientv3.EventTypeDelete, Kv: &mvccpb.KeyValue{Key: []byte(prefix + "gw-b/status"), ModRevision: 53}},
		},
	}
	events, err := decodeGatewayFleetWatchResponse(prefix, resp)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Type != GatewayFleetEventPut || events[0].Revision != 52 {
		t.Fatalf("put event = %+v", events)
	}
	if events[0].Record == nil || events[0].Record.DrainState != service.GatewayDrainDraining {
		t.Fatalf("drain transition missing: %+v", events[0])
	}
	if events[1].Type != GatewayFleetEventDelete || events[1].GatewayID != "gw-b" || events[1].Revision != 53 {
		t.Fatalf("lease-expiry delete = %+v", events[1])
	}
}

func TestGatewayFleetRootsRemainProductScoped(t *testing.T) {
	namrbd := NewEtcdRepository(nil, "/namrbd/prod")
	namros := NewEtcdRepository(nil, "/namros/prod")
	iscsi := NewEtcdRepository(nil, "/namrbd/prod/iscsi-gateways")
	keys := map[string]bool{}
	for _, key := range []string{
		namrbd.gatewayStatusKey("gw-a"),
		namros.gatewayStatusKey("gw-a"),
		iscsi.gatewayStatusKey("gw-a"),
	} {
		if keys[key] {
			t.Fatalf("gateway product root collision at %q", key)
		}
		keys[key] = true
	}
	if got := namros.gatewayStatusKey("gw-a"); got != "/namros/prod/gateways/gw-a/status" {
		t.Fatalf("NAMROS adapter key = %q", got)
	}
	if got := iscsi.gatewayStatusKey("gw-a"); got != "/namrbd/prod/iscsi-gateways/gateways/gw-a/status" {
		t.Fatalf("iSCSI adapter key = %q", got)
	}
}

func TestGatewayFleetCompatibilityRejectsMixedProducts(t *testing.T) {
	existing := service.NormalizeGatewayFleetRecord(service.GatewayRecord{GatewayID: "gw-a"})
	incoming := existing
	incoming.GatewayID = "gw-b"
	incoming.Product = service.GatewayProductNAMROS
	incoming.Role = service.GatewayRoleObject
	if err := validateGatewayRecordCompatibility(existing, incoming); err == nil {
		t.Fatal("a NAMROS record was accepted into a NAMRBD product root")
	}
}
