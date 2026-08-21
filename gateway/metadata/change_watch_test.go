package metadata

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// The reconcile loop only skips scans when the backend it got can report
// changes. If EtcdRepository stopped satisfying the interface the loop would
// silently fall back to scanning every tick, which is the cost this removes.
func TestEtcdRepositorySatisfiesChangeNotifier(t *testing.T) {
	var _ PathPlanChangeNotifier = (*EtcdRepository)(nil)
}

// A repository with no client must report the failure rather than return a
// channel that never fires, which would leave the gate clean forever.
func TestWatchWithoutClientFailsRatherThanGoingSilent(t *testing.T) {
	r := NewEtcdRepository(nil, "/namrbd")
	ch, err := r.WatchPathPlanInputs(context.Background())
	if err == nil {
		t.Fatal("a repository with no etcd client returned a watch")
	}
	if ch != nil {
		t.Error("a failed watch returned a channel")
	}
	var nilRepo *EtcdRepository
	if _, err := nilRepo.WatchPathPlanInputs(context.Background()); err == nil {
		t.Error("a nil repository returned a watch")
	}
}

func putEvent(key, value string) *clientv3.Event {
	return &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv:   &mvccpb.KeyValue{Key: []byte(key), Value: []byte(value)},
	}
}

func statusValue(state string, lastSeen int64) string {
	return fmt.Sprintf(`{"gateway_id":"gw-1","connection_state":%q,"last_seen_unix":%d,`+
		`"lease_id":"7","lease_expires_at_unix":%d,`+
		`"control_endpoints":[{"address":"10.0.0.1:9701"}],"dataplane_endpoints":[{"address":"10.0.0.1:9700"}]}`,
		state, lastSeen, lastSeen+30)
}

// A gateway rewrites its status every few seconds to refresh liveness, and that
// write lands under the watched prefix. Treating each one as a change would
// leave the gate permanently dirty on any live cluster, and the reconcile loop
// would scan on essentially every tick despite the gate.
func TestHeartbeatOnlyWritesAreNotChanges(t *testing.T) {
	w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
	key := "/namrbd/gateways/gw-1/status"

	// The first sighting is a change: nothing is known about it yet.
	if !w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, statusValue("connected", 1000))}}) {
		t.Fatal("the first sighting of a gateway was not treated as a change")
	}
	// Twenty renewals that move only the timestamps.
	for i := 1; i <= 20; i++ {
		resp := clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, statusValue("connected", int64(1000+i*15)))}}
		if w.hasMeaningfulChange(resp) {
			t.Fatalf("renewal %d was treated as a path-plan change", i)
		}
	}
}

// A field the path plan depends on must still wake the loop.
func TestRelevantFieldChangeIsAChange(t *testing.T) {
	key := "/namrbd/gateways/gw-1/status"
	for _, tc := range []struct {
		name  string
		first string
		next  string
	}{
		{"connection state", statusValue("connected", 1000), statusValue("draining", 1015)},
		{"readiness", statusValue("connected", 1000),
			`{"gateway_id":"gw-1","connection_state":"connected","readiness":"degraded","last_seen_unix":1015,` +
				`"control_endpoints":[{"address":"10.0.0.1:9701"}],"dataplane_endpoints":[{"address":"10.0.0.1:9700"}]}`},
		{"drain state", statusValue("connected", 1000),
			`{"gateway_id":"gw-1","connection_state":"connected","drain_state":"draining","last_seen_unix":1015,` +
				`"control_endpoints":[{"address":"10.0.0.1:9701"}],"dataplane_endpoints":[{"address":"10.0.0.1:9700"}]}`},
		{"dataplane endpoint", statusValue("connected", 1000),
			`{"gateway_id":"gw-1","connection_state":"connected","last_seen_unix":1015,` +
				`"control_endpoints":[{"address":"10.0.0.1:9701"}],"dataplane_endpoints":[{"address":"10.0.0.9:9700"}]}`},
		{"failure domain", statusValue("connected", 1000),
			`{"gateway_id":"gw-1","connection_state":"connected","failure_domain":"rack-2","last_seen_unix":1015,` +
				`"control_endpoints":[{"address":"10.0.0.1:9701"}],"dataplane_endpoints":[{"address":"10.0.0.1:9700"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
			w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, tc.first)}})
			if !w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, tc.next)}}) {
				t.Errorf("a %s change did not wake the loop", tc.name)
			}
		})
	}
}

// Anything the filter does not understand must count as a change. Guessing
// "nothing moved" on an unrecognized value would silently stop reconciliation.
func TestUnrecognizedEventsAreTreatedAsChanges(t *testing.T) {
	w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
	for _, tc := range []struct {
		name string
		ev   *clientv3.Event
	}{
		{"volume spec write", putEvent("/namrbd/volumes/00000001/spec", `{"id":1}`)},
		{"non-status gateway key", putEvent("/namrbd/gateways/gw-1/other", `{}`)},
		{"undecodable status", putEvent("/namrbd/gateways/gw-2/status", `not json`)},
		{"gateway removal", &clientv3.Event{Type: clientv3.EventTypeDelete,
			Kv: &mvccpb.KeyValue{Key: []byte("/namrbd/gateways/gw-3/status")}}},
	} {
		if !w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{tc.ev}}) {
			t.Errorf("%s was not treated as a change", tc.name)
		}
	}
}

// A gateway that leaves and comes back must wake the loop, not be matched
// against a record from before it disappeared.
func TestRemovalForgetsTheRecord(t *testing.T) {
	w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
	key := "/namrbd/gateways/gw-1/status"
	w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, statusValue("connected", 1000))}})
	w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{{
		Type: clientv3.EventTypeDelete, Kv: &mvccpb.KeyValue{Key: []byte(key)},
	}}})
	if !w.hasMeaningfulChange(clientv3.WatchResponse{Events: []*clientv3.Event{putEvent(key, statusValue("connected", 1100))}}) {
		t.Error("a gateway that rejoined was matched against its pre-removal record")
	}
}

// An empty response carries nothing to react to.
func TestEmptyResponseIsNotAChange(t *testing.T) {
	w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
	if w.hasMeaningfulChange(clientv3.WatchResponse{}) {
		t.Error("an empty watch response was treated as a change")
	}
}
