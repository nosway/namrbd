package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var errNoEtcdClient = errors.New("etcd client is not configured")

// PathPlanChangeNotifier is what the reconcile loop needs from a backend to
// avoid scanning on every tick.
//
// It is a separate optional interface rather than part of MetadataRepository so
// backends without change notification, the memory backend and the test fakes,
// keep working unchanged and keep scanning.
type PathPlanChangeNotifier interface {
	// WatchPathPlanInputs emits once per change under the prefixes the
	// path-plan loop reads. It closes the channel when the feed ends, which the
	// caller must treat as "resync and keep scanning".
	WatchPathPlanInputs(ctx context.Context) (<-chan struct{}, error)
}

// WatchPathPlanInputs watches the gateway and volume prefixes.
//
// The channel carries a signal, not the change itself. The reconcile loop reads
// the full state when it wakes, so what it needs to know is only whether
// anything moved; carrying the events would mean reconstructing state from a
// stream, which is a much larger change and a much larger failure surface.
func (r *EtcdRepository) WatchPathPlanInputs(ctx context.Context) (<-chan struct{}, error) {
	if r == nil || r.client == nil {
		return nil, errNoEtcdClient
	}
	out := make(chan struct{}, 1)

	w := &pathPlanWatch{lastSeen: map[string]pathPlanRelevant{}}
	gateways := r.client.Watch(ctx, r.gatewayPrefix(), clientv3.WithPrefix())
	volumes := r.client.Watch(ctx, r.volumeSpecPrefix(), clientv3.WithPrefix())

	go func() {
		defer close(out)
		for {
			var (
				resp clientv3.WatchResponse
				ok   bool
			)
			select {
			case <-ctx.Done():
				return
			case resp, ok = <-gateways:
				if !ok {
					return
				}
			case resp, ok = <-volumes:
				if !ok {
					return
				}
			}
			// A compaction or a canceled watch means the feed can no longer be
			// trusted to be complete. Ending it here makes the caller resync,
			// which is the safe direction: a missed event would leave a gateway
			// serving a stale path plan indefinitely.
			if resp.Canceled || resp.Err() != nil {
				return
			}
			if !w.hasMeaningfulChange(resp) {
				continue
			}
			r.pressure.countResync()
			select {
			case out <- struct{}{}:
			default:
				// A signal is already pending; one is enough, since the caller
				// reads full state when it wakes.
			}
		}
	}()
	return out, nil
}

// pathPlanRelevant is the part of a gateway record the reconcile loop reacts to.
//
// A gateway rewrites its own status record every few seconds to refresh its
// liveness, and that write lands under the prefix this watch covers. Treating
// every such write as a change would leave the gate permanently dirty on any
// live cluster, and the loop would scan on essentially every tick despite the
// gate. Only these fields move the path plan.
type pathPlanRelevant struct {
	Product         string   `json:"product"`
	Role            string   `json:"role"`
	ConnectionState string   `json:"connection_state"`
	Readiness       string   `json:"readiness"`
	DrainState      string   `json:"drain_state"`
	FailureDomain   string   `json:"failure_domain"`
	Capabilities    []string `json:"capabilities"`
	ControlRaw      string
	DataplaneRaw    string
}

type pathPlanWatchRecord struct {
	Product         string          `json:"product"`
	Role            string          `json:"role"`
	ConnectionState string          `json:"connection_state"`
	Readiness       string          `json:"readiness"`
	DrainState      string          `json:"drain_state"`
	FailureDomain   string          `json:"failure_domain"`
	Capabilities    []string        `json:"capabilities"`
	Control         json.RawMessage `json:"control_endpoints"`
	Dataplane       json.RawMessage `json:"dataplane_endpoints"`
}

type pathPlanWatch struct {
	mu       sync.Mutex
	lastSeen map[string]pathPlanRelevant
}

// hasMeaningfulChange reports whether a watch response carries anything the
// path plan depends on.
//
// Anything that is not a gateway status record, a delete, or an undecodable
// value is treated as meaningful. Guessing "nothing changed" on a value this
// code does not understand would silently stop reconciliation, which is the
// failure the whole gate design avoids.
func (w *pathPlanWatch) hasMeaningfulChange(resp clientv3.WatchResponse) bool {
	if len(resp.Events) == 0 {
		return false
	}
	for _, ev := range resp.Events {
		key := string(ev.Kv.Key)
		if ev.Type != clientv3.EventTypePut {
			w.forget(key)
			return true
		}
		if !strings.HasSuffix(key, "/status") {
			return true
		}
		var rec pathPlanWatchRecord
		if err := json.Unmarshal(ev.Kv.Value, &rec); err != nil {
			return true
		}
		next := pathPlanRelevant{
			Product:         rec.Product,
			Role:            rec.Role,
			ConnectionState: rec.ConnectionState,
			Readiness:       rec.Readiness,
			DrainState:      rec.DrainState,
			FailureDomain:   rec.FailureDomain,
			Capabilities:    rec.Capabilities,
			ControlRaw:      string(rec.Control),
			DataplaneRaw:    string(rec.Dataplane),
		}
		if w.observe(key, next) {
			return true
		}
	}
	return false
}

// observe records the relevant fields for a key and reports whether they moved.
// The first sighting counts as a change, since nothing is known about it yet.
func (w *pathPlanWatch) observe(key string, next pathPlanRelevant) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	prev, seen := w.lastSeen[key]
	w.lastSeen[key] = next
	if !seen {
		return true
	}
	return !samePathPlanRelevant(prev, next)
}

func (w *pathPlanWatch) forget(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.lastSeen, key)
}

func samePathPlanRelevant(a, b pathPlanRelevant) bool {
	if a.Product != b.Product || a.Role != b.Role || a.ConnectionState != b.ConnectionState || a.Readiness != b.Readiness ||
		a.DrainState != b.DrainState || a.FailureDomain != b.FailureDomain ||
		a.ControlRaw != b.ControlRaw || a.DataplaneRaw != b.DataplaneRaw {
		return false
	}
	if len(a.Capabilities) != len(b.Capabilities) {
		return false
	}
	for i := range a.Capabilities {
		if a.Capabilities[i] != b.Capabilities[i] {
			return false
		}
	}
	return true
}
