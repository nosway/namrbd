package depavail

import (
	"encoding/json"
	"net/http"
)

// Report is the readiness body: what state the process is in, what it will and
// will not do in that state, and why.
type Report struct {
	Status   Status   `json:"status"`
	Behavior Behavior `json:"behavior"`
}

// Snapshot renders the current report without moving any counter.
func (t *Tracker) Snapshot() Report {
	if t == nil {
		return Report{}
	}
	return Report{Status: t.Status(), Behavior: t.Behavior()}
}

// ReadinessHandler serves the three-way dependency readiness surface.
//
// It always answers 200. That is not an oversight, and it is the most important
// decision in this file.
//
// A readiness endpoint is wired to something that removes the process from
// service: a Kubernetes readiness gate, a load balancer health check, a
// service-mesh outlier detector. Returning 503 while etcd is unreachable would
// therefore take the data path down through the orchestrator, in exactly the
// states where entry plan Section 4 requires that already-admitted I/O keep
// being served. The matrix would say fail-open and the deployment would behave
// fail-closed, and nothing in the code would look wrong.
//
// So the state goes in the body, where a human and a dashboard can read it and
// no automation evicts a serving process on the strength of it. A process that
// genuinely cannot serve fails liveness, which is a different signal with a
// different consequence.
func ReadinessHandler(t *Tracker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(t.Snapshot())
	})
}
