// Package depbudget records every etcd and TiKV access path NAMRBD has, and the
// budget each must stay inside at a named scale tier.
//
// AA-IMPL-003A owns the inventory. Its purpose is narrow and specific: a
// dependency access that nobody has classified is a dependency access nobody
// has sized, and at t2_large the difference between a bounded read and an
// unbounded prefix scan is the difference between a cluster that runs and one
// that does not. The completeness test scans the source, so a new call site
// added later fails the gate rather than joining silently.
package depbudget

import "sort"

// Store names which dependency an access reaches.
type Store string

const (
	StoreEtcd Store = "etcd"
	StoreTiKV Store = "tikv"
)

// Cadence says how often an access happens, which is what turns a single
// expensive read into a sustained load.
type Cadence string

const (
	// CadenceStartup runs once when the process starts.
	CadenceStartup Cadence = "startup"
	// CadenceOnDemand runs in response to an operator or client request.
	CadenceOnDemand Cadence = "on_demand"
	// CadenceDataPath runs per I/O request.
	CadenceDataPath Cadence = "data_path"
	// CadenceTimer runs on a fixed interval whether or not anything changed.
	// This is the cadence that turns an unbounded read into a standing cost.
	CadenceTimer Cadence = "timer"
	// CadenceLease renews a liveness lease.
	CadenceLease Cadence = "lease"
)

// RequestClass groups accesses the way an operator reasons about them.
type RequestClass string

const (
	ClassStartup     RequestClass = "startup"
	ClassWatch       RequestClass = "watch"
	ClassResync      RequestClass = "resync"
	ClassHealth      RequestClass = "health"
	ClassPlacement   RequestClass = "placement"
	ClassRegistry    RequestClass = "registry"
	ClassWriteEffect RequestClass = "write_effect"
	ClassDebugReport RequestClass = "debug_report"
)

// Boundedness says whether one call's cost is capped.
type Boundedness string

const (
	// BoundedPoint reads or writes a single key.
	BoundedPoint Boundedness = "point"
	// BoundedBatch reads a caller-supplied key set.
	BoundedBatch Boundedness = "batch"
	// BoundedLimit scans a prefix with an explicit limit.
	BoundedLimit Boundedness = "limited_prefix"
	// UnboundedPrefix scans a whole prefix with no limit. Cost grows with the
	// number of records under it, which at t2_large is the volume or gateway
	// count.
	UnboundedPrefix Boundedness = "unbounded_prefix"
)

// Access is one classified dependency access path.
type Access struct {
	// Func is the Go function containing the access, used by the completeness
	// test to match against the source.
	Func    string
	File    string
	Store   Store
	Owner   string
	Class   RequestClass
	Cadence Cadence
	Bound   Boundedness
	// GrowsWith names what the cost scales with, empty when it does not scale.
	GrowsWith string
	// Note carries the reason a reviewer needs, especially for the accesses
	// that are a problem today.
	Note string
}

// accesses is the inventory. Every function in gateway/metadata/etcd.go,
// gateway/metadata/gateway_lease.go, and sbs/cluster/metadata/tikv.go that
// touches a dependency appears here.
var accesses = []Access{
	// --- etcd: volume records ---
	{"GetVolume", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"GetVolumeStatus", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"PutVolumeStatus", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"EnsureVolume", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"UpdateVolume", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"DeleteVolume", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", "deletes the per-volume extent and garbage prefixes in one transaction"},
	{"SetVolumeState", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"SyncVolumeSpec", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"ListVolumes", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, UnboundedPrefix, "volume count",
		"AA-IMPL-003B gated the path-plan loop on a change watch and the chunk-GC sweep now rotates through volumes, refreshing the list once per rotation rather than once per tick. What remains is on-demand: operator and API paths, plus one list every sixteen sweep passes at t2_large"},

	// --- etcd: gateway fleet records ---
	{"GetGateway", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"PutGateway", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"ListGatewayFleetPage", "gateway/metadata/gateway_fleet.go", StoreEtcd, "namrbd-gateway", ClassPlacement, CadenceOnDemand, BoundedLimit, "",
		"AA-IMPL-005 pins each bounded page to one revision; the path-plan reconcile loop remains change-gated by AA-IMPL-003B"},
	{"WatchGatewayFleet", "gateway/metadata/gateway_fleet.go", StoreEtcd, "namrbd-gateway", ClassWatch, CadenceOnDemand, BoundedPoint, "",
		"resumes at checkpoint+1 and closes with resync_required after compaction or any undecodable revision gap"},

	// --- etcd: attachment and generation ---
	{"Attach", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"Detach", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"GetAttachment", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"UnsafeClearAttachment", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassDebugReport, CadenceOnDemand, BoundedPoint, "", "lab-only recovery path"},
	{"UnsafeSetGeneration", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassDebugReport, CadenceOnDemand, BoundedPoint, "", "lab-only recovery path"},
	{"getGeneration", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},

	// --- etcd: allocation and write planning ---
	{"AllocateChunkIDs", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassWriteEffect, CadenceDataPath, BoundedPoint, "",
		"on the write path; the gateway chunk-ID allocation cache is what keeps this off every I/O"},
	{"GetExtentPage", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassWriteEffect, CadenceDataPath, BoundedPoint, "",
		"gateway/service.cachedMetadataRepository holds a 4096-entry extent page cache in front of this; without it every write would read a page"},
	{"PutExtentPage", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassWriteEffect, CadenceDataPath, BoundedPoint, "",
		"a compare-and-set write per changed page; it is not cacheable, so the page-scoped write metadata mode is what bounds how often it runs"},
	{"ListExtentPages", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassPlacement, CadenceOnDemand, UnboundedPrefix, "volume size",
		"scoped to one volume, but unbounded within it"},

	// --- etcd: chunk garbage collection ---
	{"PutChunkGarbage", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassWriteEffect, CadenceOnDemand, BoundedPoint, "", ""},
	{"ListChunkGarbage", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassPlacement, CadenceTimer, BoundedLimit, "",
		"the sweep limit is what keeps this bounded; it is the model the other list paths should follow"},
	{"DeleteChunkGarbage", "gateway/metadata/etcd.go", StoreEtcd, "namrbd-gateway", ClassWriteEffect, CadenceTimer, BoundedPoint, "", ""},

	// --- etcd: gateway liveness lease ---
	{"StartGatewayLease", "gateway/metadata/gateway_lease.go", StoreEtcd, "namrbd-gateway", ClassHealth, CadenceLease, BoundedPoint, "gateway count",
		"grant, put, and keepalive for this gateway's own liveness record; the renewal timer is jittered so a fleet that started together does not renew in the same instant"},
	{"putGatewayWithLease", "gateway/metadata/gateway_lease.go", StoreEtcd, "namrbd-gateway", ClassHealth, CadenceLease, BoundedPoint, "", ""},

	// --- TiKV: SBS metadata ---
	{"Get", "sbs/cluster/metadata/tikv.go", StoreTiKV, "sbs-service", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"BatchGet", "sbs/cluster/metadata/tikv.go", StoreTiKV, "sbs-service", ClassRegistry, CadenceOnDemand, BoundedBatch, "requested key count",
		"the caller supplies the key set, so the batch size limit is what bounds it"},
	{"Delete", "sbs/cluster/metadata/tikv.go", StoreTiKV, "sbs-service", ClassRegistry, CadenceOnDemand, BoundedPoint, "", ""},
	{"commitTiKVTxnWithTrace", "sbs/cluster/metadata/tikv.go", StoreTiKV, "sbs-service", ClassWriteEffect, CadenceOnDemand, BoundedPoint, "",
		"transaction commit; the trace wrapper is opt-in and off in production-like profiles"},
}

// All returns the inventory, stably ordered.
func All() []Access {
	out := append([]Access(nil), accesses...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Func < out[j].Func
	})
	return out
}

// InventoriedFiles lists the source files the inventory claims to cover. The
// completeness test scans exactly these.
func InventoriedFiles() []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range accesses {
		if !seen[a.File] {
			seen[a.File] = true
			out = append(out, a.File)
		}
	}
	sort.Strings(out)
	return out
}

// Lookup finds an access by file and function.
func Lookup(file, fn string) (Access, bool) {
	for _, a := range accesses {
		if a.File == file && a.Func == fn {
			return a, true
		}
	}
	return Access{}, false
}
