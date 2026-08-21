package metadata

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

// Without jitter every gateway that started together renews together forever,
// and the fleet's whole status-write load lands in the same instant of each
// period. At t2_large that is 64 writes in one moment rather than spread out.
func TestJitteredIntervalSpreadsRenewals(t *testing.T) {
	base := 5 * time.Second
	rnd := rand.New(rand.NewSource(1))

	seen := map[time.Duration]int{}
	var min, max time.Duration = math.MaxInt64, 0
	for i := 0; i < 2000; i++ {
		d := jitteredInterval(base, rnd)
		seen[d]++
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct intervals in 2000 draws; renewals are not spread", len(seen))
	}

	lo := time.Duration(float64(base) * (1 - StatusWriteJitterFraction))
	hi := time.Duration(float64(base) * (1 + StatusWriteJitterFraction))
	if min < lo || max > hi {
		t.Errorf("intervals ranged %v..%v, outside the %v..%v band", min, max, lo, hi)
	}
	// The spread has to be wide enough to actually separate a fleet; a band
	// that collapses toward the nominal value would pass the bounds check while
	// leaving the herd intact.
	if max-min < time.Duration(float64(base)*StatusWriteJitterFraction) {
		t.Errorf("observed spread %v is narrower than the jitter fraction allows", max-min)
	}
}

func TestGatewayLeaseCadenceDefaultsToPhaseAAEnvelope(t *testing.T) {
	ttl, refresh, err := normalizeGatewayLeaseCadence(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != 15*time.Second || refresh != 5*time.Second {
		t.Fatalf("lease cadence = %s/%s, want 15s/5s", ttl, refresh)
	}
	if _, _, err := normalizeGatewayLeaseCadence(5*time.Second, 5*time.Second); err == nil {
		t.Fatal("refresh interval equal to TTL was accepted")
	}
}

func TestJitteredIntervalHandlesDegenerateBase(t *testing.T) {
	rnd := rand.New(rand.NewSource(2))
	if got := jitteredInterval(0, rnd); got != 0 {
		t.Errorf("zero base produced %v", got)
	}
	if got := jitteredInterval(-time.Second, rnd); got != -time.Second {
		t.Errorf("negative base produced %v", got)
	}
	// A base small enough that the jitter could cross zero must stay positive,
	// or the timer would fire in a tight loop.
	for i := 0; i < 500; i++ {
		if got := jitteredInterval(time.Nanosecond, rnd); got <= 0 {
			t.Fatalf("tiny base produced a non-positive interval %v", got)
		}
	}
}

// The counters are what make the saving observable rather than asserted.
func TestPressureCountersRecordEachClass(t *testing.T) {
	p := &EtcdPressure{}
	r := &EtcdRepository{pressure: p}

	if got := r.PressureSnapshot(); got != (EtcdPressureSnapshot{}) {
		t.Errorf("a fresh repository reports %+v", got)
	}
	p.countPrefixScan()
	p.countPrefixScan()
	p.countStatusWrite()
	p.countPointRead()
	p.countResync()
	p.countSkippedValidation()
	p.countSkippedValidation()
	p.countSkippedValidation()

	got := r.PressureSnapshot()
	want := EtcdPressureSnapshot{
		PrefixScanCount: 2, StatusWriteCount: 1, PointReadCount: 1,
		ResyncCount: 1, SkippedValidationCount: 3,
	}
	if got != want {
		t.Errorf("snapshot = %+v, want %+v", got, want)
	}
}

// A nil pressure holder must not panic: a repository built by an older path
// should degrade to counting nothing rather than crashing a gateway.
func TestPressureCountersTolerateNilHolder(t *testing.T) {
	var p *EtcdPressure
	p.countPrefixScan()
	p.countStatusWrite()
	p.countPointRead()
	p.countResync()
	p.countSkippedValidation()

	r := &EtcdRepository{}
	if got := r.PressureSnapshot(); got != (EtcdPressureSnapshot{}) {
		t.Errorf("a repository with no counters reports %+v", got)
	}
	var nilRepo *EtcdRepository
	if got := nilRepo.PressureSnapshot(); got != (EtcdPressureSnapshot{}) {
		t.Errorf("a nil repository reports %+v", got)
	}
}

// The constructor must wire counters up, or every gateway reports zero pressure
// while doing the work.
func TestNewEtcdRepositoryEnablesCounters(t *testing.T) {
	r := NewEtcdRepository(nil, "/namrbd")
	if r.pressure == nil {
		t.Fatal("a constructed repository has no pressure counters")
	}
	r.pressure.countPrefixScan()
	if r.PressureSnapshot().PrefixScanCount != 1 {
		t.Error("the constructed repository does not count")
	}
}

// Renewals must not re-scan the fleet prefix. The counter makes the skip a
// measurement rather than a claim, and this path reaches no client at all.
func TestRenewalSkipsFleetValidation(t *testing.T) {
	r := NewEtcdRepository(nil, "/namrbd")
	if err := r.validateForLeaseWrite(context.Background(), service.GatewayRecord{GatewayID: "gw-1"}, false); err != nil {
		t.Fatalf("a renewal validation returned an error: %v", err)
	}
	snap := r.PressureSnapshot()
	if snap.SkippedValidationCount != 1 {
		t.Errorf("skipped validations = %d, want 1", snap.SkippedValidationCount)
	}
	if snap.PrefixScanCount != 0 {
		t.Errorf("a renewal performed %d prefix scans", snap.PrefixScanCount)
	}
}

// The renewal call site itself must pass false. Reverting it compiles and
// behaves, so only reading the source catches the regression.
func TestRenewalCallSitePassesNoValidation(t *testing.T) {
	raw, err := os.ReadFile("gateway_lease.go")
	if err != nil {
		t.Fatalf("read gateway_lease.go: %v", err)
	}
	src := string(raw)
	if !strings.Contains(src, "r.putGatewayWithLease(ctx, rec, lease.ID, false)") {
		t.Error("the lease renewal no longer passes validate=false; renewals would re-scan the whole fleet prefix on every tick")
	}
	if !strings.Contains(src, "r.putGatewayWithLease(ctx, rec, lease.ID, true)") {
		t.Error("registration no longer validates against the fleet registry")
	}
	// The renewal must be the jittered timer, not a fixed ticker.
	if strings.Contains(src, "time.NewTicker(ttl / 2)") {
		t.Error("the renewal uses a fixed ticker again; a fleet that started together would renew in the same instant")
	}
	if !strings.Contains(src, "jitteredInterval(base, rnd)") {
		t.Error("the renewal interval is no longer jittered")
	}
}
