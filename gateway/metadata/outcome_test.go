package metadata

import (
	"errors"
	"sync"
	"testing"
)

func TestOutcomeObserverIsOptionalAndReplaceable(t *testing.T) {
	r := NewEtcdRepository(nil, "/namrbd")
	// No observer installed: reporting must not panic.
	r.observeEtcd(errors.New("boom"))

	var mu sync.Mutex
	var got []string
	r.SetEtcdOutcomeObserver(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if err == nil {
			got = append(got, "ok")
			return
		}
		got = append(got, err.Error())
	})
	r.observeEtcd(nil)
	r.observeEtcd(errors.New("no leader"))

	r.SetEtcdOutcomeObserver(nil)
	r.observeEtcd(errors.New("ignored"))

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "ok" || got[1] != "no leader" {
		t.Errorf("observed %v", got)
	}
}

// A nil repository must tolerate both calls, so a partially constructed
// gateway does not panic on a path that only exists to report a problem.
func TestNilRepositoryToleratesObservation(t *testing.T) {
	var r *EtcdRepository
	r.SetEtcdOutcomeObserver(func(error) { t.Fatal("called on a nil repository") })
	r.observeEtcd(errors.New("boom"))
}
