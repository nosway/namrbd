package session

import "testing"

func TestSequencerRules(t *testing.T) {
	var s Sequencer
	if err := s.ObserveNewRequest(10); err != nil {
		t.Fatalf("ObserveNewRequest failed: %v", err)
	}
	if err := s.ObserveRetry(10); err != nil {
		t.Fatalf("ObserveRetry failed: %v", err)
	}
	if err := s.ObserveNewRequest(11); err != nil {
		t.Fatalf("ObserveNewRequest(11) failed: %v", err)
	}
	if err := s.ObserveNewRequest(11); err == nil {
		t.Fatalf("expected monotonicity failure")
	}
	if err := s.ObserveRetry(12); err == nil {
		t.Fatalf("expected retry id mismatch failure")
	}
}
