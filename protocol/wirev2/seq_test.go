package wirev2

import (
	"testing"
)

func TestSeqCheckerStrictInOrder(t *testing.T) {
	var s SeqChecker
	if err := s.CheckNext(1); err != nil {
		t.Fatalf("first seq 1: %v", err)
	}
	if s.Last() != 1 {
		t.Fatalf("last want 1 got %d", s.Last())
	}
	if err := s.CheckNext(2); err != nil {
		t.Fatalf("seq 2: %v", err)
	}
	if err := s.CheckNext(4); err != ErrReplaySeq {
		t.Fatalf("seq 4 should be replay, got %v", err)
	}
	if err := s.CheckNext(2); err != ErrReplaySeq {
		t.Fatalf("duplicate seq 2 should be replay, got %v", err)
	}
	if err := s.CheckNext(3); err != nil {
		t.Fatalf("seq 3: %v", err)
	}
	s.Reset()
	if s.Last() != 0 {
		t.Fatalf("after reset last want 0 got %d", s.Last())
	}
	if err := s.CheckNext(1); err != nil {
		t.Fatalf("after reset seq 1: %v", err)
	}
}
