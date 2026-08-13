package wirev2

import "errors"

// ErrReplaySeq is returned when seq_no is not exactly last_seq_no + 1.
var ErrReplaySeq = errors.New("seq_no replay or out-of-order")

// SeqChecker enforces strict in-order seq_no per session (Phase C3).
// HELLO uses session_id=0, seq_no=0 and is not checked.
// After session establishment, every frame must have seq_no == lastSeqNo + 1.
type SeqChecker struct {
	lastSeqNo uint64
}

// CheckNext validates that seq is exactly lastSeqNo+1 and updates state.
// Call for every authenticated frame (session_id != 0).
func (s *SeqChecker) CheckNext(seq uint64) error {
	next := s.lastSeqNo + 1
	if seq != next {
		return ErrReplaySeq
	}
	s.lastSeqNo = seq
	return nil
}

// Last returns the last accepted seq_no.
func (s *SeqChecker) Last() uint64 {
	return s.lastSeqNo
}

// Reset clears state (e.g. on new session).
func (s *SeqChecker) Reset() {
	s.lastSeqNo = 0
}
