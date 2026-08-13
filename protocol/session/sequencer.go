package session

import "fmt"

// Sequencer tracks per-session request_id monotonicity.
type Sequencer struct {
	initialized bool
	last        uint64
}

func (s *Sequencer) ObserveNewRequest(id uint64) error {
	if !s.initialized {
		s.initialized = true
		s.last = id
		return nil
	}
	if id <= s.last {
		return fmt.Errorf("request_id must be strictly monotonic: got=%d last=%d", id, s.last)
	}
	s.last = id
	return nil
}

// ObserveRetry validates same-session retry rule.
func (s *Sequencer) ObserveRetry(id uint64) error {
	if !s.initialized {
		return fmt.Errorf("retry observed before any request")
	}
	if id != s.last {
		return fmt.Errorf("same-session retry must reuse request_id: got=%d last=%d", id, s.last)
	}
	return nil
}
