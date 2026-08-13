package policy

import "testing"

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		code int32
		want Decision
	}{
		{StatusOK, DecisionNoop},
		{ErrRetryable, DecisionRetryAlternatePath},
		{ErrTimeout, DecisionRetryAlternatePath},
		{ErrPathDraining, DecisionRetryAlternatePath},
		{ErrGenerationMismatch, DecisionRefreshSession},
		{ErrUnauthorized, DecisionFatalDetach},
		{ErrBusy, DecisionBackoffAndRetry},
		{ErrInvalidRange, DecisionFailToHost},
		{ErrInternal, DecisionFailToHost},
		{9999, DecisionFailToHost},
	}
	for _, tc := range tests {
		got := ClassifyStatus(tc.code)
		if got != tc.want {
			t.Fatalf("ClassifyStatus(%d): got=%v want=%v", tc.code, got, tc.want)
		}
	}
}
