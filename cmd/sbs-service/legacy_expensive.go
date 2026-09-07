package main

import (
	"context"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const legacyExpensiveMaximumRecordBudget = 1_000_000

func (s *server) admitLegacyExpensive(ctx context.Context, surface string, admission *adminv1.ExpensiveCallAdmission) (uint32, error) {
	s.phaseADCurrentObservability.recordLegacyExpensive(surface, "requested")
	reason := ""
	budget := uint32(0)
	if admission != nil {
		reason = strings.TrimSpace(admission.GetReason())
		budget = admission.GetRecordBudget()
	}
	_, hasDeadline := ctx.Deadline()
	if reason == "" || budget == 0 || budget > legacyExpensiveMaximumRecordBudget || !hasDeadline || ctx.Err() != nil {
		s.phaseADCurrentObservability.recordLegacyExpensive(surface, "rejected")
		return 0, status.Errorf(codes.FailedPrecondition, "%s is a legacy expensive completion; reason, record_budget 1..%d, and request deadline are required", surface, legacyExpensiveMaximumRecordBudget)
	}
	s.phaseADCurrentObservability.recordLegacyExpensive(surface, "admitted")
	return budget, nil
}

func (s *server) completeLegacyExpensive(surface string, budget uint32, records int) error {
	if records > int(budget) {
		s.phaseADCurrentObservability.recordLegacyExpensive(surface, "budget_exceeded")
		return status.Errorf(codes.ResourceExhausted, "%s returned %d records beyond admitted budget %d", surface, records, budget)
	}
	s.phaseADCurrentObservability.recordLegacyExpensive(surface, "completed")
	return nil
}
