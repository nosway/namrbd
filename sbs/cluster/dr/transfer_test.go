package dr

import (
	"errors"
	"testing"
)

func TestTransferCompletionIsDerivedFromVerifiedScopeAndReceipt(t *testing.T) {
	transfer, err := NewTransfer(1024, 4, "compat-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := AdvanceTransfer(transfer, Progress{Attempt: 1, CallerCompleted: true}); !errors.Is(err, ErrCallerCompletion) {
		t.Fatalf("caller completion err=%v", err)
	}
	partial, decision, err := AdvanceTransfer(transfer, Progress{
		Attempt: 1, ObjectCheckpoint: "objects/2", TransferredBytes: 512, TransferredObjects: 2,
		VerifiedBytes: 256, VerifiedObjects: 1, RetryClass: RetryClassTransient, ErrorMessage: "target timeout",
	})
	if err != nil || decision.Completed || partial.State != TransferStateVerifying || partial.FirstError != "target timeout" {
		t.Fatalf("partial=%+v decision=%+v err=%v", partial, decision, err)
	}
	if _, _, err := AdvanceTransfer(partial, Progress{Attempt: 2, TransferredBytes: 511, TransferredObjects: 2, VerifiedBytes: 256, VerifiedObjects: 1}); !errors.Is(err, ErrProgressRegression) {
		t.Fatalf("regression err=%v", err)
	}
	if _, _, err := AdvanceTransfer(partial, Progress{Attempt: 2, TransferredBytes: 1024, TransferredObjects: 4, VerifiedBytes: 1024, VerifiedObjects: 4}); err == nil {
		t.Fatal("completion without receipt succeeded")
	}
	completed, decision, err := AdvanceTransfer(partial, Progress{
		Attempt: 2, ObjectCheckpoint: "objects/4", TransferredBytes: 1024, TransferredObjects: 4,
		VerifiedBytes: 1024, VerifiedObjects: 4, TargetVerificationReceipt: "receipt-target-a", RetryClass: RetryClassNone,
	})
	if err != nil || !decision.Completed || !completed.Completed || completed.State != TransferStateCompleted || completed.LastError != "" {
		t.Fatalf("completed=%+v decision=%+v err=%v", completed, decision, err)
	}
	replayed, replayDecision, err := AdvanceTransfer(completed, Progress{
		Attempt: 2, ObjectCheckpoint: "objects/4", TransferredBytes: 1024, TransferredObjects: 4,
		VerifiedBytes: 1024, VerifiedObjects: 4, TargetVerificationReceipt: "receipt-target-a",
	})
	if err != nil || !replayDecision.Replay || replayed != completed {
		t.Fatalf("replay=%+v decision=%+v err=%v", replayed, replayDecision, err)
	}
}

func TestTransferCancellationIsTerminal(t *testing.T) {
	transfer, err := NewTransfer(100, 1, "compat")
	if err != nil {
		t.Fatal(err)
	}
	canceled, decision, err := AdvanceTransfer(transfer, Progress{Attempt: 1, CancelRequested: true})
	if err != nil || decision.Completed || canceled.State != TransferStateCanceled || canceled.Completed {
		t.Fatalf("canceled=%+v decision=%+v err=%v", canceled, decision, err)
	}
	replayed, replayDecision, err := AdvanceTransfer(canceled, Progress{Attempt: 1, CancelRequested: true})
	if err != nil || !replayDecision.Replay || replayed != canceled {
		t.Fatalf("canceled replay=%+v decision=%+v err=%v", replayed, replayDecision, err)
	}
	if _, _, err := AdvanceTransfer(canceled, Progress{Attempt: 2, TransferredBytes: 1}); !errors.Is(err, ErrTransferTerminal) {
		t.Fatalf("terminal err=%v", err)
	}
}

func TestTransferRejectsInconsistentTerminalClassification(t *testing.T) {
	transfer, err := NewTransfer(100, 1, "compat")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := AdvanceTransfer(transfer, Progress{Attempt: 1, RetryClass: RetryClassCanceled}); err == nil {
		t.Fatal("canceled retry class without cancellation succeeded")
	}
	if _, _, err := AdvanceTransfer(transfer, Progress{Attempt: 1, RetryClass: RetryClassPermanent}); err == nil {
		t.Fatal("permanent retry class without error detail succeeded")
	}
}
