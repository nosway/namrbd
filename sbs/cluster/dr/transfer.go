package dr

import (
	"errors"
	"fmt"
	"strings"
)

const (
	TransferStateAdmitted     = "admitted"
	TransferStateTransferring = "transferring"
	TransferStateVerifying    = "verifying"
	TransferStateCompleted    = "completed"
	TransferStateCanceled     = "canceled"
	TransferStateFailed       = "failed"

	RetryClassNone      = "none"
	RetryClassTransient = "transient"
	RetryClassTimeout   = "timeout"
	RetryClassThrottle  = "throttle"
	RetryClassPermanent = "permanent"
	RetryClassCanceled  = "canceled"
)

var (
	ErrCallerCompletion   = errors.New("remote transfer completion is derived from target verification")
	ErrProgressRegression = errors.New("remote transfer progress cannot regress")
	ErrTransferTerminal   = errors.New("remote transfer is terminal")
)

type Transfer struct {
	Attempt                   uint32
	ObjectCheckpoint          string
	ExpectedBytes             uint64
	ExpectedObjects           uint64
	TransferredBytes          uint64
	TransferredObjects        uint64
	VerifiedBytes             uint64
	VerifiedObjects           uint64
	TargetVerificationReceipt string
	CompatibilityDigest       string
	RetryClass                string
	FirstError                string
	LastError                 string
	CancelRequested           bool
	Started                   bool
	Completed                 bool
	State                     string
}

type Progress struct {
	Attempt                   uint32
	ObjectCheckpoint          string
	TransferredBytes          uint64
	TransferredObjects        uint64
	VerifiedBytes             uint64
	VerifiedObjects           uint64
	TargetVerificationReceipt string
	RetryClass                string
	ErrorMessage              string
	CancelRequested           bool
	CallerCompleted           bool
}

type Decision struct {
	Replay    bool
	Completed bool
	State     string
}

func NewTransfer(expectedBytes, expectedObjects uint64, compatibilityDigest string) (Transfer, error) {
	compatibilityDigest = strings.TrimSpace(compatibilityDigest)
	if expectedBytes == 0 || expectedObjects == 0 || compatibilityDigest == "" {
		return Transfer{}, fmt.Errorf("expected bytes, expected objects, and compatibility digest are required")
	}
	return Transfer{
		ExpectedBytes: expectedBytes, ExpectedObjects: expectedObjects,
		CompatibilityDigest: compatibilityDigest, RetryClass: RetryClassNone,
		State: TransferStateAdmitted,
	}, nil
}

func AdvanceTransfer(current Transfer, input Progress) (Transfer, Decision, error) {
	if input.CallerCompleted {
		return current, Decision{}, ErrCallerCompletion
	}
	if current.ExpectedBytes == 0 || current.ExpectedObjects == 0 || strings.TrimSpace(current.CompatibilityDigest) == "" {
		return current, Decision{}, fmt.Errorf("remote transfer expected scope is incomplete")
	}
	if current.Completed || current.State == TransferStateCompleted || current.State == TransferStateCanceled || current.State == TransferStateFailed {
		if progressMatches(current, input) {
			return current, Decision{Replay: true, Completed: current.Completed, State: current.State}, nil
		}
		return current, Decision{}, ErrTransferTerminal
	}
	if input.Attempt == 0 {
		return current, Decision{}, fmt.Errorf("positive transfer attempt is required")
	}
	if input.Attempt < current.Attempt || input.TransferredBytes < current.TransferredBytes || input.TransferredObjects < current.TransferredObjects || input.VerifiedBytes < current.VerifiedBytes || input.VerifiedObjects < current.VerifiedObjects {
		return current, Decision{}, ErrProgressRegression
	}
	if input.TransferredBytes > current.ExpectedBytes || input.VerifiedBytes > input.TransferredBytes || input.TransferredObjects > current.ExpectedObjects || input.VerifiedObjects > input.TransferredObjects {
		return current, Decision{}, fmt.Errorf("remote transfer progress exceeds expected or transferred scope")
	}
	retryClass := strings.TrimSpace(input.RetryClass)
	if retryClass == "" {
		retryClass = RetryClassNone
	}
	if !validRetryClass(retryClass) {
		return current, Decision{}, fmt.Errorf("invalid remote transfer retry class %q", retryClass)
	}
	errorMessage := strings.TrimSpace(input.ErrorMessage)
	if len(errorMessage) > 512 {
		return current, Decision{}, fmt.Errorf("remote transfer error message exceeds 512 bytes")
	}
	if retryClass == RetryClassCanceled && !input.CancelRequested {
		return current, Decision{}, fmt.Errorf("canceled retry class requires cancel_requested")
	}
	if retryClass == RetryClassPermanent && errorMessage == "" {
		return current, Decision{}, fmt.Errorf("permanent retry class requires an error message")
	}
	receipt := strings.TrimSpace(input.TargetVerificationReceipt)
	verifiedAll := input.VerifiedBytes == current.ExpectedBytes && input.VerifiedObjects == current.ExpectedObjects
	if receipt != "" && !verifiedAll {
		return current, Decision{}, fmt.Errorf("target verification receipt requires complete verified scope")
	}
	if verifiedAll && receipt == "" && !input.CancelRequested {
		return current, Decision{}, fmt.Errorf("complete verified scope requires target verification receipt")
	}

	updated := current
	updated.Attempt = input.Attempt
	updated.ObjectCheckpoint = strings.TrimSpace(input.ObjectCheckpoint)
	updated.TransferredBytes = input.TransferredBytes
	updated.TransferredObjects = input.TransferredObjects
	updated.VerifiedBytes = input.VerifiedBytes
	updated.VerifiedObjects = input.VerifiedObjects
	updated.RetryClass = retryClass
	updated.LastError = errorMessage
	if errorMessage != "" && updated.FirstError == "" {
		updated.FirstError = errorMessage
	}
	updated.CancelRequested = input.CancelRequested
	updated.Started = true
	updated.TargetVerificationReceipt = receipt

	switch {
	case input.CancelRequested:
		updated.State = TransferStateCanceled
		updated.RetryClass = RetryClassCanceled
	case retryClass == RetryClassPermanent:
		updated.State = TransferStateFailed
	case verifiedAll:
		updated.State = TransferStateCompleted
		updated.Completed = true
		updated.LastError = ""
	case input.VerifiedBytes > 0 || input.VerifiedObjects > 0:
		updated.State = TransferStateVerifying
	default:
		updated.State = TransferStateTransferring
	}
	return updated, Decision{Completed: updated.Completed, State: updated.State}, nil
}

func progressMatches(current Transfer, input Progress) bool {
	retryClass := strings.TrimSpace(input.RetryClass)
	if retryClass == "" {
		retryClass = RetryClassNone
	}
	if input.CancelRequested {
		retryClass = RetryClassCanceled
	}
	errorMessage := strings.TrimSpace(input.ErrorMessage)
	if current.State == TransferStateCompleted {
		errorMessage = ""
	}
	return input.Attempt == current.Attempt &&
		strings.TrimSpace(input.ObjectCheckpoint) == current.ObjectCheckpoint &&
		input.TransferredBytes == current.TransferredBytes &&
		input.TransferredObjects == current.TransferredObjects &&
		input.VerifiedBytes == current.VerifiedBytes &&
		input.VerifiedObjects == current.VerifiedObjects &&
		strings.TrimSpace(input.TargetVerificationReceipt) == current.TargetVerificationReceipt &&
		input.CancelRequested == current.CancelRequested &&
		retryClass == current.RetryClass &&
		errorMessage == current.LastError
}

func validRetryClass(value string) bool {
	switch value {
	case RetryClassNone, RetryClassTransient, RetryClassTimeout, RetryClassThrottle, RetryClassPermanent, RetryClassCanceled:
		return true
	default:
		return false
	}
}
