package iscsi

import (
	"errors"

	gotgtscsi "github.com/gostor/gotgt/pkg/scsi"

	"github.com/nosway/namrbd/gateway/service"
)

type SCSIOutcome struct {
	Status               string
	SenseKey             string
	ASC                  string
	ASCQ                 string
	SBSErrorCode         string
	SBSErrorRetryable    bool
	StaleGatewayRejected bool
	StandbyWriteRejected bool
	SecurityRejected     bool
}

type SCSIConditionError struct {
	Status               string
	SenseKey             string
	ASC                  string
	ASCQ                 string
	Message              string
	StaleGatewayRejected bool
	StandbyWriteRejected bool
	SecurityRejected     bool
}

func (e *SCSIConditionError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *SCSIConditionError) GotgtSense() (byte, gotgtscsi.SCSISubError, bool) {
	if e == nil {
		return 0, 0, false
	}
	switch e.SenseKey {
	case "illegal_request":
		return gotgtscsi.ILLEGAL_REQUEST, gotgtscsi.ASC_INVALID_FIELD_IN_CDB, true
	case "aborted_command":
		return gotgtscsi.ABORTED_COMMAND, gotgtscsi.NO_ADDITIONAL_SENSE, true
	case "data_protect":
		return gotgtscsi.DATA_PROTECT, gotgtscsi.ASC_WRITE_PROTECT, true
	default:
		return 0, 0, false
	}
}

func IllegalRequestError(message string) error {
	return &SCSIConditionError{Status: "check_condition", SenseKey: "illegal_request", Message: message}
}

func AbortedCommandError(message string) error {
	return &SCSIConditionError{Status: "check_condition", SenseKey: "aborted_command", Message: message}
}

func DataProtectError(message string) error {
	return &SCSIConditionError{Status: "check_condition", SenseKey: "data_protect", Message: message}
}

func StandbyGatewayError(message string) error {
	return &SCSIConditionError{Status: "check_condition", SenseKey: "data_protect", Message: message, StandbyWriteRejected: true}
}

func MapErrorToSCSI(err error) SCSIOutcome {
	if err == nil {
		return SCSIOutcome{Status: "good"}
	}
	var cond *SCSIConditionError
	if errors.As(err, &cond) {
		return SCSIOutcome{
			Status:               nonEmpty(cond.Status, "check_condition"),
			SenseKey:             cond.SenseKey,
			ASC:                  cond.ASC,
			ASCQ:                 cond.ASCQ,
			StaleGatewayRejected: cond.StaleGatewayRejected,
			StandbyWriteRejected: cond.StandbyWriteRejected,
			SecurityRejected:     cond.SecurityRejected,
		}
	}
	var sbsErr *service.SBSError
	if errors.As(err, &sbsErr) {
		out := SCSIOutcome{
			Status:            "check_condition",
			SBSErrorCode:      string(sbsErr.Code),
			SBSErrorRetryable: sbsErr.Retryable,
		}
		switch sbsErr.Code {
		case service.SBSErrorCodeStaleGeneration, service.SBSErrorCodeAttachmentMismatch:
			out.SenseKey = "data_protect"
			out.StaleGatewayRejected = true
		case service.SBSErrorCodeSecurityRejected:
			out.SenseKey = "data_protect"
			out.SecurityRejected = true
		case service.SBSErrorCodeBadRequest, service.SBSErrorCodeNotFound:
			out.SenseKey = "illegal_request"
		default:
			out.SenseKey = "aborted_command"
		}
		return out
	}
	return SCSIOutcome{Status: "check_condition", SenseKey: "aborted_command"}
}

func nonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
