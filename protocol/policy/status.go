package policy

type Decision int

const (
	DecisionNoop Decision = iota
	DecisionRetryAlternatePath
	DecisionRefreshSession
	DecisionFatalDetach
	DecisionFailToHost
	DecisionBackoffAndRetry
)

const (
	StatusOK              int32 = 0
	ErrUnauthorized       int32 = 3
	ErrGenerationMismatch int32 = 5
	ErrInvalidRange       int32 = 6
	ErrPathDraining       int32 = 7
	ErrTimeout            int32 = 10
	ErrRetryable          int32 = 11
	ErrBusy               int32 = 12
	ErrChecksum           int32 = 13
	ErrNoSuchVolume       int32 = 4
	ErrQuorumFailed       int32 = 9
	ErrInternal           int32 = 14
	ErrBadMagic           int32 = 1
	ErrUnsupportedVersion int32 = 2
	ErrNoHealthyReplica   int32 = 8
)

func ClassifyStatus(code int32) Decision {
	switch code {
	case StatusOK:
		return DecisionNoop
	case ErrRetryable, ErrTimeout, ErrPathDraining, ErrNoHealthyReplica:
		return DecisionRetryAlternatePath
	case ErrGenerationMismatch:
		return DecisionRefreshSession
	case ErrUnauthorized:
		return DecisionFatalDetach
	case ErrBusy:
		return DecisionBackoffAndRetry
	case ErrInvalidRange, ErrChecksum, ErrNoSuchVolume:
		return DecisionFailToHost
	case ErrQuorumFailed, ErrInternal, ErrBadMagic, ErrUnsupportedVersion:
		return DecisionFailToHost
	default:
		return DecisionFailToHost
	}
}
