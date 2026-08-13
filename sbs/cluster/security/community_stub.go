//go:build !enterprise

package security

import "context"

const (
	LeasePurposeRead  = "read"
	LeasePurposeWrite = "write"
)

type KeyAccessLease struct {
	LeaseID    string
	VolumeID   string
	DataKeyID  string
	KeyVersion uint64
	Purpose    string
	ExpiresAt  string
	IssuedTo   string
	Revoked    bool
}

type KeyAccessLeaseIssueRequest struct {
	RequestID  string
	LeaseID    string
	VolumeID   string
	DataKeyID  string
	KeyVersion uint64
	Purpose    string
	TTLSeconds uint64
	IssuedTo   string
}

type KeyAccessLeaseIssuer interface {
	IssueKeyAccessLease(context.Context, KeyAccessLeaseIssueRequest) (KeyAccessLease, error)
}

type DataKeyUnwrapRequest struct {
	RequestID   string
	VolumeID    string
	DataKeyID   string
	KeyVersion  uint64
	Purpose     string
	LeaseID     string
	RequestedBy string
}

type DataKeyUnwrapper interface {
	UnwrapDataKey(context.Context, DataKeyUnwrapRequest) ([]byte, error)
}

type KeyAccessAuthority interface {
	KeyAccessLeaseIssuer
	DataKeyUnwrapper
}
