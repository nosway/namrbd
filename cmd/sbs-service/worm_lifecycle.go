package main

import (
	"context"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type enterpriseWORMRef struct {
	SubjectType string
	SubjectID   string
	VolumeID    uint64
}

type enterpriseWORMMutationMeta struct {
	RequestID string
	Actor     string
	Reason    string
}

type enterpriseWORMSubject struct {
	Ref           enterpriseWORMRef
	Revision      uint64
	Mode          string
	State         string
	CreatedAt     time.Time
	LockedUntil   time.Time
	LegalHold     bool
	HoldReason    string
	CreatedBy     string
	UpdatedAt     time.Time
	UpdatedBy     string
	UpdateReason  string
	LastRequestID string
}

type enterpriseWORMAuditEvent struct {
	Ref             enterpriseWORMRef
	SubjectRevision uint64
	Action          string
	Result          string
	Actor           string
	Reason          string
	RequestID       string
	CreatedAt       time.Time
}

type enterpriseWORMLifecycle interface {
	clustermeta.SnapshotSubjectRegistrar
	clustermeta.ProtectedSubjectDeletionGuard
	RegisterSubject(context.Context, enterpriseWORMRef, enterpriseWORMMutationMeta) (enterpriseWORMSubject, error)
	GetSubject(context.Context, enterpriseWORMRef) (enterpriseWORMSubject, bool, error)
	ListSubjects(context.Context, uint64) ([]enterpriseWORMSubject, error)
	ApplyLock(context.Context, enterpriseWORMRef, string, time.Time, enterpriseWORMMutationMeta, uint64) (enterpriseWORMSubject, error)
	SetLegalHold(context.Context, enterpriseWORMRef, bool, string, string, enterpriseWORMMutationMeta, uint64) (enterpriseWORMSubject, error)
	ListAuditEvents(context.Context, enterpriseWORMRef) ([]enterpriseWORMAuditEvent, error)
}
