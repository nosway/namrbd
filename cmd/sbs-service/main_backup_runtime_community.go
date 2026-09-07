//go:build !enterprise

package main

// enterpriseBackupRuntime keeps Enterprise-only backup I/O hooks out of the
// Community server shape. The embedded unimplemented gRPC methods remain the
// fail-closed boundary for backup and DR RPCs in Community builds.
type enterpriseBackupRuntime struct{}
