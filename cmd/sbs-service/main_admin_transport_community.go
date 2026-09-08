//go:build !enterprise

package main

import (
	"flag"
	"fmt"
	"time"
)

const adminMTLSTrustProfile = "enterprise-only"

type adminTransportOptions struct{}

type adminGRPCRuntime struct {
	address                string
	serverFingerprint      string
	serverCertificateUntil time.Time
	rbacEnforced           bool
	bootstrapRestricted    bool
}

func registerAdminTransportFlags(*flag.FlagSet) adminTransportOptions {
	return adminTransportOptions{}
}

func (adminTransportOptions) enabled() bool {
	return false
}

func (adminTransportOptions) validate() error {
	return nil
}

func newAuthenticatedAdminGRPCRuntime(*server, adminTransportOptions) (*adminGRPCRuntime, error) {
	return nil, nil
}

func (*adminGRPCRuntime) serve() error {
	return fmt.Errorf("authenticated admin transport requires an Enterprise build")
}

func (*adminGRPCRuntime) gracefulStop() {}
