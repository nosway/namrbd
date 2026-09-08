//go:build !enterprise

package main

import "testing"

func TestCommunityAuthenticatedAdminTransportRemainsDisabled(t *testing.T) {
	options := adminTransportOptions{}
	if options.enabled() {
		t.Fatal("Community admin transport unexpectedly enabled")
	}
	if err := options.validate(); err != nil {
		t.Fatalf("validate disabled Community admin transport: %v", err)
	}
	runtime, err := newAuthenticatedAdminGRPCRuntime(&server{}, options)
	if err != nil {
		t.Fatalf("construct disabled Community admin transport: %v", err)
	}
	if runtime != nil {
		t.Fatalf("disabled Community admin transport runtime=%#v want nil", runtime)
	}
}
