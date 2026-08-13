//go:build !enterprise

package main

import (
	"flag"
	"testing"
)

func TestPhasePRepositoryFlagsNotRegisteredInCommunity(t *testing.T) {
	fs := flag.NewFlagSet("gateway-community", flag.ContinueOnError)
	registerPhasePRepositoryFlags(fs)
	if got := fs.Lookup("phase-p-c6-payload-encryption"); got != nil {
		t.Fatalf("community gateway registered enterprise Phase P flag: %s", got.Name)
	}
	if got := fs.Lookup("phase-p-sbs-cluster-replicated-payload-encryption"); got != nil {
		t.Fatalf("community gateway registered enterprise Phase P flag: %s", got.Name)
	}
}
