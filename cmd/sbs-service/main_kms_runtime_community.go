//go:build !enterprise

package main

import (
	"context"
	"flag"
)

type kmsRuntimeOptions struct{}

type disabledKMSRuntime struct{}

func (disabledKMSRuntime) Close() error { return nil }

func registerKMSRuntimeFlags(*flag.FlagSet) kmsRuntimeOptions { return kmsRuntimeOptions{} }

func (kmsRuntimeOptions) validate() error { return nil }

func newKMSRuntime(context.Context, kmsRuntimeOptions) (kmsRuntimeCloser, error) {
	return disabledKMSRuntime{}, nil
}
