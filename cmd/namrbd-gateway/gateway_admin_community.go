//go:build !enterprise

package main

import (
	"flag"
	"net/http"

	"github.com/nosway/namrbd/gateway/httpapi"
)

type gatewayAdminOptions struct{}
type gatewayAdminHTTPRuntime struct{ address string }

func registerGatewayAdminFlags(*flag.FlagSet) gatewayAdminOptions { return gatewayAdminOptions{} }
func (gatewayAdminOptions) enabled() bool                         { return false }
func (gatewayAdminOptions) validate(string) error                 { return nil }

func newGatewayAdminAuthorization(gatewayAdminOptions, string, string, string, string) (httpapi.GatewayAdminAuthorizationFunc, func(), error) {
	return nil, func() {}, nil
}

func newGatewayAdminHTTPRuntime(gatewayAdminOptions, http.Handler) (*gatewayAdminHTTPRuntime, error) {
	return nil, nil
}

func (*gatewayAdminHTTPRuntime) serve() error { return nil }
func (*gatewayAdminHTTPRuntime) close() error { return nil }
