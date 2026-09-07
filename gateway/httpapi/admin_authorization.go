package httpapi

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	clusterauthz "github.com/nosway/namrbd/sbs/cluster/authz"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	GatewayAdminRBACGenerationHeader = "X-NAMRBD-RBAC-Generation"
	GatewayAdminRequestIDHeader      = "X-NAMRBD-Request-ID"
	GatewayAdminReasonHeader         = "X-NAMRBD-Reason"
)

type GatewayAdminAuthorizationRequest struct {
	Permission                 clusterauthz.GatewayHTTPPermission
	ClientCertificateChainDER  [][]byte
	RequestedBindingGeneration uint64
	RequestID                  string
	Reason                     string
}

type GatewayAdminAuthorizationFunc func(context.Context, GatewayAdminAuthorizationRequest) error

// HandlerWithoutAdminRoutes is the product listener boundary used when the
// separate authenticated admin listener is enabled. It leaves attach, detach,
// I/O, discovery, and health contracts unchanged while making the three
// classified operator mutations unreachable on the product listener.
func (s *Server) HandlerWithoutAdminRoutes() http.Handler {
	product := s.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, covered := clusterauthz.GatewayHTTPPermissionForRequest(r.Method, r.URL.Path); covered {
			http.Error(w, "gateway admin route is available only on the authenticated admin listener", http.StatusNotFound)
			return
		}
		product.ServeHTTP(w, r)
	})
}

// AdminHandler exposes only the explicitly inventoried gateway operator
// mutations. The listener must have already performed client mTLS; this
// handler refuses to delegate an unverified chain and requires the caller's
// exact RBAC binding generation before the product handler can run.
func (s *Server) AdminHandler() http.Handler {
	product := s.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		permission, covered := clusterauthz.GatewayHTTPPermissionForRequest(r.Method, r.URL.Path)
		if !covered {
			http.NotFound(w, r)
			return
		}
		if s.cfg.GatewayAdminAuthorization == nil {
			http.Error(w, "gateway admin authorization is unavailable", http.StatusServiceUnavailable)
			return
		}
		chain, ok := verifiedClientCertificateChainDER(r)
		if !ok {
			http.Error(w, "verified admin client certificate chain is required", http.StatusUnauthorized)
			return
		}
		generation, ok := positiveUint64Header(r, GatewayAdminRBACGenerationHeader)
		if !ok {
			http.Error(w, "positive X-NAMRBD-RBAC-Generation header is required", http.StatusBadRequest)
			return
		}
		requestID, ok := singleNonEmptyHeader(r, GatewayAdminRequestIDHeader)
		if !ok {
			http.Error(w, "single non-empty X-NAMRBD-Request-ID header is required", http.StatusBadRequest)
			return
		}
		reason := strings.TrimSpace(r.Header.Get(GatewayAdminReasonHeader))
		if err := s.cfg.GatewayAdminAuthorization(r.Context(), GatewayAdminAuthorizationRequest{
			Permission: permission, ClientCertificateChainDER: chain,
			RequestedBindingGeneration: generation, RequestID: requestID, Reason: reason,
		}); err != nil {
			writeGatewayAdminAuthorizationError(w, err)
			return
		}
		product.ServeHTTP(w, r)
	})
}

func verifiedClientCertificateChainDER(r *http.Request) ([][]byte, bool) {
	if r == nil || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return nil, false
	}
	chain := make([][]byte, 0, len(r.TLS.VerifiedChains[0]))
	for _, certificate := range r.TLS.VerifiedChains[0] {
		if certificate == nil || len(certificate.Raw) == 0 {
			return nil, false
		}
		chain = append(chain, append([]byte(nil), certificate.Raw...))
	}
	return chain, true
}

func positiveUint64Header(r *http.Request, name string) (uint64, bool) {
	value, ok := singleNonEmptyHeader(r, name)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil && parsed > 0
}

func singleNonEmptyHeader(r *http.Request, name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) != 1 {
		return "", false
	}
	value := strings.TrimSpace(values[0])
	return value, value != ""
}

func writeGatewayAdminAuthorizationError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpStatus := http.StatusServiceUnavailable
	switch code {
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	}
	http.Error(w, "gateway admin authorization denied", httpStatus)
}
