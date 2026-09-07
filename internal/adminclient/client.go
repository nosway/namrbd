package adminclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	TransportModeAuto     = "auto"
	TransportModeMTLS     = "mtls"
	TransportModeInsecure = "insecure"
)

type TransportConfig struct {
	Mode                  string
	CAFile                string
	CertFile              string
	KeyFile               string
	ServerName            string
	RBACBindingGeneration uint64
}

type Client struct {
	conn       *grpc.ClientConn
	Admin      adminv1.AdminServiceClient
	Operations adminv1.OperationsServiceClient
	Placement  internalv1.PlacementResolverServiceClient
}

func Dial(ctx context.Context, endpoint string) (*Client, error) {
	cfg, err := TransportConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return DialWithTransport(ctx, endpoint, cfg)
}

func DialWithTransport(ctx context.Context, endpoint string, cfg TransportConfig) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("admin endpoint is required")
	}
	transportCredentials, err := NewTransportCredentials(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure admin transport: %w", err)
	}
	target := grpcTarget(endpoint)
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(transportCredentials)}
	if cfg.RBACBindingGeneration > 0 {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(rbacBindingGenerationClientInterceptor(cfg.RBACBindingGeneration)))
	}
	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial admin endpoint %q: %w", endpoint, err)
	}
	return &Client{
		conn:       conn,
		Admin:      adminv1.NewAdminServiceClient(conn),
		Operations: adminv1.NewOperationsServiceClient(conn),
		Placement:  internalv1.NewPlacementResolverServiceClient(conn),
	}, nil
}

func TransportConfigFromEnv() (TransportConfig, error) {
	cfg := TransportConfig{
		Mode:       strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_TRANSPORT_MODE")),
		CAFile:     strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_TLS_CA_FILE")),
		CertFile:   strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_TLS_CERT_FILE")),
		KeyFile:    strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_TLS_KEY_FILE")),
		ServerName: strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_TLS_SERVER_NAME")),
	}
	generationRaw := strings.TrimSpace(os.Getenv("NAMRBD_ADMIN_RBAC_GENERATION"))
	if generationRaw != "" {
		generation, err := strconv.ParseUint(generationRaw, 10, 64)
		if err != nil || generation == 0 {
			return TransportConfig{}, fmt.Errorf("NAMRBD_ADMIN_RBAC_GENERATION must be a positive uint64")
		}
		cfg.RBACBindingGeneration = generation
	}
	return cfg, nil
}

func rbacBindingGenerationClientInterceptor(generation uint64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "namrbd-rbac-generation", strconv.FormatUint(generation, 10))
		return invoker(ctx, method, req, reply, connection, options...)
	}
}

func NewTransportCredentials(cfg TransportConfig) (credentials.TransportCredentials, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	hasTLSReference := strings.TrimSpace(cfg.CAFile) != "" || strings.TrimSpace(cfg.CertFile) != "" || strings.TrimSpace(cfg.KeyFile) != "" || strings.TrimSpace(cfg.ServerName) != ""
	if mode == "" || mode == TransportModeAuto {
		if hasTLSReference {
			mode = TransportModeMTLS
		} else {
			mode = TransportModeInsecure
		}
	}
	switch mode {
	case TransportModeInsecure:
		if hasTLSReference {
			return nil, fmt.Errorf("admin TLS references cannot be combined with insecure transport mode")
		}
		return insecure.NewCredentials(), nil
	case TransportModeMTLS:
		return newMTLSTransportCredentials(cfg)
	default:
		return nil, fmt.Errorf("unsupported admin transport mode %q", cfg.Mode)
	}
}

func newMTLSTransportCredentials(cfg TransportConfig) (credentials.TransportCredentials, error) {
	caFile := strings.TrimSpace(cfg.CAFile)
	certFile := strings.TrimSpace(cfg.CertFile)
	keyFile := strings.TrimSpace(cfg.KeyFile)
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("mTLS requires CA, certificate, and key file references")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read admin TLS CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("admin TLS CA file contains no certificates")
	}
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, fmt.Errorf("load admin TLS client certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		ServerName: strings.TrimSpace(cfg.ServerName),
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			certificate, loadErr := tls.LoadX509KeyPair(certFile, keyFile)
			if loadErr != nil {
				return nil, fmt.Errorf("reload admin TLS client certificate: %w", loadErr)
			}
			return &certificate, nil
		},
	}
	return credentials.NewTLS(tlsConfig), nil
}

func grpcTarget(endpoint string) string {
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	return "passthrough:///" + endpoint
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
