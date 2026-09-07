package adminclient

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestAdminTransportModeIsExplicitAndFailClosed(t *testing.T) {
	credentials, err := NewTransportCredentials(TransportConfig{})
	if err != nil {
		t.Fatalf("default transport: %v", err)
	}
	if got := credentials.Info().SecurityProtocol; got != "insecure" {
		t.Fatalf("default security protocol=%q want=insecure", got)
	}
	if _, err := NewTransportCredentials(TransportConfig{Mode: TransportModeMTLS, CAFile: "ca-only"}); err == nil {
		t.Fatal("partial mTLS references unexpectedly admitted")
	}
	if _, err := NewTransportCredentials(TransportConfig{Mode: TransportModeInsecure, ServerName: "admin.test"}); err == nil {
		t.Fatal("insecure transport unexpectedly accepted TLS references")
	}
	if _, err := NewTransportCredentials(TransportConfig{Mode: "fallback"}); err == nil {
		t.Fatal("unknown transport mode unexpectedly admitted")
	}

	caFile, certFile, keyFile := writeAdminClientTransportCertificate(t)
	credentials, err = NewTransportCredentials(TransportConfig{
		Mode: TransportModeMTLS, CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "admin.test",
	})
	if err != nil {
		t.Fatalf("mTLS transport: %v", err)
	}
	if got := credentials.Info().SecurityProtocol; got != "tls" {
		t.Fatalf("mTLS security protocol=%q want=tls", got)
	}
}

func TestAdminTransportConfigFromEnv(t *testing.T) {
	t.Setenv("NAMRBD_ADMIN_TRANSPORT_MODE", "mtls")
	t.Setenv("NAMRBD_ADMIN_TLS_CA_FILE", "/run/secrets/admin/ca.pem")
	t.Setenv("NAMRBD_ADMIN_TLS_CERT_FILE", "/run/secrets/admin/client.pem")
	t.Setenv("NAMRBD_ADMIN_TLS_KEY_FILE", "/run/secrets/admin/client-key.pem")
	t.Setenv("NAMRBD_ADMIN_TLS_SERVER_NAME", "admin.namrbd.test")
	t.Setenv("NAMRBD_ADMIN_RBAC_GENERATION", "7")
	got, err := TransportConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "mtls" || got.CAFile != "/run/secrets/admin/ca.pem" || got.CertFile != "/run/secrets/admin/client.pem" || got.KeyFile != "/run/secrets/admin/client-key.pem" || got.ServerName != "admin.namrbd.test" || got.RBACBindingGeneration != 7 {
		t.Fatalf("transport config=%+v", got)
	}
	t.Setenv("NAMRBD_ADMIN_RBAC_GENERATION", "stale-text")
	if _, err := TransportConfigFromEnv(); err == nil {
		t.Fatal("invalid RBAC generation unexpectedly admitted")
	}
}

func TestRBACBindingGenerationClientInterceptor(t *testing.T) {
	interceptor := rbacBindingGenerationClientInterceptor(17)
	called := false
	err := interceptor(context.Background(), "/sbs.admin.v1.AdminService/GetClusterStatus", nil, nil, nil,
		func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			called = true
			outgoing, ok := metadata.FromOutgoingContext(ctx)
			if !ok || len(outgoing.Get("namrbd-rbac-generation")) != 1 || outgoing.Get("namrbd-rbac-generation")[0] != "17" {
				t.Fatalf("outgoing metadata=%v", outgoing)
			}
			return nil
		})
	if err != nil || !called {
		t.Fatalf("interceptor called=%t err=%v", called, err)
	}
}

func writeAdminClientTransportCertificate(t *testing.T) (string, string, string) {
	t.Helper()
	now := time.Now().UTC()
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "admin-client-test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	clientPublic, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := url.Parse("spiffe://namrbd.test/operators/client-test")
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ignored-common-name"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), URIs: []*url.URL{identity},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCertificate, clientPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	caFile := filepath.Join(directory, "ca.pem")
	certFile := filepath.Join(directory, "client.pem")
	keyFile := filepath.Join(directory, "client-key.pem")
	writeAdminClientPEM(t, caFile, "CERTIFICATE", caDER)
	writeAdminClientPEM(t, certFile, "CERTIFICATE", clientDER)
	privateDER, err := x509.MarshalPKCS8PrivateKey(clientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	writeAdminClientPEM(t, keyFile, "PRIVATE KEY", privateDER)
	return caFile, certFile, keyFile
}

func writeAdminClientPEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
