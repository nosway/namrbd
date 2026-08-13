package auth

import (
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

func TestTokenIssueAndVerify(t *testing.T) {
	issuer, err := NewTokenIssuer([]byte("test-secret-key-32-bytes-long!!"))
	if err != nil {
		t.Fatal(err)
	}
	req := IssueTokenRequest{
		VolumeID:       101,
		AttachmentID:   "att-00000065-0001",
		DeviceID:       0,
		HostID:         "host-a",
		GatewayID:      "gw-1",
		Generation:     7,
		TTL:            5 * time.Minute,
		AllowedPathIDs: []uint32{0},
	}
	raw, dataplaneAuth, err := issuer.IssueDataplaneToken(req)
	if err != nil {
		t.Fatal(err)
	}
	if dataplaneAuth.Mode != AuthModeTokenHMACV1 || dataplaneAuth.Token == "" || dataplaneAuth.ExpiresAt == "" {
		t.Fatalf("unexpected dataplane_auth: %+v", dataplaneAuth)
	}
	if raw != dataplaneAuth.Token {
		t.Fatalf("raw and auth.Token should match")
	}

	verified, err := issuer.VerifyDataplaneToken(raw)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.Claims.VolumeID != service.HexVolumeID(101) || verified.Claims.AttachmentID != "att-00000065-0001" ||
		verified.Claims.DeviceID != 0 || verified.Claims.HostID != "host-a" ||
		verified.Claims.GatewayID != "gw-1" || verified.Claims.Generation != 7 {
		t.Fatalf("unexpected claims: %+v", verified.Claims)
	}
	if verified.Claims.AuthMode != AuthModeTokenHMACV1 {
		t.Fatalf("auth_mode: %s", verified.Claims.AuthMode)
	}
}

func TestTokenVerifyBadSignature(t *testing.T) {
	issuer, _ := NewTokenIssuer([]byte("key1"))
	raw, _, _ := issuer.IssueDataplaneToken(IssueTokenRequest{
		VolumeID: 1, AttachmentID: "att-00000001-0001", DeviceID: 0, HostID: "h", GatewayID: "gw", Generation: 1, TTL: time.Minute,
	})
	other, _ := NewTokenIssuer([]byte("different-key-32-bytes-long!!!!"))
	_, err := other.VerifyDataplaneToken(raw)
	if err != ErrTokenBadSignature {
		t.Fatalf("expected ErrTokenBadSignature, got %v", err)
	}
}

func TestTokenVerifyInvalid(t *testing.T) {
	issuer, _ := NewTokenIssuer([]byte("test-secret-key-32-bytes-long!!"))
	_, err := issuer.VerifyDataplaneToken("not.a.valid.token")
	if err != ErrTokenInvalid {
		t.Fatalf("expected ErrTokenInvalid for malformed, got %v", err)
	}
	_, err = issuer.VerifyDataplaneToken("aW52YWxpZGpzb24=.c2ln")
	if err != nil && err != ErrTokenBadSignature && err != ErrTokenInvalid {
		t.Fatalf("expected invalid or bad sig, got %v", err)
	}
}

func TestNewTokenIssuerRequiresKey(t *testing.T) {
	_, err := NewTokenIssuer(nil)
	if err == nil {
		t.Fatal("expected error for nil key")
	}
	_, err = NewTokenIssuer([]byte(""))
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}
