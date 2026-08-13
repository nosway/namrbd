package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

const AuthModeTokenHMACV1 = "token-hmac-v1"

var (
	ErrTokenExpired      = errors.New("token expired")
	ErrTokenInvalid      = errors.New("token invalid")
	ErrTokenBadSignature = errors.New("token signature invalid")
)

// DataplaneTokenClaims is the signed payload of a dataplane token (Phase C3).
type DataplaneTokenClaims struct {
	TokenID        string              `json:"token_id"`
	VolumeID       service.HexVolumeID `json:"volume_id"`
	AttachmentID   string              `json:"attachment_id"`
	DeviceID       uint32              `json:"device_id"`
	HostID         string              `json:"host_id"`
	GatewayID      string              `json:"gateway_id"`
	Generation     uint64              `json:"generation"`
	IssuedAt       string              `json:"issued_at"`  // RFC3339
	ExpiresAt      string              `json:"expires_at"` // RFC3339
	AuthMode       string              `json:"auth_mode"`
	AllowedPathIDs []uint32            `json:"allowed_path_ids,omitempty"`
}

// DataplaneAuth is the manifest field for dataplane authentication (Phase C3).
type DataplaneAuth struct {
	Mode       string `json:"mode"`
	Token      string `json:"token"`
	SessionKey string `json:"session_key,omitempty"`
	ExpiresAt  string `json:"expires_at"` // RFC3339
}

// IssueTokenRequest is the input for issuing a dataplane token.
type IssueTokenRequest struct {
	VolumeID       uint64
	AttachmentID   string
	DeviceID       uint32
	HostID         string
	GatewayID      string
	Generation     uint64
	TTL            time.Duration
	AllowedPathIDs []uint32
}

// VerifiedToken is the result of successful token verification.
type VerifiedToken struct {
	Claims    DataplaneTokenClaims
	ExpiresAt time.Time
}

// TokenIssuer issues and verifies dataplane tokens (HMAC-SHA256).
type TokenIssuer interface {
	IssueDataplaneToken(req IssueTokenRequest) (raw string, auth DataplaneAuth, err error)
	VerifyDataplaneToken(raw string) (*VerifiedToken, error)
}

type tokenIssuer struct {
	signingKey []byte
}

// NewTokenIssuer creates a TokenIssuer that signs with the given key.
// Key must be non-empty.
func NewTokenIssuer(signingKey []byte) (TokenIssuer, error) {
	if len(signingKey) == 0 {
		return nil, errors.New("dataplane token signing key is required")
	}
	return &tokenIssuer{signingKey: signingKey}, nil
}

func formatTokenID(volumeID, generation uint64) string {
	return fmt.Sprintf("dptok-%s-%04d", service.CanonicalVolumeID(volumeID), generation)
}

func (t *tokenIssuer) IssueDataplaneToken(req IssueTokenRequest) (raw string, auth DataplaneAuth, err error) {
	now := time.Now().UTC()
	exp := now.Add(req.TTL)
	if req.TTL <= 0 {
		exp = now.Add(5 * time.Minute)
	}
	issuedAt := now.Format(time.RFC3339)
	expiresAt := exp.Format(time.RFC3339)

	pathIDs := req.AllowedPathIDs
	if pathIDs == nil {
		pathIDs = []uint32{0}
	}

	claims := DataplaneTokenClaims{
		TokenID:        formatTokenID(req.VolumeID, req.Generation),
		VolumeID:       service.HexVolumeID(req.VolumeID),
		AttachmentID:   req.AttachmentID,
		DeviceID:       req.DeviceID,
		HostID:         req.HostID,
		GatewayID:      req.GatewayID,
		Generation:     req.Generation,
		IssuedAt:       issuedAt,
		ExpiresAt:      expiresAt,
		AuthMode:       AuthModeTokenHMACV1,
		AllowedPathIDs: pathIDs,
	}

	payload, err := canonicalJSON(claims)
	if err != nil {
		return "", DataplaneAuth{}, err
	}
	sig := hmacSHA256(t.signingKey, payload)
	raw = base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
	auth = DataplaneAuth{
		Mode:      AuthModeTokenHMACV1,
		Token:     raw,
		ExpiresAt: expiresAt,
	}
	return raw, auth, nil
}

func (t *tokenIssuer) VerifyDataplaneToken(raw string) (*VerifiedToken, error) {
	parts := strings.SplitN(raw, ".", 2)
	if len(parts) != 2 {
		return nil, ErrTokenInvalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrTokenInvalid
	}
	sigGot, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(sigGot) != sha256.Size {
		return nil, ErrTokenInvalid
	}
	sigWant := hmacSHA256(t.signingKey, payload)
	if !hmac.Equal(sigGot, sigWant) {
		return nil, ErrTokenBadSignature
	}
	var claims DataplaneTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, ErrTokenInvalid
	}
	if claims.AuthMode != AuthModeTokenHMACV1 {
		return nil, ErrTokenInvalid
	}
	exp, err := time.Parse(time.RFC3339, claims.ExpiresAt)
	if err != nil {
		return nil, ErrTokenInvalid
	}
	if time.Now().UTC().After(exp) {
		return nil, ErrTokenExpired
	}
	return &VerifiedToken{Claims: claims, ExpiresAt: exp}, nil
}

// canonicalJSON encodes v to JSON with sorted keys for deterministic HMAC.
func canonicalJSON(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return marshalSorted(m), nil
}

func marshalSorted(m map[string]interface{}) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf strings.Builder
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, _ := json.Marshal(k)
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(jsonEncodeValue(m[k]))
	}
	buf.WriteByte('}')
	return []byte(buf.String())
}

func jsonEncodeValue(v interface{}) []byte {
	if v == nil {
		return []byte("null")
	}
	if m, ok := v.(map[string]interface{}); ok {
		return marshalSorted(m)
	}
	if s, ok := v.([]interface{}); ok {
		var parts []byte
		for i, e := range s {
			if i > 0 {
				parts = append(parts, ',')
			}
			parts = append(parts, jsonEncodeValue(e)...)
		}
		return append(append([]byte{'['}, parts...), ']')
	}
	b, _ := json.Marshal(v)
	return b
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// DeriveSessionKey derives a 32-byte session key for wire v2 auth_tag (Phase C3).
// Inputs: sessionDerivationKey (from config), token raw string, client_nonce, server_nonce, session_id.
// Both gateway and client must use the same inputs to get the same key.
func DeriveSessionKey(sessionDerivationKey []byte, tokenRaw, clientNonce, serverNonce string, sessionID uint64) []byte {
	if len(sessionDerivationKey) == 0 {
		return nil
	}
	var b []byte
	b = append(b, tokenRaw...)
	b = append(b, 0)
	b = append(b, clientNonce...)
	b = append(b, 0)
	b = append(b, serverNonce...)
	b = append(b, 0)
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], sessionID)
	b = append(b, id[:]...)
	return hmacSHA256(sessionDerivationKey, b)
}
