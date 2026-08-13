package wirev2

import (
	"encoding/json"
	"testing"
)

func TestHelloPayloadRoundtrip(t *testing.T) {
	p := &HelloPayload{
		Token:           "eyJ0b2tlbiJ9.sig",
		ClientNonce:     "Y2xpZW50LW5vbmNlMTY=",
		DeviceID:        0,
		HostID:          "host-a",
		SupportedAuth:   []string{"token-hmac-v1"},
		RequestedPathID: 0,
	}
	b, err := EncodeHelloPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeHelloPayload(b)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Token != p.Token || dec.ClientNonce != p.ClientNonce || dec.DeviceID != p.DeviceID ||
		dec.HostID != p.HostID || dec.RequestedPathID != p.RequestedPathID {
		t.Fatalf("decode: %+v", dec)
	}
	if len(dec.SupportedAuth) != 1 || dec.SupportedAuth[0] != "token-hmac-v1" {
		t.Fatalf("supported_auth: %v", dec.SupportedAuth)
	}
}

func TestHelloAckPayloadRoundtrip(t *testing.T) {
	p := &HelloAckPayload{
		SessionID:           123456,
		ServerNonce:         "c2VydmVyLW5vbmNl",
		SelectedAuth:        "token-hmac-v1",
		ExpiresAt:           "2026-03-12T10:05:00Z",
		PathID:              0,
		MaxInflightRequests: 128,
		MaxInflightBytes:    8388608,
	}
	b, err := EncodeHelloAckPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeHelloAckPayload(b)
	if err != nil {
		t.Fatal(err)
	}
	if dec.SessionID != p.SessionID || dec.ServerNonce != p.ServerNonce || dec.SelectedAuth != p.SelectedAuth ||
		dec.ExpiresAt != p.ExpiresAt || dec.PathID != p.PathID ||
		dec.MaxInflightRequests != p.MaxInflightRequests || dec.MaxInflightBytes != p.MaxInflightBytes {
		t.Fatalf("decode: %+v", dec)
	}
}

func TestHelloPayloadJSONCompat(t *testing.T) {
	// Ensure JSON keys match design
	p := &HelloPayload{Token: "t", ClientNonce: "n", DeviceID: 1, HostID: "h", SupportedAuth: []string{"token-hmac-v1"}, RequestedPathID: 0}
	b, _ := json.Marshal(p)
	m := make(map[string]interface{})
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"token", "client_nonce", "device_id", "host_id", "supported_auth", "requested_path_id"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing JSON key %q", k)
		}
	}
}
