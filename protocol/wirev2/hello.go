package wirev2

import "encoding/json"

// HelloPayload is the HELLO request payload (session_id=0, seq_no=0).
type HelloPayload struct {
	Token           string   `json:"token"`
	ClientNonce     string   `json:"client_nonce"`
	DeviceID        uint32   `json:"device_id"`
	HostID          string   `json:"host_id"`
	SupportedAuth   []string `json:"supported_auth"`
	RequestedPathID uint32   `json:"requested_path_id"`
}

// HelloAckPayload is the HELLO_ACK response payload.
type HelloAckPayload struct {
	SessionID           uint64 `json:"session_id"`
	ServerNonce         string `json:"server_nonce"`
	SelectedAuth        string `json:"selected_auth"`
	ExpiresAt           string `json:"expires_at"`
	PathID              uint32 `json:"path_id"`
	MaxInflightRequests uint32 `json:"max_inflight_requests"`
	MaxInflightBytes    uint64 `json:"max_inflight_bytes"`
	MaxIOSize           uint32 `json:"max_io_size,omitempty"`
	MaxZeroLikeIOSize   uint32 `json:"max_zero_like_io_size,omitempty"`
}

// EncodeHelloPayload returns JSON bytes for HELLO payload.
func EncodeHelloPayload(p *HelloPayload) ([]byte, error) {
	return json.Marshal(p)
}

// DecodeHelloPayload parses HELLO payload from JSON.
func DecodeHelloPayload(b []byte) (*HelloPayload, error) {
	var p HelloPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// EncodeHelloAckPayload returns JSON bytes for HELLO_ACK payload.
func EncodeHelloAckPayload(p *HelloAckPayload) ([]byte, error) {
	return json.Marshal(p)
}

// DecodeHelloAckPayload parses HELLO_ACK payload from JSON.
func DecodeHelloAckPayload(b []byte) (*HelloAckPayload, error) {
	var p HelloAckPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
