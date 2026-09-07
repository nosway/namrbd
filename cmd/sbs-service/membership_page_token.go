package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const membershipPageTokenVersion = 1

type membershipPageToken struct {
	Version            int    `json:"version"`
	Cursor             string `json:"cursor"`
	ProjectionRevision uint64 `json:"projection_revision"`
	IncludeTombstones  bool   `json:"include_tombstones"`
}

func encodeMembershipPageToken(cursor string, projectionRevision uint64, includeTombstones bool) (string, error) {
	token := membershipPageToken{
		Version:            membershipPageTokenVersion,
		Cursor:             strings.TrimSpace(cursor),
		ProjectionRevision: projectionRevision,
		IncludeTombstones:  includeTombstones,
	}
	if token.Cursor == "" || token.ProjectionRevision == 0 {
		return "", fmt.Errorf("membership page token requires cursor and projection revision")
	}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("marshal membership page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeMembershipPageToken(raw string) (membershipPageToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return membershipPageToken{}, fmt.Errorf("decode membership page token: %w", err)
	}
	var token membershipPageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return membershipPageToken{}, fmt.Errorf("unmarshal membership page token: %w", err)
	}
	if token.Version != membershipPageTokenVersion || strings.TrimSpace(token.Cursor) == "" || token.ProjectionRevision == 0 {
		return membershipPageToken{}, fmt.Errorf("invalid membership page token payload")
	}
	return token, nil
}
