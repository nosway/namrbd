package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const volumePageTokenVersion = 1

type volumePageToken struct {
	Version           int    `json:"version"`
	Cursor            string `json:"cursor"`
	CatalogRevision   uint64 `json:"catalog_revision"`
	Health            int32  `json:"health"`
	RedundancyBackend string `json:"redundancy_backend,omitempty"`
	TopologyMode      string `json:"topology_mode,omitempty"`
}

func encodeVolumePageToken(cursor string, catalogRevision uint64, req *adminv1.ListVolumesPageRequest) (string, error) {
	token := volumePageToken{
		Version: volumePageTokenVersion, Cursor: strings.TrimSpace(cursor), CatalogRevision: catalogRevision,
		Health: int32(req.GetHealth()), RedundancyBackend: strings.ToLower(strings.TrimSpace(req.GetRedundancyBackend())),
		TopologyMode: strings.ToLower(strings.TrimSpace(req.GetTopologyMode())),
	}
	if token.Cursor == "" || token.CatalogRevision == 0 {
		return "", fmt.Errorf("volume page token requires cursor and catalog revision")
	}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeVolumePageToken(raw string, req *adminv1.ListVolumesPageRequest) (volumePageToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return volumePageToken{}, fmt.Errorf("decode volume page token: %w", err)
	}
	var token volumePageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return volumePageToken{}, fmt.Errorf("unmarshal volume page token: %w", err)
	}
	if token.Version != volumePageTokenVersion || token.Cursor == "" || token.CatalogRevision == 0 {
		return volumePageToken{}, fmt.Errorf("invalid volume page token payload")
	}
	if token.Health != int32(req.GetHealth()) || token.RedundancyBackend != strings.ToLower(strings.TrimSpace(req.GetRedundancyBackend())) || token.TopologyMode != strings.ToLower(strings.TrimSpace(req.GetTopologyMode())) {
		return volumePageToken{}, fmt.Errorf("volume page token filter mismatch")
	}
	return token, nil
}
