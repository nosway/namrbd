package main

import (
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

func TestParseVolumeHealthFilter(t *testing.T) {
	for raw, want := range map[string]adminv1.VolumeHealth{
		"":            adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED,
		"healthy":     adminv1.VolumeHealth_VOLUME_HEALTH_HEALTHY,
		"DEGRADED":    adminv1.VolumeHealth_VOLUME_HEALTH_DEGRADED,
		"repairing":   adminv1.VolumeHealth_VOLUME_HEALTH_REPAIRING,
		"rebalancing": adminv1.VolumeHealth_VOLUME_HEALTH_REBALANCING,
		"blocked":     adminv1.VolumeHealth_VOLUME_HEALTH_BLOCKED,
	} {
		got, err := parseVolumeHealthFilter(raw)
		if err != nil || got != want {
			t.Fatalf("parseVolumeHealthFilter(%q)=%s,%v want %s", raw, got, err, want)
		}
	}
	if _, err := parseVolumeHealthFilter("unknown"); err == nil {
		t.Fatal("unknown volume health filter was accepted")
	}
}

func TestVolumeListPageJSONExposesCursorAndNoCompletion(t *testing.T) {
	output := volumeListPageJSON(&adminv1.ListVolumesPageResponse{
		CatalogRevision: 19, NextPageToken: "opaque-next", ProjectionHealth: "healthy", ScannedRecords: 128,
	})
	if output["catalog_revision"] != uint64(19) || output["next_page_token"] != "opaque-next" || output["pages_read"] != 1 || output["automatic_page_completion"] != false {
		t.Fatalf("volume page JSON=%+v", output)
	}
}
