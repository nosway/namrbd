package main

import (
	"fmt"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const (
	volumeListDefaultPageSize = 128
	volumeListMaximumPageSize = 512
)

func parseVolumeHealthFilter(raw string) (adminv1.VolumeHealth, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED, nil
	case "healthy":
		return adminv1.VolumeHealth_VOLUME_HEALTH_HEALTHY, nil
	case "degraded":
		return adminv1.VolumeHealth_VOLUME_HEALTH_DEGRADED, nil
	case "repairing":
		return adminv1.VolumeHealth_VOLUME_HEALTH_REPAIRING, nil
	case "rebalancing":
		return adminv1.VolumeHealth_VOLUME_HEALTH_REBALANCING, nil
	case "blocked":
		return adminv1.VolumeHealth_VOLUME_HEALTH_BLOCKED, nil
	default:
		return adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED, fmt.Errorf("unsupported --health %q", raw)
	}
}

func volumeListPageJSON(resp *adminv1.ListVolumesPageResponse) map[string]any {
	return map[string]any{
		"cluster":                   resp.GetCluster(),
		"volumes":                   resp.GetVolumes(),
		"catalog_revision":          resp.GetCatalogRevision(),
		"next_page_token":           resp.GetNextPageToken(),
		"freshness_age_millis":      resp.GetFreshnessAgeMillis(),
		"projection_health":         resp.GetProjectionHealth(),
		"projection_stale":          resp.GetProjectionStale(),
		"rebuild_required":          resp.GetRebuildRequired(),
		"scanned_records":           resp.GetScannedRecords(),
		"generated_at":              resp.GetGeneratedAt(),
		"pages_read":                1,
		"automatic_page_completion": false,
	}
}
