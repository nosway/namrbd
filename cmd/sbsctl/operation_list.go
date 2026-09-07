package main

import (
	"fmt"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	operationListDefaultPageSize = 128
	operationListMaximumPageSize = 512
)

func parseOperationListTime(raw, name string) (*timestamppb.Timestamp, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339: %w", name, err)
	}
	return timestamppb.New(parsed.UTC()), nil
}

func operationListPageJSON(resp *adminv1.ListOperationsPageResponse) map[string]any {
	return map[string]any{
		"cluster":                   resp.GetCluster(),
		"operations":                resp.GetOperations(),
		"projection_revision":       resp.GetProjectionRevision(),
		"next_page_token":           resp.GetNextPageToken(),
		"freshness_age_millis":      resp.GetFreshnessAgeMillis(),
		"projection_health":         resp.GetProjectionHealth(),
		"scanned_records":           resp.GetScannedRecords(),
		"generated_at":              resp.GetGeneratedAt(),
		"pages_read":                1,
		"automatic_page_completion": false,
	}
}
