package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRunVolumeTransitionsRemoteDecodesRows(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet {
				t.Fatalf("method=%s want=GET", r.Method)
			}
			if r.URL.Path != "/debug/transitions" {
				t.Fatalf("path=%q", r.URL.Path)
			}
			if got := r.URL.Query().Get("volume_id"); got != "00a1b2c3" {
				t.Fatalf("volume_id=%q", got)
			}
			body, err := json.Marshal(map[string]any{
				"volume_id": "00a1b2c3",
				"transitions": []map[string]any{
					{
						"placement_ref":          "pl-1",
						"state":                  "queued",
						"reason":                 "drain",
						"current_replica_set_id": "rs-1",
						"target_replica_set_id":  "rs-2",
						"attempt":                1,
						"active":                 true,
						"visible":                true,
					},
				},
			})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(string(body))),
			}, nil
		}),
	}
	out, err := runVolumeTransitionsRemoteWithClient(context.Background(), client, "http://admin.example", "00a1b2c3")
	if err != nil {
		t.Fatalf("runVolumeTransitionsRemoteWithClient: %v", err)
	}
	rows, _ := out["transitions"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows=%v", out["transitions"])
	}
	row, _ := rows[0].(map[string]any)
	if row["placement_ref"].(string) != "pl-1" || row["state"].(string) != "queued" || row["active"].(bool) != true {
		t.Fatalf("row=%+v", row)
	}
}
