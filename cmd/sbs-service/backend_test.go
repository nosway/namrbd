package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenMetadataBackendPebble(t *testing.T) {
	backend, err := openMetadataBackend(context.Background(), "pebble", filepath.Join(t.TempDir(), "meta"), defaultMetadataRoot, tikvMetadataOptions{})
	if err != nil {
		t.Fatalf("openMetadataBackend(pebble): %v", err)
	}
	defer func() {
		if err := backend.close(); err != nil {
			t.Fatalf("backend.close: %v", err)
		}
	}()

	if backend.name != "pebble" {
		t.Fatalf("backend.name=%q want=pebble", backend.name)
	}
	if backend.kv == nil || backend.repo == nil {
		t.Fatalf("backend should expose kv and repo")
	}
}

func TestParseCSV(t *testing.T) {
	got := parseCSV(" pd-1:2379, ,pd-2:2379 ,pd-3:2379 ")
	want := []string{"pd-1:2379", "pd-2:2379", "pd-3:2379"}
	if len(got) != len(want) {
		t.Fatalf("len(parseCSV)=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parseCSV[%d]=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestGetenvHelpers(t *testing.T) {
	t.Setenv("NAMRBD_TEST_DURATION", "5s")
	t.Setenv("NAMRBD_TEST_BOOL", "true")
	if got := getenvDuration("NAMRBD_TEST_DURATION", time.Second); got != 5*time.Second {
		t.Fatalf("getenvDuration=%v want=5s", got)
	}
	if got := getenvBool("NAMRBD_TEST_BOOL", false); !got {
		t.Fatalf("getenvBool=%v want=true", got)
	}
}
