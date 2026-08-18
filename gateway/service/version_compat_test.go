package service

import (
	"testing"

	namrbdversion "github.com/nosway/namrbd/version"
)

func TestCheckSBSVersionCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		client  string
		server  string
		wantErr bool
	}{
		{name: "same exact version", client: namrbdversion.ProductVersion(), server: namrbdversion.ProductVersion()},
		{name: "trimmed exact version", client: " v1.0.0-rc ", server: "v1.0.0-rc"},
		{name: "empty client rejected", client: "", server: "v1.0.0-rc", wantErr: true},
		{name: "empty server rejected", client: "v1.0.0-rc", server: "", wantErr: true},
		{name: "different rc rejected", client: "v1.0.0-rc", server: "v1.0.0-rc.2", wantErr: true},
		{name: "newer server rejected", client: "v1.0.0-rc", server: "v1.0.1", wantErr: true},
		{name: "older server rejected", client: "v1.0.1", server: "v1.0.0-rc", wantErr: true},
		{name: "non numeric rejected", client: "dev", server: "v1.0.0-rc", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckSBSVersionCompatibility(tc.client, tc.server)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for client=%q server=%q", tc.client, tc.server)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for client=%q server=%q: %v", tc.client, tc.server, err)
			}
		})
	}
}

func TestNormalizeProductVersion(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "valid semver", input: "v1.0.0-rc", want: "v1.0.0-rc"},
		{name: "trim spaces", input: " v2.7.3 ", want: "v2.7.3"},
		{name: "build metadata", input: "v1.0.0-rc+20260818", want: "v1.0.0-rc+20260818"},
		{name: "major minor shorthand rejected", input: "1.0", wantErr: true},
		{name: "missing patch", input: "1", wantErr: true},
		{name: "non numeric", input: "dev", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := namrbdversion.NormalizeProductVersion(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}
