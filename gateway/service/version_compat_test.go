package service

import (
	"testing"

	namrbdversion "github.com/nosway/namrbd/version"
)

func TestCheckSBSMajorVersionCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		client  string
		server  string
		wantErr bool
	}{
		{name: "same exact version", client: namrbdversion.Current, server: namrbdversion.Current},
		{name: "server newer minor is allowed", client: "1.2", server: "1.9"},
		{name: "server newer major is allowed", client: "1.9", server: "2.0"},
		{name: "empty version tolerated", client: "", server: "1.0"},
		{name: "server older minor rejected", client: "1.9", server: "1.2", wantErr: true},
		{name: "server older major rejected", client: "2.0", server: "1.9", wantErr: true},
		{name: "patch version rejected", client: "1.2.3", server: "1.2", wantErr: true},
		{name: "non numeric rejected", client: "dev", server: "1.0", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckSBSMajorVersionCompatibility(tc.client, tc.server)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for client=%q server=%q", tc.client, tc.server)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for client=%q server=%q: %v", tc.client, tc.server, err)
			}
		})
	}
}

func TestParseMajorMinor(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantMajor int
		wantMinor int
		wantErr   bool
	}{
		{name: "valid", input: "1.0", wantMajor: 1, wantMinor: 0},
		{name: "trim spaces", input: " 2.7 ", wantMajor: 2, wantMinor: 7},
		{name: "missing minor", input: "1", wantErr: true},
		{name: "patch not allowed", input: "1.2.3", wantErr: true},
		{name: "non numeric", input: "v1.2", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			major, minor, err := namrbdversion.ParseMajorMinor(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if major != tc.wantMajor || minor != tc.wantMinor {
				t.Fatalf("got %d.%d want %d.%d", major, minor, tc.wantMajor, tc.wantMinor)
			}
		})
	}
}

func TestCompareMajorMinor(t *testing.T) {
	tests := []struct {
		name    string
		a       string
		b       string
		want    int
		wantErr bool
	}{
		{name: "equal", a: "1.0", b: "1.0", want: 0},
		{name: "greater by minor", a: "1.2", b: "1.1", want: 1},
		{name: "less by minor", a: "1.1", b: "1.2", want: -1},
		{name: "greater by major", a: "2.0", b: "1.9", want: 1},
		{name: "invalid", a: "1.2.3", b: "1.2", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := namrbdversion.CompareMajorMinor(tc.a, tc.b)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q vs %q", tc.a, tc.b)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q vs %q: %v", tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Fatalf("compare=%d want=%d for %q vs %q", got, tc.want, tc.a, tc.b)
			}
		})
	}
}
