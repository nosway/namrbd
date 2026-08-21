package cliux

import (
	"bytes"
	"flag"
	"reflect"
	"strings"
	"testing"
)

func TestRewriteDeprecatedFlagsPreservesValuesAndWarnsOnce(t *testing.T) {
	var stderr bytes.Buffer
	got := RewriteDeprecatedFlags([]string{
		"--admin-endpoint=svc-a:9443", "--admin-endpoint", "svc-b:9443", "command",
	}, []Alias{{Legacy: "admin-endpoint", Canonical: "sbs-service-endpoint"}}, &stderr)
	want := []string{"--sbs-service-endpoint=svc-a:9443", "--sbs-service-endpoint", "svc-b:9443", "command"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args=%v want=%v", got, want)
	}
	if strings.Count(stderr.String(), "deprecated flag") != 1 || !strings.Contains(stderr.String(), "--sbs-service-endpoint") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestInstallStructuredUsageHidesDevelopmentFlags(t *testing.T) {
	var out bytes.Buffer
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(&out)
	fs.String("endpoint", "127.0.0.1:1", "service endpoint")
	fs.Bool("lab-fast", false, "fixture shortcut")
	InstallStructuredUsage(fs, "test", func(name string) bool { return strings.HasPrefix(name, "lab-") })
	fs.Usage()
	if !strings.Contains(out.String(), "--endpoint") || !strings.Contains(out.String(), "service endpoint") {
		t.Fatalf("missing public flag help: %s", out.String())
	}
	if strings.Contains(out.String(), "lab-fast") {
		t.Fatalf("development flag leaked into help: %s", out.String())
	}
}

func TestRewriteCommandArgs(t *testing.T) {
	got := RewriteCommandArgs([]string{"--json", "help"}, true, false)
	want := []string{"--output=json", "--help"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args=%v want=%v", got, want)
	}
	got = RewriteCommandArgs([]string{"--json"}, true, true)
	if !reflect.DeepEqual(got, []string{"--json"}) {
		t.Fatalf("native json args=%v", got)
	}
}
