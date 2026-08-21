package serviceconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const secretValue = "s3cr3t-material-do-not-print"

func writeSecret(t *testing.T, name, content string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func strictResolver(env EnvLookup) *Resolver { return NewResolver(ProfileLargeScale, env) }

// A resolved secret must never render its value, whatever the print path.
// A mistaken verb or a struct dumped during debugging is exactly how these
// reach a log.
func TestSecretNeverRenders(t *testing.T) {
	p := writeSecret(t, "k", secretValue, 0o600)
	s, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if s.Expose() != secretValue {
		t.Fatalf("Expose returned %q", s.Expose())
	}

	renderings := map[string]string{
		"String":  s.String(),
		"%v":      fmt.Sprintf("%v", s),
		"%s":      fmt.Sprintf("%s", s),
		"%q":      fmt.Sprintf("%q", s),
		"%#v":     fmt.Sprintf("%#v", s),
		"%+v":     fmt.Sprintf("%+v", s),
		"println": fmt.Sprint(s),
		"struct":  fmt.Sprintf("%v", struct{ K Secret }{s}),
	}
	for name, out := range renderings {
		if strings.Contains(out, secretValue) {
			t.Errorf("%s leaked the secret: %s", name, out)
		}
		if !strings.Contains(out, RedactedMarker) {
			t.Errorf("%s did not redact: %s", name, out)
		}
	}

	blob, err := json.Marshal(struct {
		Key Secret `json:"key"`
	}{s})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(blob), secretValue) {
		t.Errorf("json marshal leaked the secret: %s", blob)
	}

	ym, err := yaml.Marshal(map[string]Secret{"key": s})
	if err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if strings.Contains(string(ym), secretValue) {
		t.Errorf("yaml marshal leaked the secret: %s", ym)
	}
}

// Every unresolvable reference is an error, never an empty secret. An empty
// credential that reaches a running process is an unauthenticated path that
// still starts cleanly.
func TestUnresolvableReferencesFailClosed(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		ref  SecretRef
		want string
	}{
		{"no source set", SecretRef{}, "no source is set"},
		{"missing file", SecretRef{File: filepath.Join(dir, "absent")}, "stat"},
		{"unset env", SecretRef{Env: "NAMRBD_DEFINITELY_UNSET_TEST_VAR"}, "is not set"},
		{"kms not implemented", SecretRef{KMS: "projects/p/keys/k"}, "kms references are not implemented"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := strictResolver(noEnv).Resolve("x.key", tc.ref)
			if err == nil {
				t.Fatal("an unresolvable reference returned no error")
			}
			if !errors.Is(err, ErrSecretUnresolvable) {
				t.Errorf("error does not wrap ErrSecretUnresolvable: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if !s.Empty() {
				t.Error("a failed resolution returned material")
			}
		})
	}
}

// An empty secret file or empty variable is unresolvable, not an empty secret.
func TestEmptySecretIsUnresolvable(t *testing.T) {
	p := writeSecret(t, "empty", "\n", 0o600)
	if _, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p}); err == nil {
		t.Fatal("an empty secret file resolved successfully")
	}
	env := envMap(map[string]string{"NAMRBD_EMPTY": ""})
	if _, err := strictResolver(env).Resolve("x.key", SecretRef{Env: "NAMRBD_EMPTY"}); err == nil {
		t.Fatal("an empty environment variable resolved successfully")
	}
}

// A secret file readable by group or other is refused in the strict profile.
func TestSecretFileModeIsEnforced(t *testing.T) {
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o660, 0o666} {
		t.Run(fmt.Sprintf("mode%04o", mode), func(t *testing.T) {
			p := writeSecret(t, "k", secretValue, mode)
			// os.WriteFile is umask-filtered; set the mode explicitly.
			if err := os.Chmod(p, mode); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			_, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p})
			if err == nil {
				t.Fatalf("mode %04o was accepted", mode)
			}
			if !strings.Contains(err.Error(), "group or other") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
	for _, mode := range []os.FileMode{0o600, 0o400} {
		p := writeSecret(t, "k", secretValue, mode)
		if err := os.Chmod(p, mode); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p}); err != nil {
			t.Errorf("mode %04o was rejected: %v", mode, err)
		}
	}
}

// The dev profile does not enforce file mode, so a developer is not blocked by
// a checkout's permissions. That relaxation is exactly why the shipped examples
// declare the strict profile.
func TestDevProfileDoesNotEnforceMode(t *testing.T) {
	p := writeSecret(t, "k", secretValue, 0o644)
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := NewResolver(ProfileDev, noEnv).Resolve("x.key", SecretRef{File: p}); err != nil {
		t.Errorf("dev profile rejected a 0644 secret file: %v", err)
	}
}

// An error message is one more place a secret can end up.
func TestErrorsDoNotEchoSecretMaterial(t *testing.T) {
	p := writeSecret(t, "k", secretValue, 0o644)
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p})
	if err == nil {
		t.Fatal("expected a mode rejection")
	}
	if strings.Contains(err.Error(), secretValue) {
		t.Fatalf("error echoed the secret: %v", err)
	}
}

// A trailing newline from a shell redirect is stripped; interior and leading
// whitespace is not, because it can be part of the secret.
func TestSecretTrailerHandling(t *testing.T) {
	cases := map[string]string{
		"plain\n":      "plain",
		"plain\r\n":    "plain",
		"plain":        "plain",
		"  padded  \n": "  padded  ",
		"two\nlines\n": "two\nlines",
	}
	for content, want := range cases {
		p := writeSecret(t, "k", content, 0o600)
		s, err := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p})
		if err != nil {
			t.Fatalf("resolve %q: %v", content, err)
		}
		if s.Expose() != want {
			t.Errorf("content %q resolved to %q, want %q", content, s.Expose(), want)
		}
	}
}

// Source names where the secret came from without naming the secret.
func TestSecretSourceNamesOriginNotValue(t *testing.T) {
	p := writeSecret(t, "k", secretValue, 0o600)
	s, _ := strictResolver(noEnv).Resolve("x.key", SecretRef{File: p})
	if !strings.HasPrefix(s.Source(), "file:") || strings.Contains(s.Source(), secretValue) {
		t.Errorf("bad source %q", s.Source())
	}
	env := envMap(map[string]string{"NAMRBD_K": secretValue})
	s2, _ := strictResolver(env).Resolve("x.key", SecretRef{Env: "NAMRBD_K"})
	if s2.Source() != "env:NAMRBD_K" {
		t.Errorf("bad source %q", s2.Source())
	}
}

// A pasted secret is rejected before the file is parsed, in any field.
func TestConfigWithSecretLiteralIsRejected(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(configsDir, "namrbd-gateway.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	poisoned := strings.Replace(string(raw),
		"      file: /etc/namrbd/secrets/dataplane-token.key",
		"      file: \"-----BEGIN RSA PRIVATE KEY-----\"", 1)
	if poisoned == string(raw) {
		t.Fatal("fixture did not modify the config")
	}
	p := filepath.Join(t.TempDir(), "poisoned.yaml")
	if err := os.WriteFile(p, []byte(poisoned), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Load(p, nil, noEnv, nil)
	if err == nil {
		t.Fatal("a config carrying a private key was accepted")
	}
	if !strings.Contains(err.Error(), "references") {
		t.Errorf("unhelpful error: %v", err)
	}
	if strings.Contains(err.Error(), "BEGIN RSA PRIVATE KEY") {
		t.Errorf("the rejection echoed the secret: %v", err)
	}
}

// The config file itself must not be world readable in the strict profile: its
// reference set is a map of where every credential on the host lives.
func TestConfigFileModeIsEnforcedInStrictProfile(t *testing.T) {
	raw, _ := os.ReadFile(filepath.Join(configsDir, "sbs-data.yaml"))
	p := filepath.Join(t.TempDir(), "sbs-data.yaml")
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p, nil, noEnv, nil); err == nil {
		t.Fatal("a 0644 large_scale config was accepted")
	}
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := Load(p, nil, noEnv, nil)
	if err != nil {
		t.Fatalf("a 0600 config was rejected: %v", err)
	}
	s := res.Summarize(nil)
	if !s.FileModeChecked || !s.SecretLiteralRejected || !s.SecretRedactionReady {
		t.Errorf("summary gate fields not set: %+v", s)
	}
}

func TestScanForSecretLiteralsLocatesWithoutEchoing(t *testing.T) {
	hits := ScanForSecretLiterals("a: ok\nchap_secret: hunter2hunter2\n")
	if len(hits) == 0 {
		t.Fatal("a pasted secret was not detected")
	}
	for _, h := range hits {
		if strings.Contains(h, "hunter2") {
			t.Errorf("the finding echoed the secret: %s", h)
		}
		if !strings.Contains(h, "line 2") {
			t.Errorf("the finding does not locate the line: %s", h)
		}
	}
}
