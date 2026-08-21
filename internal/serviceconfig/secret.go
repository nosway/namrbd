package serviceconfig

import (
	"fmt"
	"regexp"
	"strings"
)

// SecretRef points at a secret without carrying its value. Config files are
// reviewed, checked in, copied between hosts, and included in support bundles,
// so a literal key, certificate, or CHAP password in one of them leaks by
// design rather than by accident.
//
// The current command lines make this concrete: namrbd-gateway takes
// --dataplane-token-key and --dataplane-session-key as literal values, which
// are visible in ps output and shell history on every node they start on.
// Those become secret references here.
type SecretRef struct {
	// File reads the secret from a path. The file, not the config, carries the
	// value.
	File string `yaml:"file,omitempty"`
	// Env names an environment variable. The name is recorded; the value is not.
	Env string `yaml:"env,omitempty"`
	// KMS names a key in an external key manager.
	KMS string `yaml:"kms,omitempty"`
}

// Empty reports whether no source is set. An empty reference is legal in the
// schema; whether it is legal for a given field is a per-field decision made in
// validation, because some secrets are only required when a feature is enabled.
func (s SecretRef) Empty() bool {
	return strings.TrimSpace(s.File) == "" &&
		strings.TrimSpace(s.Env) == "" &&
		strings.TrimSpace(s.KMS) == ""
}

// String renders the reference for logs and redacted summaries. It names the
// source and never resolves it, so this is safe to print.
func (s SecretRef) String() string {
	switch {
	case s.Empty():
		return "<unset>"
	case s.File != "":
		return "file:" + s.File
	case s.Env != "":
		return "env:" + s.Env
	default:
		return "kms:" + s.KMS
	}
}

// Validate rejects a reference that names more than one source, since which one
// wins would be an undocumented precedence rule of its own.
func (s SecretRef) Validate(field string) error {
	set := 0
	for _, v := range []string{s.File, s.Env, s.KMS} {
		if strings.TrimSpace(v) != "" {
			set++
		}
	}
	if set > 1 {
		return fmt.Errorf("%s: a secret reference names %d sources; name exactly one of file, env, or kms", field, set)
	}
	return nil
}

// Patterns that indicate a secret value was pasted where a reference belongs.
// These are deliberately shaped to catch the common real cases rather than to
// be exhaustive: PEM blocks, long base64/hex blobs, and obvious inline
// assignments.
var secretLiteralPatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
	regexp.MustCompile(`-----BEGIN CERTIFICATE-----`),
	// A prefixed name such as chap_secret or dataplane_token must match too;
	// an underscore is a word character, so a leading \b would not fire.
	regexp.MustCompile(`(?i)[a-z0-9_.-]*(password|passwd|secret|token|apikey|api[_-]?key)\s*[:=]\s*\S`),
	regexp.MustCompile(`^[A-Za-z0-9+/]{40,}={0,2}$`),
	regexp.MustCompile(`^[0-9a-fA-F]{64,}$`),
}

// LooksLikeSecretLiteral reports whether a config value appears to be a secret
// value rather than a reference to one.
func LooksLikeSecretLiteral(value string) bool {
	v := strings.TrimSpace(value)
	if v == "" {
		return false
	}
	for _, re := range secretLiteralPatterns {
		if re.MatchString(v) {
			return true
		}
	}
	return false
}
