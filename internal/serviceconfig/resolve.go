package serviceconfig

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
)

// Secret holds resolved secret material.
//
// The value is unexported and every rendering path is overridden to print the
// redaction marker instead. That covers fmt.Print, %v, %s, %#v, structured
// logging that marshals to JSON, and a struct printed wholesale during
// debugging. Reaching the real value requires calling Expose, which is
// deliberately ugly to read at a call site so it stands out in review.
type Secret struct {
	value string
	// source records where it came from, for summaries. It names the source,
	// never the value.
	source string
}

// String satisfies fmt.Stringer.
func (s Secret) String() string { return RedactedMarker }

// GoString covers the %#v verb.
func (s Secret) GoString() string { return RedactedMarker }

// Format covers every fmt verb, including %s and %q, so a mistaken verb cannot
// bypass String.
func (s Secret) Format(f fmt.State, verb rune) { _, _ = f.Write([]byte(RedactedMarker)) }

// MarshalJSON covers structured logging and summary emission.
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + RedactedMarker + `"`), nil }

// MarshalYAML covers config round-tripping.
func (s Secret) MarshalYAML() (any, error) { return RedactedMarker, nil }

// Expose returns the secret material. Every call site is a place where secret
// material enters a wider scope, so each one should be obvious in review.
func (s Secret) Expose() string { return s.value }

// Source names where the secret came from, such as "file:/etc/namrbd/x.key".
func (s Secret) Source() string { return s.source }

// Empty reports whether nothing was resolved.
func (s Secret) Empty() bool { return s.value == "" }

// ErrSecretUnresolvable is returned when a reference cannot be resolved.
// Callers must treat it as fatal at startup: a process that cannot resolve its
// credentials must not fall back to an unauthenticated path.
var ErrSecretUnresolvable = errors.New("secret reference could not be resolved")

// Resolver turns secret references into secret material.
type Resolver struct {
	// Env is the environment lookup, injectable for tests.
	Env EnvLookup
	// RequireStrictMode rejects a referenced secret file that is readable by
	// group or other. The large_scale profile sets this.
	RequireStrictMode bool
	// RequireOwner rejects a secret file not owned by the running user.
	RequireOwner bool
}

// NewResolver returns a resolver configured for a profile.
func NewResolver(profile string, env EnvLookup) *Resolver {
	if env == nil {
		env = OSEnv
	}
	strict := profile == ProfileLargeScale
	return &Resolver{Env: env, RequireStrictMode: strict, RequireOwner: strict}
}

// Resolve reads the material a reference points at.
//
// It fails closed. An unset, unreadable, empty, or unsupported reference is an
// error rather than an empty secret, because an empty credential that reaches a
// running process becomes an unauthenticated path that still starts cleanly.
func (r *Resolver) Resolve(field string, ref SecretRef) (Secret, error) {
	if err := ref.Validate(field); err != nil {
		return Secret{}, err
	}
	if ref.Empty() {
		return Secret{}, fmt.Errorf("%s: %w: no source is set", field, ErrSecretUnresolvable)
	}

	switch {
	case strings.TrimSpace(ref.File) != "":
		return r.resolveFile(field, strings.TrimSpace(ref.File))
	case strings.TrimSpace(ref.Env) != "":
		return r.resolveEnv(field, strings.TrimSpace(ref.Env))
	default:
		// KMS references parse and validate so a config can be written and
		// reviewed ahead of the integration, but resolving one fails rather
		// than returning nothing.
		return Secret{}, fmt.Errorf("%s: %w: kms references are not implemented in this build (key %q)",
			field, ErrSecretUnresolvable, strings.TrimSpace(ref.KMS))
	}
}

func (r *Resolver) resolveFile(field, path string) (Secret, error) {
	if err := r.CheckSecretFileMode(field, path); err != nil {
		return Secret{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		// The path is safe to echo; the contents are not, and are not read on
		// this branch.
		return Secret{}, fmt.Errorf("%s: %w: %v", field, ErrSecretUnresolvable, err)
	}
	v := trimSecretTrailer(string(raw))
	if v == "" {
		return Secret{}, fmt.Errorf("%s: %w: %s is empty", field, ErrSecretUnresolvable, path)
	}
	return Secret{value: v, source: "file:" + path}, nil
}

func (r *Resolver) resolveEnv(field, name string) (Secret, error) {
	env := r.Env
	if env == nil {
		env = OSEnv
	}
	v, ok := env(name)
	if !ok {
		return Secret{}, fmt.Errorf("%s: %w: environment variable %s is not set", field, ErrSecretUnresolvable, name)
	}
	v = trimSecretTrailer(v)
	if v == "" {
		return Secret{}, fmt.Errorf("%s: %w: environment variable %s is empty", field, ErrSecretUnresolvable, name)
	}
	return Secret{value: v, source: "env:" + name}, nil
}

// trimSecretTrailer removes a trailing newline, which is what any operator who
// creates a secret file with a shell redirect will leave behind. Only the
// trailer is removed: leading and interior whitespace can be part of the
// secret, and silently altering it would produce an authentication failure with
// no visible cause.
func trimSecretTrailer(v string) string {
	v = strings.TrimSuffix(v, "\n")
	v = strings.TrimSuffix(v, "\r")
	return v
}

// CheckSecretFileMode enforces that a referenced secret file is not readable
// beyond its owner, and that the running process owns it.
func (r *Resolver) CheckSecretFileMode(field, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: %w: %v", field, ErrSecretUnresolvable, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s: %w: %s is a directory", field, ErrSecretUnresolvable, path)
	}
	if r.RequireStrictMode {
		if err := requireOwnerOnly(field, path, info.Mode()); err != nil {
			return err
		}
	}
	if r.RequireOwner {
		if err := requireOwnedByProcess(field, path, info); err != nil {
			return err
		}
	}
	return nil
}

// requireOwnerOnly rejects any group or other permission bit.
func requireOwnerOnly(field, path string, mode fs.FileMode) error {
	if perm := mode.Perm(); perm&0o077 != 0 {
		return fmt.Errorf("%s: %s is mode %04o; a secret file must not be readable by group or other (use 0600)",
			field, path, perm)
	}
	return nil
}

// requireOwnedByProcess rejects a secret file owned by another user. A file the
// service can read but does not own can be replaced by whoever does own it.
func requireOwnedByProcess(field, path string, info fs.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Ownership is not observable here. Say so rather than silently
		// treating the check as passed.
		return fmt.Errorf("%s: cannot determine ownership of %s on this platform", field, path)
	}
	if uid := os.Getuid(); int(st.Uid) != uid {
		return fmt.Errorf("%s: %s is owned by uid %d but this process runs as uid %d",
			field, path, st.Uid, uid)
	}
	return nil
}

// CheckConfigFileMode applies the same rule to the config file itself. It holds
// references rather than secrets, but the reference set is still a map of where
// every credential on the host lives.
func CheckConfigFileMode(path string, strict bool) error {
	if !strict {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("config file: stat %s: %v", path, err)
	}
	return requireOwnerOnly("config file", path, info.Mode())
}

// ScanForSecretLiterals reports config lines that appear to carry a secret
// value rather than a reference to one.
//
// This runs against the raw file rather than the parsed struct, so it catches a
// literal pasted into any field, including one the schema does not model as a
// secret.
func ScanForSecretLiterals(raw string) []string {
	var hits []string
	for i, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		value := trimmed
		if idx := strings.Index(trimmed, ":"); idx >= 0 {
			value = strings.TrimSpace(trimmed[idx+1:])
		}
		value = strings.Trim(value, `"'`)
		if LooksLikeSecretLiteral(value) || LooksLikeSecretLiteral(trimmed) {
			// The line number and the field name locate the problem. The value
			// is not echoed, because an error message is one more place a
			// secret can end up.
			field := trimmed
			if idx := strings.Index(trimmed, ":"); idx >= 0 {
				field = strings.TrimSpace(trimmed[:idx])
			}
			hits = append(hits, fmt.Sprintf("line %d: field %q appears to carry a secret value", i+1, field))
		}
	}
	return hits
}
