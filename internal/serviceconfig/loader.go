package serviceconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/envcompat"
	"gopkg.in/yaml.v3"
)

// Source records where a setting's final value came from.
type Source string

const (
	SourceDefault Source = "default"
	SourceFile    Source = "file"
	SourceEnv     Source = "env"
	SourceCLI     Source = "cli"
)

// Precedence is the documented order, lowest to highest.
//
//	built-in defaults < config file < environment overrides < explicit CLI overrides
var Precedence = []Source{SourceDefault, SourceFile, SourceEnv, SourceCLI}

// Rank returns a source's precedence position. A higher rank wins.
func Rank(s Source) int {
	for i, p := range Precedence {
		if p == s {
			return i
		}
	}
	return -1
}

// Overridable declares one field that may be supplied outside the config file.
//
// The registry is an allowlist, not a convenience. Anything absent from it can
// only be set in the config file, which is what keeps the file the authority
// instead of one input among many. The fields that are overridable are the
// per-node identity and endpoint settings that genuinely differ between hosts;
// a setting that should be identical across the fleet has no business being
// supplied per host.
type Overridable struct {
	// Field is the dotted config path, used in summaries and errors.
	Field string
	// Env is the environment variable that overrides it.
	Env string
	// LegacyEnvs are accepted by v1.0.x and rejected from v1.1.0. They are
	// resolved with Env as the canonical winner before apply is called.
	LegacyEnvs []envcompat.Legacy
	// Flag is the command-line flag that overrides it.
	Flag string
	// Secret marks a value that must be redacted in every summary.
	Secret bool
	// apply writes the value into the parsed config.
	apply func(*File, string) error
}

// AppliedOverride records one setting that came from outside the file.
type AppliedOverride struct {
	Field  string `json:"field"`
	Source Source `json:"source"`
	// Value is redacted for secret fields. It exists so an operator can see
	// what a node is actually running without reading its unit file.
	Value string `json:"value"`
}

// MergeAppliedOverrides preserves source order while collapsing duplicate
// field/source observations. A process may observe a canonical environment
// variable once while building flag defaults and again in the config loader;
// the operator-facing summary should still report one effective override.
func MergeAppliedOverrides(groups ...[]AppliedOverride) []AppliedOverride {
	var out []AppliedOverride
	seen := map[string]bool{}
	for _, group := range groups {
		for _, override := range group {
			key := override.Field + "\x00" + string(override.Source)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, override)
		}
	}
	return out
}

// LoadResult is what a process gets back, and what its summary is built from.
type LoadResult struct {
	File   *File
	Path   string
	Digest string
	// SourceAuthority is "file" when a config file supplied the settings and
	// "cli" when the process started without one.
	SourceAuthority Source
	Overrides       []AppliedOverride
	Warnings        []string
	// SecretLiteralsRejected records that the file was scanned for pasted
	// secret values before it was parsed.
	SecretLiteralsRejected bool
	// ConfigFileModeChecked records that the file permissions were enforced.
	// It is false outside the strict profile, where the check is not applied.
	ConfigFileModeChecked bool
}

// EnvLookup matches os.LookupEnv, so tests do not need a real environment.
type EnvLookup func(key string) (string, bool)

// OSEnv is the default lookup.
func OSEnv(key string) (string, bool) { return os.LookupEnv(key) }

// Load reads a config file, then applies environment overrides, then explicit
// CLI overrides, in that order.
//
// cliSet carries only flags the operator actually typed. The Go flag package
// reports these through FlagSet.Visit, which is the difference between "the
// flag has its default value" and "the operator asked for this value". Applying
// every flag would silently outrank the config file with defaults nobody chose,
// which is the single easiest way to get precedence wrong.
func Load(path string, registry []Overridable, env EnvLookup, cliSet map[string]string) (*LoadResult, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("config path is empty; --config is required when the config file is the source authority")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	sum := sha256.Sum256(raw)

	// A literal pasted into any field is rejected before the file is parsed, so
	// this catches fields the schema does not model as secrets.
	if hits := ScanForSecretLiterals(string(raw)); len(hits) > 0 {
		return nil, fmt.Errorf("config %s carries secret values, not references: %s",
			path, strings.Join(hits, "; "))
	}

	var f File
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	// An unrecognized key is an error. A silently ignored setting is a setting
	// the operator believes is in effect.
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	// The config file holds references rather than secrets, but the reference
	// set is still a map of where every credential on the host lives.
	if err := CheckConfigFileMode(path, f.Profile == ProfileLargeScale); err != nil {
		return nil, err
	}

	res := &LoadResult{
		File:                   &f,
		Path:                   path,
		Digest:                 hex.EncodeToString(sum[:])[:16],
		SourceAuthority:        SourceFile,
		SecretLiteralsRejected: true,
		ConfigFileModeChecked:  f.Profile == ProfileLargeScale,
	}

	byFlag := map[string]Overridable{}
	for _, o := range registry {
		if o.Flag != "" {
			byFlag[o.Flag] = o
		}
	}

	// Environment overrides, in a stable field order so summaries are
	// reproducible. Canonical and legacy names are resolved once per field;
	// applying each name independently would make lexical order an accidental
	// precedence rule.
	envOverrides := append([]Overridable(nil), registry...)
	sort.Slice(envOverrides, func(i, j int) bool { return envOverrides[i].Field < envOverrides[j].Field })
	for _, o := range envOverrides {
		if strings.TrimSpace(o.Env) == "" {
			continue
		}
		resolved, err := envcompat.ResolveCurrent(envcompat.Spec{Canonical: o.Env, Legacy: o.LegacyEnvs}, envcompat.Lookup(env))
		if err != nil {
			return nil, fmt.Errorf("environment override for %s: %w", o.Field, err)
		}
		if !resolved.Present {
			continue
		}
		if resolved.Conflict && f.Profile == ProfileLargeScale {
			return nil, fmt.Errorf("environment override for %s conflicts across canonical and legacy names; large_scale requires one unambiguous value", o.Field)
		}
		res.Warnings = append(res.Warnings, resolved.Warnings...)
		if err := o.apply(&f, resolved.Value); err != nil {
			return nil, fmt.Errorf("environment override %s: %w", resolved.Source, err)
		}
		res.Overrides = append(res.Overrides, AppliedOverride{
			Field: o.Field, Source: SourceEnv, Value: redact(o, resolved.Value),
		})
	}

	// Explicit CLI overrides last: they outrank everything.
	flagNames := make([]string, 0, len(cliSet))
	for name := range cliSet {
		flagNames = append(flagNames, name)
	}
	sort.Strings(flagNames)
	for _, name := range flagNames {
		o, ok := byFlag[name]
		if !ok {
			// A flag with no registry entry is not silently dropped. Either it
			// is a legitimate non-config flag such as --config or --json, which
			// the caller filters before calling Load, or it is a setting
			// someone is trying to supply outside the file.
			return nil, fmt.Errorf("flag --%s is not an overridable setting; put it in the config file", name)
		}
		v := cliSet[name]
		if err := o.apply(&f, v); err != nil {
			return nil, fmt.Errorf("cli override --%s: %w", name, err)
		}
		res.Overrides = append(res.Overrides, AppliedOverride{
			Field: o.Field, Source: SourceCLI, Value: redact(o, v),
		})
	}

	return res, nil
}

// RedactedMarker replaces a secret value in every summary. Square brackets are
// deliberate: Go's JSON encoder HTML-escapes angle brackets, which would render
// the marker as \u003credacted\u003e in the very logs an operator reads.
const RedactedMarker = "[redacted]"

// redact hides a value when the field is declared secret, and also when the
// value itself looks like a secret. The field's classification is the author's
// guess; the value is evidence.
func redact(o Overridable, v string) string {
	if o.Secret || LooksLikeSecretLiteral(v) {
		return RedactedMarker
	}
	return v
}

// Summary is the redacted operator-facing view of what a process is running.
// It never carries a secret value, so it is safe in logs and support bundles.
type Summary struct {
	ConfigFilePath        string            `json:"config_file_path"`
	ConfigRevision        int               `json:"config_revision"`
	ConfigDigest          string            `json:"config_digest"`
	ConfigSourceAuthority Source            `json:"config_source_authority"`
	ConfigProfile         string            `json:"config_profile"`
	ConfigProcess         string            `json:"config_process"`
	CLIOverrideCount      int               `json:"config_cli_override_count"`
	EnvOverrideCount      int               `json:"config_env_override_count"`
	Overrides             []AppliedOverride `json:"config_overrides"`
	WarningCount          int               `json:"config_warning_count"`
	Warnings              []string          `json:"config_warnings,omitempty"`
	SecretRedactionReady  bool              `json:"config_secret_redaction_ready"`
	SecretLiteralRejected bool              `json:"config_secret_literal_rejected"`
	FileModeChecked       bool              `json:"config_file_mode_checked"`
	LegacyStaticFlagsUsed bool              `json:"legacy_static_flags_used"`
	ErrorCount            int               `json:"error_count"`
	FirstError            string            `json:"first_error"`
	LastError             string            `json:"last_error"`
}

// Summarize builds the redacted summary. errs carries any load or validation
// errors so first_error and last_error are recorded even on the failure path,
// which is where an operator most needs them.
func (r *LoadResult) Summarize(errs []string) Summary {
	s := Summary{
		ConfigSourceAuthority: SourceCLI,
		SecretRedactionReady:  true,
		ErrorCount:            len(errs),
	}
	if len(errs) > 0 {
		s.FirstError = errs[0]
		s.LastError = errs[len(errs)-1]
	}
	if r == nil {
		return s
	}
	s.ConfigFilePath = r.Path
	s.ConfigDigest = r.Digest
	s.SecretLiteralRejected = r.SecretLiteralsRejected
	s.FileModeChecked = r.ConfigFileModeChecked
	s.ConfigSourceAuthority = r.SourceAuthority
	s.Overrides = r.Overrides
	s.WarningCount = len(r.Warnings)
	s.Warnings = append([]string(nil), r.Warnings...)
	for _, o := range r.Overrides {
		switch o.Source {
		case SourceCLI:
			s.CLIOverrideCount++
		case SourceEnv:
			s.EnvOverrideCount++
		}
	}
	if r.File != nil {
		s.ConfigRevision = r.File.Revision
		s.ConfigProfile = r.File.Profile
		s.ConfigProcess = r.File.Process
	}
	return s
}
