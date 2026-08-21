package version

import (
	"fmt"
	"strconv"
	"strings"
)

var Current = "v1.0.0"

var (
	Commit    = "unknown"
	BuildDate = "unknown"
	Dirty     = "unknown"
)

type BuildIdentity struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date,omitempty"`
	Dirty     string `json:"dirty,omitempty"`
}

func Info() BuildIdentity {
	return BuildIdentity{
		Version:   ProductVersion(),
		Commit:    CommitID(),
		BuildDate: strings.TrimSpace(BuildDate),
		Dirty:     strings.TrimSpace(Dirty),
	}
}

func ProductVersion() string {
	if v := strings.TrimSpace(Current); v != "" {
		return v
	}
	return "dev"
}

func CommitID() string {
	if v := strings.TrimSpace(Commit); v != "" {
		return v
	}
	return "unknown"
}

func BuildSummary() string {
	info := Info()
	parts := []string{
		info.Version,
		"commit=" + info.Commit,
	}
	if info.BuildDate != "" && info.BuildDate != "unknown" {
		parts = append(parts, "build_date="+info.BuildDate)
	}
	if info.Dirty != "" && info.Dirty != "unknown" {
		parts = append(parts, "dirty="+info.Dirty)
	}
	return strings.Join(parts, " ")
}

func NormalizeProductVersion(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", fmt.Errorf("version is empty")
	}
	core := strings.TrimPrefix(v, "v")
	if i := strings.IndexAny(core, "+-"); i >= 0 {
		core = core[:i]
	}
	if err := validateSemVerCore(core); err != nil {
		return "", err
	}
	return v, nil
}

// AtLeast reports whether current is at or beyond minimum by SemVer core.
// Prerelease and build suffixes do not postpone compatibility removals: a
// v1.1.0-rc binary must already enforce the v1.1.0 surface.
func AtLeast(current, minimum string) (bool, error) {
	currentCore, err := semVerCore(current)
	if err != nil {
		return false, fmt.Errorf("current version: %w", err)
	}
	minimumCore, err := semVerCore(minimum)
	if err != nil {
		return false, fmt.Errorf("minimum version: %w", err)
	}
	for i := range currentCore {
		if currentCore[i] != minimumCore[i] {
			return currentCore[i] > minimumCore[i], nil
		}
	}
	return true, nil
}

func semVerCore(v string) ([3]int, error) {
	var out [3]int
	v = strings.TrimSpace(strings.TrimPrefix(v, "v"))
	if i := strings.IndexAny(v, "+-"); i >= 0 {
		v = v[:i]
	}
	if err := validateSemVerCore(v); err != nil {
		return out, err
	}
	for i, part := range strings.Split(v, ".") {
		n, err := strconv.Atoi(part)
		if err != nil {
			return out, fmt.Errorf("version %q contains an invalid SemVer component: %w", v, err)
		}
		out[i] = n
	}
	return out, nil
}

func validateSemVerCore(v string) error {
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return fmt.Errorf("version %q must use SemVer format", v)
	}
	for _, part := range parts {
		if part == "" {
			return fmt.Errorf("version %q contains an empty SemVer component", v)
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return fmt.Errorf("version %q contains a non-numeric SemVer core component", v)
			}
		}
	}
	return nil
}
