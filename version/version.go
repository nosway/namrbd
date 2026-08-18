package version

import (
	"fmt"
	"strings"
)

var Current = "v1.0.0-rc"

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
