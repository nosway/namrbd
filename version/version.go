package version

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Major = 1
	Minor = 0
)

const Current = "1.0"

func ParseMajorMinor(v string) (int, int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, 0, fmt.Errorf("version is empty")
	}
	parts := strings.Split(v, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, fmt.Errorf("version %q must use Major.Minor format", v)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("parse major version %q: %w", v, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse minor version %q: %w", v, err)
	}
	if major < 0 || minor < 0 {
		return 0, 0, fmt.Errorf("version %q must be non-negative", v)
	}
	return major, minor, nil
}

func CompareMajorMinor(a, b string) (int, error) {
	aMajor, aMinor, err := ParseMajorMinor(a)
	if err != nil {
		return 0, err
	}
	bMajor, bMinor, err := ParseMajorMinor(b)
	if err != nil {
		return 0, err
	}
	if aMajor < bMajor {
		return -1, nil
	}
	if aMajor > bMajor {
		return 1, nil
	}
	if aMinor < bMinor {
		return -1, nil
	}
	if aMinor > bMinor {
		return 1, nil
	}
	return 0, nil
}
