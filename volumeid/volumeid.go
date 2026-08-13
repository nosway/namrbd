// Package volumeid defines the canonical 8-digit lowercase hexadecimal encoding
// for NAMRBD volume identifiers (lower 32 bits).
package volumeid

import (
	"fmt"
	"strconv"
	"strings"
)

const Len = 8

// Format returns exactly eight lowercase hex digits for the volume id (32-bit domain).
func Format(volumeID uint64) string {
	return fmt.Sprintf("%08x", uint32(volumeID))
}

// Parse accepts exactly eight hexadecimal digits [0-9a-fA-F] and returns the uint64 id.
// The numeric domain is uint32; the value is zero-extended to uint64 for callers.
func Parse(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) != Len {
		return 0, fmt.Errorf("volume id must be exactly %d hex digits", Len)
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return 0, fmt.Errorf("volume id must be 8 hex digits")
		}
	}
	v, err := strconv.ParseUint(strings.ToLower(s), 16, 32)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// ParseLowercase is like Parse but rejects uppercase letters so the path matches the canonical wire form.
func ParseLowercase(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) != Len {
		return 0, fmt.Errorf("volume id must be exactly %d lowercase hex digits", Len)
	}
	for _, r := range s {
		if r < '0' || r > 'f' || (r > '9' && r < 'a') {
			return 0, fmt.Errorf("volume id must be 8 lowercase hex digits")
		}
	}
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return v, nil
}
