package service

import (
	"fmt"
	"strings"

	namrbdversion "github.com/nosway/namrbd/version"
)

func CheckSBSMajorVersionCompatibility(clientVersion, serverVersion string) error {
	clientVersion = strings.TrimSpace(clientVersion)
	serverVersion = strings.TrimSpace(serverVersion)
	if clientVersion == "" || serverVersion == "" {
		return nil
	}
	if clientVersion == serverVersion {
		return nil
	}
	order, err := namrbdversion.CompareMajorMinor(serverVersion, clientVersion)
	if err != nil {
		if _, _, clientErr := namrbdversion.ParseMajorMinor(clientVersion); clientErr != nil {
			return fmt.Errorf("invalid client version %q: %w", clientVersion, clientErr)
		}
		if _, _, serverErr := namrbdversion.ParseMajorMinor(serverVersion); serverErr != nil {
			return fmt.Errorf("invalid server version %q: %w", serverVersion, serverErr)
		}
		return fmt.Errorf("compare sbs versions client=%q server=%q: %w", clientVersion, serverVersion, err)
	}
	if order >= 0 {
		return nil
	}
	return fmt.Errorf("sbs version incompatibility: client=%q server=%q", clientVersion, serverVersion)
}
