//go:build !enterprise

package iscsi

import "fmt"

const (
	ISCSIEdition           = "community"
	ISCSIExportVolumeLimit = 3
)

func ValidateExportVolumeLimit(state *ControlState, candidate LUN) error {
	if state == nil || ISCSIExportVolumeLimit <= 0 {
		return nil
	}
	volumes := map[string]struct{}{}
	for _, lun := range state.LUNs {
		if lun.VolumeID != "" {
			volumes[lun.VolumeID] = struct{}{}
		}
	}
	if candidate.VolumeID != "" {
		volumes[candidate.VolumeID] = struct{}{}
	}
	if len(volumes) > ISCSIExportVolumeLimit {
		return fmt.Errorf("community edition allows at most %d iSCSI-exported volumes", ISCSIExportVolumeLimit)
	}
	return nil
}
