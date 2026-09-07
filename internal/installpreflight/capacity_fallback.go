//go:build !linux && !darwin

package installpreflight

import "fmt"

func filesystemCapacity(path string) (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("filesystem capacity is not observable for %s on this platform", path)
}
