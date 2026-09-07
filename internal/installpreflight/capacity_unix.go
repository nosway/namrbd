//go:build linux || darwin

package installpreflight

import "golang.org/x/sys/unix"

func filesystemCapacity(path string) (uint64, uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	blockSize := uint64(stat.Bsize)
	return uint64(stat.Blocks) * blockSize, uint64(stat.Bavail) * blockSize, nil
}
