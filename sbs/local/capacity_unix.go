//go:build unix

package local

import "syscall"

func filesystemCapacity(path string) (capacityBytes, availableBytes uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	blockSize := uint64(st.Bsize)
	return uint64(st.Blocks) * blockSize, uint64(st.Bavail) * blockSize, nil
}
