//go:build !unix

package local

func filesystemCapacity(string) (capacityBytes, availableBytes uint64, err error) {
	return 0, 0, nil
}
