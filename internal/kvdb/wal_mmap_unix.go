//go:build !windows

package kvdb

import (
	"os"

	"golang.org/x/sys/unix"
)

func mapFile(f *os.File, size int64) ([]byte, func() error, func() error, error) {
	if err := f.Truncate(size); err != nil {
		return nil, nil, nil, err
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, nil, err
	}
	syncFunc := func() error {
		return unix.Msync(data, unix.MS_SYNC)
	}
	unmapFunc := func() error {
		return unix.Munmap(data)
	}
	return data, syncFunc, unmapFunc, nil
}
