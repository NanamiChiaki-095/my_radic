//go:build windows

package kvdb

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func mapFile(f *os.File, size int64) ([]byte, func() error, func() error, error) {
	if err := f.Truncate(size); err != nil {
		return nil, nil, nil, err
	}
	handle := windows.Handle(f.Fd())
	mapping, err := windows.CreateFileMapping(handle, nil, windows.PAGE_READWRITE, uint32(uint64(size)>>32), uint32(size), nil)
	if err != nil {
		return nil, nil, nil, err
	}
	addr, err := windows.MapViewOfFile(mapping, windows.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		windows.CloseHandle(mapping)
		return nil, nil, nil, err
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), int(size))
	syncFunc := func() error {
		if err := windows.FlushViewOfFile(addr, uintptr(size)); err != nil {
			return err
		}
		return windows.FlushFileBuffers(handle)
	}
	unmapFunc := func() error {
		if err := windows.UnmapViewOfFile(addr); err != nil {
			return err
		}
		return windows.CloseHandle(mapping)
	}
	return data, syncFunc, unmapFunc, nil
}
