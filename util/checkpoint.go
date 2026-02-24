package util

import (
	"encoding/binary"
	"errors"
	"os"
)

func LoadCheckpoint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) != 8 {
		return 0, errors.New("checkpoint file format error")
	}
	return binary.LittleEndian.Uint64(data), nil
}

func SaveCheckpoint(path string, offset uint64) error {
	tmp := path + ".tmp"
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, offset)
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
