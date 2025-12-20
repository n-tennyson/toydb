package storage

import (
	"encoding/binary"
	"os"
)

func (s *SSTable) Get(target string) ([]byte, bool) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	for {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return nil, false
		}

		keyBuf := make([]byte, keyLen)
		if _, err := file.Read(keyBuf); err != nil {
			return nil, false
		}
		key := string(keyBuf)

		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			return nil, false
		}

		value := make([]byte, valueLen)
		if _, err := file.Read(value); err != nil {
			return nil, false
		}

		if key == target {
			return value, true
		}

		if key > target {
			return nil, false
		}
	}

}
