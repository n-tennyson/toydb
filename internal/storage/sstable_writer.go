package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
)

func generateSSTablePath(dir string) string {
	filename := "sstable-000001.db"
	return filepath.Join(dir, filename)
}

// WriteSSTable flushes an immutable MemTable to disk as a new SSTable.
func WriteSSTable(dir string, mem *MemTable) (*SSTable, error) {

	if !mem.IsImmutable() {
		return nil, errors.New("memtable must be immutable to write SSTable")
	}

	path := generateSSTablePath(dir)

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	keys := mem.Keys()

	for _, key := range keys {

		if key == "" {
			continue
		}

		value, _ := mem.Get(key)

		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			return nil, err
		}

		if _, err := file.Write([]byte(key)); err != nil {
			return nil, err
		}

		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			return nil, err
		}

		if _, err := file.Write(value); err != nil {
			return nil, err
		}
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	return OpenSSTable(path)

}
