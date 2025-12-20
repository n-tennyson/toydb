package storage

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWriteSSTable_CreatesFile(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	if err := mem.Put("b", []byte("2")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := mem.Put("a", []byte("1")); err != nil {
		t.Fatalf("put failed:%v", err)
	}

	mem.MarkImmutable()

	sst, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	if sst == nil {
		t.Fatalf("expected SSTable, got nil")
	}

	path := filepath.Join(dir, "sstable-000001.db")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected SSTable file to exist: %v", err)
	}

}

func TestWriteSSTable_RejectsMutableMemTable(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	if err := mem.Put("a", []byte("1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	_, err := WriteSSTable(dir, mem)

	if err == nil {
		t.Fatalf("expected error when writing mutable memtable, got nil")
	}
}

func TestWriteSSTable_WritesKeysInSortedOrder(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	mem.Put("a", []byte("3"))
	mem.Put("c", []byte("1"))
	mem.Put("b", []byte("2"))

	mem.MarkImmutable()

	_, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	path := filepath.Join(dir, "sstable-000001.db")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open SSTable file: %v", err)
	}
	defer f.Close()

	var keys []string

	for {
		var keyLen uint32
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read key length: %v", err)
		}

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBuf); err != nil {
			t.Fatalf("failed to read key: %v", err)
		}

		var valueLen uint32
		if err := binary.Read(f, binary.LittleEndian, &valueLen); err != nil {
			t.Fatalf("failed to read value length: %v", err)
		}

		if _, err := f.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			t.Fatalf("failed to skip value: %v", err)
		}

		keys = append(keys, string(keyBuf))

	}

	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(keys, expected) {
		t.Fatalf("keys not sorted: got %v, want %v", keys, expected)
	}
}
