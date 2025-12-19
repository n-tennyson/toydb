package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendPut_WritesBytes(t *testing.T) {

	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WA: %v", err)
	}

	err = wal.AppendPut("key", []byte("value"))
	if err != nil {
		t.Fatalf("failed to append PUT record: %v", err)
	}

	err = wal.CloseWAL()
	if err != nil {
		t.Fatalf("error closing WAL: %v", err)
	}

	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("failed to stat WAL file: %v", err)
	}

	if info.Size() == 0 {
		t.Fatalf("expedted WAL file to contain data, but size was 0")
	}
}
