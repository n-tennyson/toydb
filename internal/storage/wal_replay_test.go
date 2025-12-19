package storage

import (
	"path/filepath"
	"testing"
)

func TestWALReplay_SinglePut(t *testing.T) {

	// 1. Setup temporary directory and WAL path
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// 2. Create WAL and append a PUT record
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	err = wal.AppendPut("key", []byte("value"))
	if err != nil {
		t.Fatalf("failed to append PUT record: %v", err)
	}
	err = wal.CloseWAL()
	if err != nil {
		t.Fatalf("error closing WAL: %v", err)
	}

	// 3. Create empty MemTable
	mem := NewMemTable(1024)

	// 4. Replay WAL into MemTable
	err = ReplayWAL(walPath, mem)
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	// 5. Verify MemTable contains the expected key/value
	val, ok := mem.Get("key")
	if !ok {
		t.Fatalf("expected key to exist after WAL replay")
	}
	if string(val) != "value" {
		t.Fatalf("expected value 'value', got %q", string(val))
	}
}
