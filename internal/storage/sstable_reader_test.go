package storage

import (
	"testing"
)

func TestSSTableGGet_FindsExistingKey(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	mem.Put("a", []byte("1"))
	mem.Put("b", []byte("2"))
	mem.MarkImmutable()

	sst, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	val, ok := sst.Get("a")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if string(val) != "1" {
		t.Fatalf("expected value '1', got %q", val)
	}
}

func TestSSTableGet_MissingKeyReturnsfalse(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	mem.Put("a", []byte("1"))
	mem.Put("b", []byte("2"))
	mem.MarkImmutable()

	sst, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	val, ok := sst.Get("c")
	if ok {
		t.Fatalf("expected key to be missing, got value %q", val)
	}

	if val != nil {
		t.Fatalf("expected nil value for missing key")
	}

}

func TestSSTableGet_EarlyExit(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	mem.Put("a", []byte("1"))
	mem.Put("c", []byte("3"))
	mem.MarkImmutable()

	sst, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	val, ok := sst.Get("b")
	if ok {
		t.Fatalf("expected key to be missing, got value %q", val)
	}

	if val != nil {
		t.Fatalf("expected nil value for missing key")
	}
}

func TestSSTableGet_Tombstone(t *testing.T) {
	dir := t.TempDir()

	mem := NewMemTable(1024)
	mem.Put("a", []byte("1"))
	mem.Delete("a")
	mem.MarkImmutable()

	sst, err := WriteSSTable(dir, mem)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	val, ok := sst.Get("a")
	if !ok {
		t.Fatalf("expected tombstone key to exist")
	}

	if len(val) != 0 {
		t.Fatalf("expected empty value for tombstone, got %q", val)
	}
}
