package main

import "testing"

func TestMemTablePutGet(t *testing.T) {
	memtable := NewMemTable(1024)

	err := memtable.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	value, ok := memtable.Get("key1")
	if !ok {
		t.Fatalf("expected key to exist")
	}

	if string(value) != "value1" {
		t.Fatalf("expected value 'value1', got %q", value)
	}
}

func TestMemTableOverwrite(t *testing.T) {
	memtable := NewMemTable(1024)

	err := memtable.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	err = memtable.Put("key1", []byte("value2"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	value, ok := memtable.Get("key1")
	if !ok {
		t.Fatalf("expected key to exist after overwrite")
	}

	if string(value) != "value2" {
		t.Fatalf("failed to overwrite value, got %q", value)
	}
}

func TestMemTableDelete(t *testing.T) {
	memtable := NewMemTable(1024)

	err := memtable.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	err = memtable.Delete("key1")
	if err != nil {
		t.Fatalf("unexpected error on Delete: %v", err)
	}

	value, ok := memtable.Get("key1")
	if !ok {
		t.Fatalf("expected key to exist as tombstone")
	}

	if value != nil {
		t.Fatalf("expected nil value for deleted key, got: %q", value)
	}
}

func TestMemTableImmutable(t *testing.T) {
	memtable := NewMemTable(1024)

	memtable.MarkImmutable()

	err := memtable.Put("key1", []byte("value1"))
	if err == nil {
		t.Fatalf("expected error on Put to immutable table")
	}

	err = memtable.Delete("key1")
	if err == nil {
		t.Fatalf("expected error on Delete to immutable table")
	}
}

func TestMemTableIsFull(t *testing.T) {
	memtable := NewMemTable(10)

	if memtable.IsFull() {
		t.Fatalf("eexpected memtable to not be full initially")
	}

	err := memtable.Put("key1", []byte("0123456789"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	if !memtable.IsFull() {
		t.Fatalf("expected memtable to be full after exceeding max size")
	}
}

func TestMemTableReset(t *testing.T) {
	memtable := NewMemTable(1024)

	err := memtable.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	memtable.MarkImmutable()

	memtable.Reset()

	_, ok := memtable.Get("key1")
	if ok {
		t.Fatalf("expected key not to exist after reset")
	}

	err = memtable.Put("key2", []byte("value2"))
	if err != nil {
		t.Fatalf("unexpected error on Put after reset: %v ", err)
	}
}
