package storage

import "errors"

type MemTable struct {
	data        map[string][]byte
	currentSize int
	maxSize     int
	immutable   bool
}

func NewMemTable(maxSize int) *MemTable {
	return &MemTable{
		data:        make(map[string][]byte),
		currentSize: 0,
		maxSize:     maxSize,
		immutable:   false,
	}
}

func (m *MemTable) Put(key string, value []byte) error {
	if m.immutable {
		return errors.New("memtable is immutable")
	}

	if oldValue, exists := m.data[key]; exists {
		m.currentSize -= len(oldValue)
	}

	m.data[key] = value

	m.currentSize += len(value)

	return nil
}

func (m *MemTable) Get(key string) ([]byte, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *MemTable) Delete(key string) error {
	if m.immutable {
		return errors.New("memtable is immutable")
	}

	if oldValue, exists := m.data[key]; exists {
		m.currentSize -= len(oldValue)
	}

	// record a tombstone for this key
	m.data[key] = nil

	return nil
}

func (m *MemTable) IsFull() bool {
	return m.currentSize >= m.maxSize
}

func (m *MemTable) MarkImmutable() {
	m.immutable = true
}

func (m *MemTable) Reset() {
	m.data = make(map[string][]byte)
	m.currentSize = 0
	m.immutable = false
}

func (m *MemTable) Iterator() {
	// return keys/values in sorted order
}
