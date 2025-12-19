package storage

import (
	"encoding/binary"
	"os"
	"sync"
)

type OpType byte

// WAL record format:
//
// All WAL records share a uniform header to simplify replay and crash recovery.
// Each record is encoded as:
//
//   [1 byte ] OpType
//   [4 bytes] KeyLen   (uint32, little-endian)
//   [4 bytes] ValueLen (uint32, little-endian)
//
// The header is followed by:
//   - KeyLen bytes of key data
//   - ValueLen bytes of value data (zero for deletes)
//
// Using a consistent header allows replay logic to decode all record types
// using the same code path, even though total record sizes vary.

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type WAL struct {
	mu   sync.Mutex // protects concurrent appends
	file *os.File   // underlying log file
	path string     // file path (useful for rotation)
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		path: path,
	}, nil
}

func (w *WAL) AppendPut(key string, value []byte) error {
	w.mu.Lock()         // lock the WAL to prevent concurrent appends
	defer w.mu.Unlock() // ensure the WAL is unlocked when the function returns

	// encode record
	keyBytes := []byte(key)         // convert key from string to byte slice
	keyLen := uint32(len(keyBytes)) // store key length as a 32-bit unsigned integer
	valueLen := uint32(len(value))  // store value length as a 32-bit unsigned integer

	// total size op (1) + keyLen (4) + valueLen (4) + key + value
	totalLen := 1 + 4 + 4 + len(keyBytes) + len(value) // determine total WAL record size

	// allocate buffer
	buf := make([]byte, totalLen) // allocate byte slice to hold encoded WAL record

	pos := 0 // current write position in buffer

	// op type
	buf[pos] = byte(OpPut) // set the operation type to "put" in the buffer
	pos++                  // increment position

	// key length
	binary.LittleEndian.PutUint32(buf[pos:pos+4], keyLen) // write key length in little-endian format in the buffer
	pos += 4                                              // increment position

	// value length
	binary.LittleEndian.PutUint32(buf[pos:pos+4], valueLen) // write the value length in little-endian format in the buffer
	pos += 4                                                // increment position

	// key bytes
	copy(buf[pos:pos+len(keyBytes)], keyBytes) // copy key bytes into the buffer
	pos += len(keyBytes)                       // increment position

	// value bytes
	copy(buf[pos:pos+len(value)], value) // copy value bytes into the buffer
	pos += len(value)                    // increment position

	// Write to WAL file
	if _, err := w.file.Write(buf); err != nil { // write the encoded buffer to the WAL file and check for errors
		return err
	}

	// Ensure durability (simple, strong version)
	// use file.Sync to flush OS buffers to disk
	// often times with file.Write, data may be in OS buffers and not yet on disk
	if err := w.file.Sync(); err != nil { // file.Sync to ensure data is flushed to disk
		return err
	}

	return nil
}

func (w *WAL) AppendDelete(key string) error {
	w.mu.Lock()         // lock the WAL to prevent concurrent appends
	defer w.mu.Unlock() // ensure the WAL is unlocked when the function returns

	keyBytes := []byte(key)         // convert key from string to byte slice
	keyLen := uint32(len(keyBytes)) // store key length as a 32-bit unsigned integer
	valueLen := uint32(0)           // deletes have no value

	// total size op (1) + keyLen (4) + valueLen (4) + key
	totalLen := 1 + 4 + 4 + len(keyBytes) // determine total WAL record size

	// allocate buffer
	buf := make([]byte, totalLen) // allocate byte slice to hold encoded WAL record

	// initalize position
	pos := 0 // current write position in buffer

	// op type
	buf[pos] = byte(OpDelete) // set the operation type to "delete" in the buffer
	pos++                     // increment position

	// key length
	binary.LittleEndian.PutUint32(buf[pos:pos+4], keyLen) // write key length in little-endian format in the buffer
	pos += 4

	// value length (0)
	binary.LittleEndian.PutUint32(buf[pos:pos+4], valueLen)
	pos += 4 // increment position

	// key bytes
	copy(buf[pos:pos+len(keyBytes)], keyBytes) // copy key bytes into the buffer
	pos += len(keyBytes)                       // increment position

	// write to WAL file
	if _, err := w.file.Write(buf); err != nil { // write the encoded buffer to the WAL file and check for errors
		return err
	}

	// ensure durability
	// use file.Sync to flush OS buffers to disk
	// often times with file.Write, data may be in OS buffers and not yet on disk
	if err := w.file.Sync(); err != nil { // file.Sync to ensure data is flushed to disk
		return err
	}

	return nil
}
