package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var ErrWALCorrupt = errors.New("wal: corrupt or unknown record")

// ReplayWAL replays WAL record from disk and applies them to the given MemTable
// It is used during startup to rebuild in-memory state after a crash.
//
// ReplayWAL reads records sequentially and stops cleanly at EOF or on a partial
// trailing record. If a corrupted record is detecdted, an error is returned.
func ReplayWAL(path string, mem *MemTable) error {
	// Open WAL file for reading
	f, err := os.Open(path)
	if err != nil {
		// If WAL does not exist, nothing to replay
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer f.Close()

	// Replay loop
	for {

		var opBuf [1]byte                                // declare opBuf as a fixed-size array containing one byte
		_, err := io.ReadFull(f, opBuf[:])               // read one byte from file into opBuf this byte should contain the operator type
		if err == io.EOF || err == io.ErrUnexpectedEOF { // if we reach end of file or encounter unexpected end of file return nil
			return nil
		}
		if err != nil { // handle real error
			return err
		}

		op := OpType(opBuf[0]) // convert the first byte of opBuf to OpType

		var lenBuf [4]byte                               // declare lenBuf as a fixed-size array of 4 bytes
		_, err = io.ReadFull(f, lenBuf[:])               // read 4 bytes from file into lenBuf this should be the bytes that contain the length of the key
		if err == io.EOF || err == io.ErrUnexpectedEOF { // if end of file or unexpected end of file return nil
			return nil
		}
		if err != nil { // handle real error
			return err
		}

		keyLen := binary.LittleEndian.Uint32(lenBuf[:]) // store the key len as a 32 bit unsigned integer in little endian format

		_, err = io.ReadFull(f, lenBuf[:])               // read the next 4 bytes into lenbuf these bytes should contain value length
		if err == io.EOF || err == io.ErrUnexpectedEOF { // if end of file or unexpected end of file return nil
			return nil
		}
		if err != nil { // if real error handle it
			return err
		}

		valueLen := binary.LittleEndian.Uint32(lenBuf[:]) // store the value length as a 32 bit unsigned integer in little endian format

		var keyBuf = make([]byte, int(keyLen)) // allocate byte slice to hold key bytes

		_, err = io.ReadFull(f, keyBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}

		key := string(keyBuf) // convert key bytes to string

		var valueBuf = make([]byte, int(valueLen)) // allocated byte slice to hold value bytes
		var value []byte                           // declare value as a byte slice

		if op == OpPut {
			_, err = io.ReadFull(f, valueBuf)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			if err != nil {
				return err
			}
			value = valueBuf // assign value bytes to value variable
		} else if op == OpDelete {
			value = nil // for delete operations set value to nil
		}

		// Apply operation to MemTable
		switch op {
		case OpPut:
			if err := mem.Put(key, value); err != nil {
				return err
			}

		case OpDelete:
			if err := mem.Delete(key); err != nil {
				return err
			}

		default:
			return ErrWALCorrupt // unknown operation type indicates WAL corruption
		}

	}
}
