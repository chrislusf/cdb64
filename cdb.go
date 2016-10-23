/*
Package cdb64 provides a native implementation of cdb, a fast constant key/value
database, but without the 4GB size limitation.

For more information on cdb, see the original design doc at http://cr.yp.to/cdb.html.

This is based on the code from https://github.com/colinmarc/cdb

*/
package cdb64

import (
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"os"
)

const (
	headerSize = 256 * 8 * 2
)

type Header [256]table

// CDB represents an open CDB database. It can only be used for reads; to
// create a database, use Writer.
type CDB struct {
	reader io.ReaderAt
	hasher hash.Hash64
	header Header
}

type table struct {
	offset uint64
	length uint64
}

// Open opens an existing CDB database at the given path.
func Open(path string) (*CDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return New(f, nil)
}

// New opens a new CDB instance for the given io.ReaderAt. It can only be used
// for reads; to create a database, use Writer.
//
// If hasher is nil, it will default to the CDB hash function. If a database
// was created with a particular hash function, that same hash function must be
// passed to New, or the database will return incorrect results.
func New(reader io.ReaderAt, hasher hash.Hash64) (*CDB, error) {
	if hasher == nil {
		hasher = newCDBHash()
	}

	cdb := &CDB{reader: reader, hasher: hasher}
	err := cdb.readHeader()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDB) Get(key []byte) ([]byte, error) {
	cdb.hasher.Reset()
	cdb.hasher.Write(key)
	hash := cdb.hasher.Sum64()

	table := cdb.header[hash&0xff]
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (hash >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (16 * slot)
		slotHash, offset, err := readTuple(cdb.reader, slotOffset)
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == hash {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
				return value, nil
			}
		}

		slot = (slot + 1) % table.length
		if slot == startingSlot {
			break
		}
	}

	return nil, nil
}

// Close closes the database to further reads.
func (cdb *CDB) Close() error {
	if closer, ok := cdb.reader.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

func (cdb *CDB) readHeader() error {
	buf := make([]byte, headerSize)
	_, err := cdb.reader.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	for i := 0; i < 256; i++ {
		off := i * 16
		cdb.header[i] = table{
			offset: binary.LittleEndian.Uint64(buf[off : off+8]),
			length: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
		}
	}

	return nil
}

func (cdb *CDB) getValueAt(offset uint64, expectedKey []byte) ([]byte, error) {
	keyLength, valueLength, err := readTuple(cdb.reader, offset)
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = cdb.reader.ReadAt(buf, int64(offset+16))
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}
