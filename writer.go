package cdb64

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var ErrTooMuchData = errors.New("CDB files are limited to 4GB of data")

// Writer provides an API for creating a CDB database record by record.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type Writer struct {
	hasher       HashFunc
	writer       io.WriteSeeker
	entries      [256][]entry
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

type entry struct {
	hash   uint64
	offset uint64
}

// Create opens a CDB database at the given path. If the file exists, it will
// be overwritten.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return NewWriter(f, nil)
}

// NewWriter opens a CDB database for the given io.WriteSeeker.
//
// If hasher is nil, it will default to the CDB hash function.
func NewWriter(writer io.WriteSeeker, hasher HashFunc) (*Writer, error) {
	// Leave 256 * 8 * 2 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, headerSize))
	if err != nil {
		return nil, err
	}

	if hasher == nil {
		hasher = newCDBHash
	}

	return &Writer{
		hasher:         hasher,
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: headerSize,
	}, nil
}

// Put adds a key/value pair to the database.
func (cdb *Writer) Put(key, value []byte) error {
	if key == nil || value == nil {
		return fmt.Errorf("key or value can not be nil.")
	}
	entrySize := int64(16 + len(key) + len(value))

	// Record the entry in the hash table, to be written out at the end.
	hasher := cdb.hasher()
	hasher.Reset()
	hasher.Write(key)
	hash := hasher.Sum64()
	table := hash & 0xff

	entry := entry{hash: hash, offset: uint64(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTuple(cdb.bufferedWriter, uint64(len(key)), uint64(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += entrySize
	cdb.estimatedFooterSize += 32
	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// Freeze finalizes the database, then opens it for reads. If the stream cannot
// be converted to a io.ReaderAt, Freeze will return os.ErrInvalid.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Freeze() (*CDB, error) {
	var err error
	var header Header
	cdb.finalizeOnce.Do(func() {
		header, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	if readerAt, ok := cdb.writer.(io.ReaderAt); ok {
		return &CDB{reader: readerAt, header: header, hasher: cdb.hasher}, nil
	} else {
		return nil, os.ErrInvalid
	}
}

func (cdb *Writer) finalize() (Header, error) {
	var index Header

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint64(len(tableEntries) << 1)

		index[i] = table{
			offset: uint64(cdb.bufferedOffset),
			length: tableSize,
		}

		sorted := make([]entry, tableSize)
		for _, entry := range tableEntries {
			slot := (entry.hash >> 8) % tableSize

			for {
				if sorted[slot].hash == 0 {
					sorted[slot] = entry
					break
				}

				slot = (slot + 1) % tableSize
			}
		}

		for _, entry := range sorted {
			err := writeTuple(cdb.bufferedWriter, entry.hash, entry.offset)
			if err != nil {
				return index, err
			}

			cdb.bufferedOffset += 16
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	buf := make([]byte, headerSize)
	for i, table := range index {
		off := i * 16
		binary.LittleEndian.PutUint64(buf[off:off+8], table.offset)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], table.length)
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	return index, nil
}
