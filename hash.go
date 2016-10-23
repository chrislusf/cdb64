package cdb64

import (
	"encoding/binary"
)

const start = 5381

type cdbHash struct {
	uint64
}

func newCDBHash() *cdbHash {
	return &cdbHash{start}
}

func (h *cdbHash) Write(data []byte) (int, error) {
	v := h.uint64
	for _, b := range data {
		v = ((v << 5) + v) ^ uint64(b)
	}

	h.uint64 = v
	return len(data), nil
}

func (h *cdbHash) Sum(b []byte) []byte {
	digest := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, h.Sum64())

	return append(b, digest...)
}

func (h *cdbHash) Sum64() uint64 {
	return h.uint64
}

func (h *cdbHash) Reset() {
	h.uint64 = start
}

func (h *cdbHash) Size() int {
	return 8
}

func (h *cdbHash) BlockSize() int {
	return 8
}
