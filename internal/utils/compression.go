package utils

import "github.com/golang/snappy"

func Compress(payload []byte) []byte {
	// Reusing a nil destination keeps the wrapper allocation-light for short-lived cache blobs.
	return snappy.Encode(nil, payload)
}

func Decompress(payload []byte) ([]byte, error) {
	// The nil destination mirrors Compress and lets snappy size the output buffer itself.
	return snappy.Decode(nil, payload)
}
