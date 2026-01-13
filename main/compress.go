package main

import (
	"fmt"

	"github.com/klauspost/compress/snappy"
)

func decompressData(data []byte, codec int32, _ int) ([]byte, error) {
	switch codec {
	case 0: // UNCOMPRESSED
		return data, nil
	case 1: // SNAPPY
		return snappy.Decode(nil, data)
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

