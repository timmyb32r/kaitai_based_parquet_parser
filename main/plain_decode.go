package main

import (
	"encoding/binary"
	"fmt"
	"math"
)

func decodePlainValues(data []byte, dataType int32, numValues int) ([]interface{}, error) {
	values := make([]interface{}, 0)
	offset := 0

	for i := 0; i < numValues && offset < len(data); i++ {
		switch dataType {
		case 1: // INT32
			if offset+4 <= len(data) {
				val := int32(binary.LittleEndian.Uint32(data[offset:]))
				values = append(values, val)
				offset += 4
			}
		case 2: // INT64
			if offset+8 <= len(data) {
				val := int64(binary.LittleEndian.Uint64(data[offset:]))
				values = append(values, val)
				offset += 8
			}
		case 4: // FLOAT
			if offset+4 <= len(data) {
				val := binary.LittleEndian.Uint32(data[offset:])
				values = append(values, float32(math.Float32frombits(val)))
				offset += 4
			}
		case 5: // DOUBLE
			if offset+8 <= len(data) {
				val := binary.LittleEndian.Uint64(data[offset:])
				values = append(values, math.Float64frombits(val))
				offset += 8
			}
		case 6: // BYTE_ARRAY
			if offset+4 <= len(data) {
				length := int(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4
				if offset+length <= len(data) {
					values = append(values, string(data[offset:offset+length]))
					offset += length
				}
			}
		default:
			return values, fmt.Errorf("unsupported data type: %d", dataType)
		}
	}

	return values, nil
}

