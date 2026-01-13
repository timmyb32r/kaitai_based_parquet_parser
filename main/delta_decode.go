package main

import (
	"fmt"
	"io"
)

func decodeDeltaBinaryPacked(data []byte, _ int32, numValues int) ([]interface{}, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("insufficient data for delta binary packed")
	}

	reader := &simpleVarintReader{data: data, offset: 0}

	// blockSize
	if _, err := reader.readVarintUnsigned(); err != nil {
		return nil, err
	}
	// numMiniBlocks
	if _, err := reader.readVarintUnsigned(); err != nil {
		return nil, err
	}
	// totalValueCount
	if _, err := reader.readVarintUnsigned(); err != nil {
		return nil, err
	}

	firstValue, err := reader.readVarint()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, 0, numValues)
	values = append(values, firstValue)

	currentValue := firstValue
	for len(values) < numValues && reader.offset < len(data)-1 {
		delta, err := reader.readVarint()
		if err != nil {
			break
		}
		currentValue += delta
		values = append(values, currentValue)
	}

	return values, nil
}

type simpleVarintReader struct {
	data   []byte
	offset int
}

func (r *simpleVarintReader) readVarint() (int64, error) {
	var result uint64
	var shift uint
	for {
		if r.offset >= len(r.data) {
			return 0, io.EOF
		}
		b := r.data[r.offset]
		r.offset++
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	decoded := int64(result >> 1)
	if (result & 1) != 0 {
		decoded = -decoded
	}
	return decoded, nil
}

func (r *simpleVarintReader) readVarintUnsigned() (uint64, error) {
	var result uint64
	var shift uint
	for {
		if r.offset >= len(r.data) {
			return 0, io.EOF
		}
		b := r.data[r.offset]
		r.offset++
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	return result, nil
}

