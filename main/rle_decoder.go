package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// decodeRLELevels decodes RLE/bit-packed levels from a Parquet page.
// Format: [4 bytes length (LE)] [encoded bytes]
// Returns: decoded levels and remaining bytes after the level section.
func decodeRLELevels(data []byte, numValues int, bitWidth uint) ([]byte, []byte, error) {
	if len(data) < 4 {
		return nil, data, io.ErrUnexpectedEOF
	}
	
	// Read encoded section length (4 bytes, little-endian).
	encodedLength := int(binary.LittleEndian.Uint32(data[0:4]))
	if encodedLength < 0 || 4+encodedLength > len(data) {
		return nil, data, fmt.Errorf("invalid encoded length: %d", encodedLength)
	}
	
	// Decode levels.
	encodedData := data[4 : 4+encodedLength]
	levels, err := decodeRLEBytes(encodedData, numValues, bitWidth)
	if err != nil {
		return nil, data, err
	}
	
	// Return decoded levels and the remaining bytes.
	return levels, data[4+encodedLength:], nil
}

// decodeRLEBytesWithOffset decodes RLE/bit-packed values and returns how many bytes were consumed.
func decodeRLEBytesWithOffset(src []byte, numValues int, bitWidth uint) ([]byte, int, error) {
	if bitWidth > 8 {
		return nil, 0, fmt.Errorf("bit width %d exceeds maximum of 8", bitWidth)
	}
	
	dst := make([]byte, 0, numValues)
	bytesRead := 0
	
	for i := 0; i < len(src) && len(dst) < numValues; {
		// Read block header varint.
		u, n := binary.Uvarint(src[i:])
		if n == 0 {
			return dst, bytesRead, fmt.Errorf("decoding run-length block header: %w", io.ErrUnexpectedEOF)
		}
		if n < 0 {
			return dst, bytesRead, fmt.Errorf("overflow after decoding %d/%d bytes of run-length block header", -n+i, len(src))
		}
		i += n
		bytesRead += n
		
		// count = number of values (or groups), bitpacked = bit-packed mode flag.
		count := uint(u >> 1)
		bitpacked := (u & 1) != 0
		
		if count > 16*1024*1024 { // maxSupportedValueCount
			return dst, bytesRead, fmt.Errorf("decoded run-length block cannot have more than %d values", 16*1024*1024)
		}
		
		if bitpacked {
			// Bit-packed mode: count*8 values, each of width bitWidth bits.
			count *= 8
			byteCount := (count*bitWidth + 7) / 8 // round up
			j := i + int(byteCount)
			
			if j > len(src) {
				return dst, bytesRead, fmt.Errorf("decoding bit-packed block of %d values: %w", count, io.ErrUnexpectedEOF)
			}
			
			// Decode bit-packed values.
			decoded := decodeBitPackedBytes(src[i:j], count, bitWidth)
			dst = append(dst, decoded...)
			bytesRead += int(byteCount)
			i = j
		} else {
			// RLE mode: repeat a single value count times.
			if bitWidth != 0 && (i+1) > len(src) {
				return dst, bytesRead, fmt.Errorf("decoding run-length block of %d values: %w", count, io.ErrUnexpectedEOF)
			}
			
			word := byte(0)
			if bitWidth != 0 {
				word = src[i]
				i++
				bytesRead++
			}
			
			// Append the same value count times.
			for k := uint(0); k < count && len(dst) < numValues; k++ {
				dst = append(dst, word)
			}
		}
		
		// Stop once we have enough values.
		if len(dst) >= numValues {
			break
		}
	}
	
	// Truncate to exactly numValues.
	if len(dst) > numValues {
		dst = dst[:numValues]
	}
	
	return dst, bytesRead, nil
}

// decodeRLEBytes decodes RLE/bit-packed values.
// Implementation is based on parquet-go's RLE decoder.
func decodeRLEBytes(src []byte, numValues int, bitWidth uint) ([]byte, error) {
	if bitWidth > 8 {
		return nil, fmt.Errorf("bit width %d exceeds maximum of 8", bitWidth)
	}
	
	dst := make([]byte, 0, numValues)
	
	for i := 0; i < len(src) && len(dst) < numValues; {
		// Read block header varint.
		u, n := binary.Uvarint(src[i:])
		if n == 0 {
			return dst, fmt.Errorf("decoding run-length block header: %w", io.ErrUnexpectedEOF)
		}
		if n < 0 {
			return dst, fmt.Errorf("overflow after decoding %d/%d bytes of run-length block header", -n+i, len(src))
		}
		i += n
		
		// count = number of values (or groups), bitpacked = bit-packed mode flag.
		count := uint(u >> 1)
		bitpacked := (u & 1) != 0
		
		if count > 16*1024*1024 { // maxSupportedValueCount
			return dst, fmt.Errorf("decoded run-length block cannot have more than %d values", 16*1024*1024)
		}
		
		if bitpacked {
			// Bit-packed mode: count*8 values, each of width bitWidth bits.
			count *= 8
			byteCount := (count*bitWidth + 7) / 8 // round up
			j := i + int(byteCount)
			
			if j > len(src) {
				return dst, fmt.Errorf("decoding bit-packed block of %d values: %w", count, io.ErrUnexpectedEOF)
			}
			
			// Decode bit-packed values.
			decoded := decodeBitPackedBytes(src[i:j], count, bitWidth)
			dst = append(dst, decoded...)
			i = j
		} else {
			// RLE mode: repeat a single value count times.
			if bitWidth != 0 && (i+1) > len(src) {
				return dst, fmt.Errorf("decoding run-length block of %d values: %w", count, io.ErrUnexpectedEOF)
			}
			
			word := byte(0)
			if bitWidth != 0 {
				word = src[i]
				i++
			}
			
			// Append the same value count times.
			for k := uint(0); k < count && len(dst) < numValues; k++ {
				dst = append(dst, word)
			}
		}
	}
	
	// Truncate to exactly numValues.
	if len(dst) > numValues {
		dst = dst[:numValues]
	}
	
	return dst, nil
}

// decodeBitPackedBytes decodes bit-packed values.
// This is a simplified implementation for bitWidth <= 8.
func decodeBitPackedBytes(src []byte, count uint, bitWidth uint) []byte {
	if bitWidth == 0 {
		return make([]byte, count)
	}
	
	dst := make([]byte, 0, count)
	
	for i := uint(0); i < count; i++ {
		byteIndex := (i * bitWidth) / 8
		bitIndex := (i * bitWidth) % 8
		
		if byteIndex >= uint(len(src)) {
			break
		}
		
		// Extract the next value from the bitstream.
		value := byte(0)
		bitsRemaining := bitWidth
		bitsToRead := bitsRemaining
		
		if 8-bitIndex < bitsToRead {
			bitsToRead = 8 - bitIndex
		}
		
		// Read first chunk.
		mask := byte((1 << bitsToRead) - 1)
		value = (src[byteIndex] >> bitIndex) & mask
		bitsRemaining -= bitsToRead
		
		// Read the next chunk if needed.
		if bitsRemaining > 0 && byteIndex+1 < uint(len(src)) {
			value |= (src[byteIndex+1] & byte((1<<bitsRemaining)-1)) << bitsToRead
		}
		
		dst = append(dst, value)
	}
	
	return dst
}
