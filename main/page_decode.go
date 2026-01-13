package main

import (
	"encoding/binary"
)

func parseDataPageWithEncoding(data []byte, dataType int32, encoding int64, numValues int64, repetitionType *int32, maxDefinitionLevel byte) ([]interface{}, error) {
	pageData := data

	hasRepetition := repetitionType != nil && *repetitionType == 2
	if hasRepetition {
		_, remaining, err := decodeRLELevels(pageData, int(numValues), 1)
		if err == nil {
			pageData = remaining
		}
	}

	if repetitionType != nil && *repetitionType == 0 {
		defLevels, remaining, err := decodeRLELevels(pageData, int(numValues), 1)
		if err == nil && len(defLevels) > 0 {
			pageData = remaining
		}
	} else if maxDefinitionLevel > 0 {
		if len(pageData) >= 4 {
			encodedLength := int(binary.LittleEndian.Uint32(pageData[0:4]))
			if encodedLength > 0 && encodedLength < len(pageData) && encodedLength < 10000 {
				defLevels, remaining, err := decodeRLELevels(pageData, int(numValues), uint(maxDefinitionLevel))
				if err == nil && len(defLevels) > 0 {
					valid := true
					for _, level := range defLevels {
						if level > maxDefinitionLevel {
							valid = false
							break
						}
					}
					if valid {
						pageData = remaining
					}
				}
			} else {
				defLevels, bytesRead, err := decodeRLEBytesWithOffset(pageData, int(numValues), uint(maxDefinitionLevel))
				if err == nil && len(defLevels) > 0 && len(defLevels) <= int(numValues)+10 {
					valid := true
					for _, level := range defLevels {
						if level > maxDefinitionLevel {
							valid = false
							break
						}
					}
					if valid && bytesRead > 0 && bytesRead < len(pageData) {
						pageData = pageData[bytesRead:]
					}
				}
			}
		}
	}

	switch encoding {
	case 0: // PLAIN
		return decodePlainValues(pageData, dataType, int(numValues))
	case 5: // DELTA_BINARY_PACKED
		return decodeDeltaBinaryPacked(pageData, dataType, int(numValues))
	default:
		return decodePlainValues(pageData, dataType, int(numValues))
	}
}

