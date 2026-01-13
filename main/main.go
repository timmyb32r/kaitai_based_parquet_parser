package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/kaitai-io/kaitai_struct_go_runtime/kaitai"
	"kaitai_parquet/kaitai_gen"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <parquet-file>\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := runParser(ctx, filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runParser(ctx context.Context, filePath string) error {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Check context
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use Kaitai Struct parser to read file structure
	// *os.File implements io.ReadSeeker which is required by kaitai.NewStream
	stream := kaitai.NewStream(file)
	parquet := kaitai_gen.NewParquet()
	err = parquet.Read(stream, nil, parquet)
	if err != nil {
		return fmt.Errorf("error parsing parquet file: %v", err)
	}

	// Verify magic numbers
	if parquet.Magic != "PAR1" {
		return fmt.Errorf("invalid magic at start: %s", parquet.Magic)
	}

	// Read footer via Kaitai-generated Parquet parser (as Thrift Compact AST).
	footerStruct, err := parquet.FooterThrift()
	if err != nil {
		return fmt.Errorf("error reading footer: %v", err)
	}

	// Verify end magic
	magicEnd, err := parquet.MagicEnd()
	if err != nil {
		return fmt.Errorf("error reading end magic: %v", err)
	}
	if magicEnd != "PAR1" {
		return fmt.Errorf("invalid magic at end: %s", magicEnd)
	}

	// Decode FileMetaData from the Thrift Compact AST.
	_ = ctx // reserved for future cancellation checks in decoding
	metadata, err := decodeFileMetaData(footerStruct)
	if err != nil {
		return fmt.Errorf("error extracting metadata: %v", err)
	}

	// Extract column names from schema (skip root element)
	columnNames := make([]string, 0)
	for i, elem := range metadata.Schema {
		if i == 0 {
			continue // Skip root schema element
		}
		columnNames = append(columnNames, elem.Name)
	}

	// Print schema information
	fmt.Println("=== Schema ===")
	for i, name := range columnNames {
		elem := metadata.Schema[i+1]
		repType := int32(0)
		if elem.RepetitionType != nil {
			repType = *elem.RepetitionType
		}
		typeName := getTypeName(elem.Type)
		fmt.Printf("%d. %s (type: %d (%s), repetition: %d)\n", i+1, name, elem.Type, typeName, repType)
	}
	fmt.Println()

	// Print column names
	fmt.Println("=== Columns ===")
	for i, name := range columnNames {
		fmt.Printf("%d. %s\n", i+1, name)
	}
	fmt.Println()

	// Read and print data
	fmt.Println("=== Data ===")

	// Create tab writer
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print header
	for _, name := range columnNames {
		fmt.Fprintf(w, "%s\t", name)
	}
	fmt.Fprintf(w, "\n")

	// Print separator
	for range columnNames {
		fmt.Fprintf(w, "---\t")
	}
	fmt.Fprintf(w, "\n")

	// Read data from row groups
	maxRows := 1000
	rowsPrinted := 0

	for _, rowGroup := range metadata.RowGroups {
		if rowsPrinted >= maxRows {
			break
		}

		// Collect values from all columns
		allColumnValues := make([][]interface{}, len(columnNames))

		// Read data from each column chunk manually
		for colIdx, colChunk := range rowGroup.Columns {
			if colIdx >= len(columnNames) {
				break
			}

			// Check if column has metadata
			if colChunk.MetaData == nil {
				allColumnValues[colIdx] = make([]interface{}, 0)
				continue
			}

			// Read column data
			var schemaElem SchemaElement
			if colIdx+1 < len(metadata.Schema) {
				schemaElem = metadata.Schema[colIdx+1]
			}

			values, err := readColumnValues(file, colChunk, schemaElem)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading column %s: %v\n", columnNames[colIdx], err)
				allColumnValues[colIdx] = make([]interface{}, 0)
				continue
			}

			allColumnValues[colIdx] = values
		}

		// Determine number of rows to print
		maxRowsInData := 0
		for _, colValues := range allColumnValues {
			if len(colValues) > maxRowsInData {
				maxRowsInData = len(colValues)
			}
		}

		if maxRowsInData > maxRows-rowsPrinted {
			maxRowsInData = maxRows - rowsPrinted
		}

		// Print rows
		for rowIdx := 0; rowIdx < maxRowsInData; rowIdx++ {
			for colIdx := 0; colIdx < len(columnNames); colIdx++ {
				if colIdx < len(allColumnValues) && rowIdx < len(allColumnValues[colIdx]) {
					value := allColumnValues[colIdx][rowIdx]
					if value == nil {
						fmt.Fprintf(w, "NULL\t")
					} else {
						fmt.Fprintf(w, "%v\t", value)
					}
				} else {
					fmt.Fprintf(w, "NULL\t")
				}
			}
			fmt.Fprintf(w, "\n")
			rowsPrinted++
		}
	}

	w.Flush()

	fmt.Printf("\nTotal rows printed: %d\n", rowsPrinted)
	fmt.Printf("Total rows in file: %d\n", metadata.NumRows)
	fmt.Printf("Row groups: %d\n", len(metadata.RowGroups))

	return nil
}

// getTypeName returns a human-readable name for a Parquet type
func getTypeName(typeID int32) string {
	switch typeID {
	case 0:
		return "BOOLEAN"
	case 1:
		return "INT32"
	case 2:
		return "INT64"
	case 3:
		return "INT96"
	case 4:
		return "FLOAT"
	case 5:
		return "DOUBLE"
	case 6:
		return "BYTE_ARRAY"
	case 7:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		return "UNKNOWN"
	}
}
