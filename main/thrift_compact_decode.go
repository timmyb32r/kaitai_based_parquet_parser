package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kaitai-io/kaitai_struct_go_runtime/kaitai"
	"kaitai_parquet/kaitai_gen"
)

type thriftField struct {
	ID   int16
	Type uint8
	Val  *kaitai_gen.ThriftCompact_CompactValue
}

func parseCompactStructFromBytes(b []byte) (*kaitai_gen.ThriftCompact_CompactStruct, int, error) {
	br := bytes.NewReader(b)
	ks := kaitai.NewStream(br)

	// Parquet stores raw Thrift structs, not message envelopes.
	root := kaitai_gen.NewThriftCompact()
	st := kaitai_gen.NewThriftCompact_CompactStruct()
	if err := st.Read(ks, nil, root); err != nil {
		return nil, 0, err
	}

	pos, _ := br.Seek(0, io.SeekCurrent)
	return st, int(pos), nil
}

func parseCompactStructFromBufio(r *bufio.Reader, maxPeek int) (*kaitai_gen.ThriftCompact_CompactStruct, int, error) {
	if maxPeek <= 0 {
		maxPeek = 64 * 1024
	}

	peekN := 256
	for {
		if peekN > maxPeek {
			peekN = maxPeek
		}

		b, err := r.Peek(peekN)
		// We can still attempt to parse on io.EOF (Peek returns available bytes).
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, err
		}
		if len(b) == 0 {
			return nil, 0, io.EOF
		}

		st, consumed, perr := parseCompactStructFromBytes(b)
		if perr == nil {
			if consumed <= 0 {
				return nil, 0, fmt.Errorf("thrift compact parse: consumed=0")
			}
			if _, derr := r.Discard(consumed); derr != nil {
				return nil, 0, derr
			}
			return st, consumed, nil
		}

		// If parsing failed due to insufficient bytes, increase peek and retry.
		if errors.Is(perr, io.EOF) || strings.Contains(perr.Error(), "EOF") {
			if peekN >= maxPeek {
				return nil, 0, perr
			}
			peekN *= 2
			continue
		}
		return nil, 0, perr
	}
}

func thriftFields(st *kaitai_gen.ThriftCompact_CompactStruct) ([]thriftField, error) {
	out := make([]thriftField, 0, len(st.Fields))
	var prevID int16

	for _, f := range st.Fields {
		isStop, err := f.IsStop()
		if err != nil {
			return nil, err
		}
		if isStop {
			break
		}

		ft, err := f.FieldType()
		if err != nil {
			return nil, err
		}

		hasExt, err := f.HasExtendedDelta()
		if err != nil {
			return nil, err
		}

		var id int16
		if hasExt {
			if f.ExtendedDelta == nil {
				return nil, fmt.Errorf("thrift compact: extended delta missing")
			}
			v, err := f.ExtendedDelta.Value()
			if err != nil {
				return nil, err
			}
			id = int16(v)
		} else {
			delta, err := f.FieldDeltaShort()
			if err != nil {
				return nil, err
			}
			id = prevID + int16(delta)
		}

		prevID = id
		out = append(out, thriftField{
			ID:   id,
			Type: uint8(ft),
			Val:  f.Value,
		})
	}

	return out, nil
}

func thriftI32(v *kaitai_gen.ThriftCompact_CompactValue) (int32, bool, error) {
	if v == nil || v.I32Value == nil {
		return 0, false, nil
	}
	x, err := v.I32Value.Value()
	return int32(x), true, err
}

func thriftI64(v *kaitai_gen.ThriftCompact_CompactValue) (int64, bool, error) {
	if v == nil || v.I64Value == nil {
		return 0, false, nil
	}
	x, err := v.I64Value.Value()
	return int64(x), true, err
}

func thriftString(v *kaitai_gen.ThriftCompact_CompactValue) (string, bool) {
	if v == nil || v.BinaryValue == nil {
		return "", false
	}
	return v.BinaryValue.Value, true
}

func thriftStruct(v *kaitai_gen.ThriftCompact_CompactValue) (*kaitai_gen.ThriftCompact_CompactStruct, bool) {
	if v == nil || v.StructValue == nil {
		return nil, false
	}
	return v.StructValue, true
}

func thriftList(v *kaitai_gen.ThriftCompact_CompactValue) (*kaitai_gen.ThriftCompact_CompactList, bool) {
	if v == nil {
		return nil, false
	}
	if v.ListValue != nil {
		return v.ListValue, true
	}
	if v.SetValue != nil {
		return v.SetValue, true
	}
	return nil, false
}

// decodeFileMetaData converts the Thrift Compact AST for Parquet FileMetaData into FileMetadata.
func decodeFileMetaData(st *kaitai_gen.ThriftCompact_CompactStruct) (*FileMetadata, error) {
	fields, err := thriftFields(st)
	if err != nil {
		return nil, err
	}

	meta := &FileMetadata{}

	for _, f := range fields {
		switch f.ID {
		case 1: // version: i32
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				meta.Version = v
			} else if err != nil {
				return nil, err
			}
		case 2: // schema: list<SchemaElement>
			lst, ok := thriftList(f.Val)
			if !ok || lst == nil {
				continue
			}
			for _, elem := range lst.Elements {
				sst, ok := thriftStruct(elem)
				if !ok {
					continue
				}
				se, err := decodeSchemaElement(sst)
				if err != nil {
					return nil, err
				}
				meta.Schema = append(meta.Schema, se)
			}
		case 3: // num_rows: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				meta.NumRows = v
			} else if err != nil {
				return nil, err
			}
		case 4: // row_groups: list<RowGroup>
			lst, ok := thriftList(f.Val)
			if !ok || lst == nil {
				continue
			}
			for _, elem := range lst.Elements {
				rst, ok := thriftStruct(elem)
				if !ok {
					continue
				}
				rg, err := decodeRowGroup(rst)
				if err != nil {
					return nil, err
				}
				meta.RowGroups = append(meta.RowGroups, rg)
			}
		default:
			// ignore
		}
	}

	return meta, nil
}

func decodeSchemaElement(st *kaitai_gen.ThriftCompact_CompactStruct) (SchemaElement, error) {
	fields, err := thriftFields(st)
	if err != nil {
		return SchemaElement{}, err
	}

	var out SchemaElement
	for _, f := range fields {
		switch f.ID {
		case 1: // type (enum): i32
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.Type = v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 2: // type_length: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.TypeLength = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 3: // repetition_type (enum): i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.RepetitionType = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 4: // name: string
			if s, ok := thriftString(f.Val); ok {
				out.Name = s
			}
		case 5: // num_children: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.NumChildren = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 6: // converted_type: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.ConvertedType = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 7: // scale: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.Scale = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 8: // precision: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.Precision = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		case 9: // field_id: i32 (optional)
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.FieldID = &v
			} else if err != nil {
				return SchemaElement{}, err
			}
		default:
			// ignore
		}
	}

	return out, nil
}

func decodeRowGroup(st *kaitai_gen.ThriftCompact_CompactStruct) (RowGroup, error) {
	fields, err := thriftFields(st)
	if err != nil {
		return RowGroup{}, err
	}

	var out RowGroup
	for _, f := range fields {
		switch f.ID {
		case 1: // columns: list<ColumnChunk>
			lst, ok := thriftList(f.Val)
			if !ok || lst == nil {
				continue
			}
			for _, elem := range lst.Elements {
				cst, ok := thriftStruct(elem)
				if !ok {
					continue
				}
				cc, err := decodeColumnChunk(cst)
				if err != nil {
					return RowGroup{}, err
				}
				out.Columns = append(out.Columns, cc)
			}
		case 2: // total_byte_size: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.TotalByteSize = v
			} else if err != nil {
				return RowGroup{}, err
			}
		case 3: // num_rows: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.NumRows = v
			} else if err != nil {
				return RowGroup{}, err
			}
		case 5: // file_offset: i64 (optional)
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.FileOffset = v
			} else if err != nil {
				return RowGroup{}, err
			}
		case 6: // total_compressed_size: i64 (optional)
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.TotalCompressedSize = v
			} else if err != nil {
				return RowGroup{}, err
			}
		default:
			// ignore
		}
	}

	return out, nil
}

func decodeColumnChunk(st *kaitai_gen.ThriftCompact_CompactStruct) (ColumnChunk, error) {
	fields, err := thriftFields(st)
	if err != nil {
		return ColumnChunk{}, err
	}

	var out ColumnChunk
	for _, f := range fields {
		switch f.ID {
		case 1: // file_path: string (optional)
			if s, ok := thriftString(f.Val); ok && s != "" {
				out.FilePath = []string{s}
			}
		case 2: // file_offset: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.FileOffset = v
			} else if err != nil {
				return ColumnChunk{}, err
			}
		case 3: // meta_data: ColumnMetaData
			if sst, ok := thriftStruct(f.Val); ok {
				md, err := decodeColumnMetaData(sst)
				if err != nil {
					return ColumnChunk{}, err
				}
				out.MetaData = md
			}
		default:
			// ignore
		}
	}

	return out, nil
}

func decodeColumnMetaData(st *kaitai_gen.ThriftCompact_CompactStruct) (*ColumnMetaData, error) {
	fields, err := thriftFields(st)
	if err != nil {
		return nil, err
	}

	out := &ColumnMetaData{}
	for _, f := range fields {
		switch f.ID {
		case 1: // type (enum): i32
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.Type = v
			} else if err != nil {
				return nil, err
			}
		case 2: // encodings: list<Encoding> (i32)
			lst, ok := thriftList(f.Val)
			if !ok || lst == nil {
				continue
			}
			for _, elem := range lst.Elements {
				if v, ok, err := thriftI32(elem); err == nil && ok {
					out.Encodings = append(out.Encodings, v)
				} else if err != nil {
					return nil, err
				}
			}
		case 3: // path_in_schema: list<string>
			lst, ok := thriftList(f.Val)
			if !ok || lst == nil {
				continue
			}
			for _, elem := range lst.Elements {
				if s, ok := thriftString(elem); ok {
					out.PathInSchema = append(out.PathInSchema, s)
				}
			}
		case 4: // codec (enum): i32
			if v, ok, err := thriftI32(f.Val); err == nil && ok {
				out.Codec = v
			} else if err != nil {
				return nil, err
			}
		case 5: // num_values: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.NumValues = v
			} else if err != nil {
				return nil, err
			}
		case 6: // total_uncompressed_size: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.TotalUncompressedSize = v
			} else if err != nil {
				return nil, err
			}
		case 7: // total_compressed_size: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.TotalCompressedSize = v
			} else if err != nil {
				return nil, err
			}
		case 9: // data_page_offset: i64
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.DataPageOffset = v
			} else if err != nil {
				return nil, err
			}
		case 11: // dictionary_page_offset: i64 (optional)
			if v, ok, err := thriftI64(f.Val); err == nil && ok {
				out.DictionaryPageOffset = &v
			} else if err != nil {
				return nil, err
			}
		default:
			// ignore
		}
	}

	return out, nil
}

func decodePageHeader(st *kaitai_gen.ThriftCompact_CompactStruct) (pageType int32, uncompressedSize int32, compressedSize int32, numValues int64, encoding int32, err error) {
	fields, err := thriftFields(st)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	for _, f := range fields {
		switch f.ID {
		case 1: // type: i32
			v, ok, e := thriftI32(f.Val)
			if e != nil {
				return 0, 0, 0, 0, 0, e
			}
			if ok {
				pageType = v
			}
		case 2: // uncompressed_page_size: i32
			v, ok, e := thriftI32(f.Val)
			if e != nil {
				return 0, 0, 0, 0, 0, e
			}
			if ok {
				uncompressedSize = v
			}
		case 3: // compressed_page_size: i32
			v, ok, e := thriftI32(f.Val)
			if e != nil {
				return 0, 0, 0, 0, 0, e
			}
			if ok {
				compressedSize = v
			}
		case 5: // data_page_header: struct
			dphSt, ok := thriftStruct(f.Val)
			if !ok {
				continue
			}
			dFields, e := thriftFields(dphSt)
			if e != nil {
				return 0, 0, 0, 0, 0, e
			}
			for _, df := range dFields {
				switch df.ID {
				case 1: // num_values: i32
					v, ok, e := thriftI32(df.Val)
					if e != nil {
						return 0, 0, 0, 0, 0, e
					}
					if ok {
						numValues = int64(v)
					}
				case 2: // encoding: i32
					v, ok, e := thriftI32(df.Val)
					if e != nil {
						return 0, 0, 0, 0, 0, e
					}
					if ok {
						encoding = v
					}
				default:
					// ignore
				}
			}
		default:
			// ignore
		}
	}

	return pageType, uncompressedSize, compressedSize, numValues, encoding, nil
}

// readColumnValues reads column values from a Parquet file, parsing Thrift PageHeader using Kaitai-generated Compact parser.
func readColumnValues(file *os.File, chunk ColumnChunk, schema SchemaElement) ([]interface{}, error) {
	if chunk.MetaData == nil {
		return nil, fmt.Errorf("no metadata for column chunk")
	}

	var pageStartOffset int64
	if chunk.MetaData.DictionaryPageOffset != nil && *chunk.MetaData.DictionaryPageOffset != 0 {
		pageStartOffset = *chunk.MetaData.DictionaryPageOffset
	} else {
		pageStartOffset = chunk.MetaData.DataPageOffset
	}

	section := io.NewSectionReader(file, pageStartOffset, chunk.MetaData.TotalCompressedSize)
	rbuf := bufio.NewReaderSize(section, 64*1024)

	values := make([]interface{}, 0)
	maxPages := 100
	pagesRead := 0
	totalValuesRead := 0

	for pagesRead < maxPages && totalValuesRead < int(chunk.MetaData.NumValues) {
		headerStruct, _, err := parseCompactStructFromBufio(rbuf, 64*1024)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			break
		}

		pageType, uncompressedSize, compressedSize, numValues, encoding, err := decodePageHeader(headerStruct)
		if err != nil {
			break
		}

		if numValues == 0 {
			numValues = chunk.MetaData.NumValues
		}
		if encoding == 0 && len(chunk.MetaData.Encodings) > 0 {
			encoding = chunk.MetaData.Encodings[0]
		}

		actualCompressedSize := int(compressedSize)
		if actualCompressedSize <= 0 {
			break
		}

		compressedData := make([]byte, actualCompressedSize)
		n, err := io.ReadFull(rbuf, compressedData)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if n < actualCompressedSize {
			compressedData = compressedData[:n]
			actualCompressedSize = n
		}

		if pageType == 0 { // DATA_PAGE
			pageData, err := decompressData(compressedData, chunk.MetaData.Codec, int(uncompressedSize))
			if err != nil {
				pageData = compressedData
			}

			maxDefinitionLevel := byte(0)
			if schema.RepetitionType != nil && *schema.RepetitionType == 0 {
				maxDefinitionLevel = 1
			} else if schema.RepetitionType != nil && *schema.RepetitionType == 1 {
				maxDefinitionLevel = 1
			}

			pageValues, err := parseDataPageWithEncoding(pageData, schema.Type, int64(encoding), numValues, schema.RepetitionType, maxDefinitionLevel)
			if err == nil && len(pageValues) > 0 {
				values = append(values, pageValues...)
				totalValuesRead += len(pageValues)
			}
		}

		pagesRead++

		if totalValuesRead >= int(chunk.MetaData.NumValues) {
			break
		}
	}

	return values, nil
}

