package main

// FileMetadata is a simplified in-memory representation of Parquet FileMetaData.
// It is populated by decoding the Thrift Compact-encoded footer.
type FileMetadata struct {
	Version   int32
	Schema    []SchemaElement
	NumRows   int64
	RowGroups []RowGroup
}

type SchemaElement struct {
	Type           int32
	TypeLength     *int32
	RepetitionType *int32
	Name           string
	NumChildren    *int32
	ConvertedType  *int32
	Scale          *int32
	Precision      *int32
	FieldID        *int32
}

type RowGroup struct {
	Columns             []ColumnChunk
	TotalByteSize       int64
	NumRows             int64
	SortingColumns      []SortingColumn
	FileOffset          int64
	TotalCompressedSize int64
	Ordinal             int32
}

type ColumnChunk struct {
	FilePath          []string
	FileOffset        int64
	MetaData          *ColumnMetaData
	OffsetIndexOffset *int64
	OffsetIndexLength *int32
	ColumnIndexOffset *int64
	ColumnIndexLength *int32
}

type ColumnMetaData struct {
	Type                  int32
	Encodings             []int32
	PathInSchema          []string
	Codec                 int32
	NumValues             int64
	TotalUncompressedSize int64
	TotalCompressedSize   int64
	KeyValueMeta          []KeyValue
	DataPageOffset        int64
	IndexPageOffset       *int64
	DictionaryPageOffset  *int64
	Statistics            *Statistics
	EncodingStats         []PageEncodingStats
	BloomFilterOffset     *int64
	BloomFilterLength     *int32
	SizeStatistics        *SizeStatistics
}

type KeyValue struct {
	Key   string
	Value *string
}

type Statistics struct {
	Max           []byte
	Min           []byte
	NullCount     *int64
	DistinctCount *int64
	MaxValue      []byte
	MinValue      []byte
}

type PageEncodingStats struct {
	PageType int32
	Encoding int32
	Count    int32
}

type SizeStatistics struct {
	UnencodedByteArrayDataBytes int64
	RepetitionLevelHistogram    []int64
	DefinitionLevelHistogram    []int64
}

type SortingColumn struct {
	ColumnIdx  int32
	Descending bool
	NullsFirst bool
}

