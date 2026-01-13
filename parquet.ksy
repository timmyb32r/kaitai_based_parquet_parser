meta:
  id: parquet
  title: Apache Parquet File Format
  application: Apache Parquet
  file-extension:
    - parquet
  license: Apache-2.0
  endian: le
  encoding: UTF-8
  xref:
    spec: https://github.com/apache/parquet-format
  imports:
    - thrift_compact

doc: |
  Apache Parquet is a columnar storage format designed for efficient
  data storage and retrieval. This specification describes the binary
  file format structure.
  
  File structure:
  - Magic "PAR1" (4 bytes) at start
  - Data blocks (row groups with column chunks)
  - Footer length (4 bytes, little-endian)
  - Footer (Thrift-encoded FileMetaData)
  - Magic "PAR1" (4 bytes) at end
  
  Note: The footer is Thrift-encoded binary data, which uses variable-length
  encoding and field tags. The type definitions below represent the logical
  structure of the metadata, but parsing the actual Thrift-encoded footer
  requires a Thrift parser or manual decoding.

seq:
  - id: magic
    type: str
    encoding: ASCII
    size: 4
    doc: Magic number "PAR1" at the beginning of the file

instances:
  footer_length:
    pos: _io.size - 8
    type: u4
    doc: Length of the footer in bytes (32-bit little-endian), stored before the final magic.
  
  data:
    pos: 4
    type: data_section
    size: _io.size - 8 - footer_length - 4
    doc: Data section containing row groups
  
  footer_raw:
    pos: _io.size - 8 - footer_length
    type: u1
    repeat: expr
    repeat-expr: footer_length
    doc: Raw footer data (Thrift-encoded FileMetaData)

  # Parsed footer as a Thrift Compact Protocol struct (Parquet stores a raw struct, not a message envelope).
  footer_thrift:
    pos: _io.size - 8 - footer_length
    type: thrift_compact::compact_struct
    size: footer_length
    doc: Footer parsed as a Thrift Compact Protocol struct.
  
  magic_end:
    pos: _io.size - 4
    type: str
    encoding: ASCII
    size: 4
    doc: Magic number "PAR1" at the end of the file

types:
  data_section:
    doc: |
      Data section containing row groups. The actual row groups are located
      using offsets stored in the footer metadata. This section contains
      the raw binary data that needs to be parsed according to the footer.
    seq:
      - id: raw_data
        type: u1
        repeat: eos
        doc: Raw binary data containing row groups and column chunks

  file_metadata:
    seq:
      - id: version
        type: u4
        doc: Version of the file format
      - id: schema
        type: schema_element
        repeat: eos
        doc: Schema elements (Thrift-encoded)
      - id: num_rows
        type: u8
        doc: Number of rows in the file
      - id: row_groups_meta
        type: row_group_metadata
        repeat: eos
        doc: Row group metadata (Thrift-encoded)
      - id: key_value_metadata
        type: key_value
        repeat: eos
        doc: Key-value metadata pairs (Thrift-encoded)
      - id: created_by
        type: strz
        encoding: UTF-8
        doc: String identifying the library that created the file
      - id: column_orders
        type: column_order
        repeat: eos
        doc: Column orders (Thrift-encoded)

  schema_element:
    doc: |
      Schema element describing a column or nested structure.
      This is a simplified representation; actual Thrift encoding
      is more complex with variable-length fields.
    seq:
      - id: type
        type: u1
        enum: type_enum
        doc: Data type of the element
      - id: type_length
        type: s4
        if: type == type_enum::fixed_len_byte_array
        doc: Length for FIXED_LEN_BYTE_ARRAY type
      - id: repetition_type
        type: u1
        enum: field_repetition_type
        doc: Repetition type (required, optional, repeated)
      - id: name
        type: strz
        encoding: UTF-8
        doc: Name of the schema element
      - id: num_children
        type: s4
        doc: Number of child elements
      - id: converted_type
        type: u1
        enum: converted_type_enum
        doc: Converted type for logical types
      - id: scale
        type: s4
        doc: Scale for DECIMAL type
      - id: precision
        type: s4
        doc: Precision for DECIMAL type
      - id: field_id
        type: s4
        doc: Field ID

  row_group_metadata:
    doc: |
      Metadata for a row group. This is a simplified representation
      of the Thrift-encoded RowGroup structure.
    seq:
      - id: columns
        type: column_chunk_metadata
        repeat: eos
        doc: Column chunk metadata
      - id: total_byte_size
        type: u8
        doc: Total byte size of all column chunks
      - id: num_rows
        type: u8
        doc: Number of rows in this row group
      - id: sorting_columns
        type: sorting_column
        repeat: eos
        doc: Sorting columns information
      - id: file_offset
        type: u8
        doc: File offset of the row group
      - id: total_compressed_size
        type: u8
        doc: Total compressed size
      - id: ordinal
        type: s4
        doc: Ordinal of the row group

  column_chunk_metadata:
    doc: |
      Metadata for a column chunk. This is a simplified representation
      of the Thrift-encoded ColumnChunk structure.
    seq:
      - id: type
        type: u1
        enum: type_enum
        doc: Data type of the column
      - id: encodings
        type: u1
        enum: encoding_enum
        repeat: eos
        doc: Encodings used in this column
      - id: path_in_schema
        type: strz
        encoding: UTF-8
        repeat: eos
        doc: Path in the schema
      - id: codec
        type: u1
        enum: compression_codec
        doc: Compression codec used
      - id: num_values
        type: u8
        doc: Number of values in this column chunk
      - id: total_uncompressed_size
        type: u8
        doc: Total uncompressed size
      - id: total_compressed_size
        type: u8
        doc: Total compressed size
      - id: key_value_metadata
        type: key_value
        repeat: eos
        doc: Key-value metadata pairs
      - id: data_page_offset
        type: u8
        doc: Offset of the first data page
      - id: index_page_offset
        type: u8
        doc: Offset of the index page (if present)
      - id: dictionary_page_offset
        type: u8
        doc: Offset of the dictionary page (if present)
      - id: statistics
        type: statistics
        doc: Statistics for this column chunk
      - id: encoding_stats
        type: page_encoding_stats
        repeat: eos
        doc: Encoding statistics
      - id: bloom_filter_offset
        type: u8
        doc: Bloom filter offset (if present)
      - id: bloom_filter_length
        type: u4
        doc: Bloom filter length (if present)
      - id: size_statistics
        type: size_statistics
        doc: Size statistics

  row_group:
    doc: |
      A row group containing column chunks. The actual data is stored
      in column chunks which may be compressed and encoded.
    seq:
      - id: column_chunks
        type: column_chunk
        repeat: eos
        doc: Column chunks in this row group

  column_chunk:
    doc: |
      A column chunk containing the actual data pages. The data
      may be compressed and encoded according to the metadata.
    seq:
      - id: pages
        type: page
        repeat: eos
        doc: Pages in this column chunk

  page:
    doc: |
      A page is the unit of storage in a column chunk. Pages can be
      data pages, dictionary pages, or index pages.
    seq:
      - id: page_type
        type: u1
        enum: page_type_enum
        doc: Type of the page
      - id: uncompressed_page_size
        type: u4
        doc: Uncompressed size of the page
      - id: compressed_page_size
        type: u4
        doc: Compressed size of the page
      - id: crc
        type: u4
        doc: CRC32 checksum (present if file format version >= 2, requires footer parsing to determine)
      - id: data
        size: compressed_page_size
        doc: Compressed page data

  statistics:
    doc: |
      Statistics for a column chunk, including min/max values,
      null count, distinct count, etc.
    seq:
      - id: max
        type: u1
        repeat: expr
        repeat-expr: 16
        doc: Maximum value (variable length in actual format)
      - id: min
        type: u1
        repeat: expr
        repeat-expr: 16
        doc: Minimum value (variable length in actual format)
      - id: null_count
        type: u8
        doc: Number of null values
      - id: distinct_count
        type: u8
        doc: Number of distinct values
      - id: max_value
        type: u1
        repeat: expr
        repeat-expr: 16
        doc: Maximum value (deprecated, use max)
      - id: min_value
        type: u1
        repeat: expr
        repeat-expr: 16
        doc: Minimum value (deprecated, use min)

  key_value:
    doc: Key-value metadata pair
    seq:
      - id: key
        type: strz
        encoding: UTF-8
        doc: Key name
      - id: value
        type: strz
        encoding: UTF-8
        doc: Value string

  sorting_column:
    doc: Information about sorting columns
    seq:
      - id: column_idx
        type: u4
        doc: Column index
      - id: descending
        type: b1
        doc: Whether sorting is descending
      - id: nulls_first
        type: b1
        doc: Whether nulls come first

  column_order:
    doc: Column order information
    seq:
      - id: type
        type: u1
        enum: column_order_type
        doc: Type of column order

  page_encoding_stats:
    doc: Encoding statistics for a page
    seq:
      - id: page_type
        type: u1
        enum: page_type_enum
        doc: Type of page
      - id: encoding
        type: u1
        enum: encoding_enum
        doc: Encoding used
      - id: count
        type: u4
        doc: Count of pages with this encoding

  size_statistics:
    doc: Size statistics
    seq:
      - id: unencoded_byte_array_data_bytes
        type: u8
        doc: Unencoded byte array data bytes
      - id: repetition_level_histogram
        type: u8
        repeat: eos
        doc: Repetition level histogram
      - id: definition_level_histogram
        type: u8
        repeat: eos
        doc: Definition level histogram

enums:
  type_enum:
    0: boolean
    1: int32
    2: int64
    3: int96
    4: float
    5: double
    6: byte_array
    7: fixed_len_byte_array

  field_repetition_type:
    0: required
    1: optional
    2: repeated

  converted_type_enum:
    0: utf8
    1: map
    2: map_key_value
    3: list
    4: enum
    5: decimal
    6: date
    7: time_millis
    8: time_micros
    9: timestamp_millis
    10: timestamp_micros
    11: uint8
    12: uint16
    13: uint32
    14: uint64
    15: int8
    16: int16
    17: int32
    18: int64
    19: json
    20: bson
    21: interval

  encoding_enum:
    0: plain
    1: plain_dictionary
    2: rle
    3: bit_packed
    4: delta_binary_packed
    5: delta_length_byte_array
    6: delta_byte_array
    7: rle_dictionary
    8: byte_stream_split

  compression_codec:
    0: uncompressed
    1: snappy
    2: gzip
    3: lzo
    4: brotli
    5: lz4
    6: zstd
    7: lz4_raw

  page_type_enum:
    0: data_page
    1: index_page
    2: dictionary_page
    3: data_page_v2

  column_order_type:
    0: type_order
    1: column_order_type_undefined
