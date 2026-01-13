## kaitai_parquet

Small CLI utility that reads a Parquet file (tested with `titanic.parquet`) and prints its contents as a human-readable table.

### How to run

```bash
go build -o parquet_reader ./main
./parquet_reader titanic.parquet
```

### Project layout

- `main/main.go`: CLI entrypoint. Reads Parquet magic/footer via Kaitai, decodes footer metadata, and prints schema + table.
- `main/parquet_types.go`: In-memory Go structs used by the tool (`FileMetadata`, `RowGroup`, `ColumnMetaData`, etc.).
- `main/thrift_compact_decode.go`: Decodes Parquet Thrift-Compact-encoded footer and page headers from the Kaitai Thrift AST.
- `main/page_decode.go`: Data page dispatch and level (def/rep) handling.
- `main/plain_decode.go`: PLAIN decoding for basic Parquet physical types used by `titanic.parquet`.
- `main/delta_decode.go`: Minimal DELTA_BINARY_PACKED decoding used by some Parquet columns/pages.
- `main/compress.go`: Page decompression (SNAPPY / UNCOMPRESSED).
- `main/rle_decoder.go`: RLE / bit-packed decoding for definition/repetition levels.

### Generated code (Kaitai)

All Kaitai-generated code lives in the separate package:

- `kaitai_gen/parquet.go`: Parquet container structure (magic, footer length, footer bytes, etc.) generated from `parquet.ksy`.
- `kaitai_gen/thrift_compact.go`: Thrift Compact Protocol AST parser generated from `thrift_compact.ksy`.

Specs:

- `parquet.ksy`: Parquet file container spec. Imports `thrift_compact.ksy` and exposes `footer_thrift` as a parsed Thrift Compact struct.
- `thrift_compact.ksy`: Thrift Compact Protocol spec.
- `parquet.thrift`: Upstream Parquet Thrift schema reference (used as documentation for what the footer contains).

Regeneration:

```bash
kaitai-struct-compiler -t go --go-package kaitai_gen --outdir . -I . thrift_compact.ksy parquet.ksy
```

