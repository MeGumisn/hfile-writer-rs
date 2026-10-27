# Arrow / Parquet integration

> [English](arrow-integration.md) | [简体中文](arrow-integration.zh-CN.md)

An HFile has no schema: it stores cells of `(row, family, qualifier, timestamp, type, value)`.
Arrow has types. The bridge between the two is a `Mapping`, and it comes in two flavours
depending on which side owns the schema.

## Two input layouts

### Raw layout ("cells as rows")

`hfile2arrow` without `--schema` writes one Parquet row per cell with a fixed schema:

| column | type | meaning |
|--------|------|---------|
| `__rowkey` | `Binary` | row key |
| `__family` | `Binary` | column family |
| `__qualifier` | `Binary` | qualifier |
| `__value` | `Binary` | cell value as stored, `NULL` for a delete marker |
| `__timestamp` | `Int64` | HBase timestamp |
| `__cell_type` | `UInt8` | `KeyValue` type byte |

The layout is lossless for row/family/qualifier/value/timestamp/type, but it does **not** carry the
mvcc/sequence id, and the payloads stay opaque bytes. Convert back with no `--row-key` at all, or
with `--row-key __rowkey`; both are treated as the raw layout.

### Wide layout ("one row per HBase row")

With a schema you get one Parquet row per HBase row and one column per cell qualifier, so the data
is usable from SQL engines:

```text
id   Int64   -> row key
name Utf8    -> cell cf:name       (family comes from --family or from "cf:name")
ts   Int64   -> HBase timestamp    (via --timestamp-column)
```

Going back, the Arrow schema (saved with `--schema-out`) tells the converter which columns are
row key parts, which column holds the timestamp, and what type each cell had. Rows whose row key
columns are all null are skipped, as are null cell values.

## Mapping

```json
{
  "row_key": ["tenant", "id"],
  "row_key_separator": "\u0000",
  "family": "cf",
  "timestamp_column": "ts",
  "timestamp": null,
  "value_encoding": "text"
}
```

| field | default | meaning |
|-------|---------|---------|
| `row_key` | `[]` | columns concatenated into the row key; empty means "use the `__rowkey` column" |
| `row_key_separator` | `"\u0000"` | bytes between row key parts. Use `"\u0001"` or similar if values can contain NUL |
| `family` | `null` | family for fields whose name has no `family:` prefix |
| `timestamp_column` | `null` | column holding the timestamp (must be an integer type) |
| `timestamp` | `null` | fixed timestamp; when neither is set the cells get `i64::MAX` (`HConstants.LATEST_TIMESTAMP`) |
| `value_encoding` | `"text"` | `"text"` or `"binary"` |

A column named `cf:name` always targets family `cf` and qualifier `name`, overriding `family`.

## Type mapping

`Utf8`, `LargeUtf8`, `Binary` and `LargeBinary` are stored byte for byte, so they round trip with
either `value_encoding`.

| Arrow type | `text` | `binary` |
|------------|--------|----------|
| `Boolean` | `true` / `false` | 1 byte `0`/`1` |
| `Int8`..`Int64`, `UInt8`..`UInt64` | decimal string | big-endian, native width |
| `Float32`, `Float64` | Rust `Display` (`1.5`, `NaN`, `inf`) | IEEE-754 bits, big-endian |
| `Date32`, `Date64` | days/ms since epoch as decimal | big-endian `i32`/`i64` |
| `Timestamp(s|ms|us|ns)` | raw value in that unit, as decimal | big-endian `i64` |
| `Decimal128(p, s)` | unscaled value as decimal | big-endian `i128` |
| `Utf8`, `LargeUtf8`, `Binary`, `LargeBinary` | raw bytes | raw bytes |

Notes that save debugging time:

* Row key parts and the timestamp are always serialized as **big-endian binary**, whatever
  `value_encoding` says, and they are decoded the same way on the way back. That keeps numeric
  row keys lossless and unambiguous; it is also why `--schema` is required to restore their
  Arrow types.
* `text` is not lossless for `Float32`/`Float64` in edge cases (`-0.0`, some subnormals) and it
  loses `Decimal128` scale; use `binary` when that matters.
* Timestamps here are the HBase timestamp *of the cell*, not the Arrow timestamp of an event;
  the two often coincide, but nothing enforces it.
* Nested types (`List`, `Struct`, `Map`), `Float16` and dictionary-encoded columns are rejected
  with `invalid argument: unsupported Arrow type ...`.

## Async conversions

`arrow::async_io` mirrors `arrow::read` / `arrow::write` on top of the official async Parquet API
(`ParquetRecordBatchStreamBuilder` for input, `AsyncArrowWriter` for output) and the tokio HFile
types. Nothing is reimplemented: the mapping, layouts, sorting and statistics are the same code
paths, so the two APIs produce identical output for identical input.

| function | direction |
|----------|-----------|
| `async_io::parquet_to_hfile(input, out, &mapping, WriterOptions)` | Parquet -> HFile |
| `async_io::hfile_to_parquet(hfile, out, &ReadOptions)` | HFile -> Parquet |
| `async_io::read_parquet(input)` / `async_io::source_schema(input)` | Parquet metadata / batches |
| `async_io::write_cells(out, cells, WriterOptions)` | sorted cells -> HFile |

```rust
use hfile_rs::arrow::Mapping;
use hfile_rs::arrow::async_io::{hfile_to_parquet, parquet_to_hfile};
use hfile_rs::arrow::read::ReadOptions;
use hfile_rs::hbase::WriterOptions;

async fn example() -> hfile_rs::Result<()> {
let mapping = Mapping {
    row_key: vec!["id".to_string()],
    family: Some("cf".to_string()),
    ..Default::default()
};
let stats = parquet_to_hfile("in.parquet", "out.hfile", &mapping, WriterOptions::default()).await?;
assert_eq!(stats.rows, 3);

let options = ReadOptions {
    mapping,
    schema: Some(hfile_rs::arrow::async_io::source_schema("in.parquet").await?),
    ..Default::default()
};
hfile_to_parquet("out.hfile", "back.parquet", &options).await?;
Ok(())
}
```

What stays the same and what changes:

* **Same format, both ways.** An async written HFile opens with `HFileReader`, and a Parquet file
  written by `AsyncArrowWriter` opens with `arrow::write::read_parquet`.
* **Sorting is still in memory.** The HFile side needs cells in `CellComparator` order, so
  `parquet_to_hfile` buffers the cells and sorts them (on the blocking pool). For data that does
  not fit, pre-sort the Parquet and stream with `AsyncHFileWriter::append`.
* **Batch field conversion runs off the runtime.** Each record batch is mapped to cells with
  `tokio::task::spawn_blocking`, so a large schema/`binary` conversion does not stall the reactor.
* **Bounded memory on the read side.** `hfile_to_parquet` walks HFile blocks and feeds
  `AsyncArrowWriter` as batches are completed; only one HFile block plus one Parquet row group is
  resident. `ReadOptions::limit` stops the scan early.
* **CLI unchanged.** `hfile arrow2hfile` / `hfile hfile2arrow` keep using the blocking API; the
  async path is for library users that already own a tokio runtime.

## CLI examples

```bash
# wide layout, schema saved for the reverse trip; `--row-key` takes a comma separated list
hfile arrow2hfile hits.parquet --out t.hfile \
    --row-key tenant,id --family cf \
    --timestamp-column ts --value-encoding binary --schema-out t.schema.json

hfile hfile2arrow t.hfile --out hits2.parquet \
    --schema t.schema.json --row-key tenant,id --family cf

# raw layout: any HFile, no schema needed
hfile hfile2arrow store.hfile --out cells.parquet --limit 1000
hfile arrow2hfile cells.parquet --out copy.hfile --encoding PREFIX --codec gz
```

The row key separator is not exposed as a flag: `--row-key a,b` always joins with a single NUL
byte. Use a mapping file when the parts may contain NUL or you want something readable:

```bash
cat > mapping.json <<'JSON'
{ "row_key": ["tenant", "id"], "row_key_separator": "|", "family": "cf" }
JSON
hfile arrow2hfile hits.parquet --out t.hfile --mapping mapping.json --timestamp-column ts
```

The same mapping file is read by `hfile2arrow` and `arrow2bulkload` (`--mapping`), so a composite
key only has to be described once.

Because the wide layout needs the row key back, columns listed in `--row-key` must be present in
the Parquet file on the way back; `--timestamp` (fixed) can be used instead of
`--timestamp-column` when the original timestamps do not matter.

## Sorting

HFiles must be sorted by `CellComparator`. `arrow2hfile` sorts in memory before writing, so
Parquet files in arbitrary order are accepted; the trade-off is that the whole file is buffered.
For data that does not fit in memory, pre-sort by `(row, family, qualifier, ts desc)` and stream
with the library API (`HFileWriter::append`).
