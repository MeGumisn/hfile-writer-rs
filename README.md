# hfile-writer-rs

> [English](README.md) | [简体中文](README.zh-CN.md)

Pure Rust reader and writer for Apache HBase **HFile v3**, plus Arrow/Parquet conversion and a
CLI. Files written by this crate are readable by HBase itself and can be fed straight into
`LoadIncrementalHFiles` (bulk load).

```bash
cargo build --release          # -> target/release/hfile
hfile --help
```

## Feature matrix

| Area | Read | Write |
|------|------|-------|
| HFile v3 blocks, CRC32C/CRC32/NULL checksums, PBUF trailer | yes | yes |
| File info (stats, last key, mvcc/tags flags, store file metadata) | yes | yes |
| Data block encoding | `NONE`, `PREFIX`, `DIFF`, `FAST_DIFF`, `ROW_INDEX_V1` | `NONE`, `PREFIX`, `ROW_INDEX_V1` |
| Compression | `NONE`, `GZ`, `SNAPPY`, `ZSTD` (both framings) | `NONE`, `GZ`, `SNAPPY`, `ZSTD` (both framings) |
| Index | single and multi level | single level |
| Bloom filter (`BLMFMET2` + `BLMFBLK2`) | `ROW` | `ROW` |
| Meta blocks (`appendMetaBlock`, meta index) | yes | yes |
| Sequential scan, `seek`, `get` | yes | – |
| Tokio async reader/writer (`AsyncHFileReader`, `AsyncHFileWriter`) | yes | yes |
| Tokio async Arrow/Parquet conversion (`arrow::async_io`) | yes | yes |
| Arrow ⇄ Parquet | raw and wide layouts | raw and wide layouts |
| Bulk load directory layout | – | yes |

Not implemented: `ROWCOL` bloom filters, encryption, `LZO`/`LZ4`/`BZIP2` codecs.

## CLI

```bash
# inspect a file written by HBase
hfile info  /path/to/store.hfile --dump --limit 5
hfile dump  /path/to/store.hfile
hfile bloom /path/to/store.hfile row1 row2 row3      # bloom membership, hex:aabb for binary rows
hfile meta  /path/to/store.hfile my-meta-block

# Parquet -> HFile (wide layout: one Arrow row per HBase row)
hfile arrow2hfile input.parquet --out store.hfile \
    --row-key id --family cf --timestamp-column ts \
    --encoding ROW_INDEX_V1 --codec zstd --bloom ROW --meta my-meta-block=hello

# HFile -> Parquet (pass the schema generated above to restore Arrow types)
hfile hfile2arrow store.hfile --out out.parquet \
    --schema schema.json --row-key id --family cf

# Bulk load layout: <dir>/<family>/<file> plus the _SUCCESS marker
hfile arrow2bulkload input.parquet --dir /tmp/bulk --family cf --row-key id
# then: hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /tmp/bulk <table>
```

`--encoding` accepts `NONE`, `PREFIX`, `DIFF`, `FAST_DIFF`, `ROW_INDEX_V1` (writing `DIFF` /
`FAST_DIFF` is refused with a clear error), `--codec` accepts `none`, `gz`, `snappy`, `zstd`, and
`--zstd-framing` accepts `raw` (Hadoop `ZStandardCodec`, the default) or `hbase`
(`hbase-compression-zstd`). See [docs/hfile-format.md](docs/hfile-format.md#compression) for
why that choice matters. `--bloom` accepts `none` (default) or `ROW`, `--bloom-error-rate`
overrides the 1% default, and `--meta NAME=VALUE` adds a meta block (repeatable).

## Library

```rust
use hfile_rs::hbase::{Cell, DataBlockEncoding, HFileReader, HFileWriter, WriterOptions};

fn main() -> hfile_rs::Result<()> {
    let mut writer = HFileWriter::create(
        "/tmp/example.hfile",
        WriterOptions {
            encoding: DataBlockEncoding::RowIndexV1,
            ..Default::default()
        },
    )?;
    writer.append(Cell::new(
        b"row1".to_vec(),
        b"cf".to_vec(),
        b"q".to_vec(),
        b"value".to_vec(),
    ))?;
    writer.finish()?;

    let reader = HFileReader::open("/tmp/example.hfile")?;
    assert_eq!(reader.read_all()?.len(), 1);
    std::fs::remove_file("/tmp/example.hfile").ok();
    Ok(())
}
```

### Async I/O

`AsyncHFileReader` / `AsyncHFileWriter` are tokio flavors of the two types above. They read and
write the same bytes, so async and blocking code can work on each other's files:

```rust
use hfile_rs::hbase::{AsyncHFileReader, AsyncHFileWriter, Cell, WriterOptions};

async fn example() -> hfile_rs::Result<()> {
    let mut writer = AsyncHFileWriter::create("/tmp/async.hfile", WriterOptions::default()).await?;
    writer
        .append(Cell::new(b"row1".to_vec(), b"cf".to_vec(), b"q".to_vec(), b"v".to_vec()))
        .await?;
    writer.finish().await?;

    let reader = AsyncHFileReader::open("/tmp/async.hfile").await?;
    reader
        .for_each(|cell| {
            assert_eq!(cell.row, b"row1");
            Ok(())
        })
        .await?;
    Ok(())
}
```

The writer core (`HFileWriter<W: Write>`) does no I/O of its own unless the sink does: `create`
wraps a `BufWriter<File>`, `with_sink` takes any `Write`, and `take_buffered` hands out the bytes
of one finished block for a `Vec<u8>` sink. `AsyncHFileWriter` builds on that and writes exactly
one block per `write_all`, so memory stays at one block no matter how large the file gets.
`finish_into_sink` returns the sink (and therefore the buffer) after the trailer is written.

`hfile_rs::arrow::async_io` provides the same for Parquet: `parquet_to_hfile` and
`hfile_to_parquet` read with `ParquetRecordBatchStreamBuilder`, write with `AsyncArrowWriter`, and
reuse the mapping/layout code of `arrow::write` / `arrow::read`, so async and blocking conversions
are interchangeable. See [docs/arrow-integration.md](docs/arrow-integration.md#async-conversions).

## Documentation

* [docs/hfile-format.md](docs/hfile-format.md) – on-disk format: blocks, checksums, encodings,
  index, file info, trailer, compression.
* [docs/read-write-flow.md](docs/read-write-flow.md) – how the reader and writer walk a file.
* [docs/arrow-integration.md](docs/arrow-integration.md) – Arrow/Parquet mapping, both layouts,
  type conversions.
* [docs/bulkload.md](docs/bulkload.md) – bulk load layout, checklist and troubleshooting.

## Testing

```bash
cargo test                       # unit tests + integration tests in tests/
scripts/hbase-crosscheck.sh      # optional: against real HBase jars
```

`scripts/hbase-crosscheck.sh` needs an HBase 2.5.x installation and validates both directions:
reading HBase-written HFiles with this crate, and reading this crate's HFiles with HBase's own
`HFileTool`. See the script header for the environment variables it expects.

## License

Apache-2.0
