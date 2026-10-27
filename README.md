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
| Compression | `NONE`, `GZ`, `ZSTD` (both framings) | `NONE`, `GZ`, `ZSTD` (both framings) |
| Index | single and multi level | single level |
| Sequential scan, `seek`, `get` | yes | – |
| Arrow ⇄ Parquet | raw and wide layouts | raw and wide layouts |
| Bulk load directory layout | – | yes |

Not implemented: bloom filters, encryption, meta blocks, `LZO`/`SNAPPY`/`LZ4`/`BZIP2` codecs.

## CLI

```bash
# inspect a file written by HBase
hfile info  /path/to/store.hfile --dump --limit 5
hfile dump  /path/to/store.hfile

# Parquet -> HFile (wide layout: one Arrow row per HBase row)
hfile arrow2hfile input.parquet --out store.hfile \
    --row-key id --family cf --timestamp-column ts \
    --encoding ROW_INDEX_V1 --codec zstd --schema-out schema.json

# HFile -> Parquet (pass the schema generated above to restore Arrow types)
hfile hfile2arrow store.hfile --out out.parquet \
    --schema schema.json --row-key id --family cf

# Bulk load layout: <dir>/<family>/<file> plus the _SUCCESS marker
hfile arrow2bulkload input.parquet --dir /tmp/bulk --family cf --row-key id
# then: hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /tmp/bulk <table>
```

`--encoding` accepts `NONE`, `PREFIX`, `DIFF`, `FAST_DIFF`, `ROW_INDEX_V1` (writing `DIFF` /
`FAST_DIFF` is refused with a clear error), `--codec` accepts `none`, `gz`, `zstd`, and
`--zstd-framing` accepts `raw` (Hadoop `ZStandardCodec`, the default) or `hbase`
(`hbase-compression-zstd`). See [docs/hfile-format.md](docs/hfile-format.md#compression) for
why that choice matters.

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
