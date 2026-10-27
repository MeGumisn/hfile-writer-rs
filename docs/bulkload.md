# Bulk load

> [English](bulkload.md) | [简体中文](bulkload.zh-CN.md)

`LoadIncrementalHFiles` takes a directory that looks like a table's store file tree, verifies and
optionally splits the HFiles, and then references them from the table without rewriting the data.
This crate produces exactly that layout.

## Producing the directory

```bash
hfile arrow2bulkload hits.parquet \
    --dir /tmp/bulk/hits \
    --family cf \
    --row-key tenant,id \
    --timestamp-column ts \
    --encoding ROW_INDEX_V1 \
    --codec zstd --zstd-framing hbase \
    --file-name 0001.hfile
```

Row key parts are joined with a single NUL byte; pass `--mapping mapping.json` when you need a
different `row_key_separator` (see [arrow-integration.md](arrow-integration.md#mapping)).

| flag | default | notes |
|------|---------|-------|
| `--dir` | – | directory handed to `LoadIncrementalHFiles` |
| `--family` | – | family name **and** the sub directory name, must exist in the table |
| `--file-name` | `<family>.hfile` | name inside the family directory; keep the `.hfile` suffix |
| `--mapping` | – | JSON mapping file, see [arrow-integration.md](arrow-integration.md) |
| `--encoding` | `NONE` | `PREFIX` or `ROW_INDEX_V1` are usually better for bulk loads |
| `--codec` | `none` | must match a codec the cluster can read |
| `--zstd-framing` | `raw` | `raw` for Hadoop's codec, `hbase` for `hbase-compression-zstd` |
| `--bloom` | `none` | `ROW` adds a bloom filter, useful when the table is read by row |
| `--meta` | – | `NAME=VALUE`, repeatable; stored as a meta block |
| `--block-size` | cluster default | target data block size in bytes |

Result:

```text
/tmp/bulk/hits/
├── _SUCCESS
└── cf/
    └── 0001.hfile
```

`_SUCCESS` is required: `LoadIncrementalHFiles` treats the directory as a job output and refuses
to run without it.

## Running the load

```bash
# the directory must be on the cluster's file system, not on your laptop
hdfs dfs -put -f /tmp/bulk/hits /tmp/bulk/hits

hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles /tmp/bulk/hits hits
```

`LoadIncrementalHFiles` first runs a "bulk load prepare" phase that reads every HFile and
computes region splits from the row keys. It therefore needs:

* the same `hbase.io.compress.zstd.codec` (or any other codec) configuration the files were
  written with – otherwise prepare fails while reading, long before the load itself;
* HFiles that are readable by the target HBase version (this crate writes HFile v3, which is what
  HBase 2.x expects).

Use `-Dcreate.table=yes` on first use if you also want the tool to create the table.

## Checklist

1. **Family exists.** `--family` must be a family of the target table; otherwise the store file
   lands in the wrong directory and the load fails with "wrong family name".
2. **Codec matches.** `--codec`/`--zstd-framing` must be readable by every region server. When in
   doubt use `--codec none`.
3. **Sorting.** `arrow2bulkload` sorts the input with `CellComparator` before writing, so the
   Parquet file may arrive in any order. Row key parts are concatenated in `--row-key` order.
4. **One family per invocation.** The directory may hold several family sub directories, but each
   file must contain cells of a single family. Run the command once per family and point them at
   the same `--dir` (they will overwrite `_SUCCESS`, which is fine).
5. **Timestamps.** Without `--timestamp-column` every cell gets `i64::MAX`
   (`HConstants.LATEST_TIMESTAMP`), i.e. it wins over anything written later with a real
   timestamp. Pass a timestamp column when the data has one.
6. **Sequence ids.** `MAX_SEQ_ID_KEY` is derived from the cells' `seq_id`, which is `0` for data
   coming from Parquet unless the Parquet file has a `__seq` column (raw layout has none).
   `LoadIncrementalHFiles` rewrites the store file metadata during the load, so this only affects
   files copied into place manually.

## Verifying before the load

```bash
# our own reader
hfile info /tmp/bulk/hits/cf/0001.hfile --dump --limit 5

# HBase's own tool (needs the HBase jars on the classpath)
hbase org.apache.hadoop.hbase.io.hfile.HFileTool read /tmp/bulk/hits/cf/0001.hfile | head
```

`scripts/hbase-crosscheck.sh` automates both, and `cargo test` covers the directory layout in
`tests/cli.rs`.

## Common failures

| message | cause |
|---------|-------|
| `Bulk load file ... not a valid HFile` | truncated file – the writer was dropped without `finish()`, or the upload was interrupted |
| `Can't read hfile` / `Problem reading data index` | codec mismatch (see 2) or a wrong `--file-name` extension |
| `NoSuchColumnFamilyException` after the load | `--family` does not match the table |
| `RetriesExhaustedWithDetailsException` during prepare | the target table's regions are not online yet |
| every value looks like `\x00\x00\x00\x00...` | row key or timestamp columns were read back with `text` instead of `binary`, see [arrow-integration.md](arrow-integration.md) |
