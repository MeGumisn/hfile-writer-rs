# HFile v3 on-disk format

> [English](hfile-format.md) | [简体中文](hfile-format.zh-CN.md)

This is the subset of the HBase HFile format that this crate implements. Field names follow
`org.apache.hadoop.hbase.io.hfile`, so they can be cross-checked against the HBase source.

## File layout

```text
offset 0                       first data block            "DATABLK*" / "DATABLKE"
        ...                    more data blocks            chained through prevBlockOffset
last_data_block_offset         (last data block)
load_on_open_data_offset       root index block            "IDXROOT2"
file_info_offset               file info block             "FILEINF2"
filesize - 4096                fixed file trailer          "TRABLK\"$"
```

Everything before `load_on_open_data_offset` belongs to the data section and is only reachable
through the index; everything from `load_on_open_data_offset` to the trailer is the
"load on open" section (`HFileInfo.initMetaAndIndex`).

## Block framing

Every block starts with a 33 byte header (`HFileBlock`):

| offset | size | field |
|-------:|-----:|-------|
| 0 | 8 | block type magic, e.g. `DATABLK*`, `IDXROOT2`, `FILEINF2` |
| 8 | 4 | `onDiskSizeWithoutHeader` – payload + checksums |
| 12 | 4 | `uncompressedSizeWithoutHeader` – payload only |
| 16 | 8 | `prevBlockOffset`, offset of the previous block of the same type (`-1` when none) |
| 24 | 1 | `checksumType`: `0` NULL, `1` CRC32, `2` CRC32C |
| 25 | 4 | `bytesPerChecksum` (HBase default 16384) |
| 29 | 4 | `onDiskDataSizeWithHeader` – header + (possibly compressed) payload |
| 33 | N | payload, compressed when the trailer names a codec |

The checksums follow the payload:

```text
chunks = max(1, ceil(onDiskDataSizeWithHeader / bytesPerChecksum))
block  = header(33) + payload + chunks * 4 bytes of CRC32C (big endian)
```

Checksums cover the block **including** the header, chunk by chunk. They always cover the bytes
as stored, i.e. the compressed payload.

`prevBlockOffset` forms a backwards linked list of data blocks. Readers do not need it, but HBase
uses it when a block is cached after a seek.

## Cells inside a data block

A data block holds a run of `KeyValue`s with no length prefix; the layout of one cell is:

```text
rowLen(2) row  familyLen(1) family  qualifier  timestamp(8, desc) type(1) valueLen(4) value
[tagsLen(2) tags]     only when the file sets hfile.MAX_TAGS_LEN
mvcc(vlong)           only when KEY_VALUE_VERSION == 1 (a.k.a. includes_mvcc)
```

Cells inside one block are sorted with `CellComparator` (row, family, qualifier ascending;
timestamp and type descending). The `mvcc` trailer is what `hfile2arrow` reports as `seq`.

Encoded blocks store a 2 byte big-endian encoding id in front of the payload and use a completely
different cell layout; see below.

## Data block encodings

| id | `DataBlockEncoding` | cell layout | this crate |
|---:|---------------------|-------------|------------|
| 0 | `NONE` | raw cells as described above, block magic `DATABLK*` | read + write |
| 2 | `PREFIX` | previous key is a prefix of the current key; key deltas are stored | read + write |
| 3 | `DIFF` | shared prefix plus separate timestamp/type/value dedup | read |
| 4 | `FAST_DIFF` | like `DIFF`, tuned for decoding speed | read |
| 7 | `ROW_INDEX_V1` | raw cells plus a per-row offset index (HBase 2 default) | read + write |

The encoding is **not** stored in the trailer. HBase keeps it in the file info under
`DATA_BLOCK_ENCODING` (`HFileDataBlockEncoderImpl.createFromFileInfo`); a reader that ignores
the key decodes `DATABLKE` blocks as if they were plain cells. `NONE` is never recorded.

## Index blocks

The root index is a protobuf message (`HFileBlockIndex.BlockIndex`):

```text
entry[]  { key  : first key of the referenced block
           offset : block offset
           onDiskSize : block size including header, used for prefetching }
firstKey, numEntries
```

This writer emits a **single level** index: one entry per data block, pointing directly at the
data block, and `numDataIndexLevels = 1`. The reader also understands multi level indexes
(`numDataIndexLevels > 1`, root entries point at `IDXINTE2` blocks), which HBase uses once the
root index itself gets large.

## File info

The file info block is a protobuf map of string keys to byte values. Keys written here:

| key | value |
|-----|-------|
| `KEY_VALUE_VERSION` | `1` – cells carry a trailing mvcc vlong |
| `MAX_SEQ_ID_KEY` | highest sequence id in the file |
| `MAJOR_COMPACTION_KEY` | `0` – store file marker written by `StoreFileWriter` |
| `MAX_MEMSTORE_TS_KEY` | highest memstore timestamp (same value as above) |
| `hfile.AVG_KEY_LEN`, `hfile.AVG_VALUE_LEN` | averages, used for sizing decisions |
| `hfile.CREATE_TIME_TS` | creation time in ms |
| `hfile.KEY_OF_BIGGEST_CELL`, `hfile.LEN_OF_BIGGEST_CELL` | biggest cell, for block sizing |
| `hfile.LASTKEY` | key of the last cell, so a reader can find the end without a scan |
| `hfile.MAX_TAGS_LEN`, `hfile.TAGS_COMPRESSED` | written when cells carry tags (`TAGS_COMPRESSED = 0`) |
| `DATA_BLOCK_ENCODING` | encoding name, only for encodings other than `NONE` |

`hfile2arrow` uses `KEY_VALUE_VERSION` to decide whether cells carry an mvcc value and
`hfile.MAX_TAGS_LEN` (written by HBase when tags are present) to decide whether cells carry tags.

## Trailer

The last 4096 bytes of the file (`FixedFileTrailer`):

```text
magic "TRABLK\"$"            8
varint length + FileTrailerProto
zero padding
materialized version         4   (minor << 24) | major
```

`FileTrailerProto` carries `fileInfoOffset`, `loadOnOpenDataOffset`,
`uncompressedDataIndexSize`, `totalUncompressedBytes`, `dataIndexCount`, `metaIndexCount`,
`entryCount`, `numDataIndexLevels`, `firstDataBlockOffset`, `lastDataBlockOffset`,
`comparatorClassName`, `compressionCodec` and an optional `encryptionKey`.

This crate writes `major = 3`, `minor = 3` – the same pair HBase 2.x reports as `version=3.3` –
and the legacy comparator name `org.apache.hadoop.hbase.KeyValue$KVComparator`, exactly like
HBase does for plain `KeyValue` cells.

## Compression

`Compression.Algorithm` ordinals are **declaration order**, not alphabetical, and `BZIP2` was
inserted in the middle:

| ordinal | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---------|---|---|---|---|---|---|---|---|---|
| algorithm | LZO | GZ | NONE | SNAPPY | LZ4 | BZIP2 | ZSTD | LZMA | BROTLI |

Writing `ZSTD` as ordinal `5` makes HBase pick the BZIP2 codec, which fails with
`BZip2DummyDecompressor.finished` – a bug this crate had and now has a regression test for.

`GZ` payloads are plain gzip containers. `ZSTD` payloads depend on the codec implementation:

```text
raw        <zstd frame>                                  Hadoop ZStandardCodec (HBase default)
hbase      <uncompressedSize:4><frameSize:4><zstd frame> hbase-compression-zstd (pure Java)
```

The two are mutually incompatible, so the framing must be chosen at write time
(`WriterOptions::zstd_framing`, CLI `--zstd-framing`). The reader detects the framing from the
payload: a zstd frame always starts with `28 b5 2f fd`, and the `hbase` framing is
self-describing because it repeats the block's uncompressed size.

## Deliberate omissions

* bloom filters (`BLMFBLK2`), meta blocks and `load on open` extensions other than the index
* encryption (`Encryption.Context`)
* `LZO`, `SNAPPY`, `LZ4`, `BZIP2`, `LZMA`, `BROTLI` – reported as unsupported instead of being
  decoded with the wrong framing
* writing multi level indexes and the `DIFF` / `FAST_DIFF` encoders
