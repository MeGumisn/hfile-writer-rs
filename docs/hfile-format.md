# HFile v3 on-disk format

> [English](hfile-format.md) | [简体中文](hfile-format.zh-CN.md)

This is the subset of the HBase HFile format that this crate implements. Field names follow
`org.apache.hadoop.hbase.io.hfile`, so they can be cross-checked against the HBase source.

## File layout

```text
offset 0                       first data block            "DATABLK*" / "DATABLKE"
        ...                    more data blocks            chained through prevBlockOffset
        ...                    bloom chunk blocks          "BLMFBLK2"
        ...                    meta blocks                 "METABLKc"
last_data_block_offset         (last data block)
load_on_open_data_offset       root index block            "IDXROOT2"
                               meta index block            "IDXROOT2"
file_info_offset               file info block             "FILEINF2"
                               bloom filter metadata       "BLMFMET2"
filesize - 4096                fixed file trailer          "TRABLK\"$"
```

Everything before `load_on_open_data_offset` belongs to the data section and is only reachable
through the index; everything from `load_on_open_data_offset` to the trailer is the
"load on open" section (`HFileInfo.initMetaAndIndex`). The load on open section is read as a
sequential run of blocks: root index, meta index, file info, then any "additional" load on open
blocks such as the bloom filter metadata.

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

`SNAPPY` payloads are what `SnappyCodec` writes, i.e. Hadoop's `BlockCompressorStream` layout:

```text
<uncompressedSize:4 BE><compressedSize:4 BE><raw snappy bytes>...
```

The header is the number of bytes the block expands to. The chunk count is not stored, so a reader
consumes length-prefixed chunk pairs until it has produced that many bytes. An HFile block is a
single write followed by `finish()`, so a block below `bufferSize - (bufferSize/6 + 32)` with the
default 256 KiB buffersize (218422 bytes) is one chunk pair; larger blocks are split into several.
The chunk bytes themselves are raw snappy, not the snappy framing format.

Two codecs produce this layout and their output is interchangeable: Hadoop's native
`org.apache.hadoop.io.compress.SnappyCodec` and HBase's pure Java
`org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec` (`hbase.io.compress.snappy.codec`, which
deliberately mirrors it). Unlike `ZSTD`, no framing choice is needed.

## Meta blocks

`HFile.Writer#appendMetaBlock(name, payload)` stores opaque bytes under a name. The blocks are
written as `METABLKc` right above the data blocks, in name order, and the meta index (a second
`IDXROOT2` block, holding `meta_index_count` entries of `(offset:long, onDiskSize:int, key)` in the
same layout as the data root index) maps name to location. The meta index is always written, even
when it lists nothing, and `FixedFileTrailer.metaIndexCount` still counts only `METABLKc` blocks —
the bloom filter metadata is not part of it. HBase's `getMetaBlock` returns the entry whose first
key is less than or equal to the requested name, so an unknown name yields a neighbouring block;
this crate returns `None` instead.

## Bloom filters

A `ROW` bloom filter is written in three places:

* one `BLMFBLK2` block per chunk, in the data section, whose payload is the bare bit array;
* a `BLMFMET2` block as the last load on open block, holding the shared parameters and a single
  level index of `(firstKey, offset, size)` per chunk;
* the file info key `BLOOM_FILTER_TYPE` = `ROW`.

The `BLMFMET2` payload (`CompoundBloomFilterWriter.MetaWriter`) is:

```text
<int VERSION=3><long totalByteSize><int hashCount><int hashType>
<long totalKeyCount><long totalMaxKeys><int numChunks>
<vint comparatorClassLength=0><byte array: 0 bytes>       # ROW needs no comparator
numChunks * ( <long offset><int onDiskSize><vint keyLength><firstKey> )
```

A row is placed in a chunk by comparator order (`firstKey` is the chunk's first row), hashed with
murmur2 (`h1 = hash(row, 0)`, `h2 = hash(row, h1)`, `hashType = 1`) and probes
`abs((h1 + i * h2) % (byteSize * 8))` for `i` in `0..hashCount`. Chunks are sized by
`io.storefile.bloom.block.size` (128 KiB of bits), which at the default 1% error rate holds about
109382 keys with 7 hash functions; a chunk that ends up far emptier than that is halved until
`maxKeys <= 2 * keyCount`, which is lossless because every probe is a modulo of a power of two.

Two details decide compatibility, and both are why this crate does not use a general purpose bloom
filter crate: the bit array is *flat* (not blocked) and the hash is HBase's `MurmurHash`, whose
final partial word shifts sign extended bytes. The `murmur2` crate is stock `MurmurHash2`, so it
agrees with HBase for every key whose length is a multiple of four and zero extends the leftover
bytes otherwise: `[0x80, 0x81, 0x82, 0x83, 0x84]` hashes to -1368132752 in HBase and -1237960112
there. Since a probe is `hash % bitSize`, such a hash puts rows in the wrong bits, so the tail is
assembled by hand here (see `src/hbase/bloom.rs`). `ROWCOL` filters hash a synthetic
`row/family/qualifier` cell key and are not implemented.

## Deliberate omissions

* `ROWCOL` and `ROWPREFIX_FIXED_LENGTH` bloom filters, and non-murmur hash types
* encryption (`Encryption.Context`)
* `LZO`, `LZ4`, `BZIP2`, `LZMA`, `BROTLI` – reported as unsupported instead of being
  decoded with the wrong framing
* writing multi level indexes and the `DIFF` / `FAST_DIFF` encoders
