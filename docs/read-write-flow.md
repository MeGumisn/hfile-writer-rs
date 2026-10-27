# Read and write flow

> [English](read-write-flow.md) | [简体中文](read-write-flow.zh-CN.md)

How `HFileReader` and `HFileWriter` walk a file. Both mirror the class of the same name in HBase,
so a stack trace from HBase can usually be mapped onto this crate one to one.

## Reading

`HFileReader::open` follows `HFile.createReader` + `HFileInfo.initMetaAndIndex`:

```text
1. read the last 4096 bytes                -> FileTrailer (offsets, counts, codec)
2. read the FILEINF2 block                 -> tags/mvcc flags, last key, statistics
3. read the IDXROOT2 block                 -> block index tree
4. read the meta index                     -> meta block names (second IDXROOT2, always present)
5. walk the rest of load on open           -> BLMFMET2 bloom filter metadata, if any
6. peek at the first data block            -> DataBlockEncoding (from the block, not the trailer)
7. build DecodeOptions { includes_tags, includes_mvcc } from the file info
```

Bloom chunks are read on demand like `CompoundBloomFilter` does: `may_contain_row(row)` picks the
chunk by comparator order and reads only that `BLMFBLK2` block. Meta blocks are read on demand too,
through `meta_block(name)`.

Every block that is read is checksum verified by default (`ReaderOptions::verify_checksums`); pass
`--no-checksum` to `hfile info` to skip it when you want to inspect a damaged file.

### Sequential scan

`iter()` walks the data section block by block, exactly like `HFileBlock.BlockIterator`:

```text
first_data_block_offset
  -> read block, decode cells, yield them
  -> next offset = offset + header.total_size()   (header + payload + checksums)
  -> stop once the offset passes last_data_block_offset
```

No index is used, so a scan also works when the index is broken. Cells come out in comparator
order and are decoded lazily per block: memory usage is one block, not the whole file.

### Index lookup

`seek(flat_key)` and `get(row, family, qualifier)` walk the index tree instead:

```text
search root entries for the last key <= target
  -> entry points at a data block   -> decode it and return the first cell >= target
  -> entry points at an index block -> repeat with that (multi level indexes)
```

`get` builds the flat key `row/family/qualifier/timestamp=MAX/type=Maximum` and filters the result
by prefix, which yields the newest version of a cell.

An index entry is the *first* key of its block and carries a timestamp, and timestamps sort
descending, so a lookup key with `timestamp = MAX` compares smaller than an entry of the same row.
The index can therefore point one block early, and a key below the first entry points nowhere at
all. `seek` walks the block chain from the located block (or from the first data block) until it
finds a cell at or after the key, so a lookup reads one index block per level plus one data block
in the common case and a few more only when the key falls between two index entries.

### Decoding a data block

`decode_data_block` dispatches on the block magic:

* `DATABLK*` – cells are stored back to back (`parse_raw_cells`), using the file info flags to
  know whether a tags length and an mvcc vlong are present.
* `DATABLKE` – the payload starts with a 2 byte encoding id; `PREFIX`, `DIFF`, `FAST_DIFF` and
  `ROW_INDEX_V1` decoders then rebuild the cells.

## Writing

`HFileWriter` is a streaming writer: cells are appended one at a time and flushed into blocks as
the buffer fills.

```text
create(path, options)      validate options, truncate file, reserve offset 0
append(cell)               - reject cells that break comparator order
                           - encode into the current block (encoder buffer)
                           - update statistics (avg lengths, max seq id, biggest cell, ...)
                           - if buffer >= block_size: flush_data_block()
flush_data_block()         build_block(...) -> append to file, record an index entry with the
                           block's first key and offset
append_meta_block(name,p)  store an opaque payload, written by finish() in name order
finish()                   flush the last block, write the bloom chunks, the meta blocks, the
                           root index, the meta index, the file info, the bloom metadata and the
                           trailer, then pad to 4096 bytes
```

Notes:

* **Ordering is the caller's job.** `append` returns an error as soon as a cell would break the
  order, which is cheaper than producing an unreadable file. `write_hfile` sorts its input with
  `compare_cells` first.
* **Block size is a target, not a limit.** A block is flushed once the encoded buffer reaches
  `block_size`; a single huge cell still produces a huge block.
* **`includes_mvcc` defaults to `true`**, matching `HFileContext`. It writes
  `KEY_VALUE_VERSION = 1` and a vlong mvcc after every cell, which is what HBase expects for a
  bulk loaded store file. Set it to `false` only if you know the reader does not need it.
* **Store file metadata** (`MAX_SEQ_ID_KEY`, `MAJOR_COMPACTION_KEY`) is written by default so the
  output looks like a `StoreFileWriter` product; `write_store_file_metadata = false` turns it off.
* **The index is written last**, so its entries already know the final block offsets. This writer
  emits a single level index; HBase switches to multi level once the root index gets large.
* **Bloom chunks are inline blocks.** They are written into the data section (below
  `load_on_open_data_offset`) at `finish()`, and their bit arrays are compressed like any other
  block. One entry is stored per distinct row, so appending the same row's qualifiers does not
  inflate the filter. `WriterOptions::bloom_filter` enables it; chunking, folding and the murmur
  hashing are HBase's, see [hfile-format.md](hfile-format.md#bloom-filters).
* **`finish()` is mandatory.** Dropping the writer without calling it leaves a file without a
  trailer, which no reader accepts.

### The writer core is I/O free

`HFileWriter<W: Write>` only ever appends, so it needs no seeking and no file handle of its own:

| entry point | sink | typical caller |
|-------------|------|----------------|
| `HFileWriter::create(path, options)` | `BufWriter<File>` (1 MiB) | CLI, bulk load, tests |
| `HFileWriter::with_sink(out, options)` | any `W: Write`, used as is | in-memory buffers, async wrappers |
| `take_buffered()` | – (`HFileWriter<Vec<u8>>` only) | hands out the bytes of one finished block |
| `finish()` / `finish_into_sink()` | – | finish; the second returns the sink |

`create` validates the options before it truncates the file, so an invalid configuration never
destroys an existing file. `take_buffered` returns an empty `Vec` when the last append did not
complete a block, which is what makes the block-by-block drain in the async writer cheap.

## Async I/O

`src/hbase/async_io.rs` adds `AsyncHFileReader` and `AsyncHFileWriter`, thin tokio layers over the
same parsing and formatting code. Neither changes the format: an async written file opens with
`HFileReader::open` and a blocking written file opens with `AsyncHFileReader::open`.

Reading: `AsyncHFileReader::open` runs the same sequence as the blocking reader (trailer, file
info, root index, meta index, bloom metadata, first block peek) with `tokio::fs::File`. A
positioned read becomes `seek` + `read_exact`; the reader serializes those through a `Mutex` so
that `&self` methods such as `read_all`, `for_each`, `get` and `may_contain_row` keep working.
Data blocks, meta blocks and bloom chunks are read on demand exactly like the blocking reader.

Writing: `AsyncHFileWriter::create` builds a `HFileWriter<Vec<u8>>` (validating the options before
the file is created), then `append` does two things:

```text
core.append(cell)        -> encoder buffer; writes a whole block into the Vec when it fills
drain()                  -> take_buffered(): empty -> nothing to do
                                              non-empty -> one write_all to the tokio File
```

`finish` writes the load-on-open section and the trailer through the same drain, then flushes. The
result is one `write_all` per block and a memory ceiling of one block plus the encoder buffer,
independent of the file size. Ordering, block sizing, encoding, compression, bloom filters and
meta blocks are all handled by the core, so every `WriterOptions` field behaves the same as in the
blocking writer.

## Error handling

`HFileError` distinguishes the cases a caller usually wants to treat differently:

| variant | meaning |
|---------|---------|
| `Io` | read/write failure |
| `Corrupt` | checksum mismatch, bad magic, impossible sizes, undecodable payload |
| `Unsupported` | valid HFile feature this crate does not implement (e.g. `LZ4`, compressed tags) |
| `InvalidArgument` | caller error: unsorted cells, unknown encoding name, empty input |
| `Proto` | malformed trailer or file info protobuf |
| `Arrow` | Parquet/Arrow conversion failure |

A corrupted block surfaces as `Corrupt` and names the chunk that failed; `Unsupported` is used
whenever the alternative would be silently returning wrong data.
