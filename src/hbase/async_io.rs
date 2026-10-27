//! Tokio flavored HFile reader and writer.
//!
//! The format work is shared with the blocking [`HFileReader`](super::HFileReader) and
//! [`HFileWriter`](super::HFileWriter); what changes here is where the bytes come from and where
//! they go:
//!
//! * [`AsyncHFileReader`] reads the trailer, the load-on-open section and the data blocks
//!   through `tokio::fs::File`, so opening and scanning never blocks a runtime thread.
//! * [`AsyncHFileWriter`] drives the I/O free core
//!   [`HFileWriter<Vec<u8>>`](super::HFileWriter) and drains exactly one block at a time into a
//!   `tokio::fs::File`. Memory stays at one block regardless of file size.
//!
//! Both speak the same on-disk format as the blocking types, so files are interchangeable:
//! an async written file can be opened with `HFileReader::open` and vice versa.
//!
//! ```no_run
//! use hfile_rs::hbase::{AsyncHFileReader, AsyncHFileWriter, Cell, WriterOptions};
//!
//! # async fn example() -> hfile_rs::Result<()> {
//! let mut writer = AsyncHFileWriter::create("/tmp/async.hfile", WriterOptions::default()).await?;
//! writer
//!     .append(Cell::new(b"row1".to_vec(), b"f".to_vec(), b"q".to_vec(), b"v".to_vec()))
//!     .await?;
//! writer.finish().await?;
//!
//! let reader = AsyncHFileReader::open("/tmp/async.hfile").await?;
//! assert_eq!(reader.entry_count(), 1);
//! # Ok(())
//! # }
//! ```

use crate::error::{Result, corrupt, unsupported};
use crate::hbase::block::{
    Block, BlockHeader, BlockType, FETCH_CHUNK, HEADER_SIZE, decode_block_bytes,
};
use crate::hbase::bloom::BloomFilter;
use crate::hbase::compression::Compression;
use crate::hbase::encoding::{DataBlockEncoding, DecodeOptions, decode_data_block};
use crate::hbase::file_info::FileInfo;
use crate::hbase::index::{RootIndex, parse_non_root_index, parse_root_index, search};
use crate::hbase::kv::{Cell, CellType, compare_cells};
use crate::hbase::reader::ReaderOptions;
use crate::hbase::trailer::{FileTrailer, TRAILER_SIZE};
use crate::hbase::writer::{HFileWriter, WriterOptions};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// positioned reads
// ---------------------------------------------------------------------------

/// Read `buf.len()` bytes at `offset`.
///
/// Tokio has no `pread`, so this is a seek plus a read. `&mut File` keeps the position private
/// to the call: callers serialize through the reader's mutex.
async fn read_at(file: &mut File, buf: &mut [u8], offset: u64) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.read_exact(buf).await?;
    Ok(())
}

/// Read the header and the payload of the block at `offset`.
///
/// Like the blocking reader, the first read fetches [`FETCH_CHUNK`] bytes so that header and
/// payload usually arrive in one syscall.
async fn read_raw_block(
    file: &mut File,
    file_size: u64,
    offset: u64,
) -> Result<(BlockHeader, Vec<u8>)> {
    if offset + HEADER_SIZE as u64 > file_size {
        return corrupt(format!(
            "block header at {offset} does not fit inside the file (size {file_size})"
        ));
    }
    let first = file_size.saturating_sub(offset).min(FETCH_CHUNK);
    let mut raw = vec![0u8; first as usize];
    read_at(file, &mut raw, offset).await?;
    let header = BlockHeader::parse(&raw)?;
    let total = header.total_size();
    if offset + total > file_size {
        return corrupt(format!(
            "block at {offset} claims {total} bytes but the file ends at {file_size}"
        ));
    }
    raw.resize(total as usize, 0);
    if (first as usize) < raw.len() {
        // Rare: the block is larger than FETCH_CHUNK (big cells, poor compression ratio).
        read_at(file, &mut raw[first as usize..], offset + first).await?;
    }
    Ok((header, raw))
}

fn check_expected(header: BlockHeader, offset: u64, expected: Option<BlockType>) -> Result<()> {
    if let Some(expected) = expected
        && !(header.block_type == expected
            || (expected == BlockType::Data && header.block_type.is_data()))
    {
        return corrupt(format!(
            "expected block {expected:?} at offset {offset} but found {:?}",
            header.block_type
        ));
    }
    Ok(())
}

/// Read, verify and decompress the block at `offset`.
async fn read_block_at(
    file: &mut File,
    file_size: u64,
    offset: u64,
    compression: Compression,
    verify_checksums: bool,
    expected: Option<BlockType>,
) -> Result<Block> {
    let (header, raw) = read_raw_block(file, file_size, offset).await?;
    check_expected(header, offset, expected)?;
    decode_block_bytes(&raw, offset, header, compression, verify_checksums)
}

/// The meta index: the second `ROOT_INDEX` block of the load-on-open section.
async fn read_meta_index(
    file: &mut File,
    file_size: u64,
    offset: u64,
    count: u32,
    compression: Compression,
    verify_checksums: bool,
) -> Result<RootIndex> {
    let empty = || RootIndex {
        entries: Vec::new(),
        mid_key_meta: None,
    };
    let end = file_size.saturating_sub(TRAILER_SIZE as u64);
    if offset + HEADER_SIZE as u64 > end {
        return if count == 0 {
            Ok(empty())
        } else {
            corrupt("file declares meta blocks but has no meta index block")
        };
    }
    let (header, raw) = read_raw_block(file, file_size, offset).await?;
    if header.block_type != BlockType::RootIndex {
        return if count == 0 {
            Ok(empty())
        } else {
            corrupt(format!(
                "expected the meta index block at {offset}, found {:?}",
                header.block_type
            ))
        };
    }
    let block = decode_block_bytes(&raw, offset, header, compression, verify_checksums)?;
    parse_root_index(&block.payload, count)
}

/// Walk the load-on-open blocks after the file info and parse the bloom filter metadata.
async fn read_bloom_filter(
    file: &mut File,
    file_size: u64,
    start: u64,
    compression: Compression,
    verify_checksums: bool,
) -> Result<Option<BloomFilter>> {
    let end = file_size.saturating_sub(TRAILER_SIZE as u64);
    let mut offset = start;
    while offset + HEADER_SIZE as u64 <= end {
        let (header, raw) = read_raw_block(file, file_size, offset).await?;
        let block = decode_block_bytes(&raw, offset, header, compression, verify_checksums)?;
        if block.block_type == BlockType::GeneralBloomMeta {
            return Ok(Some(BloomFilter::parse(&block.payload)?));
        }
        offset += block.total_size();
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// reader
// ---------------------------------------------------------------------------

/// A read-only HFile with a tokio based I/O path.
///
/// Opening mirrors [`HFileReader`](super::HFileReader): trailer, file info, index, meta index,
/// bloom metadata, then a peek at the first data block for the encoding. The parsed metadata is
/// kept in memory; data blocks, meta blocks and bloom chunks are read on demand.
pub struct AsyncHFileReader {
    /// Serialized so that `&self` methods can seek and read without `&mut self`.
    file: tokio::sync::Mutex<File>,
    path: PathBuf,
    file_size: u64,
    trailer: FileTrailer,
    file_info: FileInfo,
    compression: Compression,
    encoding: DataBlockEncoding,
    decode_options: DecodeOptions,
    root_index: RootIndex,
    meta_index: RootIndex,
    bloom: Option<BloomFilter>,
    options: ReaderOptions,
}

impl AsyncHFileReader {
    /// Open a file with checksum verification enabled.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, ReaderOptions::default()).await
    }

    pub async fn open_with_options(path: impl AsRef<Path>, options: ReaderOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path).await?;
        let file_size = file.metadata().await?.len();

        if file_size < (TRAILER_SIZE as u64).min(64) {
            return corrupt("file is too small to contain an HFile trailer");
        }
        let read_len = (TRAILER_SIZE as u64).min(file_size) as usize;
        let mut tail = vec![0u8; read_len];
        read_at(&mut file, &mut tail, file_size - read_len as u64).await?;
        let trailer = FileTrailer::parse_tail(&tail)?;
        let compression = Compression::from_ordinal(trailer.compression_codec)?;

        let file_info_block = read_block_at(
            &mut file,
            file_size,
            trailer.file_info_offset,
            compression,
            options.verify_checksums,
            Some(BlockType::FileInfo),
        )
        .await?;
        let file_info = FileInfo::parse(&file_info_block.payload)?;
        if file_info.tags_compressed() {
            return unsupported(
                "tags are compressed with a dictionary (hfile.TAGS_COMPRESSED); this file cannot be read",
            );
        }

        let root_block = read_block_at(
            &mut file,
            file_size,
            trailer.load_on_open_data_offset,
            compression,
            options.verify_checksums,
            Some(BlockType::RootIndex),
        )
        .await?;
        let root_index = parse_root_index(&root_block.payload, trailer.data_index_count)?;

        let meta_index = read_meta_index(
            &mut file,
            file_size,
            trailer.load_on_open_data_offset + root_block.total_size(),
            trailer.meta_index_count,
            compression,
            options.verify_checksums,
        )
        .await?;

        let bloom = read_bloom_filter(
            &mut file,
            file_size,
            trailer.file_info_offset + file_info_block.total_size(),
            compression,
            options.verify_checksums,
        )
        .await?;

        let decode_options = DecodeOptions {
            includes_tags: file_info.includes_tags(),
            includes_mvcc: file_info.includes_mvcc(),
        };

        let mut reader = AsyncHFileReader {
            file: tokio::sync::Mutex::new(file),
            path,
            file_size,
            trailer,
            file_info,
            compression,
            encoding: DataBlockEncoding::None,
            decode_options,
            root_index,
            meta_index,
            bloom,
            options,
        };
        reader.encoding = reader.detect_data_block_encoding().await?;
        Ok(reader)
    }

    async fn detect_data_block_encoding(&self) -> Result<DataBlockEncoding> {
        if self.trailer.entry_count == 0 || self.root_index.entries.is_empty() {
            return Ok(DataBlockEncoding::None);
        }
        let offset = self.trailer.first_data_block_offset;
        let mut file = self.file.lock().await;
        let (header, raw) = read_raw_block(&mut file, self.file_size, offset).await?;
        if header.block_type != BlockType::EncodedData {
            return Ok(DataBlockEncoding::None);
        }
        let block = decode_block_bytes(
            &raw,
            offset,
            header,
            self.compression,
            self.options.verify_checksums,
        )?;
        if block.payload.len() < 2 {
            return corrupt("encoded data block is too short to hold an encoding id");
        }
        DataBlockEncoding::from_id(u16::from_be_bytes([block.payload[0], block.payload[1]]))
    }

    // -----------------------------------------------------------------------
    // metadata
    // -----------------------------------------------------------------------

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn trailer(&self) -> &FileTrailer {
        &self.trailer
    }

    pub fn file_info(&self) -> &FileInfo {
        &self.file_info
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }

    pub fn data_block_encoding(&self) -> DataBlockEncoding {
        self.encoding
    }

    pub fn includes_tags(&self) -> bool {
        self.decode_options.includes_tags
    }

    pub fn includes_mvcc(&self) -> bool {
        self.decode_options.includes_mvcc
    }

    pub fn entry_count(&self) -> u64 {
        self.trailer.entry_count
    }

    pub fn root_index(&self) -> &RootIndex {
        &self.root_index
    }

    /// Number of data blocks, derived from the index when the file uses a single index level.
    pub fn data_block_count(&self) -> u64 {
        if self.trailer.num_data_index_levels <= 1 {
            self.root_index.entries.len() as u64
        } else {
            self.trailer.data_index_count as u64
        }
    }

    /// Names of the meta blocks, in meta index order.
    pub fn meta_block_names(&self) -> impl Iterator<Item = &[u8]> {
        self.meta_index.entries.iter().map(|e| e.key.as_slice())
    }

    pub fn meta_block_count(&self) -> usize {
        self.meta_index.entries.len()
    }

    /// Payload of the meta block called `name`, or `None` when the file has no such block.
    pub async fn meta_block(&self, name: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(entry) = self.meta_index.entries.iter().find(|e| e.key == name) else {
            return Ok(None);
        };
        let block = self
            .read_block_expected(entry.offset, Some(BlockType::Meta))
            .await?;
        Ok(Some(block.payload))
    }

    /// Bloom filter metadata, when the file carries one.
    pub fn bloom_filter(&self) -> Option<&BloomFilter> {
        self.bloom.as_ref()
    }

    /// Whether the row may be present according to the bloom filter.
    ///
    /// `false` is definitive; `true` means "maybe" and is also the answer for a file without a
    /// bloom filter. Only the chunk that can hold the row is read.
    pub async fn may_contain_row(&self, row: &[u8]) -> Result<bool> {
        let Some(bloom) = &self.bloom else {
            return Ok(true);
        };
        let Some(index) = bloom.chunk_for(row) else {
            return Ok(false);
        };
        let chunk = &bloom.chunks[index];
        let block = self.read_block(chunk.offset).await?;
        if block.block_type != BlockType::BloomChunk {
            return corrupt(format!(
                "bloom chunk {index} points at a {:?} block",
                block.block_type
            ));
        }
        Ok(bloom.contains_in_chunk(&block.payload, row))
    }

    // -----------------------------------------------------------------------
    // blocks
    // -----------------------------------------------------------------------

    async fn read_block(&self, offset: u64) -> Result<Block> {
        self.read_block_expected(offset, None).await
    }

    async fn read_block_expected(&self, offset: u64, expected: Option<BlockType>) -> Result<Block> {
        let mut file = self.file.lock().await;
        let (header, raw) = read_raw_block(&mut file, self.file_size, offset).await?;
        check_expected(header, offset, expected)?;
        decode_block_bytes(
            &raw,
            offset,
            header,
            self.compression,
            self.options.verify_checksums,
        )
    }

    /// Read and decode the data block at `offset`.
    pub async fn read_data_block(&self, offset: u64) -> Result<Vec<Cell>> {
        let block = self.read_block(offset).await?;
        if !block.block_type.is_data() {
            return corrupt(format!(
                "expected a data block at {} but found {:?}",
                block.offset, block.block_type
            ));
        }
        decode_data_block(block.block_type, &block.payload, self.decode_options)
    }

    // -----------------------------------------------------------------------
    // scanning
    // -----------------------------------------------------------------------

    /// Visit every cell in file order, one block at a time.
    ///
    /// No index is consulted, so this works even when the index is damaged. Memory usage is one
    /// block regardless of the file size, which is what makes this the right method for large
    /// scans; [`Self::read_all`] is the collected version.
    pub async fn for_each<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Cell) -> Result<()>,
    {
        if self.trailer.entry_count == 0 {
            return Ok(());
        }
        let mut offset = self.trailer.first_data_block_offset;
        while offset <= self.trailer.last_data_block_offset {
            let block = self.read_block(offset).await?;
            // The next offset follows from this block's size, no header round trip needed.
            let next = offset + block.header.total_size();
            if block.block_type.is_data() {
                for cell in
                    decode_data_block(block.block_type, &block.payload, self.decode_options)?
                {
                    f(cell)?;
                }
            }
            offset = next;
        }
        Ok(())
    }

    /// Visit every data block in file order, yielding its cells.
    ///
    /// The callback returns `false` to stop early. This is the block level counterpart of
    /// [`Self::for_each`]: a caller that writes batches out as they are produced never has more
    /// than one block of cells in memory.
    pub async fn for_each_block<F>(&self, mut f: F) -> Result<()>
    where
        F: AsyncFnMut(&Block, Vec<Cell>) -> Result<bool>,
    {
        if self.trailer.entry_count == 0 {
            return Ok(());
        }
        let mut offset = self.trailer.first_data_block_offset;
        while offset <= self.trailer.last_data_block_offset {
            let block = self.read_block(offset).await?;
            let next = offset + block.header.total_size();
            let cells = if block.block_type.is_data() {
                decode_data_block(block.block_type, &block.payload, self.decode_options)?
            } else {
                // Leaf index blocks may be interleaved with data blocks.
                Vec::new()
            };
            if !f(&block, cells).await? {
                return Ok(());
            }
            offset = next;
        }
        Ok(())
    }

    /// Collect all cells into a vector (convenience for small files and tests).
    pub async fn read_all(&self) -> Result<Vec<Cell>> {
        let mut cells = Vec::new();
        self.for_each(|cell| {
            cells.push(cell);
            Ok(())
        })
        .await?;
        Ok(cells)
    }

    /// First cell with a key greater than or equal to `key` (flat key layout).
    ///
    /// Walks the block index, so this reads one index block per level plus one data block.
    pub async fn seek(&self, key: &[u8]) -> Result<Option<Cell>> {
        if self.trailer.entry_count == 0 || self.root_index.entries.is_empty() {
            return Ok(None);
        }
        let Some(mut entry) = self
            .root_index
            .containing(key)
            .map(|i| self.root_index.entries[i].clone())
        else {
            // The key sorts before the first index entry: only the first data block can hold a
            // cell at or after it.
            return self
                .find_from(self.trailer.first_data_block_offset, key)
                .await;
        };
        for _ in 0..8 {
            let block = self.read_block(entry.offset).await?;
            if block.block_type.is_data() {
                return self.find_from(entry.offset, key).await;
            }
            let entries = parse_non_root_index(&block.payload)?;
            match search(&entries, key) {
                Some(i) => entry = entries[i].clone(),
                None => return self.find_from(entry.offset, key).await,
            }
        }
        corrupt("block index tree is too deep")
    }

    /// First cell at or after `key`, walking data blocks from `start`.
    ///
    /// See [`HFileReader::seek`](super::HFileReader::seek) for why the index may point one block
    /// early (an entry key carries a timestamp, which sorts descending).
    async fn find_from(&self, start: u64, key: &[u8]) -> Result<Option<Cell>> {
        let target = Cell::from_key_bytes(key).unwrap_or_default();
        let mut offset = start;
        while offset <= self.trailer.last_data_block_offset {
            let block = self.read_block(offset).await?;
            let next = offset + block.header.total_size();
            if block.block_type.is_data() {
                let cells =
                    decode_data_block(block.block_type, &block.payload, self.decode_options)?;
                if let Some(found) = cells
                    .into_iter()
                    .find(|cell| compare_cells(cell, &target) != std::cmp::Ordering::Less)
                {
                    return Ok(Some(found));
                }
            }
            offset = next;
        }
        Ok(None)
    }

    /// Look up a single cell by row/family/qualifier, returning the newest version.
    pub async fn get(&self, row: &[u8], family: &[u8], qualifier: &[u8]) -> Result<Option<Cell>> {
        let mut key = Cell::new(
            row.to_vec(),
            family.to_vec(),
            qualifier.to_vec(),
            Vec::new(),
        );
        key.timestamp = i64::MAX;
        key.cell_type = CellType::Maximum;
        let flat = key.key_bytes();
        let found = self.seek(&flat).await?;
        Ok(found.filter(|c| c.row == row && c.family == family && c.qualifier == qualifier))
    }
}

// ---------------------------------------------------------------------------
// writer
// ---------------------------------------------------------------------------

/// A streaming HFile writer with a tokio based I/O path.
///
/// The format logic lives in [`HFileWriter<Vec<u8>>`](super::HFileWriter); this type only moves
/// finished blocks to disk. Appending a cell can write at most one block, so each `append` does
/// at most one `write_all` and never buffers more than a block.
///
/// [`finish`](Self::finish) is mandatory: it writes the index, file info and trailer.
pub struct AsyncHFileWriter {
    core: HFileWriter<Vec<u8>>,
    file: File,
    path: PathBuf,
}

impl AsyncHFileWriter {
    /// Create (or truncate) `path`.
    ///
    /// The options are validated before the file is touched, so an invalid configuration never
    /// destroys an existing file.
    pub async fn create(path: impl AsRef<Path>, options: WriterOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let core = HFileWriter::with_sink(Vec::new(), options)?;
        let file = File::create(&path).await?;
        Ok(AsyncHFileWriter { core, file, path })
    }

    /// Path the file is written to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of cells appended so far.
    pub fn entry_count(&self) -> u64 {
        self.core.entry_count()
    }

    /// Append a cell. Cells must arrive in `CellComparator` order.
    ///
    /// When the append completes a data block, that block is written to the file before this
    /// returns; otherwise nothing is written.
    pub async fn append(&mut self, cell: Cell) -> Result<()> {
        self.core.append(cell)?;
        self.drain().await
    }

    /// Append many cells.
    pub async fn append_all<I>(&mut self, cells: I) -> Result<()>
    where
        I: IntoIterator<Item = Cell>,
    {
        for cell in cells {
            self.append(cell).await?;
        }
        Ok(())
    }

    /// Queue a meta block (`HFile.Writer#appendMetaBlock`).
    ///
    /// Meta blocks are written by [`Self::finish`] together with the rest of the load-on-open
    /// section, in name order, so nothing is written here and this is not `async`. Names must be
    /// non-empty; a repeated name replaces the earlier payload, like `HFileWriterImpl`
    /// overwriting `metaData`.
    pub fn append_meta_block(
        &mut self,
        name: impl Into<Vec<u8>>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<()> {
        self.core.append_meta_block(name, payload)
    }

    /// Write one block (or the load-on-open section) to disk.
    async fn drain(&mut self) -> Result<()> {
        let bytes = self.core.take_buffered();
        if bytes.is_empty() {
            return Ok(());
        }
        self.file.write_all(&bytes).await?;
        Ok(())
    }

    /// Finish the file: flush the last block, write the load-on-open section and the trailer.
    pub async fn finish(mut self) -> Result<()> {
        let tail = self.core.finish_into_sink()?;
        if !tail.is_empty() {
            self.file.write_all(&tail).await?;
        }
        self.file.flush().await?;
        Ok(())
    }
}
