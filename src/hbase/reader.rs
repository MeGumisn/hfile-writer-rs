//! HFile v3 reader.
//!
//! Open sequence (mirrors `HFileReaderImpl` + `HFileInfo`):
//!
//! ```text
//! 1. read + parse the 4096 byte trailer          -> offsets, counts, codec, comparator
//! 2. read the FILE_INFO block                    -> tags/mvcc flags, last key, stats
//! 3. read the ROOT_INDEX block                   -> block lookup tree
//! 4. peek at the first data block                -> data block encoding
//! 5. read the meta index (second ROOT_INDEX)     -> meta block names
//! 6. walk the rest of load-on-open               -> bloom filter metadata
//! ```
//!
//! Data blocks are located either through the index (`seek`) or by walking the data block
//! section sequentially (`iter`, identical to `HFileBlock.BlockIterator.nextBlock`).

use crate::error::{HFileError, Result, corrupt, unsupported};
use crate::hbase::block::{
    Block, BlockHeader, BlockType, DEFAULT_BLOCK_SIZE, HEADER_SIZE, decode_block_bytes,
    decode_block_payload, read_block_at, read_block_with_header, read_exact_at, read_header_at,
};
use crate::hbase::bloom::BloomFilter;
use crate::hbase::compression::Compression;
use crate::hbase::encoding::{DataBlockEncoding, DecodeOptions, decode_data_block};
use crate::hbase::file_info::FileInfo;
use crate::hbase::index::{IndexEntry, RootIndex, parse_non_root_index, parse_root_index, search};
use crate::hbase::kv::{Cell, CellType, compare_cells};
use crate::hbase::trailer::{FileTrailer, TRAILER_SIZE};
use std::fs::File;
use std::path::{Path, PathBuf};

/// Options controlling how a file is opened.
#[derive(Debug, Clone)]
pub struct ReaderOptions {
    /// Verify the CRC32C checksum of every block that is read.
    pub verify_checksums: bool,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        ReaderOptions {
            verify_checksums: true,
        }
    }
}

/// A read-only HFile.
pub struct HFileReader {
    file: File,
    path: PathBuf,
    file_size: u64,
    trailer: FileTrailer,
    file_info: FileInfo,
    compression: Compression,
    encoding: DataBlockEncoding,
    decode_options: DecodeOptions,
    root_index: RootIndex,
    /// Meta block index (`meta_index_count` entries of name -> block location).
    meta_index: RootIndex,
    /// Bloom filter metadata, when the file carries one.
    bloom: Option<BloomFilter>,
    options: ReaderOptions,
}

impl HFileReader {
    /// Open a file with checksum verification enabled.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, ReaderOptions::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: ReaderOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        let trailer = FileTrailer::read(&file, file_size)?;
        let compression = Compression::from_ordinal(trailer.compression_codec)?;

        let compression_hint = compression;
        let file_info_block = read_block_at(
            &file,
            file_size,
            trailer.file_info_offset,
            compression_hint,
            options.verify_checksums,
            Some(BlockType::FileInfo),
        )?;
        let file_info = FileInfo::parse(&file_info_block.payload)?;
        if file_info.tags_compressed() {
            return unsupported(
                "tags are compressed with a dictionary (hfile.TAGS_COMPRESSED); this file cannot be read",
            );
        }

        let root_block = read_block_at(
            &file,
            file_size,
            trailer.load_on_open_data_offset,
            compression_hint,
            options.verify_checksums,
            Some(BlockType::RootIndex),
        )?;
        let root_index = parse_root_index(&root_block.payload, trailer.data_index_count)?;

        // The meta index follows the data root index. `HFileInfo.initMetaAndIndex` reads it
        // unconditionally and HBase always writes it, even when it lists nothing.
        let meta_index = read_meta_index(
            &file,
            file_size,
            trailer.load_on_open_data_offset + root_block.total_size(),
            trailer.meta_index_count,
            compression_hint,
            options.verify_checksums,
        )?;

        // The remaining load-on-open blocks (after the file info) hold the bloom filter metadata.
        let bloom = read_bloom_filter(
            &file,
            file_size,
            trailer.file_info_offset + file_info_block.total_size(),
            compression_hint,
            options.verify_checksums,
        )?;

        let decode_options = DecodeOptions {
            includes_tags: file_info.includes_tags(),
            includes_mvcc: file_info.includes_mvcc(),
        };

        let mut reader = HFileReader {
            file,
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
        reader.encoding = reader.detect_data_block_encoding()?;
        Ok(reader)
    }

    fn detect_data_block_encoding(&self) -> Result<DataBlockEncoding> {
        if self.trailer.entry_count == 0 || self.root_index.entries.is_empty() {
            return Ok(DataBlockEncoding::None);
        }
        let offset = self.trailer.first_data_block_offset;
        let header = read_header_at(&self.file, self.file_size, offset)?;
        if header.block_type != BlockType::EncodedData {
            return Ok(DataBlockEncoding::None);
        }
        let block = self.read_block(offset)?;
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

    /// Number of meta blocks (`meta_index_count` in the trailer).
    pub fn meta_block_count(&self) -> usize {
        self.meta_index.entries.len()
    }

    /// Payload of the meta block called `name`, or `None` when the file has no such block.
    pub fn meta_block(&self, name: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(entry) = self.meta_index.entries.iter().find(|e| e.key == name) else {
            return Ok(None);
        };
        let block = read_block_at(
            &self.file,
            self.file_size,
            entry.offset,
            self.compression,
            self.options.verify_checksums,
            Some(BlockType::Meta),
        )?;
        Ok(Some(block.payload))
    }

    /// Bloom filter metadata, when the file carries one.
    pub fn bloom_filter(&self) -> Option<&BloomFilter> {
        self.bloom.as_ref()
    }

    /// Whether the row may be present according to the bloom filter.
    ///
    /// `false` is definitive (the row is not in this file); `true` means "maybe", and is also what
    /// a file without a bloom filter answers. Chunk blocks are read on demand, exactly like
    /// `CompoundBloomFilter`.
    pub fn may_contain_row(&self, row: &[u8]) -> Result<bool> {
        let Some(bloom) = &self.bloom else {
            return Ok(true);
        };
        let Some(index) = bloom.chunk_for(row) else {
            return Ok(false);
        };
        let chunk = &bloom.chunks[index];
        let block = self.read_block(chunk.offset)?;
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

    fn read_block(&self, offset: u64) -> Result<Block> {
        read_block_at(
            &self.file,
            self.file_size,
            offset,
            self.compression,
            self.options.verify_checksums,
            None,
        )
    }

    /// Read a block whose header the caller already parsed, avoiding a second `pread`.
    fn read_block_reusing_header(
        &self,
        offset: u64,
        header: crate::hbase::block::BlockHeader,
    ) -> Result<Block> {
        read_block_with_header(
            &self.file,
            self.file_size,
            offset,
            header,
            self.compression,
            self.options.verify_checksums,
            None,
        )
    }

    fn decode_block(&self, block: &Block) -> Result<Vec<Cell>> {
        if !block.block_type.is_data() {
            return corrupt(format!(
                "expected a data block at {} but found {:?}",
                block.offset, block.block_type
            ));
        }
        decode_data_block(block.block_type, &block.payload, self.decode_options)
    }

    /// Decode the cells of a block payload; leaf index blocks carry none.
    fn cells_of(&self, block_type: BlockType, payload: &[u8]) -> Result<std::vec::IntoIter<Cell>> {
        if block_type.is_data() {
            Ok(decode_data_block(block_type, payload, self.decode_options)?.into_iter())
        } else {
            Ok(Vec::new().into_iter())
        }
    }

    /// Read and decode the data block at `offset`.
    ///
    /// Single block access: use [`Self::iter`] or [`Self::for_each_block`] for scans, they read
    /// several blocks per syscall.
    pub fn read_data_block(&self, offset: u64) -> Result<Vec<Cell>> {
        let block = self.read_block(offset)?;
        self.decode_block(&block)
    }

    /// Visit every data block in file order, yielding its cells.
    pub fn for_each_block<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&Block, Vec<Cell>) -> Result<()>,
    {
        if self.trailer.entry_count == 0 {
            return Ok(());
        }
        let mut window = self.window();
        let mut offset = self.trailer.first_data_block_offset;
        while offset <= self.trailer.last_data_block_offset {
            let block = window.block(offset)?;
            // The next offset follows from this block's size, no header round trip needed.
            let next = offset + block.header.total_size();
            let cells = if block.block_type.is_data() {
                self.decode_block(&block)?
            } else {
                // Leaf index blocks may be interleaved with data blocks.
                Vec::new()
            };
            f(&block, cells)?;
            offset = next;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // scanning
    // -----------------------------------------------------------------------

    /// Iterate over all cells in file order.
    ///
    /// Files with more than a handful of blocks are read by a background thread, so I/O and
    /// decoding overlap. The lookahead is bounded, a scan that stops early releases the thread.
    pub fn iter(&self) -> CellIter<'_> {
        let feed = if self.trailer.entry_count > 0 && self.data_block_count() > PREFETCH_THRESHOLD {
            match Prefetcher::spawn(self) {
                Ok(prefetcher) => Feed::Prefetched(prefetcher),
                // No thread (fd limits, sandboxing): fall back to inline reads.
                Err(_) => Feed::Inline(self.window()),
            }
        } else {
            Feed::Inline(self.window())
        };
        CellIter {
            reader: self,
            feed,
            next_offset: if self.trailer.entry_count == 0 {
                u64::MAX
            } else {
                self.trailer.first_data_block_offset
            },
            pending: Vec::new().into_iter(),
            finished: self.trailer.entry_count == 0,
        }
    }

    fn window(&self) -> BlockWindow<'_> {
        BlockWindow::new(
            &self.file,
            self.file_size,
            self.compression,
            self.options.verify_checksums,
        )
    }

    /// Collect all cells into a vector (convenience for small files and tests).
    pub fn read_all(&self) -> Result<Vec<Cell>> {
        self.iter().collect()
    }

    /// First cell of the file.
    pub fn first_cell(&self) -> Result<Option<Cell>> {
        if self.trailer.entry_count == 0 {
            return Ok(None);
        }
        let cells = self.read_data_block(self.trailer.first_data_block_offset)?;
        Ok(cells.into_iter().next())
    }

    /// Last cell of the file, taken from the `hfile.LASTKEY` file info entry.
    pub fn last_cell(&self) -> Result<Option<Cell>> {
        match self.file_info.get(crate::hbase::file_info::keys::LASTKEY) {
            Some(bytes) => Ok(Some(Cell::from_key_bytes(bytes)?)),
            None => Ok(None),
        }
    }

    /// First cell with a key greater than or equal to `key` (flat key layout).
    ///
    /// Uses the block index, so this only reads one or two blocks instead of scanning the file.
    pub fn seek(&self, key: &[u8]) -> Result<Option<Cell>> {
        if self.trailer.entry_count == 0 || self.root_index.entries.is_empty() {
            return Ok(None);
        }
        let mut entry = match self.locate_root_entry(key) {
            Some(entry) => entry.clone(),
            // The key sorts before the first index entry, so only the first data block can
            // hold a cell at or after it.
            None => return self.find_from(self.trailer.first_data_block_offset, key),
        };
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 8 {
                return corrupt("block index tree is too deep");
            }
            let block = self.read_block(entry.offset)?;
            if block.block_type.is_data() {
                return self.find_from(entry.offset, key);
            }
            let entries = parse_non_root_index(&block.payload)?;
            match search(&entries, key) {
                Some(i) => entry = entries[i].clone(),
                None => return self.find_from(entry.offset, key),
            }
        }
    }

    /// First cell at or after `key`, walking data blocks from `start`.
    ///
    /// The index can point one block early: an entry key is the block's first cell key and
    /// carries a timestamp, which sorts descending, so a lookup key with `timestamp = MAX`
    /// compares smaller than an entry of the same row. Continuing along the block chain covers
    /// that case and also makes a `key` below the first entry resolve to the first cell.
    fn find_from(&self, start: u64, key: &[u8]) -> Result<Option<Cell>> {
        let target = seek_cell(key);
        let mut offset = start;
        while offset <= self.trailer.last_data_block_offset {
            let block = self.read_block(offset)?;
            let next = offset + block.header.total_size();
            if block.block_type.is_data() {
                let cells = self.decode_block(&block)?;
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

    fn locate_root_entry(&self, key: &[u8]) -> Option<&IndexEntry> {
        if self.trailer.num_data_index_levels <= 1 {
            self.root_index
                .containing(key)
                .map(|i| &self.root_index.entries[i])
        } else {
            // With a multi-level index the root points to the first leaf block that may
            // contain the key.
            self.root_index
                .containing(key)
                .map(|i| &self.root_index.entries[i])
        }
    }

    /// Look up a single cell by row/family/qualifier, returning the newest version.
    pub fn get(&self, row: &[u8], family: &[u8], qualifier: &[u8]) -> Result<Option<Cell>> {
        let mut key = Cell::new(
            row.to_vec(),
            family.to_vec(),
            qualifier.to_vec(),
            Vec::new(),
        );
        key.timestamp = i64::MAX;
        key.cell_type = CellType::Maximum;
        let flat = key.key_bytes();
        let found = self.seek(&flat)?;
        Ok(found.filter(|c| c.row == row && c.family == family && c.qualifier == qualifier))
    }
}

fn seek_cell(key: &[u8]) -> Cell {
    Cell::from_key_bytes(key).unwrap_or_default()
}

/// How much the sequential scan reads in one syscall.
///
/// Data blocks are typically 64 KiB, so a per block read costs one round trip per 64 KiB. On
/// storage where a syscall is a network round trip (9p, NFS, object gateways) that is the whole
/// bottleneck: `dd` with 1 MiB reads reaches ~280 MB/s on WSL's `/mnt/c` where a per block scan
/// stalls at ~95 MiB/s. Reading a megabyte at a time and cutting the blocks out of the buffer
/// closes most of that gap without any async runtime.
const READ_WINDOW: u64 = 1 << 20;

/// Blocks the background thread may read ahead of the decoder.
const PREFETCH_BLOCKS: usize = 8;

/// Sequential scans below this many blocks run without a prefetch thread: spawning it costs more
/// than it can save.
const PREFETCH_THRESHOLD: u64 = 8;

/// A sliding read window over the file.
struct BlockWindow<'a> {
    file: &'a File,
    file_size: u64,
    compression: Compression,
    verify_checksums: bool,
    start: u64,
    buf: Vec<u8>,
}

impl<'a> BlockWindow<'a> {
    fn new(
        file: &'a File,
        file_size: u64,
        compression: Compression,
        verify_checksums: bool,
    ) -> Self {
        BlockWindow {
            file,
            file_size,
            compression,
            verify_checksums,
            start: 0,
            buf: Vec::new(),
        }
    }

    /// Make sure `[offset, offset + want)` is in the buffer, refilling it if needed.
    fn ensure(&mut self, offset: u64, want: u64) -> Result<()> {
        if self.start <= offset && offset + want <= self.start + self.buf.len() as u64 {
            return Ok(());
        }
        let available = self.file_size.saturating_sub(offset);
        let len = want.max(READ_WINDOW).min(available) as usize;
        if len == 0 {
            return corrupt(format!("block at {offset} starts past the end of the file"));
        }
        self.buf.resize(len, 0);
        read_exact_at(self.file, &mut self.buf, offset)?;
        self.start = offset;
        Ok(())
    }

    /// Raw bytes of the block at `offset`, header included.
    fn raw_block(&mut self, offset: u64) -> Result<(BlockHeader, &[u8])> {
        self.ensure(offset, HEADER_SIZE as u64)?;
        let rel = (offset - self.start) as usize;
        let header = BlockHeader::parse(&self.buf[rel..rel + HEADER_SIZE])?;
        let total = header.total_size();
        if offset + total > self.file_size {
            return corrupt(format!(
                "block at {offset} claims {total} bytes but the file ends at {}",
                self.file_size
            ));
        }
        self.ensure(offset, total)?;
        let rel = (offset - self.start) as usize;
        Ok((header, &self.buf[rel..rel + total as usize]))
    }

    /// Read, verify and decompress the block at `offset`, reusing buffered bytes when possible.
    fn block(&mut self, offset: u64) -> Result<Block> {
        let compression = self.compression;
        let verify_checksums = self.verify_checksums;
        let (header, raw) = self.raw_block(offset)?;
        decode_block_bytes(raw, offset, header, compression, verify_checksums)
    }
}

/// Where a scan gets its blocks from: read inline, or handed over by the prefetch thread.
enum Feed<'a> {
    Inline(BlockWindow<'a>),
    Prefetched(Prefetcher),
}

/// Reads blocks ahead of the decoder on a background thread.
///
/// Decoding a block takes several times longer than a `pread` on cache-resident storage, but on
/// a network mount the opposite is true. Overlapping the two is what keeps both the disk and the
/// CPU busy; the thread is bounded by [`PREFETCH_BLOCKS`], so memory stays flat and a slow
/// decoder throttles the reader instead of buffering the whole file.
struct Prefetcher {
    rx: Option<std::sync::mpsc::Receiver<Result<PrefetchedBlock>>>,
    handle: Option<std::thread::JoinHandle<()>>,
    finished: bool,
}

/// Block handed over by the prefetch thread: where it starts, its header, and the read window it
/// was cut out of. The window is shared through an `Arc`, so handing a block over copies nothing.
type PrefetchedBlock = (u64, BlockHeader, std::sync::Arc<Vec<u8>>, usize);

fn window_covers(start: u64, len: usize, offset: u64, need: u64) -> bool {
    start <= offset && offset + need <= start + len as u64
}

fn read_window(file: &File, file_size: u64, offset: u64, want: u64) -> Result<Vec<u8>> {
    let available = file_size.saturating_sub(offset);
    let len = want.max(READ_WINDOW).min(available);
    if len == 0 {
        return corrupt(format!("block at {offset} starts past the end of the file"));
    }
    let mut buf = vec![0u8; len as usize];
    read_exact_at(file, &mut buf, offset)?;
    Ok(buf)
}

impl Prefetcher {
    fn spawn(reader: &HFileReader) -> Result<Self> {
        let file = reader.file.try_clone()?;
        let file_size = reader.file_size;
        let first = reader.trailer.first_data_block_offset;
        let last = reader.trailer.last_data_block_offset;
        let (tx, rx) = std::sync::mpsc::sync_channel::<Result<PrefetchedBlock>>(PREFETCH_BLOCKS);

        let handle = std::thread::Builder::new()
            .name("hfile-prefetch".to_string())
            .spawn(move || {
                // Only slicing happens here; checksums and decoding stay on the caller's thread.
                let mut window: Option<(u64, std::sync::Arc<Vec<u8>>)> = None;
                let mut offset = first;
                while offset <= last {
                    let needs_window = match &window {
                        Some((start, buf)) => {
                            !window_covers(*start, buf.len(), offset, HEADER_SIZE as u64)
                        }
                        None => true,
                    };
                    if needs_window {
                        match read_window(&file, file_size, offset, READ_WINDOW) {
                            Ok(buf) => window = Some((offset, std::sync::Arc::new(buf))),
                            Err(e) => {
                                let _ = tx.send(Err(e));
                                return;
                            }
                        }
                    }

                    let (start, buf) = window.as_ref().expect("window just filled");
                    let rel = (offset - start) as usize;
                    let header = match BlockHeader::parse(&buf[rel..rel + HEADER_SIZE]) {
                        Ok(header) => header,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return;
                        }
                    };
                    let total = header.total_size();
                    let rel = if window_covers(*start, buf.len(), offset, total) {
                        rel
                    } else {
                        // A block straddling the window edge: read one that fits.
                        match read_window(&file, file_size, offset, total) {
                            Ok(buf) => {
                                window = Some((offset, std::sync::Arc::new(buf)));
                                0
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e));
                                return;
                            }
                        }
                    };
                    let buf = std::sync::Arc::clone(&window.as_ref().expect("window").1);
                    if tx.send(Ok((offset, header, buf, rel))).is_err() {
                        return; // the scan was dropped
                    }
                    offset += total;
                }
            })?;

        Ok(Prefetcher {
            rx: Some(rx),
            handle: Some(handle),
            finished: false,
        })
    }

    fn next(&mut self) -> Result<Option<PrefetchedBlock>> {
        if self.finished {
            return Ok(None);
        }
        let Some(rx) = self.rx.as_ref() else {
            self.finished = true;
            return Ok(None);
        };
        match rx.recv() {
            Ok(Ok(block)) => Ok(Some(block)),
            Ok(Err(e)) => {
                self.finished = true;
                Err(e)
            }
            // The producer stopped: end of the data section.
            Err(_) => {
                self.finished = true;
                Ok(None)
            }
        }
    }
}

impl Drop for Prefetcher {
    fn drop(&mut self) {
        // Dropping the receiver makes the producer's `send` fail, which ends the thread.
        self.rx.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// load-on-open helpers
// ---------------------------------------------------------------------------

/// Read the meta index: the second `ROOT_INDEX` block of the load-on-open section.
fn read_meta_index(
    file: &File,
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
    let header = read_header_at(file, file_size, offset)?;
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
    let block = read_block_at(
        file,
        file_size,
        offset,
        compression,
        verify_checksums,
        Some(BlockType::RootIndex),
    )?;
    parse_root_index(&block.payload, count)
}

/// Walk the load-on-open blocks after the file info and parse the bloom filter metadata.
fn read_bloom_filter(
    file: &File,
    file_size: u64,
    start: u64,
    compression: Compression,
    verify_checksums: bool,
) -> Result<Option<BloomFilter>> {
    let end = file_size.saturating_sub(TRAILER_SIZE as u64);
    let mut offset = start;
    while offset + HEADER_SIZE as u64 <= end {
        let block = read_block_at(file, file_size, offset, compression, verify_checksums, None)?;
        if block.block_type == BlockType::GeneralBloomMeta {
            return Ok(Some(BloomFilter::parse(&block.payload)?));
        }
        offset += block.total_size();
    }
    Ok(None)
}

/// Iterator over all cells of a file.
pub struct CellIter<'a> {
    reader: &'a HFileReader,
    feed: Feed<'a>,
    next_offset: u64,
    /// Cells of the block being consumed. Handing them out by value keeps the iterator free of
    /// per cell clones (a cell owns four heap allocations).
    pending: std::vec::IntoIter<Cell>,
    finished: bool,
}

impl Iterator for CellIter<'_> {
    type Item = Result<Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cell) = self.pending.next() {
                return Some(Ok(cell));
            }
            if self.finished {
                return None;
            }
            if let Err(e) = self.load_next_block() {
                self.finished = true;
                return Some(Err(e));
            }
        }
    }
}

impl CellIter<'_> {
    fn load_next_block(&mut self) -> Result<()> {
        let reader = self.reader;
        let compression = reader.compression;
        let verify_checksums = reader.options.verify_checksums;
        // Cells are decoded straight out of the read window: for uncompressed files the payload
        // is borrowed, so a scan copies the cell data once instead of twice.
        let (cells_of, next_offset) = match &mut self.feed {
            Feed::Inline(window) => {
                let offset = self.next_offset;
                if offset > reader.trailer.last_data_block_offset {
                    self.finished = true;
                    return Ok(());
                }
                let (header, raw) = window.raw_block(offset)?;
                let payload = decode_block_payload(raw, header, compression, verify_checksums)?;
                (
                    reader.cells_of(header.block_type, &payload)?,
                    offset + header.total_size(),
                )
            }
            Feed::Prefetched(prefetcher) => {
                let Some((offset, header, window, rel)) = prefetcher.next()? else {
                    self.finished = true;
                    return Ok(());
                };
                let total = header.total_size() as usize;
                let payload = decode_block_payload(
                    &window[rel..rel + total],
                    header,
                    compression,
                    verify_checksums,
                )?;
                (
                    reader.cells_of(header.block_type, &payload)?,
                    offset + header.total_size(),
                )
            }
        };
        self.pending = cells_of;
        self.next_offset = next_offset;
        if next_offset > reader.trailer.last_data_block_offset {
            self.finished = true;
        }
        Ok(())
    }
}

/// Default HBase data block size, re-exported for convenience.
pub const DEFAULT_DATA_BLOCK_SIZE: usize = DEFAULT_BLOCK_SIZE;

impl From<HFileError> for String {
    fn from(e: HFileError) -> Self {
        e.to_string()
    }
}
