//! HFile v3 reader.
//!
//! Open sequence (mirrors `HFileReaderImpl` + `HFileInfo`):
//!
//! ```text
//! 1. read + parse the 4096 byte trailer          -> offsets, counts, codec, comparator
//! 2. read the FILE_INFO block                    -> tags/mvcc flags, last key, stats
//! 3. read the ROOT_INDEX block                   -> block lookup tree
//! 4. peek at the first data block                -> data block encoding
//! ```
//!
//! Data blocks are located either through the index (`seek`) or by walking the data block
//! section sequentially (`iter`, identical to `HFileBlock.BlockIterator.nextBlock`).

use crate::error::{HFileError, Result, corrupt, unsupported};
use crate::hbase::block::{
    Block, BlockType, DEFAULT_BLOCK_SIZE, read_block_at, read_header_at,
};
use crate::hbase::compression::Compression;
use crate::hbase::encoding::{DataBlockEncoding, DecodeOptions, decode_data_block};
use crate::hbase::file_info::FileInfo;
use crate::hbase::index::{IndexEntry, RootIndex, parse_non_root_index, parse_root_index, search};
use crate::hbase::kv::{Cell, CellType, compare_cells};
use crate::hbase::trailer::FileTrailer;
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

    fn decode_block(&self, block: &Block) -> Result<Vec<Cell>> {
        if !block.block_type.is_data() {
            return corrupt(format!(
                "expected a data block at {} but found {:?}",
                block.offset, block.block_type
            ));
        }
        decode_data_block(block.block_type, &block.payload, self.decode_options)
    }

    /// Read and decode the data block at `offset`.
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
        let mut offset = self.trailer.first_data_block_offset;
        while offset <= self.trailer.last_data_block_offset {
            let header = read_header_at(&self.file, self.file_size, offset)?;
            let next = offset + header.total_size();
            let block = self.read_block(offset)?;
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
    pub fn iter(&self) -> CellIter<'_> {
        CellIter {
            reader: self,
            next_offset: if self.trailer.entry_count == 0 {
                u64::MAX
            } else {
                self.trailer.first_data_block_offset
            },
            buffer: Vec::new(),
            index: 0,
            finished: self.trailer.entry_count == 0,
        }
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
            None => return Ok(None),
        };
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 8 {
                return corrupt("block index tree is too deep");
            }
            let block = self.read_block(entry.offset)?;
            if block.block_type.is_data() {
                let cells = self.decode_block(&block)?;
                return Ok(cells
                    .into_iter()
                    .find(|cell| compare_cells(cell, &seek_cell(key)) != std::cmp::Ordering::Less));
            }
            let entries = parse_non_root_index(&block.payload)?;
            match search(&entries, key) {
                Some(i) => entry = entries[i].clone(),
                None => return Ok(None),
            }
        }
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
        Ok(found.filter(|c| {
            c.row == row && c.family == family && c.qualifier == qualifier
        }))
    }
}

fn seek_cell(key: &[u8]) -> Cell {
    Cell::from_key_bytes(key).unwrap_or_default()
}

/// Iterator over all cells of a file.
pub struct CellIter<'a> {
    reader: &'a HFileReader,
    next_offset: u64,
    buffer: Vec<Cell>,
    index: usize,
    finished: bool,
}

impl Iterator for CellIter<'_> {
    type Item = Result<Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.buffer.len() {
                let cell = self.buffer[self.index].clone();
                self.index += 1;
                return Some(Ok(cell));
            }
            if self.finished {
                return None;
            }
            let result = self.load_next_block();
            if let Err(e) = result {
                self.finished = true;
                return Some(Err(e));
            }
            if self.finished && self.buffer.is_empty() {
                return None;
            }
        }
    }
}

impl CellIter<'_> {
    fn load_next_block(&mut self) -> Result<()> {
        let reader = self.reader;
        let offset = self.next_offset;
        if offset > reader.trailer.last_data_block_offset {
            self.finished = true;
            return Ok(());
        }
        let header = read_header_at(&reader.file, reader.file_size, offset)?;
        self.next_offset = offset + header.total_size();
        if self.next_offset > reader.trailer.last_data_block_offset {
            self.finished = true;
        }
        let block = reader.read_block(offset)?;
        self.buffer.clear();
        self.index = 0;
        if block.block_type.is_data() {
            self.buffer = reader.decode_block(&block)?;
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
