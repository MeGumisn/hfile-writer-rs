//! HFile v3 writer.
//!
//! The produced file has the exact layout HBase's `HFileWriterImpl` creates for a single level
//! block index:
//!
//! ```text
//! [data block 0][data block 1] ... [data block N]
//! [root index block]        <- load_on_open_data_offset
//! [empty meta index block]  <- always written by HBase, even with no meta blocks
//! [file info block]         <- file_info_offset
//! [4096 byte trailer]
//! ```
//!
//! Notes:
//! * cells must be appended in HBase `CellComparator` order; the writer enforces this.
//! * data block encoding can be `NONE`, `PREFIX` or `ROW_INDEX_V1`; HBase itself defaults to
//!   `NONE` for `HFile.Writer` and reads all of them.
//! * the block index is always a single root level. HBase only switches to a multi-level index
//!   when the serialized index exceeds `hbase.hfile.index.maxchunksize`; a single level is valid
//!   and is what `singleLevelOnly` mode in `HFileBlockIndex` produces.

use crate::error::{HFileError, Result, corrupt};
use crate::hbase::block::{
    BlockType, DEFAULT_BLOCK_SIZE, DEFAULT_BYTES_PER_CHECKSUM, HEADER_SIZE, build_block, checksum,
};
use crate::hbase::compression::{Compression, ZstdFraming};
use crate::hbase::encoding::{BlockEncoder, DataBlockEncoding, DecodeOptions};
use crate::hbase::file_info::{FileInfo, KEY_VALUE_VER_WITH_MEMSTORE, keys};
use crate::hbase::index::{IndexEntry, build_root_index, root_index_size};
use crate::hbase::kv::{Cell, compare_cells};
use crate::hbase::trailer::{
    FORMAT_VERSION, FileTrailer, LEGACY_KV_COMPARATOR, MAX_MINOR_VERSION, TRAILER_SIZE,
};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Writer configuration.
#[derive(Debug, Clone)]
pub struct WriterOptions {
    /// Target (uncompressed) data block size, HBase default 64 KiB.
    pub block_size: usize,
    /// Checksum chunk size, HBase default 16 KiB.
    pub bytes_per_checksum: u32,
    /// Checksum algorithm (`checksum::CRC32C` by default).
    pub checksum_type: u8,
    /// Data block encoding.
    pub encoding: DataBlockEncoding,
    /// Block compression.
    pub compression: Compression,
    /// Byte layout of zstd blocks; ignored for every other codec.
    pub zstd_framing: ZstdFraming,
    /// Write a 2 byte tags length for every cell (needed when cells carry tags).
    pub includes_tags: bool,
    /// Write a vlong mvcc after every cell (HBase's `HFileContext` default is `true`).
    pub includes_mvcc: bool,
    /// Add `MAX_SEQ_ID_KEY` / `MAJOR_COMPACTION_KEY`, like `StoreFileWriter` does for bulk loads.
    pub write_store_file_metadata: bool,
}

impl Default for WriterOptions {
    fn default() -> Self {
        WriterOptions {
            block_size: DEFAULT_BLOCK_SIZE,
            bytes_per_checksum: DEFAULT_BYTES_PER_CHECKSUM,
            checksum_type: checksum::CRC32C,
            encoding: DataBlockEncoding::None,
            compression: Compression::None,
            zstd_framing: ZstdFraming::default(),
            includes_tags: false,
            includes_mvcc: true,
            write_store_file_metadata: true,
        }
    }
}

impl WriterOptions {
    pub fn decode_options(&self) -> DecodeOptions {
        DecodeOptions {
            includes_tags: self.includes_tags,
            includes_mvcc: self.includes_mvcc,
        }
    }
}

/// Streaming HFile writer.
pub struct HFileWriter {
    out: BufWriter<File>,
    path: PathBuf,
    options: WriterOptions,
    offset: u64,

    encoder: Option<BlockEncoder>,
    block_first_key: Vec<u8>,
    prev_data_block_offset: i64,

    entries: u64,
    total_key_len: u64,
    total_value_len: u64,
    max_seq_id: u64,
    min_timestamp: i64,
    max_timestamp: i64,
    max_tags_len: u32,
    biggest_cell_key: Vec<u8>,
    biggest_cell_len: u32,

    last_cell: Option<Cell>,
    index_entries: Vec<IndexEntry>,

    first_data_block_offset: u64,
    last_data_block_offset: u64,
    total_uncompressed_bytes: u64,

    finished: bool,
}

impl HFileWriter {
    /// Create (or truncate) `path` and write the HFile header lazily on the first block.
    pub fn create(path: impl AsRef<Path>, options: WriterOptions) -> Result<Self> {
        if options.block_size == 0 {
            return Err(HFileError::InvalidArgument(
                "block size must be greater than zero".into(),
            ));
        }
        if !options.encoding.can_encode() {
            return Err(HFileError::InvalidArgument(format!(
                "writing {} data blocks is not supported",
                options.encoding.name()
            )));
        }
        if !options.compression.is_supported() {
            return Err(HFileError::InvalidArgument(format!(
                "writing {} compressed blocks is not supported",
                options.compression.name()
            )));
        }
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        Ok(HFileWriter {
            out: BufWriter::new(file),
            path,
            options,
            offset: 0,
            encoder: None,
            block_first_key: Vec::new(),
            prev_data_block_offset: -1,
            entries: 0,
            total_key_len: 0,
            total_value_len: 0,
            max_seq_id: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            max_tags_len: 0,
            biggest_cell_key: Vec::new(),
            biggest_cell_len: 0,
            last_cell: None,
            index_entries: Vec::new(),
            first_data_block_offset: 0,
            last_data_block_offset: 0,
            total_uncompressed_bytes: 0,
            finished: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a cell. Cells must arrive in `CellComparator` order.
    pub fn append(&mut self, cell: Cell) -> Result<()> {
        if self.finished {
            return Err(HFileError::InvalidArgument(
                "cannot append to a finished HFile writer".into(),
            ));
        }
        if let Some(last) = &self.last_cell
            && compare_cells(last, &cell) == std::cmp::Ordering::Greater
        {
            return Err(HFileError::InvalidArgument(format!(
                "cells must be appended in HBase order: {:?} sorts before {:?}",
                cell.row, last.row
            )));
        }
        if cell.tags.len() > u16::MAX as usize {
            return Err(HFileError::InvalidArgument(
                "cell tags exceed the 2 byte tags length limit".into(),
            ));
        }
        if !self.options.includes_tags && !cell.tags.is_empty() {
            return Err(HFileError::InvalidArgument(
                "cell carries tags but `includes_tags` is disabled".into(),
            ));
        }

        if self.encoder.is_none() {
            self.encoder = Some(BlockEncoder::new(self.options.encoding)?);
            self.block_first_key = cell.key_bytes();
        }

        let decode_options = self.options.decode_options();
        let raw_len = cell.raw_len(decode_options.includes_tags, decode_options.includes_mvcc);
        {
            let encoder = self.encoder.as_mut().expect("encoder exists");
            encoder.add(&cell, decode_options)?;
        }

        self.entries += 1;
        self.total_key_len += cell.key_len() as u64;
        self.total_value_len += cell.value.len() as u64;
        self.max_seq_id = self.max_seq_id.max(cell.seq_id);
        self.min_timestamp = self.min_timestamp.min(cell.timestamp);
        self.max_timestamp = self.max_timestamp.max(cell.timestamp);
        self.max_tags_len = self.max_tags_len.max(cell.tags.len() as u32);
        if raw_len > self.biggest_cell_len as usize {
            self.biggest_cell_len = raw_len as u32;
            self.biggest_cell_key = cell.key_bytes();
        }
        self.last_cell = Some(cell);

        let block_full = self.encoder.as_ref().map(|e| e.len()).unwrap_or(0)
            >= self.options.block_size;
        if block_full {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Append many cells.
    pub fn append_all<I>(&mut self, cells: I) -> Result<()>
    where
        I: IntoIterator<Item = Cell>,
    {
        for cell in cells {
            self.append(cell)?;
        }
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.out.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    /// Write one block at the current offset and return `(offset, total size)`.
    fn write_block(&mut self, block_type: BlockType, payload: &[u8], prev: i64) -> Result<(u64, u32)> {
        let bytes = build_block(
            block_type,
            payload,
            prev,
            self.options.compression,
            self.options.zstd_framing,
            self.options.checksum_type,
            self.options.bytes_per_checksum,
        )?;
        let offset = self.offset;
        self.total_uncompressed_bytes += (HEADER_SIZE + payload.len()) as u64;
        self.write_bytes(&bytes)?;
        Ok((offset, bytes.len() as u32))
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let Some(encoder) = self.encoder.take() else {
            return Ok(());
        };
        if encoder.is_empty() {
            return Ok(());
        };
        let (block_type, payload) = encoder.finish();
        let (offset, on_disk_size) =
            self.write_block(block_type, &payload, self.prev_data_block_offset)?;
        if self.index_entries.is_empty() {
            self.first_data_block_offset = offset;
        }
        self.last_data_block_offset = offset;
        self.prev_data_block_offset = offset as i64;
        self.index_entries.push(IndexEntry {
            key: self.block_first_key.clone(),
            offset,
            on_disk_size,
        });
        Ok(())
    }

    /// Finish the file: write index, meta index, file info and trailer.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()?;
        self.finished = true;
        self.out.flush()?;
        Ok(())
    }

    fn finish_inner(&mut self) -> Result<()> {
        self.flush_data_block()?;

        // The data index: one root level containing one entry per data block.
        let root_payload = build_root_index(&self.index_entries, None);
        let (root_offset, _) =
            self.write_block(BlockType::RootIndex, &root_payload, -1)?;
        let uncompressed_data_index_size = root_index_size(&self.index_entries, false);

        // The meta index: HBase always writes it, even when it is empty.
        let meta_payload = build_root_index(&[] as &[IndexEntry], None);
        self.write_block(BlockType::RootIndex, &meta_payload, -1)?;

        // File info.
        let file_info = self.build_file_info()?;
        let file_info_payload = file_info.to_bytes()?;
        let (file_info_offset, _) =
            self.write_block(BlockType::FileInfo, &file_info_payload, -1)?;

        let trailer = FileTrailer {
            file_info_offset,
            load_on_open_data_offset: root_offset,
            uncompressed_data_index_size,
            total_uncompressed_bytes: self.total_uncompressed_bytes + TRAILER_SIZE as u64,
            data_index_count: self.index_entries.len() as u32,
            meta_index_count: 0,
            entry_count: self.entries,
            num_data_index_levels: 1,
            first_data_block_offset: self.first_data_block_offset,
            last_data_block_offset: self.last_data_block_offset,
            comparator_class_name: LEGACY_KV_COMPARATOR.to_string(),
            compression_codec: self.options.compression.ordinal(),
            encryption_key: None,
            major_version: FORMAT_VERSION,
            minor_version: MAX_MINOR_VERSION,
        };
        let trailer_bytes = trailer.to_bytes()?;
        self.write_bytes(&trailer_bytes)?;
        Ok(())
    }

    fn build_file_info(&self) -> Result<FileInfo> {
        let mut info = FileInfo::new();
        let last_cell = self
            .last_cell
            .as_ref()
            .ok_or_else(|| HFileError::InvalidArgument("cannot finish an empty HFile".into()))?;
        // HBase stores the data block encoding in the file info, not in the trailer; without
        // this key `HFileDataBlockEncoderImpl.createFromFileInfo` falls back to `NONE` and the
        // reader decodes `DATABLKE` blocks as if they were plain cells.
        if self.options.encoding != DataBlockEncoding::None {
            info.insert(keys::DATA_BLOCK_ENCODING, self.options.encoding.name());
        }
        info.insert(keys::LASTKEY, last_cell.key_bytes());
        let entries = self.entries.max(1);
        info.insert(
            keys::AVG_KEY_LEN,
            ((self.total_key_len / entries) as i32).to_be_bytes(),
        );
        info.insert(
            keys::AVG_VALUE_LEN,
            ((self.total_value_len / entries) as i32).to_be_bytes(),
        );
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        info.insert(keys::CREATE_TIME_TS, now.to_be_bytes());
        if !self.biggest_cell_key.is_empty() {
            info.insert(keys::KEY_OF_BIGGEST_CELL, self.biggest_cell_key.clone());
            info.insert(
                keys::LEN_OF_BIGGEST_CELL,
                (self.biggest_cell_len as i32).to_be_bytes(),
            );
        }
        if self.options.includes_tags {
            info.insert(keys::MAX_TAGS_LEN, self.max_tags_len.to_be_bytes());
            info.insert(keys::TAGS_COMPRESSED, vec![0u8]);
        }
        if self.options.includes_mvcc {
            info.insert(
                keys::KEY_VALUE_VERSION,
                KEY_VALUE_VER_WITH_MEMSTORE.to_be_bytes(),
            );
            info.insert(
                keys::MAX_MEMSTORE_TS_KEY,
                (self.max_seq_id as i64).to_be_bytes(),
            );
        }
        if self.options.write_store_file_metadata {
            info.insert(
                keys::MAX_SEQ_ID_KEY,
                (self.max_seq_id as i64).to_be_bytes(),
            );
            info.insert(keys::MAJOR_COMPACTION_KEY, vec![0u8]);
        }
        Ok(info)
    }

    /// Number of cells appended so far.
    pub fn entry_count(&self) -> u64 {
        self.entries
    }

    /// Timestamp range of the appended cells (used for bulk load metadata).
    pub fn timestamp_range(&self) -> Option<(i64, i64)> {
        if self.entries == 0 {
            None
        } else {
            Some((self.min_timestamp, self.max_timestamp))
        }
    }
}

/// Convenience: write all cells (already sorted) to a new HFile.
pub fn write_hfile(
    path: impl AsRef<Path>,
    cells: impl IntoIterator<Item = Cell>,
    options: WriterOptions,
) -> Result<()> {
    let mut writer = HFileWriter::create(path, options)?;
    writer.append_all(cells)?;
    writer.finish()
}

/// Sanity helper used by the CLI when the input is known to be sorted.
pub fn check_sorted(cells: &[Cell]) -> Result<()> {
    for pair in cells.windows(2) {
        if compare_cells(&pair[0], &pair[1]) == std::cmp::Ordering::Greater {
            return corrupt("input cells are not sorted with the HBase comparator");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hbase::reader::HFileReader;

    fn sample_cells() -> Vec<Cell> {
        (0..4)
            .map(|row| {
                let mut cell = Cell::new(
                    format!("row{row:04}").into_bytes(),
                    b"f".to_vec(),
                    b"q".to_vec(),
                    format!("value{row}").into_bytes(),
                );
                cell.timestamp = 1_000 + row as i64;
                cell.seq_id = row as u64 + 1;
                cell
            })
            .collect()
    }

    #[test]
    fn file_info_records_data_block_encoding() {
        for encoding in [
            DataBlockEncoding::None,
            DataBlockEncoding::Prefix,
            DataBlockEncoding::RowIndexV1,
        ] {
            let path =
                std::env::temp_dir().join(format!("hfile-rs-encoding-{}.hfile", encoding.name()));
            let options = WriterOptions {
                encoding,
                block_size: 512,
                ..Default::default()
            };
            write_hfile(&path, sample_cells(), options).unwrap();

            let reader = HFileReader::open(&path).unwrap();
            let recorded = reader.file_info().get(keys::DATA_BLOCK_ENCODING);
            if encoding == DataBlockEncoding::None {
                // HBase only writes the key for non-NONE encodings.
                assert!(recorded.is_none(), "NONE must not be recorded");
            } else {
                assert_eq!(
                    recorded.map(|v| v.to_vec()),
                    Some(encoding.name().as_bytes().to_vec()),
                    "{} must be recorded in the file info",
                    encoding.name()
                );
            }
            assert_eq!(reader.data_block_encoding(), encoding);
            drop(reader);
            let _ = std::fs::remove_file(&path);
        }
    }
}
