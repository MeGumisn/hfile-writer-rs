//! Data block encodings.
//!
//! HFile v3 supports these `DataBlockEncoding` ids (stored as a 2 byte big-endian id at the
//! start of every `DATABLKE` block):
//!
//! | id | encoding       | notes                                            |
//! |----|----------------|--------------------------------------------------|
//! | 0  | NONE           | plain cells inside `DATABLK*`                    |
//! | 2  | PREFIX         | prefix compressed keys                           |
//! | 3  | DIFF           | key/value/type dedup + timestamp diff            |
//! | 4  | FAST_DIFF      | like DIFF but faster to decode                   |
//! | 7  | ROW_INDEX_V1   | plain cells + per-row offset index (HBase 2 default) |
//!
//! Reading is implemented for all of them. Writing is implemented for `NONE`, `PREFIX` and
//! `ROW_INDEX_V1`, which covers the HBase defaults and the cheapest encoder.

mod diff;
mod fast_diff;
mod prefix;
mod row_index;

use crate::error::{HFileError, Result, corrupt, unsupported};
use crate::hbase::block::BlockType;
use crate::hbase::kv::{Cell, parse_raw_cells};

/// `org.apache.hadoop.hbase.io.encoding.DataBlockEncoding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataBlockEncoding {
    /// Cells are stored as-is.
    #[default]
    None,
    /// Prefix compression (`PrefixKeyDeltaEncoder`).
    Prefix,
    /// Diff encoding (`DiffKeyDeltaEncoder`).
    Diff,
    /// Faster diff encoding (`FastDiffDeltaEncoder`).
    FastDiff,
    /// Row index encoding, the HBase 2.x default (`RowIndexCodecV1`).
    RowIndexV1,
}

impl DataBlockEncoding {
    /// Id written at the beginning of an encoded data block.
    pub fn id(self) -> u16 {
        match self {
            DataBlockEncoding::None => 0,
            DataBlockEncoding::Prefix => 2,
            DataBlockEncoding::Diff => 3,
            DataBlockEncoding::FastDiff => 4,
            DataBlockEncoding::RowIndexV1 => 7,
        }
    }

    pub fn from_id(id: u16) -> Result<Self> {
        Ok(match id {
            0 => DataBlockEncoding::None,
            2 => DataBlockEncoding::Prefix,
            3 => DataBlockEncoding::Diff,
            4 => DataBlockEncoding::FastDiff,
            7 => DataBlockEncoding::RowIndexV1,
            other => return corrupt(format!("unknown data block encoding id {other}")),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            DataBlockEncoding::None => "NONE",
            DataBlockEncoding::Prefix => "PREFIX",
            DataBlockEncoding::Diff => "DIFF",
            DataBlockEncoding::FastDiff => "FAST_DIFF",
            DataBlockEncoding::RowIndexV1 => "ROW_INDEX_V1",
        }
    }

    pub fn parse_name(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_uppercase().replace('-', "_").as_str() {
            "NONE" => DataBlockEncoding::None,
            "PREFIX" => DataBlockEncoding::Prefix,
            "DIFF" => DataBlockEncoding::Diff,
            "FAST_DIFF" => DataBlockEncoding::FastDiff,
            "ROW_INDEX_V1" => DataBlockEncoding::RowIndexV1,
            other => {
                return Err(HFileError::InvalidArgument(format!(
                    "unknown data block encoding '{other}' (expected NONE, PREFIX, DIFF, FAST_DIFF or ROW_INDEX_V1)"
                )));
            }
        })
    }

    /// Whether this crate can encode data blocks with this encoding.
    pub fn can_encode(self) -> bool {
        matches!(
            self,
            DataBlockEncoding::None | DataBlockEncoding::Prefix | DataBlockEncoding::RowIndexV1
        )
    }
}

/// Flags that decide how cells are laid out inside a data block.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodeOptions {
    /// Cells carry a 2 byte tags length (`hfile.MAX_TAGS_LEN` present).
    pub includes_tags: bool,
    /// Cells carry a trailing mvcc vlong (`KEY_VALUE_VERSION == 1`).
    pub includes_mvcc: bool,
}

/// Decode the payload of a data block into cells.
pub fn decode_data_block(
    block_type: BlockType,
    payload: &[u8],
    options: DecodeOptions,
) -> Result<Vec<Cell>> {
    match block_type {
        BlockType::Data => {
            Ok(parse_raw_cells(payload, options.includes_tags, options.includes_mvcc)?)
        }
        BlockType::EncodedData => {
            if payload.len() < 2 {
                return corrupt("encoded data block is too short to hold an encoding id");
            }
            let id = u16::from_be_bytes([payload[0], payload[1]]);
            let encoding = DataBlockEncoding::from_id(id)?;
            let body = &payload[2..];
            match encoding {
                DataBlockEncoding::None => Ok(parse_raw_cells(
                    body,
                    options.includes_tags,
                    options.includes_mvcc,
                )?),
                DataBlockEncoding::Prefix => prefix::decode(body, options),
                DataBlockEncoding::Diff => diff::decode(body, options),
                DataBlockEncoding::FastDiff => fast_diff::decode(body, options),
                DataBlockEncoding::RowIndexV1 => row_index::decode(body, options),
            }
        }
        other => corrupt(format!("block type {other:?} is not a data block")),
    }
}

/// Incremental encoder for one data block.
pub struct BlockEncoder {
    encoding: DataBlockEncoding,
    buf: Vec<u8>,
    unencoded_size: usize,
    rows: Vec<u32>,
    last_row: Option<Vec<u8>>,
    prev_key: Vec<u8>,
    entries: usize,
}

impl BlockEncoder {
    pub fn new(encoding: DataBlockEncoding) -> Result<Self> {
        if !encoding.can_encode() {
            return unsupported(format!(
                "writing {} encoded data blocks is not implemented (use NONE, PREFIX or ROW_INDEX_V1)",
                encoding.name()
            ));
        }
        Ok(BlockEncoder {
            encoding,
            buf: Vec::new(),
            unencoded_size: 0,
            rows: Vec::new(),
            last_row: None,
            prev_key: Vec::new(),
            entries: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.buf.len() + if self.encoding == DataBlockEncoding::None { 0 } else { 6 }
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn entries(&self) -> usize {
        self.entries
    }

    pub fn add(&mut self, cell: &Cell, options: DecodeOptions) -> Result<()> {
        let raw_len = cell.raw_len(options.includes_tags, options.includes_mvcc);
        self.unencoded_size += raw_len;
        match self.encoding {
            DataBlockEncoding::None => {
                cell.write_raw(&mut self.buf, options.includes_tags, options.includes_mvcc);
            }
            DataBlockEncoding::RowIndexV1 => {
                if self.last_row.as_deref() != Some(cell.row.as_slice()) {
                    self.rows.push(self.buf.len() as u32);
                    self.last_row = Some(cell.row.clone());
                }
                cell.write_raw(&mut self.buf, options.includes_tags, options.includes_mvcc);
            }
            DataBlockEncoding::Prefix => {
                let key = cell.key_bytes();
                let common = common_prefix_len(&self.prev_key, &key);
                let mut out = Vec::with_capacity(raw_len);
                crate::hbase::kv::write_compressed_int(&mut out, (key.len() - common) as i32);
                crate::hbase::kv::write_compressed_int(&mut out, cell.value.len() as i32);
                crate::hbase::kv::write_compressed_int(&mut out, common as i32);
                out.extend_from_slice(&key[common..]);
                out.extend_from_slice(&cell.value);
                if options.includes_tags {
                    // Encoded blocks store the tags length as a 7-bit compressed int.
                    crate::hbase::kv::write_compressed_int(&mut out, cell.tags.len() as i32);
                    out.extend_from_slice(&cell.tags);
                }
                if options.includes_mvcc {
                    crate::hbase::kv::write_vlong(&mut out, cell.seq_id as i64);
                }
                self.buf.extend_from_slice(&out);
                self.prev_key = key;
            }
            other => {
                return unsupported(format!("writing {} is not implemented", other.name()));
            }
        }
        self.entries += 1;
        Ok(())
    }

    /// Finish the block, returning its block type and uncompressed payload.
    pub fn finish(self) -> (BlockType, Vec<u8>) {
        match self.encoding {
            DataBlockEncoding::None => (BlockType::Data, self.buf),
            DataBlockEncoding::Prefix => {
                let mut out =
                    Vec::with_capacity(6 + self.buf.len());
                out.extend_from_slice(&DataBlockEncoding::Prefix.id().to_be_bytes());
                out.extend_from_slice(&(self.unencoded_size as i32).to_be_bytes());
                out.extend_from_slice(&self.buf);
                (BlockType::EncodedData, out)
            }
            DataBlockEncoding::RowIndexV1 => {
                let mut out = Vec::with_capacity(6 + self.buf.len() + self.rows.len() * 4);
                out.extend_from_slice(&DataBlockEncoding::RowIndexV1.id().to_be_bytes());
                let data_size = self.buf.len() as i32;
                out.extend_from_slice(&self.buf);
                out.extend_from_slice(&(self.rows.len() as i32).to_be_bytes());
                for row in &self.rows {
                    out.extend_from_slice(&(*row as i32).to_be_bytes());
                }
                out.extend_from_slice(&data_size.to_be_bytes());
                (BlockType::EncodedData, out)
            }
            DataBlockEncoding::Diff | DataBlockEncoding::FastDiff => unreachable!("not writable"),
        }
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Small forward-only reader used by the block decoders.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return corrupt(format!(
                "encoded block claims {n} bytes but only {} remain",
                self.remaining()
            ));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Fixed 4 byte big-endian integer (the "unencoded size" header of PREFIX/DIFF blocks).
    pub(crate) fn i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(crate) fn compressed_int(&mut self) -> Result<i32> {
        let (value, used) = crate::hbase::kv::read_compressed_int(&self.buf[self.pos..])?;
        self.pos += used;
        Ok(value)
    }

    pub(crate) fn vlong(&mut self) -> Result<u64> {
        let (value, used) = crate::hbase::kv::read_vlong(&self.buf[self.pos..])?;
        self.pos += used;
        Ok(value as u64)
    }

    /// Little-endian integer of `n` bytes, as used by `DiffKeyDeltaEncoder` timestamps.
    pub(crate) fn le_long(&mut self, n: usize) -> Result<i64> {
        let bytes = self.take(n)?;
        let mut value = 0i64;
        for (i, b) in bytes.iter().enumerate() {
            value |= (*b as i64) << (8 * i);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells() -> Vec<Cell> {
        let mut out = Vec::new();
        for row in 0..5 {
            for q in 0..3 {
                let mut c = Cell::new(
                    format!("row{row:04}").into_bytes(),
                    b"f".to_vec(),
                    format!("q{q}").into_bytes(),
                    format!("value-{row}-{q}").into_bytes(),
                );
                c.timestamp = 1_000_000 + row as i64;
                c.seq_id = (row * 3 + q) as u64;
                out.push(c);
            }
        }
        out
    }

    #[test]
    fn write_and_read_back_all_supported_encodings() {
        for (encoding, options) in [
            (DataBlockEncoding::None, DecodeOptions::default()),
            (DataBlockEncoding::Prefix, DecodeOptions::default()),
            (DataBlockEncoding::RowIndexV1, DecodeOptions::default()),
            (
                DataBlockEncoding::Prefix,
                DecodeOptions {
                    includes_tags: true,
                    includes_mvcc: true,
                },
            ),
            (
                DataBlockEncoding::RowIndexV1,
                DecodeOptions {
                    includes_tags: true,
                    includes_mvcc: true,
                },
            ),
        ] {
            let mut encoder = BlockEncoder::new(encoding).unwrap();
            for c in cells() {
                encoder.add(&c, options).unwrap();
            }
            let (block_type, payload) = encoder.finish();
            let decoded = decode_data_block(block_type, &payload, options).unwrap();
            assert_eq!(decoded.len(), 15, "encoding {encoding:?}");
            assert_eq!(decoded[3].row, cells()[3].row);
            assert_eq!(decoded[3].value, cells()[3].value);
            assert_eq!(decoded[3].timestamp, cells()[3].timestamp);
            assert_eq!(
                decoded[3].seq_id,
                if options.includes_mvcc { 3 } else { 0 }
            );
        }
    }

    #[test]
    fn cannot_encode_diff() {
        assert!(BlockEncoder::new(DataBlockEncoding::Diff).is_err());
        assert!(BlockEncoder::new(DataBlockEncoding::FastDiff).is_err());
    }

    #[test]
    fn encoding_ids_match_hbase() {
        assert_eq!(DataBlockEncoding::None.id(), 0);
        assert_eq!(DataBlockEncoding::Prefix.id(), 2);
        assert_eq!(DataBlockEncoding::Diff.id(), 3);
        assert_eq!(DataBlockEncoding::FastDiff.id(), 4);
        assert_eq!(DataBlockEncoding::RowIndexV1.id(), 7);
    }
}
