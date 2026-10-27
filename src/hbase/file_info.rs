//! The `FILE_INFO` block: `"PBUF"` magic + varint length + `FileInfoProto`.
//!
//! The file info is a string -> bytes map. Besides bookkeeping (average key/value length,
//! creation time) it carries the two flags that decide how data block cells must be parsed:
//!
//! * `hfile.MAX_TAGS_LEN` present  => every serialized cell has a 2 byte tags length
//! * `KEY_VALUE_VERSION == 1`      => every serialized cell is followed by a vlong mvcc
//!
//! Without these flags a reader cannot split a plain `DATABLK*` payload into cells.

use crate::error::{Result, corrupt};
use crate::hbase::pb::FileInfoProto;
use crate::hbase::pb::BytesBytesPair;
use prost::Message;
use std::collections::BTreeMap;

/// Magic prefix of the file info block payload (`ProtobufMagic.PB_MAGIC`).
pub const PB_MAGIC: &[u8; 4] = b"PBUF";

/// Well known file info keys (`HFileInfo`, `HFileWriterImpl`, `HStoreFile`).
pub mod keys {
    /// Last cell key of the file (flat key, no length prefix).
    pub const LASTKEY: &str = "hfile.LASTKEY";
    /// Average serialized key length.
    pub const AVG_KEY_LEN: &str = "hfile.AVG_KEY_LEN";
    /// Average value length.
    pub const AVG_VALUE_LEN: &str = "hfile.AVG_VALUE_LEN";
    /// Creation timestamp of the file.
    pub const CREATE_TIME_TS: &str = "hfile.CREATE_TIME_TS";
    /// Maximum tags length of any cell; presence marks "tags enabled".
    pub const MAX_TAGS_LEN: &str = "hfile.MAX_TAGS_LEN";
    /// Whether the tags themselves are compressed (HBase never enables this by default).
    pub const TAGS_COMPRESSED: &str = "hfile.TAGS_COMPRESSED";
    /// Key of the biggest cell (used by the compaction cell-size policy).
    pub const KEY_OF_BIGGEST_CELL: &str = "hfile.KEY_OF_BIGGEST_CELL";
    /// Size of the biggest cell.
    pub const LEN_OF_BIGGEST_CELL: &str = "hfile.LEN_OF_BIGGEST_CELL";
    /// Written by `HFile.Writer` when mvcc/sequence ids are stored in the file.
    pub const KEY_VALUE_VERSION: &str = "KEY_VALUE_VERSION";
    /// Highest sequence id stored in the file.
    pub const MAX_MEMSTORE_TS_KEY: &str = "MAX_MEMSTORE_TS_KEY";
    /// Written by `StoreFileWriter` for bulk loaded store files.
    pub const MAX_SEQ_ID_KEY: &str = "MAX_SEQ_ID_KEY";
    /// Written by `StoreFileWriter` when the file is the result of a major compaction.
    pub const MAJOR_COMPACTION_KEY: &str = "MAJOR_COMPACTION_KEY";
    /// Minimum/maximum timestamp of the file.
    pub const TIMERANGE: &str = "TIMERANGE";
    /// Data block encoding name (`PREFIX`, `DIFF`, `FAST_DIFF`, `ROW_INDEX_V1`).
    ///
    /// Unlike the keys above this one has no `hfile.` prefix; it is
    /// `HFileDataBlockEncoder.DATA_BLOCK_ENCODING` and is written by
    /// `HFileDataBlockEncoderImpl.saveMetadata` only when the encoding is not `NONE`.
    pub const DATA_BLOCK_ENCODING: &str = "DATA_BLOCK_ENCODING";
}

/// Value of `KEY_VALUE_VERSION` that means "cells carry an mvcc vlong".
pub const KEY_VALUE_VER_WITH_MEMSTORE: i32 = 1;

/// String keyed map stored in the file info block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileInfo {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl FileInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.map.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.map.get(key.as_bytes()).map(|v| v.as_slice())
    }

    pub fn get_i32(&self, key: &str) -> Option<i32> {
        self.get(key).and_then(|v| {
            if v.len() >= 4 {
                Some(i32::from_be_bytes([v[0], v[1], v[2], v[3]]))
            } else {
                None
            }
        })
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|v| {
            if v.len() >= 8 {
                let mut b = [0u8; 8];
                b.copy_from_slice(&v[..8]);
                Some(i64::from_be_bytes(b))
            } else {
                None
            }
        })
    }

    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key.as_bytes())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// True when every cell in a data block is followed by a 2 byte tags length.
    pub fn includes_tags(&self) -> bool {
        self.contains(keys::MAX_TAGS_LEN)
    }

    /// True when cells carry an mvcc vlong, decided exactly like `HFileInfo.initMetaInfo`.
    pub fn includes_mvcc(&self) -> bool {
        self.get_i32(keys::KEY_VALUE_VERSION) == Some(KEY_VALUE_VER_WITH_MEMSTORE)
    }

    /// Highest sequence id of the file (`MAX_SEQ_ID_KEY`, falling back to `MAX_MEMSTORE_TS_KEY`).
    pub fn max_seq_id(&self) -> u64 {
        if let Some(v) = self.get_i64(keys::MAX_SEQ_ID_KEY) {
            return v.max(0) as u64;
        }
        self.get_i64(keys::MAX_MEMSTORE_TS_KEY).unwrap_or(0).max(0) as u64
    }

    /// True when tags were compressed inside the data blocks (not supported).
    pub fn tags_compressed(&self) -> bool {
        self.get(keys::TAGS_COMPRESSED)
            .map(|v| v == [1u8])
            .unwrap_or(false)
    }

    /// Parse a `FILE_INFO` block payload.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < PB_MAGIC.len() + 1 {
            return corrupt("file info block is too small");
        }
        if &data[..PB_MAGIC.len()] != PB_MAGIC {
            return corrupt("file info block does not start with the PBUF magic");
        }
        let (len, used) = crate::hbase::kv::read_compressed_int(&data[PB_MAGIC.len()..])?;
        let start = PB_MAGIC.len() + used;
        let len = len as usize;
        if len > data.len() - start {
            return corrupt("file info protobuf length exceeds the block payload");
        }
        let proto = FileInfoProto::decode(&data[start..start + len])?;
        let mut map = BTreeMap::new();
        for BytesBytesPair { first, second } in proto.map_entry {
            map.insert(first, second);
        }
        Ok(FileInfo { map })
    }

    /// Serialize to a `FILE_INFO` block payload.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let proto = FileInfoProto {
            map_entry: self
                .map
                .iter()
                .map(|(k, v)| BytesBytesPair {
                    first: k.clone(),
                    second: v.clone(),
                })
                .collect(),
        };
        let mut pb = Vec::with_capacity(proto.encoded_len());
        proto
            .encode(&mut pb)
            .map_err(|e| crate::error::corrupt_error(format!("failed to encode file info protobuf: {e}")))?;
        let mut out = Vec::with_capacity(PB_MAGIC.len() + 5 + pb.len());
        out.extend_from_slice(PB_MAGIC);
        crate::hbase::kv::write_compressed_int(&mut out, pb.len() as i32);
        out.extend_from_slice(&pb);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_info_roundtrip_and_flags() {
        let mut info = FileInfo::new();
        info.insert(keys::LASTKEY, vec![0u8, 1, b'r']);
        info.insert(keys::AVG_KEY_LEN, 50i32.to_be_bytes());
        info.insert(keys::AVG_VALUE_LEN, 13i32.to_be_bytes());
        info.insert(keys::MAX_TAGS_LEN, 0i32.to_be_bytes());
        info.insert(keys::KEY_VALUE_VERSION, 1i32.to_be_bytes());
        info.insert(keys::MAX_MEMSTORE_TS_KEY, 42i64.to_be_bytes());

        assert!(info.includes_tags());
        assert!(info.includes_mvcc());
        assert_eq!(info.max_seq_id(), 42);

        let bytes = info.to_bytes().unwrap();
        assert_eq!(&bytes[..4], PB_MAGIC);
        let parsed = FileInfo::parse(&bytes).unwrap();
        assert_eq!(parsed, info);
    }

    #[test]
    fn no_tags_no_mvcc() {
        let mut info = FileInfo::new();
        info.insert(keys::AVG_KEY_LEN, 10i32.to_be_bytes());
        assert!(!info.includes_tags());
        assert!(!info.includes_mvcc());
        assert_eq!(info.max_seq_id(), 0);
    }
}
