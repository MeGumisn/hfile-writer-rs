//! HFile v3 fixed trailer.
//!
//! The last 4096 bytes of every HFile v3 file are
//!
//! ```text
//! [magic "TRABLK\"$"]                       8 bytes
//! [varint pb length][FileTrailerProto]      varint + N bytes
//! [zero padding]                            up to 4092 - N
//! [materialized version]                    4 bytes = (minor << 24) | major
//! ```
//!
//! This is `FixedFileTrailer.serializeAsPB` / `deserializeFromPB` in HBase.

use crate::error::{Result, corrupt};
use crate::hbase::block::{MAGIC_SIZE, read_exact_at};
use crate::hbase::pb::FileTrailerProto;
use prost::Message;
use std::fs::File;

/// Trailer size written by HBase (`FixedFileTrailer.MAX_TRAILER_SIZE`).
pub const TRAILER_SIZE: usize = 4096;
/// Bytes in front of the protobuf: magic + varint length placeholder.
const NOT_PB_SIZE: usize = MAGIC_SIZE + 4;
/// Magic that starts the trailer.
pub const TRAILER_MAGIC: &[u8; MAGIC_SIZE] = b"TRABLK\"$";

/// Major version this implementation writes.
pub const FORMAT_VERSION: u32 = 3;
/// Minor version written by HBase 2.x (`HFileInfo.MAX_MINOR_VERSION`).
pub const MAX_MINOR_VERSION: u32 = 3;

/// Comparator class name written by HBase for plain `KeyValue` cells.
///
/// `FixedFileTrailer` maps the current comparator name back to this HBase 1.x compatible name
/// so that older readers keep working; HBase itself writes the very same string.
pub const LEGACY_KV_COMPARATOR: &str = "org.apache.hadoop.hbase.KeyValue$KVComparator";

/// Combine major/minor into the 4 byte integer stored at the end of the file.
pub fn materialize_version(major: u32, minor: u32) -> u32 {
    (minor << 24) | major
}

/// Split the materialized version into `(major, minor)`.
pub fn parse_version(value: u32) -> (u32, u32) {
    (value & 0x00ff_ffff, value >> 24)
}

/// Mirrors `FileTrailerProto` plus the version that lives outside the protobuf.
#[derive(Debug, Clone, Default)]
pub struct FileTrailer {
    pub file_info_offset: u64,
    pub load_on_open_data_offset: u64,
    pub uncompressed_data_index_size: u64,
    pub total_uncompressed_bytes: u64,
    pub data_index_count: u32,
    pub meta_index_count: u32,
    pub entry_count: u64,
    pub num_data_index_levels: u32,
    pub first_data_block_offset: u64,
    pub last_data_block_offset: u64,
    pub comparator_class_name: String,
    pub compression_codec: u32,
    pub encryption_key: Option<Vec<u8>>,
    pub major_version: u32,
    pub minor_version: u32,
}

impl FileTrailer {
    pub fn to_proto(&self) -> FileTrailerProto {
        FileTrailerProto {
            file_info_offset: Some(self.file_info_offset),
            load_on_open_data_offset: Some(self.load_on_open_data_offset),
            uncompressed_data_index_size: Some(self.uncompressed_data_index_size),
            total_uncompressed_bytes: Some(self.total_uncompressed_bytes),
            data_index_count: Some(self.data_index_count),
            meta_index_count: Some(self.meta_index_count),
            entry_count: Some(self.entry_count),
            num_data_index_levels: Some(self.num_data_index_levels),
            first_data_block_offset: Some(self.first_data_block_offset),
            last_data_block_offset: Some(self.last_data_block_offset),
            comparator_class_name: Some(self.comparator_class_name.clone()),
            compression_codec: Some(self.compression_codec),
            encryption_key: self.encryption_key.clone(),
        }
    }

    pub fn from_proto(proto: FileTrailerProto, major: u32, minor: u32) -> Self {
        FileTrailer {
            file_info_offset: proto.file_info_offset.unwrap_or(0),
            load_on_open_data_offset: proto.load_on_open_data_offset.unwrap_or(0),
            uncompressed_data_index_size: proto.uncompressed_data_index_size.unwrap_or(0),
            total_uncompressed_bytes: proto.total_uncompressed_bytes.unwrap_or(0),
            data_index_count: proto.data_index_count.unwrap_or(0),
            meta_index_count: proto.meta_index_count.unwrap_or(0),
            entry_count: proto.entry_count.unwrap_or(0),
            num_data_index_levels: proto.num_data_index_levels.unwrap_or(0),
            first_data_block_offset: proto.first_data_block_offset.unwrap_or(0),
            last_data_block_offset: proto.last_data_block_offset.unwrap_or(0),
            comparator_class_name: proto.comparator_class_name.unwrap_or_default(),
            compression_codec: proto.compression_codec.unwrap_or(0),
            encryption_key: proto.encryption_key,
            major_version: major,
            minor_version: minor,
        }
    }

    /// Read the trailer from the tail of `file`.
    pub fn read(file: &File, file_size: u64) -> Result<Self> {
        if file_size < (TRAILER_SIZE as u64).min(64) {
            return corrupt("file is too small to contain an HFile trailer");
        }
        let read_len = (TRAILER_SIZE as u64).min(file_size) as usize;
        let mut buf = vec![0u8; read_len];
        read_exact_at(file, &mut buf, file_size - read_len as u64)?;
        Self::parse_tail(&buf)
    }

    /// Parse a buffer that ends exactly at the end of the file.
    pub fn parse_tail(buf: &[u8]) -> Result<Self> {
        if buf.len() < MAGIC_SIZE + 4 {
            return corrupt("tail buffer is too small for an HFile trailer");
        }
        let magic_pos = buf
            .windows(MAGIC_SIZE)
            .rposition(|w| w == TRAILER_MAGIC)
            .ok_or_else(|| {
                crate::error::HFileError::Corrupt("HFile trailer magic not found".to_string())
            })?;
        let mut pos = magic_pos + MAGIC_SIZE;
        let (pb_len, used) = crate::hbase::kv::read_compressed_int(&buf[pos..])?;
        pos += used;
        let pb_len = pb_len as usize;
        if pb_len > buf.len() - pos {
            return corrupt("trailer protobuf length exceeds the tail buffer");
        }
        let proto = FileTrailerProto::decode(&buf[pos..pos + pb_len])?;
        let version_pos = buf.len() - 4;
        let version = u32::from_be_bytes([
            buf[version_pos],
            buf[version_pos + 1],
            buf[version_pos + 2],
            buf[version_pos + 3],
        ]);
        let (major, minor) = parse_version(version);
        if major != 3 && major != 2 {
            return corrupt(format!("unsupported HFile major version {major}"));
        }
        let trailer = FileTrailer::from_proto(proto, major, minor);
        if trailer.comparator_class_name.is_empty() {
            return corrupt("trailer has no comparator class name");
        }
        Ok(trailer)
    }

    /// Serialize the full 4096 byte trailer.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let proto = self.to_proto();
        let mut pb = Vec::with_capacity(proto.encoded_len());
        proto
            .encode(&mut pb)
            .map_err(|e| crate::error::corrupt_error(format!("failed to encode trailer protobuf: {e}")))?;
        if NOT_PB_SIZE + pb.len() > TRAILER_SIZE {
            return corrupt(format!(
                "trailer protobuf is too large ({} bytes)",
                pb.len()
            ));
        }
        let mut out = Vec::with_capacity(TRAILER_SIZE);
        out.extend_from_slice(TRAILER_MAGIC);
        crate::hbase::kv::write_compressed_int(&mut out, pb.len() as i32);
        out.extend_from_slice(&pb);
        out.resize(TRAILER_SIZE - 4, 0);
        out.extend_from_slice(
            &materialize_version(self.major_version, self.minor_version).to_be_bytes(),
        );
        debug_assert_eq!(out.len(), TRAILER_SIZE);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_materialization() {
        assert_eq!(materialize_version(3, 3), 0x0300_0003);
        assert_eq!(parse_version(0x0300_0003), (3, 3));
    }

    #[test]
    fn trailer_roundtrip() {
        let trailer = FileTrailer {
            file_info_offset: 894,
            load_on_open_data_offset: 757,
            data_index_count: 1,
            entry_count: 10,
            num_data_index_levels: 1,
            comparator_class_name: LEGACY_KV_COMPARATOR.to_string(),
            compression_codec: 2,
            major_version: 3,
            minor_version: 3,
            ..Default::default()
        };
        let bytes = trailer.to_bytes().unwrap();
        assert_eq!(bytes.len(), TRAILER_SIZE);
        assert_eq!(&bytes[0..8], TRAILER_MAGIC);
        let parsed = FileTrailer::parse_tail(&bytes).unwrap();
        assert_eq!(parsed.file_info_offset, 894);
        assert_eq!(parsed.entry_count, 10);
        assert_eq!(parsed.major_version, 3);
        assert_eq!(parsed.minor_version, 3);
        assert_eq!(parsed.compression_codec, 2);
    }
}
