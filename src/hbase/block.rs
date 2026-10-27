//! HFile block framing: 33 byte header, optional compression and CRC chunk checksums.
//!
//! ```text
//! offset 0   8   block type magic                    e.g. "DATABLK*", "IDXROOT2", "FILEINF2"
//! offset 8   4   onDiskSizeWithoutHeader             data + checksums (no header)
//! offset 12  4   uncompressedSizeWithoutHeader       payload size, checksums excluded
//! offset 16  8   prevBlockOffset                      offset of previous block of the same type
//! offset 24  1   checksumType                         0 = NULL, 1 = CRC32, 2 = CRC32C
//! offset 25  4   bytesPerChecksum                     usually 16384
//! offset 29  4   onDiskDataSizeWithHeader             header + (possibly compressed) data
//! offset 33  N   payload                              compressed when the trailer says so
//! offset ..  M   checksums                            M = ceil(bytesPerChecksum chunks) * 4
//! ```
//!
//! Checksums cover the block **including** the header and are chunked every
//! `bytesPerChecksum` bytes. This mirrors `HFileBlock` + `ChecksumUtil`.

use crate::error::{Result, corrupt, unsupported};
use crate::hbase::compression::{Compression, ZstdFraming};
use std::fs::File;

/// Length of a block magic.
pub const MAGIC_SIZE: usize = 8;
/// Size of the fixed block header (minor version >= 1, i.e. every HFile v2/v3 block).
pub const HEADER_SIZE: usize = 33;
/// Default chunk size used by HBase (`HFile.DEFAULT_BYTES_PER_CHECKSUM`).
pub const DEFAULT_BYTES_PER_CHECKSUM: u32 = 16 * 1024;
/// Default HBase data block size.
pub const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;

/// Checksum algorithms supported by the HFile format (`HFile.ChecksumType`).
pub mod checksum {
    pub const NULL: u8 = 0;
    pub const CRC32: u8 = 1;
    pub const CRC32C: u8 = 2;
}

/// `org.apache.hadoop.hbase.io.hfile.BlockType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    /// Raw data block (`DATABLK*`).
    Data,
    /// Encoded data block (`DATABLKE`), payload starts with a 2 byte encoding id.
    EncodedData,
    /// Leaf level index block, stored inside the data section (`IDXLEAF2`).
    LeafIndex,
    /// Bloom filter chunk (`BLMFBLK2`).
    BloomChunk,
    /// Meta block (`METABLKc`).
    Meta,
    /// Intermediate index level (`IDXINTE2`).
    IntermediateIndex,
    /// Root level index (`IDXROOT2`).
    RootIndex,
    /// File info block (`FILEINF2`).
    FileInfo,
    /// General bloom filter metadata (`BLMFMET2`).
    GeneralBloomMeta,
    /// Delete family bloom filter metadata (`DFBLMET2`).
    DeleteFamilyBloomMeta,
    /// Fixed file trailer magic (`TRABLK"$`).
    Trailer,
    /// Legacy v1 index (`IDXBLK)+`).
    IndexV1,
}

impl BlockType {
    pub fn magic(self) -> &'static [u8; MAGIC_SIZE] {
        match self {
            BlockType::Data => b"DATABLK*",
            BlockType::EncodedData => b"DATABLKE",
            BlockType::LeafIndex => b"IDXLEAF2",
            BlockType::BloomChunk => b"BLMFBLK2",
            BlockType::Meta => b"METABLKc",
            BlockType::IntermediateIndex => b"IDXINTE2",
            BlockType::RootIndex => b"IDXROOT2",
            BlockType::FileInfo => b"FILEINF2",
            BlockType::GeneralBloomMeta => b"BLMFMET2",
            BlockType::DeleteFamilyBloomMeta => b"DFBLMET2",
            BlockType::Trailer => b"TRABLK\"$",
            BlockType::IndexV1 => b"IDXBLK)+",
        }
    }

    pub fn from_magic(magic: &[u8]) -> Result<Self> {
        const ALL: [BlockType; 12] = [
            BlockType::Data,
            BlockType::EncodedData,
            BlockType::LeafIndex,
            BlockType::BloomChunk,
            BlockType::Meta,
            BlockType::IntermediateIndex,
            BlockType::RootIndex,
            BlockType::FileInfo,
            BlockType::GeneralBloomMeta,
            BlockType::DeleteFamilyBloomMeta,
            BlockType::Trailer,
            BlockType::IndexV1,
        ];
        for t in ALL {
            if t.magic() == magic {
                return Ok(t);
            }
        }
        corrupt(format!("unknown block magic {magic:?}"))
    }

    pub fn is_data(self) -> bool {
        matches!(self, BlockType::Data | BlockType::EncodedData)
    }

    pub fn is_index(self) -> bool {
        matches!(
            self,
            BlockType::LeafIndex
                | BlockType::IntermediateIndex
                | BlockType::RootIndex
                | BlockType::IndexV1
        )
    }
}

/// Parsed block header.
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    pub block_type: BlockType,
    pub on_disk_size_without_header: u32,
    pub uncompressed_size_without_header: u32,
    pub prev_block_offset: i64,
    pub checksum_type: u8,
    pub bytes_per_checksum: u32,
    pub on_disk_data_size_with_header: u32,
}

impl BlockHeader {
    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return corrupt("truncated block header");
        }
        let block_type = BlockType::from_magic(&buf[0..MAGIC_SIZE])?;
        let on_disk_size_without_header = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let uncompressed_size_without_header =
            u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let mut prev = [0u8; 8];
        prev.copy_from_slice(&buf[16..24]);
        let prev_block_offset = i64::from_be_bytes(prev);
        let ctype = buf[24];
        let bytes_per_checksum = u32::from_be_bytes([buf[25], buf[26], buf[27], buf[28]]);
        let on_disk_data_size_with_header =
            u32::from_be_bytes([buf[29], buf[30], buf[31], buf[32]]);
        if on_disk_data_size_with_header < HEADER_SIZE as u32 {
            return corrupt("block header reports data size smaller than the header");
        }
        Ok(BlockHeader {
            block_type,
            on_disk_size_without_header,
            uncompressed_size_without_header,
            prev_block_offset,
            checksum_type: ctype,
            bytes_per_checksum,
            on_disk_data_size_with_header,
        })
    }

    /// Total size of the block on disk, header and checksums included.
    pub fn total_size(&self) -> u64 {
        HEADER_SIZE as u64 + self.on_disk_size_without_header as u64
    }

    pub fn num_checksums(&self) -> usize {
        num_checksums(
            self.on_disk_data_size_with_header as usize,
            self.bytes_per_checksum,
        )
    }
}

/// Number of checksum values stored at the end of a block.
pub fn num_checksums(data_size_with_header: usize, bytes_per_checksum: u32) -> usize {
    if bytes_per_checksum == 0 {
        return 0;
    }
    data_size_with_header.div_ceil(bytes_per_checksum as usize)
}

/// A fully materialized (decompressed) block payload.
#[derive(Debug, Clone)]
pub struct Block {
    pub block_type: BlockType,
    pub offset: u64,
    pub header: BlockHeader,
    /// Payload after decompression, checksums stripped.
    pub payload: Vec<u8>,
}

impl Block {
    pub fn total_size(&self) -> u64 {
        self.header.total_size()
    }
}

/// Positioned read helper, works on Unix (`pread`) and Windows (`seek_read`).
pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                ));
            }
            done += n;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.read_exact(buf)
    }
}

/// Read only the header at `offset`.
pub fn read_header_at(file: &File, file_size: u64, offset: u64) -> Result<BlockHeader> {
    if offset + HEADER_SIZE as u64 > file_size {
        return corrupt(format!(
            "block header at {offset} does not fit inside the file (size {file_size})"
        ));
    }
    let mut buf = [0u8; HEADER_SIZE];
    read_exact_at(file, &mut buf, offset)?;
    BlockHeader::parse(&buf)
}

/// Read, verify and decompress the block at `offset`.
pub fn read_block_at(
    file: &File,
    file_size: u64,
    offset: u64,
    compression: Compression,
    verify_checksums: bool,
    expected: Option<BlockType>,
) -> Result<Block> {
    let header = read_header_at(file, file_size, offset)?;
    if let Some(expected) = expected
        && !(header.block_type == expected
            || (expected == BlockType::Data && header.block_type.is_data()))
    {
        return corrupt(format!(
            "expected block {:?} at offset {offset} but found {:?}",
            expected, header.block_type
        ));
    }
    let total = header.total_size();
    if offset + total > file_size {
        return corrupt(format!(
            "block at {offset} claims {total} bytes but the file ends at {file_size}"
        ));
    }
    let mut raw = vec![0u8; total as usize];
    read_exact_at(file, &mut raw, offset)?;

    // onDiskDataSizeWithHeader includes the 33 byte header, so the payload ends here.
    let payload_end = header.on_disk_data_size_with_header as usize;
    if payload_end < HEADER_SIZE || payload_end > raw.len() {
        return corrupt("block header reports an invalid data size");
    }

    if verify_checksums {
        verify_block_checksums(
            &raw,
            payload_end,
            header.checksum_type,
            header.bytes_per_checksum,
        )?;
    }

    let compressed = &raw[HEADER_SIZE..payload_end];
    let payload = if compression == Compression::None {
        compressed.to_vec()
    } else {
        let out =
            compression.decompress(compressed, header.uncompressed_size_without_header as usize)?;
        if out.len() != header.uncompressed_size_without_header as usize {
            return corrupt(format!(
                "decompressed size {} does not match header value {}",
                out.len(),
                header.uncompressed_size_without_header
            ));
        }
        out
    };

    Ok(Block {
        block_type: header.block_type,
        offset,
        header,
        payload,
    })
}

fn verify_block_checksums(
    raw: &[u8],
    payload_end: usize,
    ctype: u8,
    bytes_per_checksum: u32,
) -> Result<()> {
    if ctype == checksum::NULL {
        return Ok(());
    }
    if bytes_per_checksum == 0 {
        return corrupt("bytesPerChecksum is zero");
    }
    let chunks = num_checksums(payload_end, bytes_per_checksum);
    let expected_len = payload_end + chunks * 4;
    if raw.len() < expected_len {
        return corrupt("block is missing its checksums");
    }
    for i in 0..chunks {
        let start = i * bytes_per_checksum as usize;
        let end = ((i + 1) * bytes_per_checksum as usize).min(payload_end);
        let stored = u32::from_be_bytes([
            raw[payload_end + i * 4],
            raw[payload_end + i * 4 + 1],
            raw[payload_end + i * 4 + 2],
            raw[payload_end + i * 4 + 3],
        ]);
        let actual = chunk_checksum(ctype, &raw[start..end])?;
        if actual != stored {
            return corrupt(format!(
                "checksum mismatch in chunk {i}: stored {stored:#010x}, computed {actual:#010x}"
            ));
        }
    }
    Ok(())
}

fn chunk_checksum(ctype: u8, bytes: &[u8]) -> Result<u32> {
    Ok(match ctype {
        checksum::CRC32 => crc32fast::hash(bytes),
        checksum::CRC32C => crc32c::crc32c(bytes),
        other => return unsupported(format!("unknown checksum type {other}")),
    })
}

/// Build a complete on-disk block (header + compressed payload + checksums).
///
/// `zstd_framing` is ignored unless `compression` is [`Compression::Zstd`]; see
/// [`ZstdFraming`].
pub fn build_block(
    block_type: BlockType,
    payload: &[u8],
    prev_block_offset: i64,
    compression: Compression,
    zstd_framing: ZstdFraming,
    ctype: u8,
    bytes_per_checksum: u32,
) -> Result<Vec<u8>> {
    let compressed = compression.compress_with(zstd_framing, payload)?;
    let data_size_with_header = HEADER_SIZE + compressed.len();
    let chunks = num_checksums(data_size_with_header, bytes_per_checksum);
    let mut out = Vec::with_capacity(data_size_with_header + chunks * 4);
    out.extend_from_slice(block_type.magic());
    out.extend_from_slice(&((compressed.len() + chunks * 4) as u32).to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&prev_block_offset.to_be_bytes());
    out.push(ctype);
    out.extend_from_slice(&bytes_per_checksum.to_be_bytes());
    out.extend_from_slice(&(data_size_with_header as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    if ctype != checksum::NULL && bytes_per_checksum > 0 {
        for i in 0..chunks {
            let start = i * bytes_per_checksum as usize;
            let end = ((i + 1) * bytes_per_checksum as usize).min(data_size_with_header);
            let sum = chunk_checksum(ctype, &out[start..end])?;
            out.extend_from_slice(&sum.to_be_bytes());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_roundtrip_all_checksums() {
        let payload =
            b"some payload that will be chunked because bytesPerChecksum is tiny".repeat(20);
        for ctype in [checksum::CRC32C, checksum::CRC32] {
            let block = build_block(
                BlockType::RootIndex,
                &payload,
                -1,
                Compression::None,
                ZstdFraming::Raw,
                ctype,
                64,
            )
            .unwrap();
            let header = BlockHeader::parse(&block).unwrap();
            assert_eq!(header.block_type, BlockType::RootIndex);
            assert_eq!(
                header.uncompressed_size_without_header as usize,
                payload.len()
            );
            verify_block_checksums(
                &block,
                header.on_disk_data_size_with_header as usize,
                ctype,
                64,
            )
            .unwrap();

            let mut broken = block.clone();
            broken[HEADER_SIZE] ^= 0xff;
            assert!(
                verify_block_checksums(
                    &broken,
                    header.on_disk_data_size_with_header as usize,
                    ctype,
                    64
                )
                .is_err()
            );
        }
    }

    #[test]
    fn block_roundtrip_compressed() {
        let payload = b"compressible compressible compressible".repeat(100);
        for codec in [Compression::None, Compression::Gz, Compression::Zstd] {
            for framing in [ZstdFraming::Raw, ZstdFraming::HBaseBlock] {
                let block = build_block(
                    BlockType::FileInfo,
                    &payload,
                    -1,
                    codec,
                    framing,
                    checksum::CRC32C,
                    16384,
                )
                .unwrap();
                let header = BlockHeader::parse(&block).unwrap();
                assert_eq!(
                    header.uncompressed_size_without_header as usize,
                    payload.len()
                );
                verify_block_checksums(
                    &block,
                    header.on_disk_data_size_with_header as usize,
                    checksum::CRC32C,
                    16384,
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn num_checksums_matches_hbase() {
        assert_eq!(num_checksums(0, 16384), 0);
        assert_eq!(num_checksums(1, 16384), 1);
        assert_eq!(num_checksums(16384, 16384), 1);
        assert_eq!(num_checksums(16385, 16384), 2);
    }
}
