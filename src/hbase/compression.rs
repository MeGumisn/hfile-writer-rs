//! HBase block compression.
//!
//! The trailer stores `Compression.Algorithm.ordinal()`, whose values are **not** in
//! alphabetical order. The declaration order below is copied verbatim from HBase's
//! `Compression.Algorithm`, so a codec inserted in the middle (as `BZIP2` was, between
//! `LZ4` and `ZSTD`) shifts every later ordinal:
//!
//! | ordinal | algorithm |
//! |---------|-----------|
//! | 0       | LZO       |
//! | 1       | GZ        |
//! | 2       | NONE      |
//! | 3       | SNAPPY    |
//! | 4       | LZ4       |
//! | 5       | BZIP2     |
//! | 6       | ZSTD      |
//! | 7       | LZMA      |
//! | 8       | BROTLI    |
//!
//! LZO/LZ4/SNAPPY/BZIP2/LZMA/BROTLI need Hadoop native libraries and their framing is not
//! implemented here; they are reported as unsupported instead of silently mis-decoding a block.
//!
//! A `GZ` payload is a plain gzip container. A `ZSTD` payload depends on which zstd codec the
//! cluster runs, because the two implementations in the Hadoop/HBase ecosystem are mutually
//! incompatible; [`ZstdFraming`] selects the layout when writing and [`Compression::decompress`]
//! detects it when reading, so files written by either codec can be read back.

use crate::error::{HFileError, Result, corrupt, unsupported};
use std::io::{Read, Write};

/// HBase `Compression.Algorithm`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    Lzo,
    Gz,
    #[default]
    None,
    Snappy,
    Lz4,
    Bzip2,
    Zstd,
    Lzma,
    Brotli,
}

impl Compression {
    /// Parse the enum ordinal found in `FileTrailerProto.compression_codec`.
    pub fn from_ordinal(ordinal: u32) -> Result<Self> {
        Ok(match ordinal {
            0 => Compression::Lzo,
            1 => Compression::Gz,
            2 => Compression::None,
            3 => Compression::Snappy,
            4 => Compression::Lz4,
            5 => Compression::Bzip2,
            6 => Compression::Zstd,
            7 => Compression::Lzma,
            8 => Compression::Brotli,
            other => return corrupt(format!("unknown compression codec ordinal {other}")),
        })
    }

    /// Ordinal written into the trailer.
    pub fn ordinal(self) -> u32 {
        match self {
            Compression::Lzo => 0,
            Compression::Gz => 1,
            Compression::None => 2,
            Compression::Snappy => 3,
            Compression::Lz4 => 4,
            Compression::Bzip2 => 5,
            Compression::Zstd => 6,
            Compression::Lzma => 7,
            Compression::Brotli => 8,
        }
    }

    /// Configuration name used by HBase (`Compression.Algorithm#getName`).
    pub fn name(self) -> &'static str {
        match self {
            Compression::Lzo => "lzo",
            Compression::Gz => "gz",
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Lz4 => "lz4",
            Compression::Bzip2 => "bzip2",
            Compression::Zstd => "zstd",
            Compression::Lzma => "lzma",
            Compression::Brotli => "brotli",
        }
    }

    pub fn parse_name(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "none" | "n" => Compression::None,
            "gz" | "gzip" => Compression::Gz,
            "zstd" => Compression::Zstd,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            "lzo" => Compression::Lzo,
            "bzip2" | "bz2" => Compression::Bzip2,
            "lzma" => Compression::Lzma,
            "brotli" => Compression::Brotli,
            other => {
                return Err(HFileError::InvalidArgument(format!(
                    "unknown compression codec '{other}'"
                )));
            }
        })
    }

    pub fn is_supported(self) -> bool {
        matches!(
            self,
            Compression::None | Compression::Gz | Compression::Zstd
        )
    }

    /// Compress `data` using the default framing ([`ZstdFraming::Raw`] for zstd).
    pub fn compress(self, data: &[u8]) -> Result<Vec<u8>> {
        self.compress_with(ZstdFraming::default(), data)
    }

    /// Compress `data`; `framing` only matters for [`Compression::Zstd`].
    pub fn compress_with(self, framing: ZstdFraming, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Gz => {
                // Hadoop's GzipCodec produces a gzip container (1f 8b ...), not raw zlib.
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(data)?;
                Ok(encoder.finish()?)
            }
            Compression::Zstd => {
                let frame = zstd::stream::encode_all(data, 3)?;
                match framing {
                    ZstdFraming::Raw => Ok(frame),
                    ZstdFraming::HBaseBlock => Ok(ZstdFraming::HBaseBlock.wrap(data.len(), &frame)),
                }
            }
            other => unsupported(format!(
                "compression codec '{}' is not supported for writing",
                other.name()
            )),
        }
    }

    /// Decompress a block payload. The zstd framing is detected from the payload itself.
    pub fn decompress(self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Gz => {
                let mut out = Vec::with_capacity(uncompressed_size);
                if data.starts_with(&[0x1f, 0x8b]) {
                    flate2::read::GzDecoder::new(data).read_to_end(&mut out)?;
                } else {
                    // Some very old Hadoop versions produced raw zlib streams.
                    flate2::read::ZlibDecoder::new(data).read_to_end(&mut out)?;
                }
                Ok(out)
            }
            Compression::Zstd => {
                let body = ZstdFraming::detect(data, uncompressed_size)?;
                Ok(zstd::stream::decode_all(body)?)
            }
            other => unsupported(format!(
                "compression codec '{}' is not supported for reading",
                other.name()
            )),
        }
    }
}

/// Byte layout of a compressed zstd block payload.
///
/// Two implementations coexist in the Hadoop/HBase ecosystem and neither can read the other's
/// output, so the framing has to be chosen when writing:
///
/// ```text
/// Raw        <zstd frame>
/// HBaseBlock <uncompressedSize:4 BE><compressedSize:4 BE><zstd frame>
/// ```
///
/// `Raw` is what `org.apache.hadoop.io.compress.ZStandardCodec` produces; it is the codec behind
/// HBase's default `hbase.io.compress.zstd.codec`, so it is the default here.
/// `HBaseBlock` is produced by HBase's optional pure Java codec
/// `org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec` (module `hbase-compression-zstd`), which
/// is the usual choice when libhadoop was built without zstd support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ZstdFraming {
    /// A bare zstd frame (Hadoop's native `ZStandardCodec`, HBase default).
    #[default]
    Raw,
    /// HBase's pure Java codec: uncompressed size and frame size, then the frame.
    HBaseBlock,
}

/// Magic number of a zstd frame, little endian `0xFD2FB528`.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
/// Bytes preceding the zstd frame in [`ZstdFraming::HBaseBlock`].
pub const HBASE_BLOCK_ZSTD_PREFIX: usize = 8;

impl ZstdFraming {
    pub fn name(self) -> &'static str {
        match self {
            ZstdFraming::Raw => "raw",
            ZstdFraming::HBaseBlock => "hbase",
        }
    }

    pub fn parse_name(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "raw" | "hadoop" => ZstdFraming::Raw,
            "hbase" | "hbase-block" => ZstdFraming::HBaseBlock,
            other => {
                return Err(HFileError::InvalidArgument(format!(
                    "unknown zstd framing '{other}' (expected raw or hbase)"
                )));
            }
        })
    }

    fn wrap(self, uncompressed_size: usize, frame: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HBASE_BLOCK_ZSTD_PREFIX + frame.len());
        out.extend_from_slice(&(uncompressed_size as u32).to_be_bytes());
        out.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        out.extend_from_slice(frame);
        out
    }

    /// Work out the framing from the payload itself; blocks are self describing because a zstd
    /// frame always starts with [`ZSTD_FRAME_MAGIC`].
    pub(crate) fn detect(data: &[u8], uncompressed_size: usize) -> Result<&[u8]> {
        if data.starts_with(&ZSTD_FRAME_MAGIC) {
            return Ok(data);
        }
        if data.len() >= HBASE_BLOCK_ZSTD_PREFIX {
            let prefix = &data[..HBASE_BLOCK_ZSTD_PREFIX];
            let size = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
            let frame_len =
                u32::from_be_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]) as usize;
            if size == uncompressed_size && frame_len + HBASE_BLOCK_ZSTD_PREFIX == data.len() {
                let body = &data[HBASE_BLOCK_ZSTD_PREFIX..];
                if body.starts_with(&ZSTD_FRAME_MAGIC) {
                    return Ok(body);
                }
            }
        }
        corrupt(format!(
            "zstd payload starts with {:02x?} which is neither a zstd frame nor an \
             hbase-compression-zstd block",
            &data[..data.len().min(8)]
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinals_match_hbase() {
        assert_eq!(Compression::Lzo.ordinal(), 0);
        assert_eq!(Compression::Gz.ordinal(), 1);
        assert_eq!(Compression::None.ordinal(), 2);
        assert_eq!(Compression::Snappy.ordinal(), 3);
        assert_eq!(Compression::Lz4.ordinal(), 4);
        // HBase inserted BZIP2 between LZ4 and ZSTD, so ZSTD is 6, not 5.
        assert_eq!(Compression::Bzip2.ordinal(), 5);
        assert_eq!(Compression::Zstd.ordinal(), 6);
        assert_eq!(Compression::Lzma.ordinal(), 7);
        assert_eq!(Compression::Brotli.ordinal(), 8);
        for codec in [
            Compression::Lzo,
            Compression::Gz,
            Compression::None,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Bzip2,
            Compression::Zstd,
            Compression::Lzma,
            Compression::Brotli,
        ] {
            assert_eq!(Compression::from_ordinal(codec.ordinal()).unwrap(), codec);
        }
    }

    #[test]
    fn roundtrip() {
        let data = b"hello hello hello hello hbase".repeat(100);
        for codec in [Compression::None, Compression::Gz, Compression::Zstd] {
            let compressed = codec.compress(&data).unwrap();
            let out = codec.decompress(&compressed, data.len()).unwrap();
            assert_eq!(out, data);
        }
    }

    #[test]
    fn zstd_framings_roundtrip() {
        let data = b"hello hello hello hello hbase".repeat(100);
        for framing in [ZstdFraming::Raw, ZstdFraming::HBaseBlock] {
            let compressed = Compression::Zstd.compress_with(framing, &data).unwrap();
            match framing {
                ZstdFraming::Raw => assert!(compressed.starts_with(&ZSTD_FRAME_MAGIC)),
                ZstdFraming::HBaseBlock => {
                    assert_eq!(&compressed[..4], &(data.len() as u32).to_be_bytes());
                    assert_eq!(
                        &compressed[4..8],
                        &((compressed.len() - 8) as u32).to_be_bytes()
                    );
                    assert!(compressed[8..].starts_with(&ZSTD_FRAME_MAGIC));
                }
            }
            // The reader works out the framing from the payload, so both decode.
            assert_eq!(
                Compression::Zstd
                    .decompress(&compressed, data.len())
                    .unwrap(),
                data
            );
        }
    }

    #[test]
    fn zstd_framing_names() {
        for framing in [ZstdFraming::Raw, ZstdFraming::HBaseBlock] {
            assert_eq!(ZstdFraming::parse_name(framing.name()).unwrap(), framing);
        }
        assert!(ZstdFraming::parse_name("snappy").is_err());
    }
}
