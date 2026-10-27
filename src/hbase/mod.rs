//! HBase HFile v3 format support.
//!
//! Module map:
//!
//! | module       | HBase counterpart                                     |
//! |--------------|-------------------------------------------------------|
//! | `block`      | `HFileBlock`, `ChecksumUtil`                          |
//! | `trailer`    | `FixedFileTrailer`                                    |
//! | `file_info`  | `HFileInfo`, `HFileWriterImpl` file info keys          |
//! | `kv`         | `KeyValue`, `KeyValueUtil`, `WritableUtils`           |
//! | `index`      | `HFileBlockIndex`, `NoOpIndexBlockEncoder`            |
//! | `encoding`   | `DataBlockEncoding` + the four codecs                 |
//! | `compression`| `Compression.Algorithm`                               |
//! | `reader`     | `HFileReaderImpl`, `HFileScannerImpl`                 |
//! | `writer`     | `HFileWriterImpl`                                     |

pub mod block;
pub mod compression;
pub mod encoding;
pub mod file_info;
pub mod index;
pub mod kv;
pub mod reader;
pub mod trailer;
pub mod writer;

/// Protobuf messages copied from `hbase-protocol` (`hfile.proto`).
pub mod pb {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/hbase.pb.rs"));
}

pub use block::{Block, BlockHeader, BlockType, HEADER_SIZE};
pub use compression::{Compression, ZstdFraming};
pub use encoding::{BlockEncoder, DataBlockEncoding, DecodeOptions};
pub use file_info::FileInfo;
pub use kv::{Cell, CellType, compare_cells};
pub use reader::{CellIter, HFileReader, ReaderOptions};
pub use trailer::{FileTrailer, TRAILER_SIZE};
pub use writer::{HFileWriter, WriterOptions, write_hfile};
