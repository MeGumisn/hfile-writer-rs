//! Read and write Apache HBase HFile v3 files, and convert between HFile and Arrow.
//!
//! ```
//! use hfile_rs::hbase::{Cell, HFileReader, HFileWriter, WriterOptions};
//!
//! # fn main() -> hfile_rs::error::Result<()> {
//! let path = std::env::temp_dir().join("example.hfile");
//! let mut writer = HFileWriter::create(&path, WriterOptions::default())?;
//! writer.append(Cell::new(
//!     b"row1".to_vec(),
//!     b"f".to_vec(),
//!     b"q".to_vec(),
//!     b"value".to_vec(),
//! ))?;
//! writer.finish()?;
//!
//! let reader = HFileReader::open(&path)?;
//! let cells = reader.read_all()?;
//! assert_eq!(cells.len(), 1);
//! assert_eq!(cells[0].value, b"value");
//! # std::fs::remove_file(&path).ok();
//! # Ok(())
//! # }
//! ```

pub mod arrow;
pub mod error;
pub mod hbase;

pub use error::{HFileError, Result};
pub use hbase::reader::HFileReader;
pub use hbase::writer::{HFileWriter, WriterOptions};
