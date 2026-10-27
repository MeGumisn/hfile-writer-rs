//! Error type shared by the HFile reader/writer and the Arrow bridge.

use std::fmt;

/// Result alias used across the crate.
pub type Result<T> = std::result::Result<T, HFileError>;

/// Every failure mode of this crate.
#[derive(Debug)]
pub enum HFileError {
    /// Underlying I/O error.
    Io(std::io::Error),
    /// The file does not match the HFile specification.
    Corrupt(String),
    /// A feature exists in HBase but is intentionally not implemented here.
    Unsupported(String),
    /// Caller passed invalid arguments (unsorted cells, bad mapping, ...).
    InvalidArgument(String),
    /// Protobuf decoding error while reading the trailer / file info.
    Proto(prost::DecodeError),
    /// Arrow / Parquet related error.
    Arrow(String),
}

impl fmt::Display for HFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HFileError::Io(e) => write!(f, "i/o error: {e}"),
            HFileError::Corrupt(msg) => write!(f, "corrupt hfile: {msg}"),
            HFileError::Unsupported(msg) => write!(f, "unsupported hfile feature: {msg}"),
            HFileError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            HFileError::Proto(e) => write!(f, "protobuf error: {e}"),
            HFileError::Arrow(msg) => write!(f, "arrow error: {msg}"),
        }
    }
}

impl std::error::Error for HFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HFileError::Io(e) => Some(e),
            HFileError::Proto(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for HFileError {
    fn from(e: std::io::Error) -> Self {
        HFileError::Io(e)
    }
}

impl From<prost::DecodeError> for HFileError {
    fn from(e: prost::DecodeError) -> Self {
        HFileError::Proto(e)
    }
}

impl From<arrow_schema::ArrowError> for HFileError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        HFileError::Arrow(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for HFileError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        HFileError::Arrow(e.to_string())
    }
}

pub(crate) fn corrupt<T>(msg: impl Into<String>) -> Result<T> {
    Err(HFileError::Corrupt(msg.into()))
}

/// Build a `Corrupt` error value (for `map_err` call sites).
pub(crate) fn corrupt_error(msg: impl Into<String>) -> HFileError {
    HFileError::Corrupt(msg.into())
}

pub(crate) fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(HFileError::InvalidArgument(msg.into()))
}

pub(crate) fn unsupported<T>(msg: impl Into<String>) -> Result<T> {
    Err(HFileError::Unsupported(msg.into()))
}
