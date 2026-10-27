//! `ROW_INDEX_V1` decoding (`RowIndexCodecV1` + `RowIndexEncoderV1`).
//!
//! ```text
//! cells (raw KeyValue layout, tags length as u16, optional mvcc vlong)
//! i32 number of rows
//! i32 * rows  offset of the first cell of every row, relative to the cell area
//! i32 size of the cell area
//! ```
//!
//! The row offsets are only used for seeks (they map a row number to a byte offset), so a full
//! scan only needs the cell area.

use super::DecodeOptions;
use crate::error::{Result, corrupt};
use crate::hbase::kv::{Cell, parse_raw_cells};

/// Split a `ROW_INDEX_V1` payload into `(cell area, row offsets)`.
pub fn split(body: &[u8]) -> Result<(&[u8], Vec<u32>)> {
    if body.len() < 8 {
        return corrupt("row index block is too short");
    }
    let data_size = i32::from_be_bytes([
        body[body.len() - 4],
        body[body.len() - 3],
        body[body.len() - 2],
        body[body.len() - 1],
    ]);
    if data_size < 0 || data_size as usize + 8 > body.len() {
        return corrupt("row index block has an invalid cell area size");
    }
    let data_size = data_size as usize;
    let num_rows = i32::from_be_bytes([
        body[data_size],
        body[data_size + 1],
        body[data_size + 2],
        body[data_size + 3],
    ]);
    if num_rows < 0 || data_size + 4 + num_rows as usize * 4 + 4 != body.len() {
        return corrupt("row index block has an invalid row count");
    }
    let mut rows = Vec::with_capacity(num_rows as usize);
    for i in 0..num_rows as usize {
        let pos = data_size + 4 + i * 4;
        rows.push(u32::from_be_bytes([
            body[pos],
            body[pos + 1],
            body[pos + 2],
            body[pos + 3],
        ]));
    }
    Ok((&body[..data_size], rows))
}

pub fn decode(body: &[u8], options: DecodeOptions) -> Result<Vec<Cell>> {
    let (cell_area, _rows) = split(body)?;
    Ok(parse_raw_cells(
        cell_area,
        options.includes_tags,
        options.includes_mvcc,
    )?)
}
