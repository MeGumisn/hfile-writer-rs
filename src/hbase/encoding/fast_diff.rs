//! `FAST_DIFF` decoding (`FastDiffDeltaEncoder`).
//!
//! ```text
//! i32 total unencoded size
//! repeat:
//!   u8 flag
//!   [cint key length]      if !(flag & 8)
//!   [cint value length]    if !(flag & 16)
//!   cint common prefix
//!   row / qualifier suffix / timestamp suffix / [type] / [value]
//!   [cint tags length + tags] | [vlong mvcc]
//! ```
//!
//! Flag bits: 0x7 = number of leading timestamp bytes shared with the previous cell,
//! 8 = same key length, 16 = same value length, 32 = same type, 64 = same value.

use super::{Cursor, DecodeOptions};
use crate::error::{Result, corrupt};
use crate::hbase::kv::{Cell, CellType};

const FLAG_SAME_KEY_LENGTH: u8 = 1 << 3;
const FLAG_SAME_VALUE_LENGTH: u8 = 1 << 4;
const FLAG_SAME_TYPE: u8 = 1 << 5;
const FLAG_SAME_VALUE: u8 = 1 << 6;
const MASK_TIMESTAMP_LENGTH: u8 = 0x07;
const TIMESTAMP_WITH_TYPE_LEN: usize = 9;

#[derive(Default)]
struct State {
    key: Vec<u8>,
    value: Vec<u8>,
    key_length: usize,
    value_length: usize,
    row_length: usize,
    family_len: usize,
    cell_type: CellType,
}

pub fn decode(body: &[u8], options: DecodeOptions) -> Result<Vec<Cell>> {
    let mut cursor = Cursor::new(body);
    let decompressed_size = cursor.i32()?;
    if decompressed_size < 0 {
        return corrupt("fast diff encoded block has a negative size header");
    }
    let mut state = State::default();
    let mut cells = Vec::new();
    while !cursor.is_empty() {
        let cell = decode_one(&mut cursor, &mut state, options)?;
        state.key = cell.key_bytes();
        state.value = cell.value.clone();
        cells.push(cell);
    }
    Ok(cells)
}

fn decode_one(cursor: &mut Cursor<'_>, state: &mut State, options: DecodeOptions) -> Result<Cell> {
    let flag = cursor.u8()?;
    let first = state.key.is_empty();

    if flag & FLAG_SAME_KEY_LENGTH == 0 {
        state.key_length = cursor.compressed_int()? as usize;
    }
    if flag & FLAG_SAME_VALUE_LENGTH == 0 {
        state.value_length = cursor.compressed_int()? as usize;
    }
    let common = cursor.compressed_int()? as usize;

    if state.key_length < TIMESTAMP_WITH_TYPE_LEN {
        return corrupt("fast diff encoded key length is too small");
    }

    let key_length = state.key_length;
    let mut key = vec![0u8; key_length];
    let value;

    if first {
        key.copy_from_slice(cursor.take(key_length)?);
        value = cursor.take(state.value_length)?.to_vec();
        state.row_length = u16::from_be_bytes([key[0], key[1]]) as usize;
        if 2 + state.row_length + 1 + TIMESTAMP_WITH_TYPE_LEN > key_length {
            return corrupt("fast diff encoded first key is too short for its row and family");
        }
        state.family_len = key[2 + state.row_length] as usize;
        if 2 + state.row_length + 1 + state.family_len + TIMESTAMP_WITH_TYPE_LEN > key_length {
            return corrupt("fast diff encoded first key is too short for its family");
        }
        state.cell_type = CellType::from_code(key[key_length - 1])?;
    } else {
        let prev_key = &state.key;
        if common > prev_key.len() || common > key_length {
            return corrupt("fast diff encoded block has an invalid common prefix length");
        }
        key[..common].copy_from_slice(&prev_key[..common]);

        let mut row_with_size = state.row_length + 2;
        if common < row_with_size {
            if common < 2 {
                key[common..2].copy_from_slice(cursor.take(2 - common)?);
                state.row_length = u16::from_be_bytes([key[0], key[1]]) as usize;
                row_with_size = state.row_length + 2;
                key[2..row_with_size].copy_from_slice(cursor.take(state.row_length)?);
            } else {
                key[common..row_with_size].copy_from_slice(cursor.take(row_with_size - common)?);
            }
            // The family is never stored again; copy it from the previous key.
            let family_len = state.family_len;
            let fam_start = 2 + state.row_length;
            if fam_start + 1 + family_len > prev_key.len()
                || row_with_size + 1 + family_len > key_length
            {
                return corrupt("fast diff encoded block has an invalid family offset");
            }
            key[row_with_size..row_with_size + 1 + family_len]
                .copy_from_slice(&prev_key[fam_start..fam_start + 1 + family_len]);

            let qual_len = key_length - row_with_size - (1 + family_len) - TIMESTAMP_WITH_TYPE_LEN;
            let qual_start = row_with_size + 1 + family_len;
            key[qual_start..qual_start + qual_len].copy_from_slice(cursor.take(qual_len)?);
        } else {
            let rest = key_length - TIMESTAMP_WITH_TYPE_LEN - common;
            key[common..common + rest].copy_from_slice(cursor.take(rest)?);
        }

        // Timestamp: a prefix of the previous timestamp plus a suffix from the stream.
        let ts_pos = key_length - TIMESTAMP_WITH_TYPE_LEN;
        let prefix_ts = (flag & MASK_TIMESTAMP_LENGTH) as usize;
        let prev_ts_pos = prev_key.len() - TIMESTAMP_WITH_TYPE_LEN;
        if prefix_ts > 8 || prev_ts_pos + 8 > prev_key.len() {
            return corrupt("fast diff encoded block has an invalid timestamp prefix");
        }
        key[ts_pos..ts_pos + prefix_ts]
            .copy_from_slice(&prev_key[prev_ts_pos..prev_ts_pos + prefix_ts]);
        key[ts_pos + prefix_ts..ts_pos + 8].copy_from_slice(cursor.take(8 - prefix_ts)?);

        // Type and value.
        let cell_type = if flag & FLAG_SAME_TYPE != 0 {
            state.cell_type
        } else {
            CellType::from_code(cursor.u8()?)?
        };
        key[key_length - 1] = cell_type.code();
        state.cell_type = cell_type;

        value = if flag & FLAG_SAME_VALUE != 0 {
            if state.value.len() < state.value_length {
                return corrupt("fast diff encoded block reuses a value that is too short");
            }
            state.value[..state.value_length].to_vec()
        } else {
            cursor.take(state.value_length)?.to_vec()
        };
    }

    let mut cell = Cell::from_key_bytes(&key)?;
    cell.value = value;
    if options.includes_tags {
        let tags_len = cursor.compressed_int()?;
        if tags_len < 0 {
            return corrupt("negative tags length in fast diff encoded block");
        }
        cell.tags = cursor.take(tags_len as usize)?.to_vec();
    }
    if options.includes_mvcc {
        cell.seq_id = cursor.vlong()?;
    }
    Ok(cell)
}
