//! `DIFF` decoding (`DiffKeyDeltaEncoder`).
//!
//! ```text
//! i32 total unencoded size
//! u8 family length | family                       (written once, applies to all cells)
//! repeat:
//!   u8 flag
//!   [cint key length]      if !(flag & 1)
//!   [cint value length]    if !(flag & 2)
//!   cint common prefix
//!   rest of row / qualifier suffix / timestamp / [type] / value
//!   [cint tags length + tags] | [vlong mvcc]
//! ```
//!
//! Flag bits: 1 = same key length, 2 = same value length, 4 = same type,
//! 8 = timestamp is a diff, 0x70 = number of bytes the timestamp needs, 0x80 = negative timestamp.

use super::{Cursor, DecodeOptions};
use crate::error::{Result, corrupt};
use crate::hbase::kv::{Cell, CellType};

const FLAG_SAME_KEY_LENGTH: u8 = 1;
const FLAG_SAME_VALUE_LENGTH: u8 = 2;
const FLAG_SAME_TYPE: u8 = 4;
const FLAG_TIMESTAMP_IS_DIFF: u8 = 8;
const MASK_TIMESTAMP_LENGTH: u8 = 0x70;
const SHIFT_TIMESTAMP_LENGTH: u8 = 4;
const FLAG_TIMESTAMP_SIGN: u8 = 0x80;
const TIMESTAMP_WITH_TYPE_LEN: usize = 9;

struct State {
    key_length: usize,
    value_length: usize,
    row_length: usize,
    timestamp: i64,
    cell_type: CellType,
    family_with_size: Vec<u8>,
}

pub fn decode(body: &[u8], options: DecodeOptions) -> Result<Vec<Cell>> {
    let mut cursor = Cursor::new(body);
    let decompressed_size = cursor.i32()?;
    if decompressed_size < 0 {
        return corrupt("diff encoded block has a negative size header");
    }
    let family_len = cursor.u8()? as usize;
    let mut family_with_size = Vec::with_capacity(family_len + 1);
    family_with_size.push(family_len as u8);
    family_with_size.extend_from_slice(cursor.take(family_len)?);

    let mut state = State {
        key_length: 0,
        value_length: 0,
        row_length: 0,
        timestamp: 0,
        cell_type: CellType::Put,
        family_with_size,
    };

    let mut cells = Vec::new();
    let mut prev_key: Vec<u8> = Vec::new();
    while !cursor.is_empty() {
        let cell = decode_one(&mut cursor, &mut state, &prev_key, options)?;
        prev_key = cell.key_bytes();
        cells.push(cell);
    }
    Ok(cells)
}

fn decode_one(
    cursor: &mut Cursor<'_>,
    state: &mut State,
    prev_key: &[u8],
    options: DecodeOptions,
) -> Result<Cell> {
    let flag = cursor.u8()?;
    let mut carried_type: Option<CellType> = None;

    if flag & FLAG_SAME_KEY_LENGTH == 0 {
        if !prev_key.is_empty() {
            carried_type = Some(CellType::from_code(*prev_key.last().unwrap())?);
        }
        state.key_length = cursor.compressed_int()? as usize;
    }
    if flag & FLAG_SAME_VALUE_LENGTH == 0 {
        state.value_length = cursor.compressed_int()? as usize;
    }
    let common = cursor.compressed_int()? as usize;
    if common > prev_key.len() || common > state.key_length {
        return corrupt("diff encoded block has an invalid common prefix length");
    }

    let key_length = state.key_length;
    let mut key = vec![0u8; key_length];
    key[..common].copy_from_slice(&prev_key[..common]);

    if common < 2 {
        // Row length differs (or is not yet known): copy row length + row.
        let row_len_bytes = 2 - common;
        key[common..2].copy_from_slice(cursor.take(row_len_bytes)?);
        let row_len = u16::from_be_bytes([key[0], key[1]]) as usize;
        let row_with_size = row_len + 2;
        key[2..row_with_size].copy_from_slice(cursor.take(row_len)?);
        state.row_length = row_len;

        let family = &state.family_with_size;
        if row_with_size + family.len() + TIMESTAMP_WITH_TYPE_LEN > key_length {
            return corrupt("diff encoded block key is too short for its family");
        }
        key[row_with_size..row_with_size + family.len()].copy_from_slice(family);

        let qualifier_len = key_length - row_with_size - family.len() - TIMESTAMP_WITH_TYPE_LEN;
        let start = row_with_size + family.len();
        key[start..start + qualifier_len].copy_from_slice(cursor.take(qualifier_len)?);
    } else {
        let row_with_size = state.row_length + 2;
        if common >= row_with_size {
            // Whole row (and family) is part of the common prefix: only the qualifier suffix
            // is stored.
            let rest = key_length - TIMESTAMP_WITH_TYPE_LEN - common;
            key[common..common + rest].copy_from_slice(cursor.take(rest)?);
        } else {
            let rest = row_with_size - common;
            key[common..row_with_size].copy_from_slice(cursor.take(rest)?);
            let family = &state.family_with_size;
            key[row_with_size..row_with_size + family.len()].copy_from_slice(family);
            let qualifier_len = key_length - row_with_size - family.len() - TIMESTAMP_WITH_TYPE_LEN;
            let start = row_with_size + family.len();
            key[start..start + qualifier_len].copy_from_slice(cursor.take(qualifier_len)?);
        }
    }

    // Timestamp: either an absolute value or a diff against the previous cell's timestamp.
    let ts_pos = key_length - TIMESTAMP_WITH_TYPE_LEN;
    let fits = 1 + ((flag & MASK_TIMESTAMP_LENGTH) >> SHIFT_TIMESTAMP_LENGTH) as usize;
    let mut timestamp = cursor.le_long(fits)?;
    if flag & FLAG_TIMESTAMP_SIGN != 0 {
        timestamp = -timestamp;
    }
    if flag & FLAG_TIMESTAMP_IS_DIFF != 0 {
        timestamp = state.timestamp - timestamp;
    }
    key[ts_pos..ts_pos + 8].copy_from_slice(&timestamp.to_be_bytes());

    // Type.
    let cell_type = if flag & FLAG_SAME_TYPE != 0 {
        state.cell_type
    } else {
        CellType::from_code(cursor.u8()?)?
    };
    if let Some(carried) = carried_type
        && flag & FLAG_SAME_TYPE != 0
    {
        // HBase copies the type of the previous key when neither the key length nor the
        // type changed; keep the value that was actually written into the key buffer.
        debug_assert_eq!(carried, cell_type);
    }
    key[key_length - 1] = cell_type.code();

    let value = cursor.take(state.value_length)?.to_vec();

    let mut cell = Cell::from_key_bytes(&key)?;
    cell.value = value;
    if options.includes_tags {
        let tags_len = cursor.compressed_int()?;
        if tags_len < 0 {
            return corrupt("negative tags length in diff encoded block");
        }
        cell.tags = cursor.take(tags_len as usize)?.to_vec();
    }
    if options.includes_mvcc {
        cell.seq_id = cursor.vlong()?;
    }

    state.timestamp = timestamp;
    state.cell_type = cell_type;
    Ok(cell)
}
