//! `PREFIX` decoding (`PrefixKeyDeltaEncoder`).
//!
//! ```text
//! i32 total unencoded size
//! repeat:
//!   cint key length without common prefix
//!   cint value length
//!   cint common prefix length
//!   bytes key suffix | bytes value | [u16 tags length + tags] | [vlong mvcc]
//! ```

use super::{Cursor, DecodeOptions};
use crate::error::{Result, corrupt};
use crate::hbase::kv::Cell;

pub fn decode(body: &[u8], options: DecodeOptions) -> Result<Vec<Cell>> {
    let mut cursor = Cursor::new(body);
    let decompressed_size = cursor.i32()?;
    if decompressed_size < 0 {
        return corrupt("prefix encoded block has a negative size header");
    }
    let mut cells = Vec::new();
    let mut prev_key: Vec<u8> = Vec::new();
    while !cursor.is_empty() {
        let key_rest = cursor.compressed_int()?;
        let value_len = cursor.compressed_int()?;
        let common = cursor.compressed_int()?;
        if key_rest < 0 || value_len < 0 || common < 0 {
            return corrupt("negative length in prefix encoded block");
        }
        let (key_rest, value_len, common) = (key_rest as usize, value_len as usize, common as usize);
        if common > prev_key.len() {
            return corrupt("prefix encoded block references a common prefix longer than the key");
        }
        let mut key = Vec::with_capacity(common + key_rest);
        key.extend_from_slice(&prev_key[..common]);
        key.extend_from_slice(cursor.take(key_rest)?);
        let value = cursor.take(value_len)?.to_vec();

        let mut cell = Cell::from_key_bytes(&key)?;
        cell.value = value;
        if options.includes_tags {
            // Encoded blocks store the tags length as a 7-bit compressed int.
            let tags_len = cursor.compressed_int()?;
            if tags_len < 0 {
                return corrupt("negative tags length in prefix encoded block");
            }
            cell.tags = cursor.take(tags_len as usize)?.to_vec();
        }
        if options.includes_mvcc {
            cell.seq_id = cursor.vlong()?;
        }
        prev_key = key;
        cells.push(cell);
    }
    Ok(cells)
}
