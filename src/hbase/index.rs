//! Block index (`INDEX_V1`, the only index format HFile v3 uses).
//!
//! Two layouts exist.
//!
//! **Root index** (`IDXROOT2`), read with `data_index_count` entries from the trailer:
//!
//! ```text
//! repeat data_index_count times:
//!   i64 block offset | i32 on-disk size (header included) | vint key length | key
//! optional mid-key metadata (only when num_data_index_levels > 1):
//!   i64 leaf block offset | i32 on-disk size | i32 entry index
//! ```
//!
//! **Non-root index** (`IDXLEAF2` / `IDXINTE2`):
//!
//! ```text
//! i32 num entries
//! i32 * num entries        cumulative end offset of every entry, relative to entries start
//! i32                      total size of all entries
//! repeat num entries:
//!   i64 block offset | i32 on-disk size | key (length inferred from neighbouring offsets)
//! ```
//!
//! Index keys are the **first key of the referenced block** (HBase may store a shorter
//! "midpoint" key; both are handled by the binary search below).

use crate::error::{Result, corrupt};
use crate::hbase::kv::{
    compare_flat_keys, read_compressed_int, read_i32, read_i64, write_compressed_int,
};

/// One block index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    /// First key of the referenced block (serialized flat key).
    pub key: Vec<u8>,
    /// Absolute file offset of the referenced block.
    pub offset: u64,
    /// On-disk size of the referenced block, header and checksums included.
    pub on_disk_size: u32,
}

/// Parsed root index block.
#[derive(Debug, Clone, Default)]
pub struct RootIndex {
    pub entries: Vec<IndexEntry>,
    /// `(leaf offset, leaf on-disk size, entry index)` for multi-level indexes.
    pub mid_key_meta: Option<(u64, u32, u32)>,
}

impl RootIndex {
    /// Index of the block that may contain `key` (largest entry <= key).
    ///
    /// Mirrors `HFileBlockIndex.rootBlockContainingKey`: an exact match returns that entry,
    /// otherwise the previous one, and `None` when `key` sorts before the first entry.
    pub fn containing(&self, key: &[u8]) -> Option<usize> {
        search(&self.entries, key)
    }
}

/// Binary search over index entries with the HBase comparator.
pub fn search(entries: &[IndexEntry], key: &[u8]) -> Option<usize> {
    if entries.is_empty() {
        return None;
    }
    let mut lo = 0i64;
    let mut hi = entries.len() as i64 - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        match compare_flat_keys(&entries[mid as usize].key, key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Equal => return Some(mid as usize),
            std::cmp::Ordering::Greater => hi = mid - 1,
        }
    }
    if lo == 0 { None } else { Some((lo - 1) as usize) }
}

/// Parse the root index block payload.
pub fn parse_root_index(data: &[u8], num_entries: u32) -> Result<RootIndex> {
    let mut pos = 0usize;
    let mut entries = Vec::with_capacity(num_entries as usize);
    for i in 0..num_entries {
        let offset = read_i64(data, pos)?;
        let on_disk_size = read_i32(data, pos + 8)?;
        if on_disk_size < 0 {
            return corrupt(format!("root index entry {i} has a negative size"));
        }
        let (key_len, used) = read_compressed_int(&data[pos + 12..])?;
        let key_start = pos + 12 + used;
        let key_len = key_len as usize;
        if key_len > data.len() - key_start {
            return corrupt("root index key length exceeds the block payload");
        }
        entries.push(IndexEntry {
            key: data[key_start..key_start + key_len].to_vec(),
            offset: offset as u64,
            on_disk_size: on_disk_size as u32,
        });
        pos = key_start + key_len;
    }
    let mid_key_meta = if data.len() - pos >= 16 {
        let offset = read_i64(data, pos)? as u64;
        let size = read_i32(data, pos + 8)? as u32;
        let index = read_i32(data, pos + 12)? as u32;
        Some((offset, size, index))
    } else {
        None
    };
    Ok(RootIndex {
        entries,
        mid_key_meta,
    })
}

/// Serialize a root index block payload.
pub fn build_root_index(entries: &[IndexEntry], mid_key_meta: Option<(u64, u32, u32)>) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out.extend_from_slice(&(e.offset as i64).to_be_bytes());
        out.extend_from_slice(&(e.on_disk_size as i32).to_be_bytes());
        write_compressed_int(&mut out, e.key.len() as i32);
        out.extend_from_slice(&e.key);
    }
    if let Some((offset, size, index)) = mid_key_meta {
        out.extend_from_slice(&(offset as i64).to_be_bytes());
        out.extend_from_slice(&(size as i32).to_be_bytes());
        out.extend_from_slice(&(index as i32).to_be_bytes());
    }
    out
}

/// Size the root index payload will have (used for `uncompressed_data_index_size`).
pub fn root_index_size(entries: &[IndexEntry], with_mid_key: bool) -> u64 {
    let mut size = 0u64;
    for e in entries {
        size += 12 + compressed_int_size(e.key.len() as i32) + e.key.len() as u64;
    }
    if with_mid_key {
        size += 16;
    }
    size
}

fn compressed_int_size(value: i32) -> u64 {
    let mut v = value as u32;
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Parse a leaf / intermediate index block payload.
pub fn parse_non_root_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
    let num_entries = read_i32(data, 0)?;
    if num_entries < 0 {
        return corrupt("negative number of non-root index entries");
    }
    let num_entries = num_entries as usize;
    let entries_start = 4 * (num_entries + 2);
    if data.len() < entries_start {
        return corrupt("non-root index block is truncated");
    }
    let mut ends = Vec::with_capacity(num_entries + 1);
    for i in 0..=num_entries {
        let v = read_i32(data, 4 * (i + 1))?;
        if v < 0 {
            return corrupt("negative secondary index offset");
        }
        ends.push(v as usize);
    }
    let mut entries = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        let rel = ends[i];
        let end = ends[i + 1];
        if end < rel + 12 {
            return corrupt("invalid secondary index offsets");
        }
        let key_start = entries_start + rel + 12;
        let key_end = entries_start + end;
        if key_end > data.len() || key_start > key_end {
            return corrupt("non-root index entry points outside the block");
        }
        let offset = read_i64(data, entries_start + rel)?;
        let on_disk_size = read_i32(data, entries_start + rel + 8)?;
        if on_disk_size < 0 {
            return corrupt("negative on-disk size in non-root index");
        }
        entries.push(IndexEntry {
            key: data[key_start..key_end].to_vec(),
            offset: offset as u64,
            on_disk_size: on_disk_size as u32,
        });
    }
    Ok(entries)
}

/// Serialize a leaf / intermediate index block payload.
pub fn build_non_root_index(entries: &[IndexEntry]) -> Vec<u8> {
    const OVERHEAD: usize = 12;
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    let mut cumulative = 0usize;
    for e in entries {
        // The marks are the *start* offset of every entry, relative to the entry area.
        out.extend_from_slice(&(cumulative as i32).to_be_bytes());
        cumulative += OVERHEAD + e.key.len();
    }
    out.extend_from_slice(&(cumulative as i32).to_be_bytes());
    for e in entries {
        out.extend_from_slice(&(e.offset as i64).to_be_bytes());
        out.extend_from_slice(&(e.on_disk_size as i32).to_be_bytes());
        out.extend_from_slice(&e.key);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hbase::kv::{Cell, CellType};

    fn key(row: &str, qual: &str) -> Vec<u8> {
        let mut c = Cell::new(
            row.as_bytes().to_vec(),
            b"f".to_vec(),
            qual.as_bytes().to_vec(),
            Vec::new(),
        );
        c.timestamp = 7;
        c.cell_type = CellType::Put;
        c.key_bytes()
    }

    #[test]
    fn root_index_roundtrip() {
        let entries = vec![
            IndexEntry {
                key: key("row1", "q"),
                offset: 0,
                on_disk_size: 100,
            },
            IndexEntry {
                key: key("row5", "q"),
                offset: 100,
                on_disk_size: 120,
            },
        ];
        let bytes = build_root_index(&entries, None);
        assert_eq!(bytes.len() as u64, root_index_size(&entries, false));
        let parsed = parse_root_index(&bytes, 2).unwrap();
        assert_eq!(parsed.entries, entries);
        assert!(parsed.mid_key_meta.is_none());

        assert_eq!(parsed.containing(&key("row0", "q")), None);
        assert_eq!(parsed.containing(&key("row1", "q")), Some(0));
        assert_eq!(parsed.containing(&key("row3", "q")), Some(0));
        assert_eq!(parsed.containing(&key("row5", "q")), Some(1));
        assert_eq!(parsed.containing(&key("row9", "q")), Some(1));
    }

    #[test]
    fn non_root_index_roundtrip() {
        let entries = vec![
            IndexEntry {
                key: key("row1", "q"),
                offset: 33,
                on_disk_size: 150,
            },
            IndexEntry {
                key: key("row2", "q"),
                offset: 300,
                on_disk_size: 250,
            },
        ];
        let bytes = build_non_root_index(&entries);
        let parsed = parse_non_root_index(&bytes).unwrap();
        assert_eq!(parsed, entries);
    }

    #[test]
    fn root_index_with_mid_key_meta() {
        let entries = vec![IndexEntry {
            key: key("row1", "q"),
            offset: 0,
            on_disk_size: 100,
        }];
        let bytes = build_root_index(&entries, Some((4096, 512, 3)));
        let parsed = parse_root_index(&bytes, 1).unwrap();
        assert_eq!(parsed.mid_key_meta, Some((4096, 512, 3)));
    }
}
