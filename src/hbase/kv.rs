//! HBase `Cell` / `KeyValue` wire format.
//!
//! A cell on disk is a key/value pair serialized as
//!
//! ```text
//! +------------------+-------------------+------+-------+--------+------+
//! | key length i32   | value length i32  | key  | value | [tags] | [mvcc]|
//! +------------------+-------------------+------+-------+--------+------+
//! ```
//!
//! and the key itself is
//!
//! ```text
//! row length u16 | row | family length u8 | family | qualifier |
//! timestamp i64 (big endian) | type u8
//! ```
//!
//! These constants and the comparator semantics mirror
//! `org.apache.hadoop.hbase.KeyValue` / `KeyValueUtil`.

use crate::error::{HFileError, Result, corrupt};
use std::cmp::Ordering;

/// Size of the row length prefix inside a serialized key.
pub const ROW_LENGTH_SIZE: usize = 2;
/// Size of the family length prefix inside a serialized key.
pub const FAMILY_LENGTH_SIZE: usize = 1;
/// Size of the timestamp field inside a serialized key.
pub const TIMESTAMP_SIZE: usize = 8;
/// Size of the type field inside a serialized key.
pub const TYPE_SIZE: usize = 1;
/// key length + value length prefix of a serialized cell.
pub const ROW_OFFSET: usize = 8;
/// Additional size taken by the 2 byte tags length when tags are enabled.
pub const TAGS_LENGTH_SIZE: usize = 2;

/// `org.apache.hadoop.hbase.KeyValue.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CellType {
    Minimum = 0,
    #[default]
    Put = 4,
    Delete = 8,
    DeleteFamilyVersion = 10,
    DeleteColumn = 12,
    DeleteFamily = 14,
    Maximum = 255,
}

impl CellType {
    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        Ok(match code {
            0 => CellType::Minimum,
            4 => CellType::Put,
            8 => CellType::Delete,
            10 => CellType::DeleteFamilyVersion,
            12 => CellType::DeleteColumn,
            14 => CellType::DeleteFamily,
            255 => CellType::Maximum,
            other => return corrupt(format!("unknown cell type code {other}")),
        })
    }

    pub fn is_delete(self) -> bool {
        matches!(
            self,
            CellType::Delete
                | CellType::DeleteColumn
                | CellType::DeleteFamily
                | CellType::DeleteFamilyVersion
        )
    }
}

/// A single HBase cell.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cell {
    pub row: Vec<u8>,
    pub family: Vec<u8>,
    pub qualifier: Vec<u8>,
    pub timestamp: i64,
    pub cell_type: CellType,
    pub value: Vec<u8>,
    pub tags: Vec<u8>,
    /// HBase "memstore timestamp" a.k.a. sequence id (0 when unknown).
    pub seq_id: u64,
}

impl Cell {
    pub fn new(
        row: impl Into<Vec<u8>>,
        family: impl Into<Vec<u8>>,
        qualifier: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Cell {
            row: row.into(),
            family: family.into(),
            qualifier: qualifier.into(),
            timestamp: i64::MAX,
            cell_type: CellType::Put,
            value: value.into(),
            tags: Vec::new(),
            seq_id: 0,
        }
    }

    /// Length of the serialized key (row + family + qualifier + timestamp + type).
    pub fn key_len(&self) -> usize {
        ROW_LENGTH_SIZE
            + self.row.len()
            + FAMILY_LENGTH_SIZE
            + self.family.len()
            + self.qualifier.len()
            + TIMESTAMP_SIZE
            + TYPE_SIZE
    }

    /// Serialize only the key part, in HBase "flat key" layout.
    pub fn write_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.row.len() as u16).to_be_bytes());
        out.extend_from_slice(&self.row);
        out.push(self.family.len() as u8);
        out.extend_from_slice(&self.family);
        out.extend_from_slice(&self.qualifier);
        out.extend_from_slice(&self.timestamp.to_be_bytes());
        out.push(self.cell_type.code());
    }

    pub fn key_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.key_len());
        self.write_key(&mut out);
        out
    }

    /// Parse a key in HBase "flat key" layout (no length prefixes).
    pub fn from_key_bytes(key: &[u8]) -> Result<Self> {
        let mut cell = Cell::default();
        cell.read_key_prefix(key)?;
        Ok(cell)
    }

    fn read_key_prefix(&mut self, key: &[u8]) -> Result<()> {
        if key.len() < ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + TIMESTAMP_SIZE + TYPE_SIZE {
            return corrupt("key is too short");
        }
        let row_len = u16::from_be_bytes([key[0], key[1]]) as usize;
        let mut pos = ROW_LENGTH_SIZE;
        if key.len() < pos + row_len + FAMILY_LENGTH_SIZE + TIMESTAMP_SIZE + TYPE_SIZE {
            return corrupt("key is too short for the declared row length");
        }
        self.row = key[pos..pos + row_len].to_vec();
        pos += row_len;
        let family_len = key[pos] as usize;
        pos += FAMILY_LENGTH_SIZE;
        if key.len() < pos + family_len + TIMESTAMP_SIZE + TYPE_SIZE {
            return corrupt("key is too short for the declared family length");
        }
        self.family = key[pos..pos + family_len].to_vec();
        pos += family_len;
        let tail = TIMESTAMP_SIZE + TYPE_SIZE;
        self.qualifier = key[pos..key.len() - tail].to_vec();
        pos = key.len() - tail;
        let mut ts = [0u8; TIMESTAMP_SIZE];
        ts.copy_from_slice(&key[pos..pos + TIMESTAMP_SIZE]);
        self.timestamp = i64::from_be_bytes(ts);
        self.cell_type = CellType::from_code(key[key.len() - 1])?;
        Ok(())
    }

    /// Serialize the cell in the raw (unencoded) HFile data block layout.
    pub fn write_raw(&self, out: &mut Vec<u8>, includes_tags: bool, includes_mvcc: bool) {
        let key_len = self.key_len();
        out.extend_from_slice(&(key_len as i32).to_be_bytes());
        out.extend_from_slice(&(self.value.len() as i32).to_be_bytes());
        self.write_key(out);
        out.extend_from_slice(&self.value);
        if includes_tags {
            out.extend_from_slice(&(self.tags.len() as u16).to_be_bytes());
            out.extend_from_slice(&self.tags);
        }
        if includes_mvcc {
            write_vlong(out, self.seq_id as i64);
        }
    }

    /// Number of bytes [`Cell::write_raw`] will produce.
    pub fn raw_len(&self, includes_tags: bool, includes_mvcc: bool) -> usize {
        let mut len = ROW_OFFSET + self.key_len() + self.value.len();
        if includes_tags {
            len += TAGS_LENGTH_SIZE + self.tags.len();
        }
        if includes_mvcc {
            len += vlong_size(self.seq_id as i64);
        }
        len
    }
}

/// Decode a raw (unencoded) cell stream as stored in `DATABLK*` blocks and in
/// `ROW_INDEX_V1` blocks.
pub fn parse_raw_cells(buf: &[u8], includes_tags: bool, includes_mvcc: bool) -> Result<Vec<Cell>> {
    parse_raw_cells_limited(buf, includes_tags, includes_mvcc)
}

/// Same as [`parse_raw_cells`] but stops at `limit` bytes (used by `ROW_INDEX_V1`,
/// where the cell area is followed by the row offsets).
pub fn parse_raw_cells_limited(
    buf: &[u8],
    includes_tags: bool,
    includes_mvcc: bool,
) -> Result<Vec<Cell>> {
    let mut cells = Vec::new();
    let mut pos = 0usize;
    while pos < buf.len() {
        let (cell, next) = parse_raw_cell(buf, pos, includes_tags, includes_mvcc)?;
        cells.push(cell);
        pos = next;
    }
    Ok(cells)
}

/// Decode one raw cell starting at `pos`, returning the cell and the offset just past it.
pub fn parse_raw_cell(
    buf: &[u8],
    pos: usize,
    includes_tags: bool,
    includes_mvcc: bool,
) -> Result<(Cell, usize)> {
    if buf.len() - pos < ROW_OFFSET {
        return corrupt("truncated cell header");
    }
    let key_len = read_i32(buf, pos)? as usize;
    let value_len = read_i32(buf, pos + 4)? as usize;
    let mut p = pos + ROW_OFFSET;
    if key_len > buf.len() - p {
        return corrupt("cell key length exceeds block size");
    }
    let key = &buf[p..p + key_len];
    p += key_len;
    if value_len > buf.len() - p {
        return corrupt("cell value length exceeds block size");
    }
    let value = &buf[p..p + value_len];
    p += value_len;

    let mut cell = Cell::default();
    cell.read_key_prefix(key)?;
    cell.value = value.to_vec();

    if includes_tags {
        if buf.len() - p < TAGS_LENGTH_SIZE {
            return corrupt("truncated tags length");
        }
        let tags_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
        p += TAGS_LENGTH_SIZE;
        if tags_len > buf.len() - p {
            return corrupt("tags length exceeds block size");
        }
        cell.tags = buf[p..p + tags_len].to_vec();
        p += tags_len;
    }
    if includes_mvcc {
        let (seq_id, used) = read_vlong(&buf[p..])?;
        cell.seq_id = seq_id as u64;
        p += used;
    }
    Ok((cell, p))
}

/// HBase `CellComparator` without mvcc: row/family/qualifier ascending,
/// then timestamp descending, then type descending.
pub fn compare_cells(left: &Cell, right: &Cell) -> Ordering {
    left.row
        .cmp(&right.row)
        .then_with(|| left.family.cmp(&right.family))
        .then_with(|| left.qualifier.cmp(&right.qualifier))
        .then_with(|| right.timestamp.cmp(&left.timestamp))
        .then_with(|| right.cell_type.code().cmp(&left.cell_type.code()))
}

/// Compare two serialized flat keys with the HBase comparator.
pub fn compare_flat_keys(left: &[u8], right: &[u8]) -> Ordering {
    match (Cell::from_key_bytes(left), Cell::from_key_bytes(right)) {
        (Ok(l), Ok(r)) => compare_cells(&l, &r),
        _ => left.cmp(right),
    }
}

// ---------------------------------------------------------------------------
// Hadoop WritableUtils VarLong
// ---------------------------------------------------------------------------

/// Number of bytes `write_vlong` will produce (matches `WritableUtils.getVIntSize`).
pub fn vlong_size(value: i64) -> usize {
    if (-112..=127).contains(&value) {
        return 1;
    }
    let mut v = if value < 0 { !value } else { value };
    let mut len = 1;
    while v != 0 {
        v >>= 8;
        len += 1;
    }
    len
}

/// Encode an integer using Hadoop's `WritableUtils.writeVLong` format.
pub fn write_vlong(out: &mut Vec<u8>, value: i64) {
    if (-112..=127).contains(&value) {
        out.push(value as i8 as u8);
        return;
    }
    let negative = value < 0;
    let mut v = if negative { !value } else { value };
    let mut tmp = v;
    let mut len: i8 = -112;
    if negative {
        len = -120;
    }
    while tmp != 0 {
        tmp >>= 8;
        len -= 1;
    }
    out.push(len as u8);
    let mut bytes = Vec::with_capacity(8);
    while v != 0 {
        bytes.push((v & 0xff) as u8);
        v >>= 8;
    }
    bytes.reverse();
    out.extend_from_slice(&bytes);
}

/// Decode a Hadoop `WritableUtils` VarLong, returning the value and the bytes consumed.
pub fn read_vlong(buf: &[u8]) -> Result<(i64, usize)> {
    let first = match buf.first() {
        Some(b) => *b as i8,
        None => return corrupt("truncated vlong"),
    };
    if first >= -112 {
        return Ok((first as i64, 1));
    }
    let len = if first < -120 {
        -(first as i32 + 120)
    } else {
        -(first as i32 + 112)
    } as usize;
    if len > 8 || buf.len() < 1 + len {
        return corrupt("invalid vlong");
    }
    let mut value: i64 = 0;
    for i in 0..len {
        value = (value << 8) | (buf[1 + i] as i64);
    }
    if first < -120 {
        value = !value;
    }
    Ok((value, 1 + len))
}

// ---------------------------------------------------------------------------
// HBase 7-bit "compressed int" (unsigned LEB128)
// ---------------------------------------------------------------------------

/// Encode an integer with `ByteBufferUtils.putCompressedInt` semantics.
pub fn write_compressed_int(out: &mut Vec<u8>, value: i32) {
    let mut v = value as u32;
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        out.push(b);
        if v == 0 {
            break;
        }
    }
}

/// Decode a 7-bit compressed int, returning the value and the bytes consumed.
pub fn read_compressed_int(buf: &[u8]) -> Result<(i32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    for (i, b) in buf.iter().enumerate() {
        if shift >= 32 {
            return corrupt("compressed int is too long");
        }
        result |= ((b & 0x7f) as u32) << shift;
        if b & 0x80 == 0 {
            return Ok((result as i32, i + 1));
        }
        shift += 7;
    }
    corrupt("truncated compressed int")
}

pub fn read_i32(buf: &[u8], pos: usize) -> Result<i32> {
    if buf.len() < pos + 4 {
        return corrupt("truncated i32");
    }
    Ok(i32::from_be_bytes([
        buf[pos],
        buf[pos + 1],
        buf[pos + 2],
        buf[pos + 3],
    ]))
}

pub fn read_u32(buf: &[u8], pos: usize) -> Result<u32> {
    if buf.len() < pos + 4 {
        return corrupt("truncated u32");
    }
    Ok(u32::from_be_bytes([
        buf[pos],
        buf[pos + 1],
        buf[pos + 2],
        buf[pos + 3],
    ]))
}

pub fn read_i64(buf: &[u8], pos: usize) -> Result<i64> {
    if buf.len() < pos + 8 {
        return corrupt("truncated i64");
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[pos..pos + 8]);
    Ok(i64::from_be_bytes(b))
}

impl From<HFileError> for std::io::Error {
    fn from(e: HFileError) -> Self {
        match e {
            HFileError::Io(io) => io,
            other => std::io::Error::other(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vlong_roundtrip() {
        for v in [
            0i64,
            1,
            63,
            127,
            128,
            -1,
            -112,
            -113,
            16383,
            i64::MAX,
            -1234567890123,
        ] {
            let mut buf = Vec::new();
            write_vlong(&mut buf, v);
            assert_eq!(buf.len(), vlong_size(v), "size mismatch for {v}");
            let (decoded, used) = read_vlong(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn vlong_matches_hbase_reference_bytes() {
        let mut buf = Vec::new();
        write_vlong(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);
        buf.clear();
        write_vlong(&mut buf, -112);
        assert_eq!(buf, vec![0x90]);
        buf.clear();
        write_vlong(&mut buf, 128);
        assert_eq!(buf, vec![0x8f, 0x80]);
    }

    #[test]
    fn compressed_int_roundtrip() {
        for v in [0i32, 1, 127, 128, 300, 16384, 1_000_000] {
            let mut buf = Vec::new();
            write_compressed_int(&mut buf, v);
            let (decoded, used) = read_compressed_int(&buf).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(used, buf.len());
        }
        assert_eq!(read_compressed_int(&[0x80, 0x01]).unwrap().0, 128);
    }

    #[test]
    fn cell_raw_roundtrip() {
        let mut cell = Cell::new(
            b"row1".to_vec(),
            b"f".to_vec(),
            b"q".to_vec(),
            b"v".to_vec(),
        );
        cell.timestamp = 123456789;
        cell.seq_id = 42;
        for (tags, mvcc) in [(false, false), (true, false), (false, true), (true, true)] {
            let mut buf = Vec::new();
            cell.write_raw(&mut buf, tags, mvcc);
            assert_eq!(buf.len(), cell.raw_len(tags, mvcc));
            let decoded = parse_raw_cells(&buf, tags, mvcc).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].row, b"row1");
            assert_eq!(decoded[0].family, b"f");
            assert_eq!(decoded[0].qualifier, b"q");
            assert_eq!(decoded[0].value, b"v");
            assert_eq!(decoded[0].timestamp, 123456789);
            assert_eq!(decoded[0].cell_type, CellType::Put);
            assert_eq!(decoded[0].seq_id, if mvcc { 42 } else { 0 });
        }
    }

    #[test]
    fn comparator_orders_like_hbase() {
        let mut a = Cell::new(b"r1".to_vec(), b"f".to_vec(), b"q".to_vec(), b"".to_vec());
        a.timestamp = 10;
        let mut b = a.clone();
        b.timestamp = 20;
        // higher timestamp sorts first
        assert_eq!(compare_cells(&b, &a), Ordering::Less);

        let mut c = a.clone();
        c.cell_type = CellType::Delete;
        // higher type code sorts first for identical timestamps
        assert_eq!(compare_cells(&c, &a), Ordering::Less);
        assert_eq!(compare_cells(&a, &c), Ordering::Greater);

        let mut d = a.clone();
        d.qualifier = b"q2".to_vec();
        assert_eq!(compare_cells(&a, &d), Ordering::Less);
    }
}
